/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.multitenancy

import java.io.{File, IOException}
import java.nio.charset.Charset
import java.util.{ArrayList, List}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.framework.api.ACLProvider
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.hive.cli.OptionsProcessor
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.hive.ql.util.ZooKeeperHiveHelper
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.common.util.HiveVersionInfo
import org.apache.hive.service.CompositeService
import org.apache.zookeeper._
import org.apache.zookeeper.data.ACL

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.monitor.{MultiTenancyThriftServerListener, ThriftServerMonitor}
import org.apache.spark.util.{ShutdownHookManager, Utils => SparkUtils}

private[multitenancy] class MultiTenancyThriftServer private (name: String)
  extends CompositeService(name) with Logging {

  private[this] val conf = new SparkConf(loadDefaults = true)
  private[this] var cliService: ThriftServerCLIService = _
  private[this] var clientCLIService: ThriftClientCLIService = _

  private var zooKeeperClient: CuratorFramework = _
  private var znode: PersistentEphemeralNode = null
  private var znodePath: String = _
  // Set to true only when deregistration happens
  private var deregisteredWithZooKeeper = false

  private[this] val started = new AtomicBoolean(false)

  def this() = {
    this(getClass.getSimpleName)
  }

  override def init(hiveConf: HiveConf): Unit = synchronized {
    cliService = new ThriftServerCLIService(this)
    clientCLIService = new ThriftClientCLIService(cliService)
    addService(cliService)
    addService(clientCLIService)
    super.init(hiveConf)
    ShutdownHookManager.addShutdownHook {
      () => this.stop()
    }
  }

  def getSparkConf(): SparkConf = conf

  //  If `hive.server2.support.dynamic.service.discovery = true` &&
  // `hive.zookeeper.quorum` unempty, return true.
  private def isSupportDynamicServiceDiscovery(hiveConf: HiveConf): Boolean = {
    hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY) &&
      hiveConf.getVar(ConfVars.HIVE_ZOOKEEPER_QUORUM).split(",").length > 0
  }

  override def start(): Unit = {
    super.start()
    started.set(true)
  }

  override def stop(): Unit = {
    if (started.getAndSet(false)) {
      super.stop()
    }
  }

  @throws[Exception]
  private def addServerInstanceToZooKeeper(hiveConf: HiveConf): Unit = {
    val zooKeeperEnsemble = ZooKeeperHiveHelper.getQuorumServers(hiveConf)
    val rootNamespace = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE)
    val instanceURI = getServerInstanceURI
    setUpZooKeeperAuth(hiveConf)
    val sessionTimeout = hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
      TimeUnit.MILLISECONDS).toInt
    val baseSleepTime = hiveConf.getTimeVar(
      HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
      TimeUnit.MILLISECONDS).toInt
    val maxRetries = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES)
    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    zooKeeperClient =
      CuratorFrameworkFactory.builder.connectString(zooKeeperEnsemble)
        .sessionTimeoutMs(sessionTimeout)
        .aclProvider(zooKeeperAclProvider)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
        .build
    zooKeeperClient.start()
    // Create the parent znodes recursively; ignore if the parent already exists.
    try {
      zooKeeperClient
        .create
        .creatingParentsIfNeeded
        .withMode(CreateMode.PERSISTENT)
        .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace)
      logInfo("Created the root name space: " + rootNamespace + " on ZooKeeper for HiveServer2")
    } catch {
      case e: KeeperException =>
        if (e.code ne KeeperException.Code.NODEEXISTS) {
          logError("Unable to create HiveServer2 namespace: " + rootNamespace + " on ZooKeeper", e)
          throw e
        }
    }
    // Create a znode under the rootNamespace parent for this instance of the server
    // Znode name: serverUri=host:port;version=versionInfo;sequence=sequenceNumber
    try {
      val pathPrefix = ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR +
        rootNamespace + ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR +
        "serverUri=" + instanceURI + ";" +
        "version=" + HiveVersionInfo.getVersion + ";" + "sequence="
      var znodeData = ""
      znodeData = instanceURI
      val znodeDataUTF8 = znodeData.getBytes(Charset.forName("UTF-8"))
      znode = new PersistentEphemeralNode(
        zooKeeperClient,
        PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL,
        pathPrefix,
        znodeDataUTF8)
      znode.start()
      // We'll wait for 120s for node creation
      val znodeCreationTimeout = 120
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted")
      }
      setDeregisteredWithZooKeeper(false)
      znodePath = znode.getActualPath
      // Set a watch on the znode
      if (zooKeeperClient.checkExists.usingWatcher(new DeRegisterWatcher)
        .forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this HiveServer2 instance on ZooKeeper.")
      }
      logInfo("Created a znode on ZooKeeper for HiveServer2 uri: " + instanceURI)
    } catch {
      case e: Exception =>
        logError("Unable to create a znode for this server instance", e)
        if (znode != null) znode.close()
        throw e
    }
  }

  private class DeRegisterWatcher extends Watcher {
    override def process(event: WatchedEvent): Unit = {
      if (event.getType == Watcher.Event.EventType.NodeDeleted) {
        if (znode != null) {
          try {
            znode.close()
            logWarning("This Spark ThriftServer instance is now de-registered from ZooKeeper. " +
              "The server will be shut down after the last client sesssion completes.")
          } catch {
            case e: IOException =>
              logError("Failed to close the persistent ephemeral znode", e)
          } finally {
            setDeregisteredWithZooKeeper(true)
            // If there are no more active client sessions, stop the server
            if (cliService.getSessionManager.getOpenSessionCount == 0) {
              logWarning("This instance of Spark ThriftServer has been removed from the list of " +
                "server instances available for dynamic service discovery. The last client " +
                "session has ended - will shutdown now.")
              stop()
            }
          }
        }
      }
    }
  }

  /**
   * For a kerberized cluster, we dynamically set up the client's JAAS conf.
   *
   * @param hiveConf
   * @return
   * @throws Exception
   */
  @throws[Exception]
  private def setUpZooKeeperAuth(hiveConf: HiveConf): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      var principal = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL)
      var keyTabFile = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB)

      if (principal.isEmpty || keyTabFile.isEmpty) {
        principal = conf.get("spark.yarn.principal")
        keyTabFile = conf.get("spark.yarn.keytab")
      }
      if (!(new File(keyTabFile).exists())) {
        throw new IOException("key tab does not exists")
      }
      // Install the JAAS Configuration for the runtime
      Utils.setZookeeperClientKerberosJaasConfig(principal, keyTabFile)
    }
  }

  @throws[Exception]
  private def getServerInstanceURI = {
    if ((clientCLIService == null) || (clientCLIService.getServerIPAddress == null)) {
      throw new Exception("Unable to get the server address; it hasn't been initialized yet.")
    }
    clientCLIService.getServerIPAddress.getHostName + ":" + clientCLIService.getPortNumber
  }

  /**
   * ACLProvider for providing appropriate ACLs to CuratorFrameworkFactory
   */
  private val zooKeeperAclProvider = new ACLProvider() {
    override def getDefaultAcl: List[ACL] = {
      val nodeAcls = new ArrayList[ACL]
      if (UserGroupInformation.isSecurityEnabled) {
        // Read all to the world
        nodeAcls.addAll(ZooDefs.Ids.READ_ACL_UNSAFE)
        // Create/Delete/Write/Admin to the authenticated user
        nodeAcls.add(new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS))
      } else {
        // ACLs for znodes on a non-kerberized cluster
        // Create/Read/Delete/Write/Admin to the world
        nodeAcls.addAll(ZooDefs.Ids.OPEN_ACL_UNSAFE)
      }
      nodeAcls
    }

    override def getAclForPath(path: String): List[ACL] = getDefaultAcl
  }

  private[this] def setDeregisteredWithZooKeeper(deregisteredWithZooKeeper: Boolean): Unit = {
    this.deregisteredWithZooKeeper = deregisteredWithZooKeeper
  }
}

object MultiTenancyThriftServer extends Logging {

  def main(args: Array[String]): Unit = {
    SparkUtils.initDaemon(log)
    val op = new OptionsProcessor()
    if (!op.process_stage1(args)) {
      System.exit(1)
    }

    val hiveConf = new HiveConf(classOf[SessionState])

    try {
      val server = new MultiTenancyThriftServer()
      hiveConf.addResource(SparkHadoopUtil.get.newConfiguration(server.getSparkConf()))
      server.init(hiveConf)
      server.start()
      logInfo(server.getName + " started!")
      if (server.isSupportDynamicServiceDiscovery(hiveConf)) {
        logInfo(s"HA mode: start to add this ${server.getName} instance to Zookeeper...")
        server.addServerInstanceToZooKeeper(hiveConf)
      }

    } catch {
      case e: Exception =>
        logError("Error starting MultiTenancyThriftServer", e)
        System.exit(-1)
    }
  }
}
