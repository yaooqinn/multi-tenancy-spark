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

package org.apache.spark.sql.hive.thriftserver

import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hive.service.cli.thrift.{ThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._
import org.apache.spark.sql.hive.thriftserver.monitor.{HiveThriftServer2Listener, ThriftServerMonitor}
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * The main entry point for the Spark SQL port of HiveServer2.  Starts up a `SparkSQLContext` and a
 * `HiveThriftServer2` thrift server.
 */
object HiveThriftServer2 extends Logging {

  def main(args: Array[String]) {
    Utils.initDaemon(log)
    val optionsProcessor = new HiveServer2.ServerOptionsProcessor("HiveThriftServer2")
    optionsProcessor.parse(args)

    logInfo("Starting SparkContext")
    MultiSparkSQLEnv.init()

    ShutdownHookManager.addShutdownHook { () =>
      MultiSparkSQLEnv.stop()
      ThriftServerMonitor.detachAllUITabs()
    }

    val hiveConf = new HiveConf(classOf[SessionState])
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(MultiSparkSQLEnv.originConf)
    hadoopConf.iterator().asScala.foreach { entry =>
      val key = entry.getKey
      val value = entry.getValue
      hiveConf.set(key, value)
    }

    try {
      val server = new HiveThriftServer2
      server.init(hiveConf)
      server.start()
      logInfo("HiveThriftServer2 started")

      MultiSparkSQLEnv.userToSession.foreach { us =>
        val user = us._1
        val ss = us._2
        ThriftServerMonitor.setListener(user,
          new HiveThriftServer2Listener(server, MultiSparkSQLEnv.originConf))
        ss.sparkContext.addSparkListener(ThriftServerMonitor.getListener(user))
        val uiTab = new ThriftServerTab(user, ss.sparkContext)
        ThriftServerMonitor.addUITab(us._1, uiTab)
      }

      // If application was killed before HiveThriftServer2 start successfully then SparkSubmit
      // process can not exit, so check whether if SparkContext was stopped.
      if (!MultiSparkSQLEnv.userToSession.forall { case ((_, ss)) =>
        !ss.sparkContext.isStopped
      }) {
        logError("SparkContext has stopped even if HiveServer2 has started, so exit")
        System.exit(-1)
      }

      if (server.isSupportDynamicServiceDiscovery(hiveConf)) {
        logInfo("HiveServer2 HA mode: start to add this HiveServer2 instance to Zookeeper...")
        invoke(classOf[HiveServer2], server, "addServerInstanceToZooKeeper",
          classOf[HiveConf] -> hiveConf)
      }
    }
    catch {
      case e: Exception =>
        logError("Error starting HiveThriftServer2", e)
        System.exit(-1)
    }
  }
}

private[hive] class HiveThriftServer2 extends HiveServer2
  with ReflectedCompositeService {
  // state is tracked internally so that the server only attempts to shut down if it successfully
  // started, and then once only.
  private val started = new AtomicBoolean(false)

  override def init(hiveConf: HiveConf) {
    this.cliService = new SparkSQLCLIService(this)
    addService(cliService)
    this.thriftCLIService = if (isHTTPTransportMode(hiveConf)) {
      new ThriftHttpCLIService(cliService)
    } else {
      new ThriftBinaryCLIService(cliService)
    }
    addService(thriftCLIService)
    System.setProperty("SPARK_MULTI_TENANCY_MODE", "true")
    initCompositeService(hiveConf)
  }

  private def isHTTPTransportMode(hiveConf: HiveConf): Boolean = {
    val transportMode = hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE)
    transportMode.toLowerCase(Locale.ENGLISH).equals("http")
  }

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
}
