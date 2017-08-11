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

import java.security.PrivilegedExceptionAction
import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}
import org.apache.hive.service.cli.session._
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.{CredentialCache, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.thriftserver.server.SparkSQLOperationManager

private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService with Logging {

  private val defaultQueue = MultiSparkSQLEnv.originConf.get("spark.yarn.queue", "default")

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()

  protected val handleToProxyUser: JMap[SessionHandle, UserGroupInformation] =
    new ConcurrentHashMap()


  override def init(hiveConf: HiveConf) {
    this.hiveConf = hiveConf
    // Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      initOperationLogRootDir()
    }
    createBackgroundOperationPool()
    this.operationManager = sparkSqlOperationManager
    addService(sparkSqlOperationManager)
    initCompositeService(hiveConf)
  }

  /**
   * Opens a new session and creates a session handle.
   * The username passed to this method is the effective username.
   * If withImpersonation is true (==doAs true) we wrap all the calls in HiveSession
   * within a UGI.doAs, where UGI corresponds to the effective user.
   *
   * @see org.apache.hive.service.cli.thrift.ThriftCLIService getUserName()
   * @param protocol
   * @param username
   * @param passwd
   * @param ipAddress
   * @param sessionConf
   * @param withImpersonation
   * @param delegationToken
   * @return
   * @throws HiveSQLException
   */
  override def openSession(
      protocol: TProtocolVersion,
      realUser: String,
      username: String,
      passwd: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    var hiveSession: HiveSession = null
    var sessionUGI: UserGroupInformation = Utils.getUGI
    // If doAs is set to true for HiveServer2, we will create a proxy object for the session impl.
    // Within the proxy object, we wrap the method call in a UserGroupInformation#doAs
    if (withImpersonation) {
      val sessionWithUGI = new HiveSessionImplwithUGI(protocol, realUser, username,
        passwd, hiveConf, ipAddress, delegationToken)
      sessionUGI = sessionWithUGI.getSessionUgi
      hiveSession = HiveSessionProxy.getProxy(sessionWithUGI, sessionUGI)
      sessionWithUGI.setProxySession(hiveSession)
    } else {
      hiveSession = new HiveSessionImpl(protocol, realUser, username, passwd, hiveConf, ipAddress)
    }
    hiveSession.setSessionManager(this)
    hiveSession.setOperationManager(sparkSqlOperationManager)

    if (isOperationLogEnabled) hiveSession.setOperationLogSessionDir(operationLogRootDir)

    val sessionHandle = hiveSession.getSessionHandle
    handleToSession.put(sessionHandle, hiveSession)
    handleToProxyUser.put(sessionHandle, sessionUGI)

    HiveThriftServer2.listener.onSessionCreated(
      hiveSession.getIpAddress, sessionHandle.getSessionId.toString, hiveSession.getUsername)

    val (rangerUser, _, database) = configureSession(sessionConf)

    val sparkSession = getUserSession(username)
    sparkSession.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)

    // 1. `metastoreUser` is necessary to create a client which will specify SessionState's user;
    // 2. Reuse sharedSession's client to avoid creating a new classLoader;
    // Warning: no use `HiveUtils.newClientForMetadata()` to avoid metastore mysql connection leaks.
    val metastoreUser = rangerUser.getOrElse(username)
    val client = sessionUGI.doAs(new PrivilegedExceptionAction[HiveClient]() {
      override def run(): HiveClient = {
        sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog]
          .client.newSession(metastoreUser)
      }
    })

    sessionUGI.doAs( new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        if (rangerUser.isDefined) {
          val statement = s"set hivevar:ranger.user.name = ${rangerUser.get}"
          sparkSession.sql(statement)
        }
        sparkSession.sql(database.get)
      }
    })

    sparkSqlOperationManager.sessionToSparkSession.put(sessionHandle, sparkSession)
    sparkSqlOperationManager.sessionToClient.put(sessionHandle, client)
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle) {
    HiveThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    super.closeSession(sessionHandle)
    sparkSqlOperationManager.sessionToActivePool.remove(sessionHandle)

    val user = handleToProxyUser.remove(sessionHandle)
    val credentials = CredentialCache.get(user.getShortUserName)
    if (credentials != null) {
      logInfo(s"Adding Fresh Credentials For User:[${user.getShortUserName}]")
      user.addCredentials(credentials)
    }
    sparkSqlOperationManager.sessionToSparkSession.remove(sessionHandle)
    val hiveClient = sparkSqlOperationManager.sessionToClient.remove(sessionHandle)
    if (hiveClient != null) {
      val closeClient = new PrivilegedExceptionAction[Unit] {
        override def run(): Unit = hiveClient.close()
      }
      logInfo(s"Closing HiveClient for User: [${user.getShortUserName}]")
      user.doAs(closeClient)
    }
  }

  /**
   * Extract ranger.user, spark.yarn.queue and database string from session configuration
   * @param sessionConf session configuration
   * @return updated (rangerUser, queue, database) by session configuration
   */
  private def configureSession(
      sessionConf: JMap[String, String]): (Option[String], Option[String], Option[String]) = {
    var rangerUser: Option[String] = None
    var queue: Option[String] = Some(defaultQueue)
    var database: Option[String] = Some("use default")
    if (sessionConf != null) {
      sessionConf.asScala.foreach { case ((key, value)) =>
          if (key == "set:hivevar:ranger.user.name") {
            rangerUser = Some(value)
          } else if (key == "use:database") {
            database = Some("use" + " " + value)
          } else if (key == "set:hiveconf:mapred.job.queue.name") {
            queue = Some(value)
          }
      }
    }
    (rangerUser, queue, database)
  }

  private def getUserSession(user: String): SparkSession = {
    if (!MultiSparkSQLEnv.userToQueue.containsKey(user)) {
      throw new SparkException(s"Connecting Spark Thrift Server with User [$user] is forbidden," +
        s"no resource is prepared for it. Please change to an available user or add [$user] to " +
        s"the `spark.sql.proxy.users` on the server side." )
    }
    if (!MultiSparkSQLEnv.userToSession.containsKey(user)) {
      throw new SparkException(s"SparkContext for User [$user] is not initialized please restart" +
        s" the server.")
    } else {
      MultiSparkSQLEnv.userToSession.get(user) match {
        case ss: SparkSession if (!ss.sparkContext.isStopped) =>
          ss.newSession()
        case _ =>
          throw new SparkException(s"SparkContext stopped for User [$user]")
      }
    }
  }
}
