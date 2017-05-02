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

import java.util.{Map => JMap}
import java.util.concurrent.Executors

import scala.collection.JavaConverters._

import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.SessionHandle
import org.apache.hive.service.cli.session.SessionManager
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.ReflectionUtils.{getAncestorField, invoke, setSuperField}
import org.apache.spark.sql.hive.thriftserver.server.SparkSQLOperationManager

private[hive] class SparkSQLSessionManager(hiveServer: HiveServer2)
  extends SessionManager(hiveServer)
  with ReflectedCompositeService {

  private val defaultQueue = MultiSparkSQLEnv.originConf.get("spark.yarn.queue", "default")

  private lazy val sparkSqlOperationManager = new SparkSQLOperationManager()

  override def init(hiveConf: HiveConf) {
    setSuperField(this, "hiveConf", hiveConf)

    // Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      invoke(classOf[SessionManager], this, "initOperationLogRootDir")
    }

    val backgroundPoolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS)
    setSuperField(this, "backgroundOperationPool", Executors.newFixedThreadPool(backgroundPoolSize))
    getAncestorField[Log](this, 3, "LOG").info(
      s"HiveServer2: Async execution pool size $backgroundPoolSize")

    setSuperField(this, "operationManager", sparkSqlOperationManager)
    addService(sparkSqlOperationManager)

    initCompositeService(hiveConf)
  }

  override def openSession(
      protocol: TProtocolVersion,
      realUser: String,
      username: String,
      passwd: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val sessionHandle =
      super.openSession(protocol, realUser,
        username, passwd, ipAddress, sessionConf, withImpersonation, delegationToken)
    val session = super.getSession(sessionHandle)
    HiveThriftServer2.listener.onSessionCreated(
      session.getIpAddress, sessionHandle.getSessionId.toString, session.getUsername)

    val (rangerUser, _, database) = configureSession(sessionConf)

    val ss = getUserSession(username)
    val metastoreUser = rangerUser.getOrElse(username)

    val client = HiveUtils.newClientForMetadata(
      ss.sparkContext.conf,
      ss.sparkContext.hadoopConfiguration,
      metastoreUser)

    ss.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)

    if (rangerUser.isDefined) {
      val statement = s"set hivevar:ranger.user.name = ${rangerUser.get}"
      ss.sql(statement)
    }
    ss.sql(database.get)

    sparkSqlOperationManager.sessionToSparkSession.put(sessionHandle, ss)
    sparkSqlOperationManager.sessionToClient.put(sessionHandle, client)
    sessionHandle
  }

  override def closeSession(sessionHandle: SessionHandle) {
    HiveThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    super.closeSession(sessionHandle)
    sparkSqlOperationManager.sessionToActivePool.remove(sessionHandle)
    sparkSqlOperationManager.sessionToSparkSession.remove(sessionHandle)
    sparkSqlOperationManager.sessionToClient.remove(sessionHandle)
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
        s"the `spark.yarn.proxy.users` on the server side." )
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
