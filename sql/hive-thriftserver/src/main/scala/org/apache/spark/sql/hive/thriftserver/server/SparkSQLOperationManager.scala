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

package org.apache.spark.sql.hive.thriftserver.server

import java.security.PrivilegedExceptionAction
import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.util.{Failure, Success, Try}

import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.execution.command.{CreateFunctionCommand, SetCommand}
import org.apache.spark.sql.hive.HiveSessionState
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation

/**
 * Executes queries using Spark SQL, and maintains a list of handles to active queries.
 */
private[thriftserver] class SparkSQLOperationManager()
  extends OperationManager with Logging {

  val sessionToActivePool = new ConcurrentHashMap[SessionHandle, String]
  val sessionToSparkSession = new ConcurrentHashMap[SessionHandle, SparkSession]
  val sessionToClient = new ConcurrentHashMap[SessionHandle, HiveClient]

  override def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: JMap[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val sessionHandle = parentSession.getSessionHandle
    val sparkSession = sessionToSparkSession.get(sessionHandle)
    var client = sessionToClient.get(sessionHandle)

    require(sparkSession != null, s"Session sessionHandle: ${sessionHandle} has not been" +
      s" initialized or had already closed.")

    val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
    val plan = sessionState.sqlParser.parsePlan(statement)

    plan match {
      case setCmd: SetCommand =>
        setCmd.kv match {
          case Some(("hivevar:ranger.user.name", Some(name))) if name != client.getCurrentUser() =>
          verifyChangeRangerUser(parentSession)
          val currentDatabase = client.getCurrentDatabase()
          val sessionUGI = Utils.getUGI
          client = sessionUGI.doAs(new PrivilegedExceptionAction[HiveClient]() {
            override def run(): HiveClient = {
              client.newSession(name)
            }
          })
          client.setCurrentDatabase(currentDatabase)
          sessionToClient.remove(sessionHandle)
          sessionToClient.put(sessionHandle, client)
          case _ =>
      }

      case func: CreateFunctionCommand if func.isTemp =>
        if (func.databaseName.isDefined) {
          throw new AnalysisException(s"Specifying a database in CREATE TEMPORARY FUNCTION " +
            s"is not allowed: '${func.databaseName.get}'")
        }
        client.registerTemporaryUDF(func.functionName, func.className, func.resources)

      case _ =>
    }

    val runInBackground = async && sessionState.hiveThriftServerAsync
    val operation = new SparkExecuteStatementOperation(
      parentSession,
      plan,
      statement,
      client,
      confOverlay,
      runInBackground)(sparkSession, sessionToActivePool)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }

  /**
   * Verify whether a real user passed by remote user has rights to change ranger.user.name
   */
  private def verifyChangeRangerUser(session: HiveSession): Unit = {
    val hiveConf = session.getHiveConf
    val ipAddress = session.getIpAddress
    val realUser = session.getRealUsername
    Try {
      val loginUser = UserGroupInformation.getLoginUser.getShortUserName
      HiveAuthFactory.verifyProxyAccess(realUser, loginUser, ipAddress, hiveConf)
    } match {
      case Success(_) =>
      case Failure(e) =>
        logError(e.getMessage)
        throw new HiveSQLException(
          "user " + realUser + " doesn't have access to set ranger.user.name")
    }
  }
}

