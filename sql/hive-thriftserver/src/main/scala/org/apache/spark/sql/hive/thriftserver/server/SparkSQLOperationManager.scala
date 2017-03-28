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

import java.util.concurrent.ConcurrentHashMap
import java.util.{Map => JMap}

import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
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
    val formatted = statement.toLowerCase.split("//s+").mkString(" ")
    if (formatted.startsWith("set hivevar:ranger.user.name")) {
      val vars = formatted.split("=")
      val rangerUser = if (vars.size > 1) {
        vars(1)
      } else {
        logInfo(s"Please remove `hivevar:` to check hive variables e.g. `set ranger.user.name;`")
        null
      }
      if (rangerUser != null && rangerUser != client.getCurrentUser()) {
        val currentDatabase = client.getCurrentDatabase()
        client = client.newSession(rangerUser)
        client.setCurrentDatabase(currentDatabase)
        sessionToClient.remove(sessionHandle)
        sessionToClient.put(sessionHandle, client)
      }
    }
    require(sparkSession != null, s"Session sessionHandle: ${sessionHandle} has not been" +
      s" initialized or had already closed.")
    val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
    val runInBackground = async && sessionState.hiveThriftServerAsync
    val operation = new SparkExecuteStatementOperation(
      parentSession,
      statement,
      client,
      confOverlay,
      runInBackground)(sparkSession, sessionToActivePool)
    handleToOperation.put(operation.getHandle, operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }
}
