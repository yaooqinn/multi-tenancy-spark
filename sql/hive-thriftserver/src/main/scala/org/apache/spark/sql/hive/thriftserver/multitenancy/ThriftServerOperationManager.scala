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

import java.sql.SQLException
import java.util.{ArrayList, HashMap => JMap, List => JList}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.AbstractService
import org.apache.hive.service.cli.{SessionHandle, _}
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, Operation}
import org.apache.hive.service.cli.session.HiveSession
import org.apache.log4j.Logger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveSessionState}
import org.apache.spark.sql.hive.thriftserver.SparkExecuteStatementOperation
import org.apache.spark.util.Utils

private[multitenancy] class ThriftServerOperationManager private(name: String)
  extends AbstractService(name) with Logging {

  def this() = this(classOf[ThriftServerOperationManager].getSimpleName)

  private[this] val handleToOperation = new JMap[OperationHandle, Operation]

  val sessionToActivePool = new ConcurrentHashMap[SessionHandle, String]
  private[this] val sessionToSparkSession = new ConcurrentHashMap[SessionHandle, SparkSession]
  val userToOperationLog = new ConcurrentHashMap[String, OperationLog]()

  override def init(hiveConf: HiveConf): Unit = synchronized {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      initOperationLogCapture()
    }
    else {
      logDebug("Operation level logging is turned off")
    }
    super.init(hiveConf)
  }

  def addSparkSession(sessionHandle: SessionHandle, sparkSession: SparkSession): Unit = {
    sessionToSparkSession.put(sessionHandle, sparkSession)
  }

  def getSparkSession(sessionHandle: SessionHandle): SparkSession = {
    sessionToSparkSession.get(sessionHandle)
  }

  def removeSparkSession(sessionHandle: SessionHandle): Unit = {
    sessionToSparkSession.remove(sessionHandle)
  }

  private[this] def initOperationLogCapture(): Unit = {
    // Register another Appender (with the same layout) that talks to us.
    val ap = new SparkLogDivertAppender(this)
    Logger.getRootLogger.addAppender(ap)
  }

  private[this] def getOperationLogByThread: OperationLog = OperationLog.getCurrentOperationLog

  def getOperationLog: OperationLog =
    Option(getOperationLogByThread).getOrElse(userToOperationLog.get(Utils.getCurrentUserName()))

  def setOperationLog(user: String, log: OperationLog): Unit = {
    OperationLog.setCurrentOperationLog(log)
    userToOperationLog.put(Option(user).getOrElse(Utils.getCurrentUserName()), log)
  }

  def unregisterOperationLog(user: String): Unit = {
    OperationLog.removeCurrentOperationLog()
    userToOperationLog.remove(user)
  }

  def newExecuteStatementOperation(
      parentSession: HiveSession,
      statement: String,
      confOverlay: java.util.Map[String, String],
      async: Boolean): ExecuteStatementOperation = synchronized {

    val sessionHandle = parentSession.getSessionHandle
    val sparkSession = sessionToSparkSession.get(sessionHandle)

    require(sparkSession != null, s"Session sessionHandle: $sessionHandle has not been" +
      s" initialized or had already closed.")

    val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
    val plan = sessionState.sqlParser.parsePlan(statement)

    val runInBackground = async && sessionState.hiveThriftServerAsync
    val operation = new SparkExecuteStatementOperation(
      parentSession,
      plan,
      statement,
      sparkSession.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client,
      confOverlay,
      runInBackground)(sparkSession, sessionToActivePool)
    addOperation(operation)
    logDebug(s"Created Operation for $statement with session=$parentSession, " +
      s"runInBackground=$runInBackground")
    operation
  }

  def getOperation(operationHandle: OperationHandle): Operation = {
    val operation = getOperationInternal(operationHandle)
    if (operation == null) throw new HiveSQLException("Invalid OperationHandle: " + operationHandle)
    operation
  }

  private[this] def getOperationInternal(operationHandle: OperationHandle) =
    handleToOperation.get(operationHandle)

  private[this] def removeTimedOutOperation(operationHandle: OperationHandle): Operation = {
    val operation = handleToOperation.get(operationHandle)
    if (operation != null && operation.isTimedOut(System.currentTimeMillis)) {
      handleToOperation.remove(operationHandle)
      return operation
    }
    null
  }

  private[this] def addOperation(operation: Operation): Unit = {
    handleToOperation.put(operation.getHandle, operation)
  }

  private[this] def removeOperation(opHandle: OperationHandle) =
    handleToOperation.remove(opHandle)

  @throws[HiveSQLException]
  def cancelOperation(opHandle: OperationHandle): Unit = {
    val operation = getOperation(opHandle)
    val opState = operation.getStatus.getState
    if ((opState eq OperationState.CANCELED)
      || (opState eq OperationState.CLOSED)
      || (opState eq OperationState.FINISHED)
      || (opState eq OperationState.ERROR)
      || (opState eq OperationState.UNKNOWN)) {
      // Cancel should be a no-op in either cases
      logDebug(opHandle + ": Operation is already aborted in state - " + opState)
    }
    else {
      logDebug(opHandle + ": Attempting to cancel from state - " + opState)
      operation.cancel()
    }
  }

  @throws[HiveSQLException]
  def closeOperation(opHandle: OperationHandle): Unit = {
    val operation = removeOperation(opHandle)
    if (operation == null) throw new HiveSQLException("Operation does not exist!")
    operation.close()
  }

  @throws[HiveSQLException]
  def getOperationNextRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation, maxRows: Long): RowSet =
    getOperation(opHandle).getNextRowSet(orientation, maxRows)

  @throws[HiveSQLException]
  def getOperationLogRowSet(
      opHandle: OperationHandle,
      orientation: FetchOrientation, maxRows: Long): RowSet = {
    // get the OperationLog object from the operation
    val operationLog: OperationLog = getOperation(opHandle).getOperationLog
    if (operationLog == null) {
      throw new HiveSQLException("Couldn't find log associated with operation handle: " + opHandle)
    }
    // read logs
    var logs: JList[String] = new ArrayList[String]()
    try
      logs = operationLog.readOperationLog(isFetchFirst(orientation), maxRows)
    catch {
      case e: SQLException =>
        throw new HiveSQLException(e.getMessage, e.getCause)
    }
    // convert logs to RowSet
    val tableSchema: TableSchema = new TableSchema(getLogSchema)
    val rowSet: RowSet =
      RowSetFactory.create(tableSchema, getOperation(opHandle).getProtocolVersion)
    for (log <- logs.asScala) {
      rowSet.addRow(Array[AnyRef](log))
    }
    rowSet
  }

  private[this] def isFetchFirst(fetchOrientation: FetchOrientation): Boolean = {
    fetchOrientation == FetchOrientation.FETCH_FIRST
  }

  private[this] def getLogSchema: Schema = {
    val schema: Schema = new Schema
    val fieldSchema: FieldSchema = new FieldSchema
    fieldSchema.setName("operation_log")
    fieldSchema.setType("string")
    schema.addToFieldSchemas(fieldSchema)
    schema
  }

  def removeExpiredOperations(handles: Array[OperationHandle]): Array[Operation] = {
    val removed = new ArrayBuffer[Operation]()
    for (handle <- handles) {
      val operation: Operation = removeTimedOutOperation(handle)
      if (operation != null) {
        logWarning("Operation " + handle + " is timed-out and will be closed")
        removed += operation
      }
    }
    removed.toArray
  }
}
