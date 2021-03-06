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
import java.sql.{Date, Timestamp}
import java.util.{Arrays, Map => JMap, UUID}
import java.util.concurrent.RejectedExecutionException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, Row => SparkRow, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.thriftserver.monitor.ThriftServerMonitor
import org.apache.spark.sql.hive.thriftserver.multitenancy.SparkHiveSessionImpl
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.{Utils => SparkUtils}

private[hive] class SparkExecuteStatementOperation(
    parentSession: HiveSession,
    plan: LogicalPlan,
    statement: String,
    client: HiveClient,
    confOverlay: JMap[String, String],
    runInBackground: Boolean = true)
    (sparkSession: SparkSession, sessionToActivePool: JMap[SessionHandle, String])
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
  with Logging {

  private var result: DataFrame = _
  private var iter: Iterator[SparkRow] = _
  private var iterHeader: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _
  private var statementId: String = _

  private lazy val resultSchema: TableSchema = {
    if (result == null || result.schema.isEmpty) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      logInfo(s"Result Schema: ${result.schema}")
      SparkExecuteStatementOperation.getTableSchema(result.schema)
    }
  }

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    logDebug(s"CLOSING $statementId")
    cleanup(OperationState.CLOSED)
    sparkSession.sparkContext.clearJobGroup()
  }

  def addNonNullColumnValue(from: SparkRow, to: ArrayBuffer[Any], ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to += from.getString(ordinal)
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType() =>
        to += from.getDecimal(ordinal)
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case DateType =>
        to += from.getAs[Date](ordinal)
      case TimestampType =>
        to += from.getAs[Timestamp](ordinal)
      case BinaryType =>
        to += from.getAs[Array[Byte]](ordinal)
      case _: ArrayType | _: StructType | _: MapType =>
        val hiveString = HiveUtils.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion)

    // Reset iter to header when fetching start from first row
    if (order.equals(FetchOrientation.FETCH_FIRST)) {
      val (ita, itb) = iterHeader.duplicate
      iter = ita
      iterHeader = itb
    }

    if (!iter.hasNext) {
      resultRowSet
    } else {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = maxRowsL.toInt
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val sparkRow = iter.next()
        val row = ArrayBuffer[Any]()
        var curCol = 0
        while (curCol < sparkRow.length) {
          if (sparkRow.isNullAt(curCol)) {
            row += null
          } else {
            addNonNullColumnValue(sparkRow, row, curCol)
          }
          curCol += 1
        }
        resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
      resultRowSet
    }
  }

  def getResultSetSchema: TableSchema = resultSchema

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      execute()
    } else {
      val sparkServiceUGI = parentSession.getSessionUgi

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {
        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            registerCurrentOperationLog()
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException => setOperationException(e)
              } finally {
                unregisterOperationLog()
              }
            }
          }
          try {
            sparkServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              logError("Error running hive query as user : " + sparkServiceUGI.getShortUserName, e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle = parentSession match {
          case s: SparkHiveSessionImpl =>
            s.getThriftServerSessionManager.submitBackgroundOperation(backgroundOperation)
          case h: HiveSession =>
            h.getSessionManager.submitBackgroundOperation(backgroundOperation)
        }

        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          logError(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          throw e
      }
    }
  }

  private def execute(): Unit = {
    statementId = UUID.randomUUID().toString
    logInfo(s"Running query '$statement' with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sparkSession.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)
    ThriftServerMonitor.getListener(parentSession.getUserName).onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      statement,
      statementId,
      parentSession.getUsername)
    sparkSession.sparkContext.setJobGroup(statementId, statement)
    val pool = sessionToActivePool.get(parentSession.getSessionHandle)
    if (pool != null) {
      sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    }
    try {
      result = Dataset.ofRows(sparkSession, plan)
      ThriftServerMonitor.getListener(parentSession.getUserName)
        .onStatementParsed(statementId, result.queryExecution.toString())
      logDebug(result.queryExecution.toString())
      result.queryExecution.logical match {
        case SetCommand(Some((SQLConf.THRIFTSERVER_POOL.key, Some(value)))) =>
          sessionToActivePool.put(parentSession.getSessionHandle, value)
          logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
        case _ =>
      }
      iter = {
        val useIncrementalCollect = sparkSession.conf.get(SQLConf.THRIFTSERVER_INCREMENTAL_COLLECT)
        if (useIncrementalCollect) {
          result.toLocalIterator().asScala
        } else {
          result.collect().iterator
        }
      }
      val (itra, itrb) = iter.duplicate
      iterHeader = itra
      iter = itrb
      dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
    } catch {
      case e: HiveSQLException =>
        if (getStatus.getState == OperationState.CANCELED
          || getStatus.getState == OperationState.CLOSED) {
          return
        } else {
          setState(OperationState.ERROR)
          ThriftServerMonitor.getListener(parentSession.getUserName).onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw e
        }
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        val currentState = getStatus.getState
        logError(s"Error executing query, currentState $currentState, ", e)
        if (currentState == OperationState.CANCELED || currentState == OperationState.CLOSED) {
          return
        } else {
          setState(OperationState.ERROR)
          ThriftServerMonitor.getListener(parentSession.getUserName).onStatementError(
            statementId, e.getMessage, SparkUtils.exceptionString(e))
          throw new HiveSQLException(e.toString)
        }
    } finally {
      if (statementId != null) {
        sparkSession.sparkContext.cancelJobGroup(statementId)
      }
    }
    setState(OperationState.FINISHED)
    ThriftServerMonitor.getListener(parentSession.getUserName).onStatementFinish(statementId)
  }

  override def cancel(): Unit = {
    logInfo(s"Cancel '$statement' with $statementId")
    cleanup(OperationState.CANCELED)
  }

  private def cleanup(state: OperationState) {
    if (getStatus.getState != OperationState.CLOSED) {
      setState(state)
    }
    if (runInBackground) {
      val backgroundHandle = getBackgroundHandle
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
    if (statementId != null) {
      sparkSession.sparkContext.cancelJobGroup(statementId)
    }
  }

  private def registerCurrentOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      if (operationLog == null) {
        logWarning("Failed to get current OperationLog object of Operation: "
          + getHandle.getHandleIdentifier)
        isOperationLogEnabled = false
      } else {
        parentSession match {
          case spark: SparkHiveSessionImpl =>
            spark.getThriftServerSessionManager.getOperationManager
              .setOperationLog(spark.getUserName, operationLog)
          case _ =>
            OperationLog.setCurrentOperationLog(operationLog)
        }
      }
    }
  }

  override def unregisterOperationLog(): Unit = {
    if (isOperationLogEnabled) {
      parentSession match {
        case spark: SparkHiveSessionImpl =>
          spark.getThriftServerSessionManager.getOperationManager
            .unregisterOperationLog(parentSession.getUserName)
        case _ =>
          OperationLog.removeCurrentOperationLog()
      }
    }
  }

}

object SparkExecuteStatementOperation {
  def getTableSchema(structType: StructType): TableSchema = {
    val schema = structType.map { field =>
      val attrTypeString = if (field.dataType == NullType) "void" else field.dataType.catalogString
      new FieldSchema(field.name, attrTypeString, field.getComment().getOrElse(""))
    }
    new TableSchema(schema.asJava)
  }
}
