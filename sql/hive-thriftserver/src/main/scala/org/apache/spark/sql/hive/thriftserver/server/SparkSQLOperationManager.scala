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

import org.apache.hadoop.hive.ql.plan.HiveOperation
import org.apache.hadoop.hive.ql.security.authorization.plugin.{HiveAuthzContext, HiveOperationType}
import org.apache.hadoop.hive.shims.Utils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{ExecuteStatementOperation, OperationManager}
import org.apache.hive.service.cli.session.HiveSession

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Command, InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTable, CreateTempViewUsing, InsertIntoDataSourceCommand, InsertIntoHadoopFsRelationCommand}
import org.apache.spark.sql.hive.HiveSessionState
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
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
    import org.apache.spark.sql.hive.HivePrivObjsFromPlan

    val sessionHandle = parentSession.getSessionHandle
    val sparkSession = sessionToSparkSession.get(sessionHandle)
    var client = sessionToClient.get(sessionHandle)

    require(sparkSession != null, s"Session sessionHandle: $sessionHandle has not been" +
      s" initialized or had already closed.")

    val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
    val plan = sessionState.sqlParser.parsePlan(statement)
    // add this to support the old static multi thrift server
    val optimizedPlan = sessionState.optimizer.execute(plan)
    // authorize happens here
    val opType = toHiveOperationType(optimizedPlan)
    val (in, out) = HivePrivObjsFromPlan.build(optimizedPlan, client.getCurrentDatabase)
    client.checkPrivileges(opType, in, out, getHiveAuthzContext(optimizedPlan))

    optimizedPlan match {
      case setCmd: SetCommand =>
        setCmd.kv match {
          case Some(("hivevar:ranger.user.name", Some(name))) if name != client.getCurrentUser =>
          verifyChangeRangerUser(parentSession)
          val currentDatabase = client.getCurrentDatabase
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
      optimizedPlan,
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


  ///////////////////////////////////////////////////////////////////////////////////
  // the old static sts support user set ranger.user.name to switch hive client,   //
  // spark-authorizer don't support such operation, for compatibility concern, i   //
  // copied some methods from there                                                //
  ///////////////////////////////////////////////////////////////////////////////////
  /**
   * Mapping of [[LogicalPlan]] -> [[HiveOperation]]
   * @param logicalPlan a spark LogicalPlan
   * @return
   */
  private def logicalPlan2HiveOperation(logicalPlan: LogicalPlan): HiveOperation = {
    logicalPlan match {
      case c: Command => c match {
        case e: ExplainCommand => logicalPlan2HiveOperation(e.logicalPlan)
        case _: LoadDataCommand => HiveOperation.LOAD
        case _: InsertIntoHadoopFsRelationCommand => HiveOperation.QUERY
        case _: InsertIntoDataSourceCommand => HiveOperation.QUERY
        case _: CreateDatabaseCommand => HiveOperation.CREATEDATABASE
        case _: DropDatabaseCommand => HiveOperation.DROPDATABASE
        case _: SetDatabaseCommand => HiveOperation.SWITCHDATABASE
        case _: DropTableCommand => HiveOperation.DROPTABLE
        case _: DescribeTableCommand => HiveOperation.DESCTABLE
        case _: DescribeFunctionCommand => HiveOperation.DESCFUNCTION
        case _: AlterTableRecoverPartitionsCommand => HiveOperation.MSCK
        case _: AlterTableRenamePartitionCommand => HiveOperation.ALTERTABLE_RENAMEPART
        case AlterTableRenameCommand(_, _, isView) =>
          if (!isView) HiveOperation.ALTERTABLE_RENAME else HiveOperation.ALTERVIEW_RENAME
        case _: AlterTableDropPartitionCommand => HiveOperation.ALTERTABLE_DROPPARTS
        case _: AlterTableAddPartitionCommand => HiveOperation.ALTERTABLE_ADDPARTS
        case _: AlterTableSetPropertiesCommand
             | _: AlterTableUnsetPropertiesCommand => HiveOperation.ALTERTABLE_PROPERTIES
        case _: AlterTableSerDePropertiesCommand => HiveOperation.ALTERTABLE_SERDEPROPERTIES
        // case _: AnalyzeTableCommand => HiveOperation.ANALYZE_TABLE
        // Hive treat AnalyzeTableCommand as QUERY, obey it.
        case _: AnalyzeTableCommand => HiveOperation.QUERY
        case _: ShowDatabasesCommand => HiveOperation.SHOWDATABASES
        case _: ShowTablesCommand => HiveOperation.SHOWTABLES
        case _: ShowColumnsCommand => HiveOperation.SHOWCOLUMNS
        case _: ShowTablePropertiesCommand => HiveOperation.SHOW_TBLPROPERTIES
        case _: ShowCreateTableCommand => HiveOperation.SHOW_CREATETABLE
        case _: ShowFunctionsCommand => HiveOperation.SHOWFUNCTIONS
        case _: ShowPartitionsCommand => HiveOperation.SHOWPARTITIONS
        case SetCommand(Some((_, None))) | SetCommand(None) => HiveOperation.SHOWCONF
        case _: CreateFunctionCommand => HiveOperation.CREATEFUNCTION
        // Hive don't check privileges for `drop function command`, what about a unverified user
        // try to drop functions.
        // We treat permanent functions as tables for verifying.
        case DropFunctionCommand(_, _, _, false) => HiveOperation.DROPTABLE
        case DropFunctionCommand(_, _, _, true) => HiveOperation.DROPFUNCTION
        case _: CreateViewCommand
             | _: CacheTableCommand
             | _: CreateTempViewUsing => HiveOperation.CREATEVIEW
        case _: UncacheTableCommand => HiveOperation.DROPVIEW
        case _: AlterTableSetLocationCommand => HiveOperation.ALTERTABLE_LOCATION
        case _: CreateTable
             | _: CreateTableCommand
             | _: CreateDataSourceTableCommand => HiveOperation.CREATETABLE
        case _: TruncateTableCommand => HiveOperation.TRUNCATETABLE
        case _: CreateDataSourceTableAsSelectCommand
             | _: CreateHiveTableAsSelectCommand => HiveOperation.CREATETABLE_AS_SELECT
        case _: CreateTableLikeCommand => HiveOperation.CREATETABLE
        case _: AlterDatabasePropertiesCommand => HiveOperation.ALTERDATABASE
        case _: DescribeDatabaseCommand => HiveOperation.DESCDATABASE
        // case _: AlterViewAsCommand => HiveOperation.ALTERVIEW_AS
        case _: AlterViewAsCommand => HiveOperation.QUERY
        case _ =>
          // AddFileCommand
          // AddJarCommand
          HiveOperation.EXPLAIN
      }
      case _: InsertIntoTable => HiveOperation.QUERY
      case _ => HiveOperation.QUERY
    }
  }

  private[this] def toHiveOperationType(logicalPlan: LogicalPlan): HiveOperationType = {
    HiveOperationType.valueOf(logicalPlan2HiveOperation(logicalPlan).name())
  }

  /**
   * Provides context information in authorization check call that can be used for
   * auditing and/or authorization.
   */
  private[this] def getHiveAuthzContext(
      logicalPlan: LogicalPlan,
      command: Option[String] = None): HiveAuthzContext = {
    val authzContextBuilder = new HiveAuthzContext.Builder()
    // set the sql query string, [[LogicalPlan]] contains such information in 2.2 or higher version
    // so this is for evolving..
    authzContextBuilder.build()
  }
}

