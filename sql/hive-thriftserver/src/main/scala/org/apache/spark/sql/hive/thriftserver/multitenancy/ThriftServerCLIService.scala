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

import java.util.{List => JList, Map => JMap}

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import org.apache.hive.service.{CompositeService, ServiceException}
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.Operation
import org.apache.hive.service.cli.thrift.TProtocolVersion

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging

private[hive] class ThriftServerCLIService private(
    name: String,
    private val thriftServer: MultiTenancyThriftServer)
  extends CompositeService(name) with ICLIService with Logging {

  private[this] var hiveConf: HiveConf = _
  private[this] var sessionManager: ThriftServerSessionManager = _

  def this(thriftServer: MultiTenancyThriftServer) = {
    this(getClass.getSimpleName, thriftServer)
  }

  override def init(hiveConf: HiveConf): Unit = synchronized {
    this.hiveConf = hiveConf
    sessionManager = new ThriftServerSessionManager(thriftServer)
    addService(sessionManager)
    super.init(hiveConf)
  }

  override def start(): Unit = {
    super.start()
    // Initialize and test a connection to the metastore
    var metastoreClient: HiveMetaStoreClient = null
    try {
      metastoreClient = new HiveMetaStoreClient(hiveConf)
      metastoreClient.getDatabases("default")
    } catch {
      case e: Exception =>
        throw new ServiceException("Unable to connect to MetaStore!", e)
    } finally {
      if (metastoreClient != null) {
        metastoreClient.close()
      }
    }
  }

  def getSessionManager(): ThriftServerSessionManager = this.sessionManager

  def openSession(
      protocol: TProtocolVersion,
      realUser: String,
      username: String,
      password: String,
      ipAddress: String,
      configuration: JMap[String, String]): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      protocol, realUser, username, password, ipAddress, configuration, false, null)
    sessionHandle
  }

  def openSessionWithImpersonation(
      protocol: TProtocolVersion,
      realUser: String,
      username: String,
      password: String,
      ipAddress: String,
      configuration: JMap[String, String],
      delegationToken: String): SessionHandle = {
    val sessionHandle = sessionManager.openSession(
      protocol, realUser, username, password, ipAddress, configuration, true, delegationToken)
    sessionHandle
  }
  override def openSession(
      username: String,
      password: String,
      configuration: JMap[String, String]): SessionHandle = {
    throw new SparkException("Method Not Implemented")
  }

  override def closeSession(sessionHandle: SessionHandle): Unit = {
    sessionManager.closeSession(sessionHandle)
  }

  override def getInfo(sessionHandle: SessionHandle, infoType: GetInfoType): GetInfoValue = {
    sessionManager.getSession(sessionHandle).getInfo(infoType)
  }

  override def executeStatement(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: JMap[String, String]): OperationHandle = {
    sessionManager.getSession(sessionHandle).executeStatement(statement, confOverlay)
  }

  override def executeStatementAsync(
      sessionHandle: SessionHandle,
      statement: String,
      confOverlay: JMap[String, String]): OperationHandle = {
    sessionManager.getSession(sessionHandle).executeStatementAsync(statement, confOverlay)
  }

  override def getTypeInfo(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getTypeInfo
  }

  override def getCatalogs(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getCatalogs
  }

  override def getSchemas(
      sessionHandle: SessionHandle, catalogName: String, schemaName: String): OperationHandle = {
    sessionManager.getSession(sessionHandle).getSchemas(catalogName, schemaName)
  }

  override def getTables(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: JList[String]): OperationHandle = {
    sessionManager.getSession(sessionHandle)
      .getTables(catalogName, schemaName, tableName, tableTypes)
  }

  override def getTableTypes(sessionHandle: SessionHandle): OperationHandle = {
    sessionManager.getSession(sessionHandle).getTableTypes
  }

  override def getColumns(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String, tableName: String, columnName: String): OperationHandle = {
    sessionManager.getSession(sessionHandle)
      .getColumns(catalogName, schemaName, tableName, catalogName)
  }

  override def getFunctions(
      sessionHandle: SessionHandle,
      catalogName: String,
      schemaName: String, functionName: String): OperationHandle = {
    sessionManager.getSession(sessionHandle).getFunctions(catalogName, schemaName, functionName)
  }

  override def getOperationStatus(opHandle: OperationHandle): OperationStatus = {
    sessionManager.getOperationManager.getOperation(opHandle).getStatus
  }

  override def cancelOperation(opHandle: OperationHandle): Unit = {
    sessionManager.getOperationManager.getOperation(opHandle).cancel()
  }

  override def closeOperation(opHandle: OperationHandle): Unit = {
    sessionManager.getOperationManager.getOperation(opHandle).close()
  }

  override def getResultSetMetadata(opHandle: OperationHandle): TableSchema = {
    sessionManager.getOperationManager.getOperation(opHandle).getResultSetSchema
  }

  override def fetchResults(opHandle: OperationHandle): RowSet = {
    fetchResults(opHandle, Operation.DEFAULT_FETCH_ORIENTATION,
      Operation.DEFAULT_FETCH_MAX_ROWS, FetchType.QUERY_OUTPUT)
  }

  override def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long, fetchType: FetchType): RowSet = {
    sessionManager.getOperationManager.getOperation(opHandle)
      .getParentSession.fetchResults(opHandle, orientation, maxRows, fetchType)

  }

  override def getDelegationToken(
      sessionHandle: SessionHandle,
      authFactory: HiveAuthFactory,
      owner: String, renewer: String): String = null

  override def cancelDelegationToken(
      sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit = {}

  override def renewDelegationToken(
      sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String): Unit = {}
}

object ThriftServerCLIService {
  final val SERVER_VERSION = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8
}
