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
import java.security.PrivilegedExceptionAction
import java.util.{HashSet, List => JList, Map}

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.{Credentials, UserGroupInformation}
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{Operation, OperationManager}
import org.apache.hive.service.cli.session.{HiveSession, SessionManager}
import org.apache.hive.service.cli.thrift.TProtocolVersion

import org.apache.spark.{CredentialCache, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

class SparkHiveSessionImpl(
    protocol: TProtocolVersion,
    realUser: String,
    var username: String,
    password: String,
    serverConf: HiveConf,
    var ipAddress: String,
    withImpersonation: Boolean,
    sessionManager: ThriftServerSessionManager,
    operationManager: ThriftServerOperationManager) extends HiveSession with Logging {

  private[this] val sessionHandle: SessionHandle = new SessionHandle(protocol)
  private[this] val hiveConf: HiveConf = new HiveConf(serverConf)
  private[this] val opHandleSet = new HashSet[OperationHandle]
  private[this] var _isOperationLogEnabled = false
  private[this] var sessionLogDir: File = _
  private[this] var lastAccessTime = 0L
  private[this] var lastIdleTime = 0L
  private[this] val sessionUGI: UserGroupInformation = {
    val currentUser = UserGroupInformation.getCurrentUser
    if (withImpersonation) {
      if (UserGroupInformation.isSecurityEnabled) {
        try {
          val ugi = UserGroupInformation.createProxyUser(username, currentUser)
          val creds = Option(CredentialCache.get(username)).getOrElse(new Credentials())
          ugi.addCredentials(creds)
          ugi
        } catch {
          case e: IOException =>
            throw new HiveSQLException("Couldn't setup proxy user", e)
        }
      } else {
        UserGroupInformation.createRemoteUser(username)
      }
    } else {
      currentUser
    }
  }

  private[this] var sparkSession: SparkSession = _

  def setSparkSession(sparkSession: SparkSession): Unit = this.sparkSession = sparkSession

  def getSparkSession(): SparkSession = this.sparkSession

  def removeSparkSession(): Unit = {
    sparkSession = null
  }

  def getSessionUgi: UserGroupInformation = this.sessionUGI

  private[this] def acquire(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
  }

  private[this] def release(userAccess: Boolean): Unit = {
    if (userAccess) {
      lastAccessTime = System.currentTimeMillis
    }
    if (opHandleSet.isEmpty) {
      lastIdleTime = System.currentTimeMillis
    } else {
      lastIdleTime = 0
    }
  }

  @throws[HiveSQLException]
  private[this] def executeStatementInternal(
      statement: String, confOverlay: Map[String, String], runAsync: Boolean) = {
    acquire(true)
    val operation =
      operationManager.newExecuteStatementOperation(this, statement, confOverlay, runAsync)
    val opHandle = operation.getHandle
    try {
      operation.run()
      opHandleSet.add(opHandle)
      opHandle
    } catch {
      case e: HiveSQLException =>
        operationManager.closeOperation(opHandle)
        throw e
    } finally {
      release(true)
    }
  }

  override def open(sessionConfMap: Map[String, String]): Unit = {
    configureSession(sessionConfMap)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  override def getMetaStoreClient: IMetaStoreClient = {
    throw new SparkException("Method Not Implemented!")
  }

  override def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    acquire(true)
    try {
      getInfoType match {
        case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.CLI_DBMS_VER => new GetInfoValue(getSparkSession().version)
        case _ =>
          throw new SparkException("Unrecognized GetInfoType value: " + getInfoType.toString)
      }
    } finally {
      release(true)
    }
  }

  /**
   * execute operation handler
   *
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  override def executeStatement(
      statement: String, confOverlay: Map[String, String]): OperationHandle = {
    executeStatementInternal(statement, confOverlay, false)
  }

  /**
   * execute operation handler
   *
   * @param statement
   * @param confOverlay
   * @return
   * @throws HiveSQLException
   */
  override def executeStatementAsync(
      statement: String, confOverlay: Map[String, String]): OperationHandle = {
    executeStatementInternal(statement, confOverlay, true)
  }

  override def getTypeInfo: OperationHandle = {
    throw new SparkException("Method Not Implemented!")
  }

  override def getCatalogs: OperationHandle = {
    throw new SparkException("Method Not Implemented!")
  }

  override def getSchemas(catalogName: String, schemaName: String): OperationHandle = {
    throw new SparkException("Method Not Implemented!")
  }

  override def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: JList[String]): OperationHandle = {
    throw new SparkException("Method Not Implemented!")

  }

  override def getTableTypes: OperationHandle = {
    throw new SparkException("Method Not Implemented!")
  }

  override def getColumns(
     catalogName: String,
     schemaName: String,
     tableName: String,
     columnName: String): OperationHandle = {
    throw new SparkException("Method Not Implemented!")
  }

  override def getFunctions(
      catalogName: String, schemaName: String, functionName: String): OperationHandle = {
    throw new SparkException("Method Not Implemented!")
  }

  /**
   * close the session
   *
   * @throws HiveSQLException
   */
  override def close(): Unit = {
    acquire(true)
    try {
      // Iterate through the opHandles and close their operations
      for (opHandle <- opHandleSet.asScala) {
        operationManager.closeOperation(opHandle)
      }
      opHandleSet.clear()
      // Cleanup session log directory.
      cleanupSessionLogDir()
      removeSparkSession()
    } finally {
      release(true)
      try
        FileSystem.closeAllForUGI(sessionUGI)
      catch {
        case ioe: IOException =>
          throw new HiveSQLException("Could not clean up file-system handles for UGI: "
            + sessionUGI, ioe)
      }
    }
  }

  private[this] def cleanupSessionLogDir(): Unit = {
    if (_isOperationLogEnabled) try
      FileUtils.forceDelete(sessionLogDir)
    catch {
      case e: Exception =>
        logError("Failed to cleanup session log dir: " + sessionHandle, e)
    }
  }

  override def cancelOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try
      operationManager.cancelOperation(opHandle)
    finally {
      release(true)
    }
  }

  override def closeOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      operationManager.closeOperation(opHandle)
      opHandleSet.remove(opHandle)
    } finally {
      release(true)
    }
  }

  override def getResultSetMetadata(opHandle: OperationHandle): TableSchema = {
    acquire(true)
    try {
      operationManager.getOperation(opHandle).getResultSetSchema
    } finally {
      release(true)
    }
  }

  override def fetchResults(
      opHandle: OperationHandle,
      orientation: FetchOrientation,
      maxRows: Long,
      fetchType: FetchType): RowSet = {
    acquire(true)
    try {
      if (fetchType == FetchType.QUERY_OUTPUT) {
        operationManager.getOperationNextRowSet(opHandle, orientation, maxRows)
      } else {
        operationManager.getOperationLogRowSet(opHandle, orientation, maxRows)
      }
    } finally {
      release(true)
    }
  }

  override def getDelegationToken(
      authFactory: HiveAuthFactory, owner: String, renewer: String): String = {
    throw new SparkException("Method Not Implemented!")
  }

  override def cancelDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    throw new SparkException("Method Not Implemented!")
  }

  override def renewDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    throw new SparkException("Method Not Implemented!")
  }

  override def closeExpiredOperations(): Unit = {
    val handles = opHandleSet.asScala.toArray
    if (handles.length > 0) {
      val operations = operationManager.removeExpiredOperations(handles)
      if (!operations.isEmpty) {
        closeTimedOutOperations(operations)
      }
    }
  }

  private[this] def closeTimedOutOperations(operations: JList[Operation]): Unit = {
    acquire(false)
    try {
      for (operation <- operations.asScala) {
        opHandleSet.remove(operation.getHandle)
        try
          operation.close()
        catch {
          case e: Exception =>
            logWarning("Exception is thrown closing timed-out operation " + operation.getHandle, e)
        }
      }
    } finally {
      release(false)
    }
  }

  override def getNoOperationTime: Long = {
    if (lastIdleTime > 0) {
      System.currentTimeMillis - lastIdleTime
    } else {
      0
    }
  }

  override def getProtocolVersion: TProtocolVersion = sessionHandle.getProtocolVersion

  /**
   * Set the session manager for the session
   *
   * @param sessionManager
   */
  override def setSessionManager(sessionManager: SessionManager): Unit = {
    throw new SparkException("Method NOT Implemented!")
  }

  /**
   * Get the session manager for the session
   */
  override def getSessionManager: SessionManager = {
    throw new SparkException("Method NOT Implemented! Please use " +
      "getThriftServerSessionManager instead!")
  }

  def getThriftServerSessionManager: ThriftServerSessionManager = sessionManager

  /**
   * Set operation manager for the session
   *
   * @param operationManager
   */
  override def setOperationManager(operationManager: OperationManager): Unit = {}

  /**
   * Check whether operation logging is enabled and session dir is created successfully
   */
  override def isOperationLogEnabled(): Boolean = _isOperationLogEnabled

  /**
   * Get the session dir, which is the parent dir of operation logs
   *
   * @return a file representing the parent directory of operation logs
   */
  override def getOperationLogSessionDir: File = sessionLogDir

  /**
   * Set the session dir, which is the parent dir of operation logs
   *
   * @param operationLogRootDir the parent dir of the session dir
   */
  override def setOperationLogSessionDir(operationLogRootDir: File): Unit = {
    sessionLogDir = new File(operationLogRootDir, sessionHandle.getHandleIdentifier.toString)
    _isOperationLogEnabled = true
    if (!sessionLogDir.exists) {
      if (!sessionLogDir.mkdir) {
        logWarning("Unable to create operation log session directory: "
          + sessionLogDir.getAbsolutePath)
        _isOperationLogEnabled = false
      }
    }
    if (isOperationLogEnabled) {
      logInfo("Operation log session directory is created: " + sessionLogDir.getAbsolutePath)
    }
  }

  override def getSessionHandle: SessionHandle = sessionHandle

  override def getRealUsername: String = realUser

  override def getUsername: String = username

  override def getPassword: String = password

  override def getHiveConf: HiveConf = hiveConf

  override def getSessionState: SessionState = {
    throw new SparkException("Method NOT Implemented!")
  }

  override def setUserName(userName: String): Unit = {
    this.username = userName
  }

  override def getIpAddress: String = ipAddress

  override def setIpAddress(ipAddress: String): Unit = {
    this.ipAddress = ipAddress
  }

  override def getLastAccessTime: Long = lastAccessTime

  override def getUserName: String = username

  private[this] def configureSession(sessionConfMap: Map[String, String]): Unit = {
    assert(sparkSession != null)
    for (entry <- sessionConfMap.entrySet.asScala) {
      val key = entry.getKey
      if (key.startsWith("set:")) {
        sparkSession.conf.set(key.substring(4), entry.getValue)
      }
      else if (key.startsWith("use:")) {
        sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            sparkSession.sql("use " + entry.getValue)
          }
        })
      }
      else {
        hiveConf.verifyAndSet(key, entry.getValue)
      }
    }
  }
}
