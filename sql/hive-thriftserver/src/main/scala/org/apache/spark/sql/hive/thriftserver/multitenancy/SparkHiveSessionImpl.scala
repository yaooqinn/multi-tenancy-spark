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
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashSet

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hadoop.hive.ql.session.SessionState
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.{Operation, OperationManager}
import org.apache.hive.service.cli.session.{HiveSession, SessionManager}
import org.apache.hive.service.cli.thrift.TProtocolVersion

import org.apache.spark.{CredentialCache, SparkConf}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.monitor.{MultiTenancyThriftServerListener, ThriftServerMonitor}
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab

class SparkHiveSessionImpl(
    protocol: TProtocolVersion,
    realUser: String,
    var username: String,
    password: String,
    serverConf: HiveConf,
    sparkConf: SparkConf,
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
          val proxyUgi = UserGroupInformation.createProxyUser(username, currentUser)
          Option(CredentialCache.get(username)).foreach { credentials =>
            proxyUgi.addCredentials(credentials)
          }
          proxyUgi
        } catch {
          case e: Exception =>
            val errorMsg = s"${currentUser.getShortUserName} could not impersonate $username"
            throw new HiveSQLException(errorMsg, e)
        }
      } else {
        UserGroupInformation.createRemoteUser(username)
      }
    } else {
      currentUser
    }
  }

  private[this] var _sparkSession: SparkSession = _

  private[this] var initialDatabase: String = "use default"

  def sparkSession(): SparkSession = this._sparkSession

  private[this] def getOrCreateSparkSession(): Unit = synchronized {
    val userName = sessionUGI.getShortUserName
    var checkRound = math.max(sparkConf.getInt("spark.yarn.report.times.on.start", 60), 15)
    val interval = sparkConf.getTimeAsMs("spark.yarn.report.interval", "1s")
    while (sessionManager.isSCPartiallyConstructed(userName)) {
      wait(interval)
      checkRound -= 1
      if (checkRound <= 0) {
        throw new HiveSQLException(s"A partially constructed SparkContext for [$userName] " +
          s"has last more than ${checkRound * interval} seconds")
      }
    }

    sessionManager.getExistSparkSession(userName) match {
      case Some((ss, times)) if !ss.sparkContext.isStopped =>
        logInfo(s"SparkSession for [$userName] is reused " + times.incrementAndGet() + "times")
        _sparkSession = ss.newSession()
      case _ =>
        sessionManager.setSCPartiallyConstructed(userName)
        notifyAll()
        createSparkSession()
    }
  }

  private[this] def createSparkSession(): Unit = {
    val userName = sessionUGI.getShortUserName
    sparkConf.setAppName(s"SparkThriftServer[$userName]")
    sparkConf.set("spark.ui.port", "0") // avoid max port retries reached
    try {
      _sparkSession = sessionUGI.doAs(new PrivilegedExceptionAction[SparkSession] {
        override def run(): SparkSession = {
          try {
            SparkSession.builder()
              .config(sparkConf)
              .enableHiveSupport()
              .user(userName)
              .getOrCreate()
          } catch {
            case e: Exception =>
              val exception = new RuntimeException(
                s"Failed initializing SparkSession for user[$userName] due to\n" +
                  s" ${e.getMessage}", e)
              throw exception
          }
        }
      })
      sessionManager.setSparkSession(userName, _sparkSession)
      // set sc fully constructed immediately
      sessionManager.setSCFullyConstructed(userName)
      ThriftServerMonitor.setListener(userName, new MultiTenancyThriftServerListener(sparkConf))
      _sparkSession.sparkContext.addSparkListener(ThriftServerMonitor.getListener(userName))
      val uiTab = new ThriftServerTab(userName, _sparkSession.sparkContext)
      ThriftServerMonitor.addUITab(_sparkSession.sparkContext.sparkUser, uiTab)
    } catch {
      case re: RuntimeException =>
        throw new HiveSQLException(re)
      case e: Exception =>
        val hiveSQLException =
          new HiveSQLException(s"Failed initializing SparkSession for user[$userName]", e)
        throw hiveSQLException
    } finally {
      sessionManager.setSCFullyConstructed(userName)
    }
  }

  def removeSparkSession(): Unit = {
    _sparkSession = null
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
      statement: String, confOverlay: JMap[String, String], runAsync: Boolean) = {
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

  override def open(sessionConfMap: JMap[String, String]): Unit = {
    configureSession(sessionConfMap)
    getOrCreateSparkSession()
    assert(_sparkSession != null)

    sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        _sparkSession.sql(initialDatabase)
      }
    })
    _sparkSession.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)
    lastAccessTime = System.currentTimeMillis
    lastIdleTime = lastAccessTime
  }

  override def getMetaStoreClient: IMetaStoreClient = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def getInfo(getInfoType: GetInfoType): GetInfoValue = {
    acquire(true)
    try {
      getInfoType match {
        case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
        case GetInfoType.CLI_DBMS_VER => new GetInfoValue(this._sparkSession.version)
        case _ =>
          throw new HiveSQLException("Unrecognized GetInfoType value: " + getInfoType.toString)
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
      statement: String, confOverlay: JMap[String, String]): OperationHandle = {
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
      statement: String, confOverlay: JMap[String, String]): OperationHandle = {
    executeStatementInternal(statement, confOverlay, true)
  }

  override def getTypeInfo: OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def getCatalogs: OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def getSchemas(catalogName: String, schemaName: String): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def getTables(
      catalogName: String,
      schemaName: String,
      tableName: String,
      tableTypes: JList[String]): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")

  }

  override def getTableTypes: OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def getColumns(
     catalogName: String,
     schemaName: String,
     tableName: String,
     columnName: String): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def getFunctions(
      catalogName: String, schemaName: String, functionName: String): OperationHandle = {
    throw new HiveSQLException("Method Not Implemented!")
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
      for (opHandle <- opHandleSet) {
        operationManager.closeOperation(opHandle)
      }
      opHandleSet.clear()
      // Cleanup session log directory.
      cleanupSessionLogDir()
      removeSparkSession()
    } finally {
      release(true)
      try {
        FileSystem.closeAllForUGI(sessionUGI)
      } catch {
        case ioe: IOException =>
          throw new HiveSQLException("Could not clean up file-system handles for UGI: "
            + sessionUGI, ioe)
      }
    }
  }

  private[this] def cleanupSessionLogDir(): Unit = {
    if (_isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(sessionLogDir)
      } catch {
        case e: Exception =>
          logError("Failed to cleanup session log dir: " + sessionLogDir, e)
      }
    }
  }

  override def cancelOperation(opHandle: OperationHandle): Unit = {
    acquire(true)
    try {
      operationManager.cancelOperation(opHandle)
    }
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
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def cancelDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def renewDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit = {
    throw new HiveSQLException("Method Not Implemented!")
  }

  override def closeExpiredOperations(): Unit = {
    val handles = opHandleSet.toArray
    if (handles.length > 0) {
      val operations = operationManager.removeExpiredOperations(handles)
      if (!operations.isEmpty) {
        closeTimedOutOperations(operations)
      }
    }
  }

  private[this] def closeTimedOutOperations(operations: Array[Operation]): Unit = {
    acquire(false)
    try {
      for (operation <- operations) {
        opHandleSet.remove(operation.getHandle)
        try {
          operation.close()
        } catch {
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
    throw new HiveSQLException("Method NOT Implemented!")
  }

  /**
   * Get the session manager for the session
   */
  override def getSessionManager: SessionManager = {
    throw new HiveSQLException("Method NOT Implemented! Please use " +
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
      if (!sessionLogDir.mkdirs) {
        logWarning("Unable to create operation log session directory: "
          + sessionLogDir.getAbsolutePath)
        _isOperationLogEnabled = false
      }
    }
    if (_isOperationLogEnabled) {
      logInfo("Operation log session directory is created: " + sessionLogDir.getAbsolutePath)
    }
  }

  override def getSessionHandle: SessionHandle = sessionHandle

  override def getRealUsername: String = realUser

  override def getUsername: String = username

  override def getPassword: String = password

  override def getHiveConf: HiveConf = hiveConf

  override def getSessionState: SessionState = {
    throw new HiveSQLException("Method NOT Implemented!")
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

  private[this] def configureSession(sessionConfMap: JMap[String, String]): Unit = {
    for (entry <- sessionConfMap.entrySet.asScala) {
      val key = entry.getKey
      if (key == "set:hivevar:mapred.job.queue.name") {
        sparkConf.set("spark.yarn.queue", entry.getValue)
      } else if (key.startsWith("set:hivevar:")) {
        val realKey = key.substring(12)
        if (realKey.startsWith("spark.")) {
          sparkConf.set(realKey, entry.getValue)
        } else {
          sparkConf.set("spark.hadoop." + realKey, entry.getValue)
        }
      } else if (key.startsWith("use:")) {
        // deal with database later after sparkSession initialized
        initialDatabase = "use " + entry.getValue
      } else {
        hiveConf.verifyAndSet(key, entry.getValue)
      }
    }
  }
}
