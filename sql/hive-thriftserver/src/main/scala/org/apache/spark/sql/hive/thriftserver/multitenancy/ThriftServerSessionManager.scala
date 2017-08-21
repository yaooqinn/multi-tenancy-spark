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
import java.util.{Date, Map => JMap}
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hive.service.CompositeService
import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.util.Utils

private[hive] class ThriftServerSessionManager private(
    name: String,
    thriftServer: MultiTenancyThriftServer)
  extends CompositeService(name) with Logging {

  private[this] val thriftServerOperationManager = new ThriftServerOperationManager()
  private[this] var hiveConf: HiveConf = _
  private[this] val sparkConf = new SparkConf(loadDefaults = true)
  private[this] val handleToSession = new ConcurrentHashMap[SessionHandle, HiveSession]
  private[this] val handleToSessionUser = new ConcurrentHashMap[SessionHandle, String]
  private[this] val userToSparkSession =
    new ConcurrentHashMap[String, (SparkSession, AtomicInteger)]

  private[this] var backgroundOperationPool: ThreadPoolExecutor = _
  private[this] var isOperationLogEnabled = false
  private[this] var operationLogRootDir: File = _

  private[this] var checkInterval: Long = _
  private[this] var sessionTimeout: Long = _
  private[this] var checkOperation = false
  private[this] val sparkSessionCleanInterval =
    Utils.timeStringAsMs(sys.props.getOrElse("spark.thrift.session.clean.interval", "20min"))
  private[this] var shutdown = false

  def this(thriftServer: MultiTenancyThriftServer) = {
    this(getClass.getSimpleName, thriftServer)
  }

  override def init(hiveConf: HiveConf): Unit = synchronized {
    this.hiveConf = hiveConf
    // Create operation log root directory, if operation logging is enabled
    if (hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      initOperationLogRootDir()
    }
    createBackgroundOperationPool()
    addService(thriftServerOperationManager)
    super.init(hiveConf)
  }

  protected def createBackgroundOperationPool(): Unit = {
    val poolSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS)
    logInfo("SparkThriftServer: Background operation thread pool size: " + poolSize)
    val poolQueueSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_WAIT_QUEUE_SIZE)
    logInfo("SparkThriftServer: Background operation thread wait queue size: " + poolQueueSize)
    val keepAliveTime = HiveConf.getTimeVar(hiveConf,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME, TimeUnit.SECONDS)
    logInfo("SparkThriftServer: Background operation thread keepalive time: "
      + keepAliveTime + " seconds")
    val threadPoolName = "SparkThriftServer-Background-Pool"
    backgroundOperationPool =
      new ThreadPoolExecutor(
        poolSize,
        poolSize,
        keepAliveTime,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable](poolQueueSize),
        new ThreadFactoryWithGarbageCleanup(threadPoolName))
    backgroundOperationPool.allowCoreThreadTimeOut(true)
    checkInterval = HiveConf.getTimeVar(hiveConf,
      ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL, TimeUnit.MILLISECONDS)
    sessionTimeout = HiveConf.getTimeVar(hiveConf,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT, TimeUnit.MILLISECONDS)
    checkOperation = HiveConf.getBoolVar(hiveConf,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_CHECK_OPERATION)
  }

  protected def initOperationLogRootDir(): Unit = {
    operationLogRootDir =
      new File(hiveConf.getVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION))
    isOperationLogEnabled = true
    if (operationLogRootDir.exists && !operationLogRootDir.isDirectory) {
      logWarning("The operation log root directory exists, but it is not a directory: "
        + operationLogRootDir.getAbsolutePath)
      isOperationLogEnabled = false
    }
    if (!operationLogRootDir.exists) if (!operationLogRootDir.mkdirs) {
      logWarning("Unable to create operation log root directory: "
        + operationLogRootDir.getAbsolutePath)
      isOperationLogEnabled = false
    }
    if (isOperationLogEnabled) {
      logInfo("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath)
      try
        FileUtils.forceDeleteOnExit(operationLogRootDir)
      catch {
        case e: IOException =>
          logWarning("Failed to schedule cleanup HS2 operation logging root dir: "
            + operationLogRootDir.getAbsolutePath, e)
      }
    }
  }

  override def start(): Unit = {
    super.start()
    if (checkInterval > 0) {
      startTimeoutChecker()
    }
    startSparkSessionCleaner()
  }

  private[this] def startTimeoutChecker(): Unit = {
    val interval: Long = math.max(checkInterval, 3000L)
    // minimum 3 seconds
    val timeoutChecker: Runnable = new Runnable() {
      override def run(): Unit = {
        sleepInterval(interval)
        while (!shutdown) {
          val current: Long = System.currentTimeMillis
          handleToSession.values.asScala.foreach { session =>
            if (sessionTimeout > 0 && session.getLastAccessTime + sessionTimeout <= current
              && (!checkOperation || session.getNoOperationTime > sessionTimeout)) {
              val handle: SessionHandle = session.getSessionHandle
              logWarning("Session " + handle + " is Timed-out (last access : "
                + new Date(session.getLastAccessTime) + ") and will be closed")
              try
                closeSession(handle)
              catch {
                case e: HiveSQLException =>
                  logWarning("Exception is thrown closing session " + handle, e)
              }
            }
            else session.closeExpiredOperations()
          }
          sleepInterval(interval)
        }
      }

    }
    backgroundOperationPool.execute(timeoutChecker)
  }

  private[this] def startSparkSessionCleaner(): Unit = {
    // at least 10 min
    val interval = math.max(sparkSessionCleanInterval, 10 * 60 * 1000L)
    val sessionCleaner = new Runnable {
      override def run(): Unit = {
        sleepInterval(interval)
        while(!shutdown) {
          userToSparkSession.asScala.foreach {
            case (user, (session, times)) =>
              if (times.get() <= 0) {
                userToSparkSession.remove(user)
                session.stop()
              }
            case _ =>
          }

        }
      }
    }
    backgroundOperationPool.execute(sessionCleaner)
  }

  private[this] def sleepInterval(interval: Long): Unit = {
    try
      Thread.sleep(interval)
    catch {
      case e: InterruptedException =>
      // ignore
    }
  }

  def getSparkSession(
      user: UserGroupInformation,
      sessionConf: JMap[String, String]): SparkSession = {
    val userName = user.getShortUserName
    val kv = userToSparkSession.get(userName)
    if (kv == null) {
      val conf = sparkConf.clone
      conf.setAppName(s"SparkThriftServer[$user]")
      conf.set("spark.ui.port", "0") // avoid max port retry reach
      setQueue(sessionConf, conf)
      try {
        val sparkSession = user.doAs(new PrivilegedExceptionAction[SparkSession] {
          override def run(): SparkSession = {
            SparkSession.builder()
              .config(conf)
              .enableHiveSupport()
              .user(userName)
              .getOrCreate()
          }
        })
        val times = new AtomicInteger(1)
        userToSparkSession.put(userName, (sparkSession, times))
        sparkSession
      } catch {
        case e: Exception =>
          throw new SparkException("Failed Init SparkSession" + e, e)
      }
    } else {
      logInfo(s"SparkSession for [$user] is reused " + kv._2.incrementAndGet() + "times")
      kv._1
    }
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
   * @param password
   * @param ipAddress
   * @param sessionConf
   * @param withImpersonation
   * @param delegationToken
   * @return SessionHandle
   */
  def openSession(
      protocol: TProtocolVersion,
      realUser: String,
      username: String,
      password: String,
      ipAddress: String,
      sessionConf: JMap[String, String],
      withImpersonation: Boolean,
      delegationToken: String): SessionHandle = {
    val hiveSession =
      new SparkHiveSessionImpl(protocol, realUser, username, password, hiveConf,
        ipAddress, withImpersonation, this, thriftServerOperationManager)
    hiveSession.open(sessionConf)

    // TODO: Add listener
//    HiveThriftServer2.listener.onSessionCreated(
//      hiveSession.getIpAddress, sessionHandle.getSessionId.toString, hiveSession.getUsername)

    val sessionUGI = hiveSession.getSessionUgi

    val sparkSession = getSparkSession(sessionUGI, sessionConf)

    if (sparkSession != null && !sparkSession.sparkContext.isStopped) {
      hiveSession.setSparkSession(sparkSession)
    } else {
      throw new SparkException("Initialize SparkSession Failed")
    }

    sparkSession.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)

    sessionUGI.doAs(new PrivilegedExceptionAction[Unit] {
      override def run(): Unit = {
        sparkSession.sql(getDatabase(sessionConf))
      }
    })

    if (isOperationLogEnabled) {
      hiveSession.setOperationLogSessionDir(operationLogRootDir)
    }

    val sessionHandle = hiveSession.getSessionHandle
    handleToSession.put(sessionHandle, hiveSession)
    handleToSessionUser.put(sessionHandle, username)
    thriftServerOperationManager.addSparkSession(sessionHandle, sparkSession)
    sessionHandle
  }

  def closeSession(sessionHandle: SessionHandle) {
    // TODO: HiveThriftServer2.listener.onSessionClosed(sessionHandle.getSessionId.toString)
    thriftServerOperationManager.sessionToActivePool.remove(sessionHandle)
    thriftServerOperationManager.removeSparkSession(sessionHandle)
    val sessionUser = handleToSessionUser.remove(sessionHandle)
    val sessionAndTimes = userToSparkSession.get(sessionUser)
    if (sessionAndTimes != null) {
      sessionAndTimes._2.decrementAndGet()
    } else {
      throw new SparkException(s"SparkSession for [$sessionUser] does not exist")
    }
    val session = handleToSession.remove(sessionHandle)
    if (session == null) {
      throw new HiveSQLException(s"Session for [$sessionUser] does not exist!")
    }
    session.close()
  }

  private[this] def setQueue(sessionConf: JMap[String, String], conf: SparkConf): Unit = {
    if (sessionConf != null) {
      sessionConf.asScala.foreach { kv =>
        if (kv._1 == "set:hiveconf:mapred.job.queue.name") {
          sparkConf.set("spark.yarn.queue", kv._2)
        }
      }
    }
  }

  private[this] def getDatabase(sessionConf: JMap[String, String]): String = {
    if (sessionConf != null) {
      sessionConf.asScala.foreach { kv =>
        if (kv._1 == "use:database") {
          return "use " + kv._2
        }
      }
    }
    "use default"
  }

  @throws[HiveSQLException]
  def getSession(sessionHandle: SessionHandle): HiveSession = {
    val session = handleToSession.get(sessionHandle)
    if (session == null) {
      throw new HiveSQLException("Invalid SessionHandle: " + sessionHandle)
    }
    session
  }

  def getOperationManager: ThriftServerOperationManager = thriftServerOperationManager

  override def stop(): Unit = {
    super.stop()
    shutdown = true
    shutdown = true
    if (backgroundOperationPool != null) {
      backgroundOperationPool.shutdown()
      val timeout =
        hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS)
      try
        backgroundOperationPool.awaitTermination(timeout, TimeUnit.SECONDS)
      catch {
        case e: InterruptedException =>
          logWarning("HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e)
      }
      backgroundOperationPool = null
    }
    cleanupLoggingRootDir()
  }

  private[this] def cleanupLoggingRootDir(): Unit = {
    if (isOperationLogEnabled) try
      FileUtils.forceDelete(operationLogRootDir)
    catch {
      case e: Exception =>
        logWarning("Failed to cleanup root dir of HS2 logging: "
          + operationLogRootDir.getAbsolutePath, e)
    }
  }

  def getOpenSessionCount: Int = handleToSession.size

  def submitBackgroundOperation(r: Runnable): Future[_] = backgroundOperationPool.submit(r)
}
