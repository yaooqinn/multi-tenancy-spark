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
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.config._
import org.apache.spark.util.ThreadUtils

/**
 * The manager for hive's proxy-user-specified [[SparkSession]] s. If the proxy user is not the
 * actual user start the [[HiveThriftServer2]], the manager will try to set a [[SparkSession]]
 * which associated with this user. If this session is already started, just return a new copy of
 * it using the existing [[SparkContext]]. Otherwise, a [[SparkContext]] will be initialized with
 * the proxy user and the yarn queue if specified.
 */
private[thriftserver] class SparkSessionManager extends Logging {

  // A map stores proxy user as key and [[SparkSession]] as value which is binding to a unique
  // [[SparkContext]] which belong to a particular proxy user.
  private val userToSparkSession: ConcurrentHashMap[String, SparkSession] = new ConcurrentHashMap
  
  // A map stores sessionHandle as key and proxy user as value. When a user opens a session as
  // proxy-user, we put a <sessionHandle, proxy-user> to it. When close, remove this kv pair.
  private val hiveSessionToUser: ConcurrentHashMap[SessionHandle, String] = new ConcurrentHashMap
  
  private val sparkConf: SparkConf = SparkSQLEnv.originalConf.clone
  
  // Decide whether close user-specified spark environment, after there is no connections to it.
  private val cleanSessionOnClose: Boolean = sparkConf.get(CLEAN_SESSION_ON_CLOSE)
  
  // Interval for the sessionCleaner to stop unused sparkContext
  private val sessionCleanInterval = sparkConf.get(SESSION_CLEAN_INTERVAL)
  
  if (!cleanSessionOnClose) {
    new SparkSessionCleaner().scheduleCleanSession()
  }
  
  /**
   * Check whether the user specified spark environment has been launched.
   * @param user the proxy-user who launch the entire spark environment.
   * @return true if sparkSession is available, otherwise false.
   */
  private def isCtxStarted(user: String): Boolean = {
    assert(user != null, "proxy-user could not be null")
    
    val ss = userToSparkSession.get(user)
    if (ss != null && ss.sparkContext != null && !ss.sparkContext.isStopped) {
      logInfo(s"SparkContext has been ready for proxy-user [$user], using this existing one to" +
        s"create new SparkSession.")
      true
    } else if (ss == null) {
      logInfo(s"No existing SparkContext has been started for current proxy-user [$user], trying" +
        s"to launch a new one.")
      false
    } else {
      logWarning(s"Current proxy-user [$user]'s SparkSession is binding to a dead SparkContext," +
        s"remove it and create a new one.")
      userToSparkSession.remove(user)
      false
    }
  }
  
  /**
   * Generate a [[SparkSession]] for a new connection by a proxy user.
   * @param sessionHandle a new connection
   * @param user the user who started this connection
   * @param queue the specified queue to start a yarn application
   * @return if this proxy user has connected before, just return a copy of existing sparkSession,
   *         otherwise generate a new [[SparkSession]] for this user. null if error occurred.
   */
  def getSessionOrCreate(
    sessionHandle: SessionHandle,
    user: String,
    queue: String): SparkSession = synchronized {
    assert(user != null, "proxy-user could not be null")
    assert(queue != null, "the yarn queue must be specified")
    
    if (isCtxStarted(user)) {
      val ss = userToSparkSession.get(user).newSession()
      hiveSessionToUser.put(sessionHandle, user)
      return ss
    } else {
      logInfo(s"Starting a new SparkContext in QUEUE: [$queue] for proxy-user [$user]")
      val conf = sparkConf.clone
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = conf.getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
      conf.set("spark.yarn.queue", queue)
      conf.setAppName(maybeAppName.getOrElse(s"SPARK-SQL::$user::$queue"))
      val proxyUser = SparkHadoopUtil.get.createProxyUser(user)
      try {
        proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            val sparkContext = new SparkContext(conf, Some(user))
            tryOrStopSparkContext(sparkContext) {
              val ss =
                SparkSession
                  .builder()
                  .enableHiveSupport()
                  .createWithContext(sparkContext)
              hiveSessionToUser.put(sessionHandle, user)
              userToSparkSession.put(user, ss)
            }
          }
        })
        return userToSparkSession.get(user)
      } catch {
        case e: Exception =>
          // Hadoop's AuthorizationException suppresses the exception's stack trace, which
          // makes the message printed to the output by the JVM not very helpful. Instead,
          // detect exceptions with empty stack traces here, and treat them differently.
          val hiveSQLException =
            new HiveSQLException("Failed to open new session cause by init sparkSession", e)
          if (e.getStackTrace.length == 0) {
            logWarning(s"ERROR: ${e.getClass.getName}: ${e.getMessage}")
            throw hiveSQLException
          } else {
            throw hiveSQLException
          }
      }
    }
    
    null // never reached
  }

  /**
   * Reset the proxy-user state, after the proxy-user closed one session
   * @param sessionHandle stand for a link from a normal user
   */
  def closeSession(sessionHandle: SessionHandle): Unit = {
    val user = hiveSessionToUser.remove(sessionHandle)
    
    if (user == null) {
      logWarning(s"Session $sessionHandle has been closed already and has a connected " +
        s"user no more.")
      return
    }
    
    logInfo(s"Session $sessionHandle Closing, clear the connectivity with proxy-user [$user]")
    
    if (cleanSessionOnClose && !hiveSessionToUser.containsValue(user)) {
      var ss = userToSparkSession.remove(user)
      if (ss != null && !ss.sparkContext.isStopped) {
        ss.stop()
      } else {
        ss = null
        logWarning("The sparkContext has been stopped")
      }
    }
  }
  
  /**
   * Execute a block of code that evaluates to Unit, stop SparkContext if there is any uncaught
   * exception
   *
   * NOTE: This method is to be called by the driver-side components to avoid stopping the
   * user-started JVM process completely
   */
  private def tryOrStopSparkContext(sc: SparkContext)(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable =>
        val currentThreadName = Thread.currentThread().getName
        if (sc != null) {
          logError(s"uncaught error in thread $currentThreadName, stopping SparkContext", t)
          sc.stopInNewThread()
        }
        if (!NonFatal(t)) {
          logError(s"throw uncaught fatal error in thread $currentThreadName", t)
          throw t
        }
    }
  }
  
  /**
   * Clean the sparkContext which is no hive session interacted with. This will periodically
   * called by the [[SparkSessionCleaner]]
   */
  private def cleanUnusedSparkSession(): Unit = {
    userToSparkSession.asScala.foreach { item =>
      val user = item._1
      val session = item._2
      if (!hiveSessionToUser.containsValue(user)) {
      
        if (session != null && !session.sparkContext.isStopped) {
          // try to stop the none used de sc if it is active
          try {
            session.stop()
            userToSparkSession.remove(user)
          } catch {
            case e: Exception =>
              logWarning(
                s"""
                   |SparkContext belong to proxy-user [$user] doesn't stop properly,
                   |because ${e.getMessage} and will try again in $sessionCleanInterval
                   |minutes later.
                    """.stripMargin, e)
          }
        } else {
          userToSparkSession.remove(user)
        }
      }
    }
  }
  
  /**
   * A cleaner thread for stopping unused sparkContext
   */
  private class SparkSessionCleaner {
  
    private val sessionCleaner =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("SparkSession Clean Thread")
  
  
    private[hive] def scheduleCleanSession(): Unit = {
    
      // This thread periodically runs on the HiveThriftServer2 to clean unused SparkContext.
      val sessionCleanerRunnable = new Runnable {
        override def run(): Unit = {
          // close sc never used and expired
          cleanUnusedSparkSession()
        }
      }
    
      sessionCleaner.scheduleAtFixedRate(
        sessionCleanerRunnable, sessionCleanInterval, sessionCleanInterval, TimeUnit.MINUTES)
    }
  }
}
