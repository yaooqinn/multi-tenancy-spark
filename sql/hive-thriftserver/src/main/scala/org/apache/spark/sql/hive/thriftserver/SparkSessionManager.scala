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
import java.util.concurrent.ConcurrentHashMap

import scala.util.control.{ControlThrowable, NonFatal}

import org.apache.hive.service.cli.SessionHandle

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

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
  // proxy user, we put a <sessionHandle, proxy-user> to it. When close, remove this kv pair.
  private val hiveSessionToUser: ConcurrentHashMap[SessionHandle, String] = new ConcurrentHashMap
  
  private val sparkConf: SparkConf = SparkSQLEnv.originalConf.clone
  
  // Decide whether close user-specified spark environment, after there is no connections to it.
  private val cleanSessionOnClose: Boolean =
    sparkConf.getBoolean("spark.sql.cleanSessionOnClose", false)
  
  /**
   * Check whether the user specified spark environment has been launched.
   * @param user the proxy-user who launch the entire spark environment.
   * @return true if sparkSession is available, otherwise false.
   */
  private def isCtxStarted(user: String): Boolean = {
    assert(user != null, "proxy-user could not be null")
    
    var ss = userToSparkSession.get(user)
    if (ss != null && ss.sparkContext != null && !ss.sparkContext.stopped.get) {
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
      userToSparkSession.remove(user) = null
      ss = null
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
    queue: String = "default"): SparkSession = {
    assert(user != null, "proxy-user could not be null")
    
    if (isCtxStarted(user)) {
      val ss = userToSparkSession.get(user).newSession()
      hiveSessionToUser.put(sessionHandle, user)
      ss
    } else {
      logInfo(s"Starting a new SparkContext in QUEUE: [$queue] for proxy-user [$user]")
      val conf = sparkConf.clone
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = conf.getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
      conf.set("spark.yarn.queue", queue)
      conf.set("spark.driver.allowMultipleContexts", "true")
      conf.setAppName(maybeAppName.getOrElse(s"SPARK-SQL::$user::$queue"))
      conf.set("spark.yarn.proxy.enabled", "true")
      val proxyUser = SparkHadoopUtil.get.createProxyUser(user)
      try {
        proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            val sparkContext = new SparkContext(conf, Some(user))
            tryOrStopSparkContext(sparkContext) {
              val ss =
                SparkSession.builder().enableHiveSupport().createWithContext(sparkContext)
  
              hiveSessionToUser.put(sessionHandle, user)
              userToSparkSession.put(user, ss)
            }
          }
        })
      } catch {
        case e: Exception =>
          // Hadoop's AuthorizationException suppresses the exception's stack trace, which
          // makes the message printed to the output by the JVM not very helpful. Instead,
          // detect exceptions with empty stack traces here, and treat them differently.
          if (e.getStackTrace().length == 0) {
            // scalastyle:off println
            logWarning(s"ERROR: ${e.getClass().getName()}: ${e.getMessage()}")
          } else {
            throw e
          }
      }
      userToSparkSession.get(user)
    }
  }

  /**
   * Reset the proxy-user state, after the proxy-user closed one session
   * @param sessionHandle stand for a link from a normal user
   */
  def closeSession(sessionHandle: SessionHandle): Unit = {
    val user = hiveSessionToUser.remove(sessionHandle)
    
    if (user eq null) {
      logWarning(s"Session $sessionHandle has been closed already and has a connected " +
        s"user no more.")
      return
    }
    
    logInfo(s"Session ${sessionHandle} Closing, clear the connectivity with proxy-user [$user]")
    
    if (cleanSessionOnClose && !hiveSessionToUser.containsValue(user)) {
      var ss = userToSparkSession.remove(user)
      if (ss ne null && !ss.sparkContext.isStopped) {
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
}
