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

import java.util.concurrent.ConcurrentHashMap

import org.apache.hive.service.cli.SessionHandle

import org.apache.spark.SparkContext
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

  private val userToSparkSession = new ConcurrentHashMap[String, SparkSession]
  private val sessionToUser = new ConcurrentHashMap[SessionHandle, String]
  private val userToNum = new ConcurrentHashMap[String, Int]

  private def isContextStarted(user: String): Boolean = {
    val sc = userToSparkSession.get(user)
    sc != null && sc.sparkContext != null && !sc.sparkContext.isStopped
  }

  /**
   * Generate a [[SparkSession]] for a new connection by a proxy user.
   * @param sessionHandle a new connection
   * @param user the user who started this connection
   * @param queue the specified queue to start yarn app
   * @return if this proxy user has connected before, just return a copy of existing sparkSession,
   *         otherwise generate a new [[SparkSession]] for this user.
   */
  def getSessionOrCreate(
    sessionHandle: SessionHandle,
    user: String,
    queue: String = "default"): SparkSession = {
    if (isContextStarted(user)) {
      val ss = userToSparkSession.get(user).newSession()
      sessionToUser.put(sessionHandle, user)
      userToNum.put(user, userToNum.get(user) + 1)
      ss
    } else {
      logInfo(s"Starting a new SparkContext in QUEUE: [$queue] for proxy-user $user")
      val conf = SparkSQLEnv.conf
      // If user doesn't specify the appName, we want to get [SparkSQL::localHostName] instead of
      // the default appName [SparkSQLCLIDriver] in cli or beeline.
      val maybeAppName = conf.getOption("spark.app.name")
        .filterNot(_ == classOf[SparkSQLCLIDriver].getName)
      conf.set("spark.yarn.queue", queue)
      conf.set("spark.driver.allowMultipleContexts", "true")
      conf.setAppName(maybeAppName.getOrElse(s"SPARK-SQL::$user::$queue"))
      val sparkContext = new SparkContext(conf, Some(user))
      sessionToUser.put(sessionHandle, user)
      val ss =
        SparkSession
          .builder()
          .enableHiveSupport()
          .createWithContext(sparkContext)

      userToNum.put(user, 1)
      sessionToUser.put(sessionHandle, user)
      userToSparkSession.put(user, ss)
      ss
    }
  }

  /**
   * Reset the proxy user state, after the proxy user closed one session
   * @param sessionHandle
   */
  def closeSession(sessionHandle: SessionHandle): Unit = {
    val user = sessionToUser.remove(sessionHandle)
    logInfo(s"Session ${sessionHandle} Closing, clear the connectivity with proxy user [$user]")
    if (user ne null) {
      logInfo(s"This message is used for debug, " +
        s"$sessionHandle is not connected [$user] any more..")
    } else {
      val oldNum = userToNum.get(user)
      if (oldNum == null) {
        logInfo(s"This message is used for debug, " +
          s"$user has already been removed")
      } else {
        val num = userToNum.put(user, oldNum - 1)
        if (num <= 0) {
          logInfo(s"There are no more active connection under proxy user [$user] closing the sc")
          userToNum.remove(user)
          val ss = userToSparkSession.remove(user)
          if (ss ne null) {
            ss.stop()
          } else {
            logInfo("The sc has been removed before")
          }
        }
      }
    }
  }

}
