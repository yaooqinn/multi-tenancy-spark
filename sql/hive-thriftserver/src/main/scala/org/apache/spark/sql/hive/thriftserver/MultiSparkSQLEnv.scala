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

import java.io.{File, IOException}
import java.security.PrivilegedExceptionAction
import java.util
import javax.security.auth.login.LoginException

import scala.collection.mutable.LinkedHashMap
import scala.collection.JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.{SPARK_VERSION, SparkConf, SparkContext, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.config._

private[hive] object MultiSparkSQLEnv extends Logging{

  logInfo("Initializing multiple Spark SQL Environments...")

  val version = SPARK_VERSION
  val originConf = new SparkConf(loadDefaults = true)
  val globalUgi: UserGroupInformation = UserGroupInformation.getCurrentUser

  val users = originConf.get(PROXY_USERS).distinct.filter(_ != globalUgi.getShortUserName)

  val userToSession = new LinkedHashMap[String, SparkSession]()

  val userToQueue = new util.HashMap[String, String]()

  def init(): Unit = {

    require(users.nonEmpty, s"No user is configured in ${PROXY_USERS.key}, please specify the " +
      s"users who can impersonate the Real User [${globalUgi.getUserName}]")

    initQueues(users, originConf)

    users.foreach { user =>
      logInfo(s"Starting SparkContext for $user")
      val proxyUser = SparkHadoopUtil.get.createProxyUser(user, globalUgi)

      val userConf = originConf.clone
      userConf.set("spark.ui.port", "0")
      userConf.set("spark.yarn.queue", userToQueue.get(user))
      userConf.set("spark.app.name", userConf.get("spark.app.name") + " to " + user)

      try {
        proxyUser.doAs(new PrivilegedExceptionAction[Unit]() {
          override def run(): Unit = {
            val sparkContext = new SparkContext(userConf, Some(user))
            tryOrStopSparkContext(sparkContext) {
              val ss =
                SparkSession
                  .builder()
                  .enableHiveSupport()
                  .createWithContext(sparkContext)
              ss.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)
              userToSession.put(user, ss)
            }
          }
        })
      } catch {
        case e: Exception =>
          // Hadoop's AuthorizationException suppresses the exception's stack trace, which
          // makes the message printed to the output by the JVM not very helpful. Instead,
          // detect exceptions with empty stack traces here, and treat them differently.
          val exception =
            new SparkException("Failed to open new session cause by init sparkSession", e)
          if (e.getStackTrace.length == 0) {
            logWarning(s"ERROR: ${e.getClass.getName}: ${e.getMessage}")
            throw exception
          } else {
            throw exception
          }
      }
    }
  }

  /** Cleans up and shuts down all the Spark SQL environments. */
  def stop() {
    userToSession.foreach { case ((user, sparkSession)) =>
      if (sparkSession != null && !sparkSession.sparkContext.isStopped) {
        logInfo(s"Shutting down Spark SQL Environment for user: [$user].")
        // Stop the SparkContext
        sparkSession.stop()
      }
    }
  }

  private def loginUserFromKeytab(conf: SparkConf): UserGroupInformation = {
    if (conf.contains("spark.yarn.principal") && conf.contains("spark.yarn.keytab")) {
      val principal = conf.get("spark.yarn.principal")
      val keytab = conf.get("spark.yarn.keytab")
      if (!new File(keytab).exists()) {
        throw new SparkException(s"Keytab file: $keytab" +
          " specified in spark.yarn.keytab does not exist")
      } else {
        logInfo("Attempting to login to Kerberos" +
          s" using principal: $principal and keytab: $keytab")
        try {
          UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab)
        } catch {
          case e @ (_: IOException | _: LoginException) =>
            throw new SparkException("Unable to login to kerberos with given principal/keytab", e)
        }
      }
    } else {
      throw new SparkException("spark.yarn.keytab/spark.yarn.principal must be specified " +
        "for access secured YARN Cluster")
    }
  }

  private def verifyProxyConfigs(conf: SparkConf): Unit = {
    if (conf.get(PROXY_USERS).nonEmpty) {
      assert(conf.get("spark.yarn.principal").nonEmpty, "PRINCIPAL MISSING")
      assert(conf.get("spark.yarn.keytab").nonEmpty, "KEYTAB MISSING")
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

  private def initQueues(users: Seq[String], conf: SparkConf): Unit = {
    logInfo(s"Getting queue for users: ${users.toString}")
    val defaultQueue = conf.get("spark.yarn.queue", "default")
    users.foreach { user =>
      val qKey = s"spark.sql.queue.$user"
      val queue = conf.get(qKey, defaultQueue)

      if (queue == defaultQueue) {
        logWarning(s"Using the default queue configured in `spark.yarn.queue` for $user, " +
          s"please verify whether $qKey be set properly.")
      } else {
        logInfo(s"Queue for user [$user] is [$queue]")
      }
      userToQueue.put(user, queue)
    }
  }

}
