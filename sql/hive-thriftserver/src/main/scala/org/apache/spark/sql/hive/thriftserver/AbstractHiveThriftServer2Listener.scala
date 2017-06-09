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

import org.apache.spark.scheduler.SparkListener

/**
  * As for HiveThriftServer2, listener should not only extends SparkListener, but also
  * extends some session/statement methods, so define `AbstractHiveThriftServer2Listener`
  * abstract class to use it easily.
  *
  */
abstract class AbstractHiveThriftServer2Listener extends SparkListener{
  /**
    * Called when statement started.
    * @param id
    * @param sessionId
    * @param statement
    * @param groupId
    * @param userName
    */
  def onStatementStart(
                        id: String,
                        sessionId: String,
                        statement: String,
                        groupId: String,
                        userName: String = "UNKNOWN"): Unit
  
  /**
    * Called when statement parsed.
    * @param id
    * @param executionPlan
    */
  def onStatementParsed(id: String, executionPlan: String): Unit
  
  /**
    * Called when statement errored.
    * @param id
    * @param errorMessage
    * @param errorTrace
    */
  def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit
  
  /**
    * Called when statement finished.
    * @param id
    */
  def onStatementFinish(id: String): Unit
  
  /**
    * Called when session created.
    * @param ip
    * @param sessionId
    * @param userName
    * @param proxyUser
    * @param rangerUser
    */
  def onSessionCreated(ip: String, sessionId: String,
                       userName: String, proxyUser: String, rangerUser: String)
  
  /**
    * Called when session closed.
    * @param sessionId
    */
  def onSessionClosed(sessionId: String): Unit
}
