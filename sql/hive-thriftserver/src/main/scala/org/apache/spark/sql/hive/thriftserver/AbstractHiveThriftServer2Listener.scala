
package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.scheduler.SparkListener

/**
  * Created by lishuming on 2017/5/23.
  */
abstract class AbstractHiveThriftServer2Listener extends SparkListener{
  def onStatementStart(
                        id: String,
                        sessionId: String,
                        statement: String,
                        groupId: String,
                        userName: String = "UNKNOWN"): Unit
  
  def onStatementParsed(id: String, executionPlan: String): Unit
  
  def onStatementError(id: String, errorMessage: String, errorTrace: String): Unit
  
  def onStatementFinish(id: String): Unit
  
  def onSessionCreated(ip: String, sessionId: String,
                       userName: String = "UNKNOWN", proxyUser: String, rangerUser: String)
  
  def onSessionClosed(sessionId: String): Unit
}
