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

import java.util.{List => JList}

import scala.collection.JavaConverters._

import org.apache.commons.logging.Log
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.{AbstractService, Service}
import org.apache.hive.service.Service.STATE
import org.apache.hive.service.cli._
import org.apache.hive.service.server.HiveServer2

import org.apache.spark.sql.hive.thriftserver.ReflectionUtils._

private[hive] class SparkSQLCLIService(
    hiveServer: HiveServer2)
  extends CLIService(hiveServer)
  with ReflectedCompositeService {

  override def init(hiveConf: HiveConf) {
    this.hiveConf = hiveConf
    this.sessionManager = new SparkSQLSessionManager(hiveServer)
    addService(sessionManager)
    SparkSQLCLIService.this.serviceUGI = MultiSparkSQLEnv.globalUgi
    initCompositeService(hiveConf)
  }

  override def getInfo(sessionHandle: SessionHandle, getInfoType: GetInfoType): GetInfoValue = {
    getInfoType match {
      case GetInfoType.CLI_SERVER_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_NAME => new GetInfoValue("Spark SQL")
      case GetInfoType.CLI_DBMS_VER => new GetInfoValue(MultiSparkSQLEnv.version)
      case _ => super.getInfo(sessionHandle, getInfoType)
    }
  }
}

private[thriftserver] trait ReflectedCompositeService { this: AbstractService =>
  def initCompositeService(hiveConf: HiveConf) {
    // Emulating `CompositeService.init(hiveConf)`
    val serviceList = getAncestorField[JList[Service]](this, 2, "serviceList")
    serviceList.asScala.foreach(_.init(hiveConf))

    // Emulating `AbstractService.init(hiveConf)`
    invoke(classOf[AbstractService], this, "ensureCurrentState", classOf[STATE] -> STATE.NOTINITED)
    setAncestorField(this, 3, "hiveConf", hiveConf)
    invoke(classOf[AbstractService], this, "changeState", classOf[STATE] -> STATE.INITED)
    getAncestorField[Log](this, 3, "LOG").info(s"Service: $getName is inited.")
  }
}
