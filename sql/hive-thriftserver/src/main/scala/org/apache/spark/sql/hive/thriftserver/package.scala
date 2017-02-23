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

package org.apache.spark.sql.hive

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder


package object config {
  
  private[hive] val CLEAN_SESSION_ON_CLOSE = ConfigBuilder("spark.sql.cleanSessionOnClose")
    .doc("Whether to check for cleaning sparkSession after user's session ended every time")
    .booleanConf
    .createWithDefault(false)
  
  private[hive] val SESSION_CLEAN_INTERVAL =
    ConfigBuilder("spark.sql.session.clean.interval")
    .doc(s"if the parameter of spark.sql.cleanSessionOnClose is false, we will use periodic" +
      s"sparkSession cleaner to clean the unused sparkContexts, and use this value as interval")
    .timeConf(TimeUnit.MINUTES)
    .createWithDefault(300L)
  
}
