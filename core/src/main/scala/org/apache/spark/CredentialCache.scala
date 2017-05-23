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

package org.apache.spark

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.security.Credentials

/**
 * Created by Kent on 2017/5/18.
 * A cache mgr for setting/updating/getting credentials at driver side for different session users.
 */
private[spark] object CredentialCache {

  private val credentials = new ConcurrentHashMap[String, Credentials]()

  /**
   * Set the credentials or remove it if `cred` is null
   * @param user The SPARK_USER who talks to a session
   * @param cred Driver side or executor side environment
   */
  def set(user: String, cred: Credentials) {
    if (cred == null) {
      credentials.remove(user)
    } else {
      credentials.put(user, cred)
    }
  }

  /**
   * Returns the Credentials by a specified user
   * @param user The SPARK_USER who talks to a spark session
   * @return SparkEnv
   */
  def get(user: String): Credentials = {
    credentials.get(user)
  }


}
