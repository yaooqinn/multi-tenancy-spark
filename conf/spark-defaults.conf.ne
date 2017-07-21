#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Default system properties included when running spark-submit.
# This is useful for setting default environmental settings.

###############################################################################
##                          Check Everytime                                  ##
###############################################################################

## Thrift/JDBC Server Settings ##
spark.yarn.queue                                      spark_adhoc
spark.sql.proxy.users                                 da_music,music
spark.sql.queue.da_music                              spark_adhoc
spark.sql.queue.music                                 spark_adhoc
spark.sql.queue.bdms_qatest_lt                        bdms_qatest_lt

## Hive Settings ##
spark.hadoop.hive.server2.thrift.port                 10000
spark.hadoop.hive.server2.authentication              KERBEROS
spark.hadoop.hive.server2.enable.doAs                 true
spark.hadoop.hive.server2.thrift.min.worker.threads   20

## Security Settings ##
# spark.yarn.keytab                                     /home/hadoop/hive.keytab
# spark.yarn.principal                                  hive/app-20.photo.163.org@HADOOP.HZ.NETEASE.COM
# spark.yarn.access.namenodes                           hdfs://hz-cluster3,hdfs://hz-cluster4

# spark.local.dir                                       /mnt/dfs/0/data,/mnt/dfs/1/data,/mnt/dfs/2/data,/mnt/dfs/3/data,/mnt/dfs/4/data,/mnt/dfs/5/data,/mnt/dfs/6/data,/mnt/dfs/7/data,/mnt/dfs/8/data,/mnt/dfs/9/data,/mnt/dfs/10/data,/mnt/dfs/11/data

## SQL Configurations ##
spark.sql.autoBroadcastJoinThreshold                  104857600
spark.sql.warehouse.dir                               /user/spark/warehouse
# spark.sql.hive.convertCTAS                            true
# spark.sql.sources.default                             parquet
spark.sql.shuffle.partitions                          600

###############################################################################
##                          Change If Require                                ##
###############################################################################

## Basic Settings For Spark ##
spark.master                                          yarn
spark.submit.deployMode                               client
spark.serializer                                      org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max                       256m

## Hadoop Settings ##
spark.hadoop.fs.hdfs.impl.disable.cache               true
spark.hadoop.fs.file.impl.disable.cache               true
spark.hadoop.hadoop.proxyuser.hive.hosts              *
spark.hadoop.hadoop.proxyuser.hive.groups             *

## Driver/AM Settings ##
spark.yarn.am.waitTime                                100s
spark.yarn.am.cores                                   4
spark.yarn.am.memory                                  10g
spark.yarn.am.memoryOverhead                          2048
spark.yarn.am.extraJavaOptions                        -XX:PermSize=1024m -XX:MaxPermSize=2048m -XX:MaxDirectMemorySize=512m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution
spark.driver.maxResultSize                            2g

## Executor Settings ##
spark.executor.instances                              0
spark.executor.cores                                  4
spark.executor.memory                                 20g
spark.yarn.executor.memoryOverhead                    4096
spark.executor.extraJavaOptions                       -XX:PermSize=1024m -XX:MaxPermSize=1024m -XX:MaxDirectMemorySize=512m -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution

## Dynamic Allocation Settings ##
spark.shuffle.service.enabled                         true
spark.dynamicAllocation.enabled                       true
spark.dynamicAllocation.initialExecutors              0
spark.dynamicAllocation.minExecutors                  0
spark.dynamicAllocation.maxExecutors                  30
spark.dynamicAllocation.executorIdleTimeout           60s

## Network Settings ##
spark.network.timeout                                 240s

## History Server Settings ##
# requied
# spark.history.fs.logDirectory
# spark.eventLog.enabled                                true
# if sercured
# spark.history.kerberos.enabled                        true
# spark.history.kerberos.principal
# spark.history.kerberos.keytab
# spark.history.ui.acls.enable                          true
# spark.ui.view.acls
# optional
# spark.history.fs.cleaner.enabled                      true
# spark.history.fs.cleaner.interval                     1d
# spark.history.fs.cleaner.maxAge                       14d
# spark.history.fs.numReplayThreads                     4

## History Server Client Settings ##
# spark.eventLog.enabled
# spark.eventLog.compress
# spark.eventLog.dir
# spark.yarn.historyServer.address