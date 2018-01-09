<?xml version="1.0"?>
<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

<configuration>
    <!-- +------------------------------------------+ -->
    <!-- |-------- Hive MetaStore Database ---------| -->
    <!-- +------------------------------------------+ -->

    <property>
      <name>javax.jdo.option.ConnectionURL</name>
      <value>jdbcURL</value>
      <description>JDBC connection string for the data store which contains metadata. Such as: jdbc:mysql://10.172.121.127:3306/hive_cluster3?characterEncoding=utf-8&amp;useSSL=false</description>
    </property>

    <property>
      <name>javax.jdo.option.ConnectionDriverName</name>
      <value>com.mysql.jdbc.Driver</value>
      <description>JDBC Driver class name for the data store which contains metadata</description>
    </property>

    <property>
      <name>javax.jdo.option.ConnectionUserName</name>
      <value>connUser</value>
      <description>user name for connecting to MySQL server</description>
    </property>

    <property>
      <name>javax.jdo.option.ConnectionPassword</name>
      <value>connPassword</value>
      <description>password for connecting to mysql server</description>
    </property>

    <!-- +------------------------------------------+ -->
    <!-- |--------- Hive MetaStore Server ----------| -->
    <!-- +------------------------------------------+ -->

    <property>
      <name>hive.metastore.uris</name>
      <value>thrift://hadoop979.hz.163.org:9083,thrift://hadoop986.hz.163.org:9083</value>
      <description>Hive connects to one of these URIs to make metadata requests to a remote Metastore (comma separated list of URIs)</description>
    </property>

    <property>
      <name>hive.metastore.sasl.enabled</name>
      <value>true</value>
      <description>If true, the metastore thrift interface will be secured with SASL. Clients must authenticate with Kerberos.</description>
    </property>

    <property>
      <name>hive.metastore.kerberos.principal</name>
      <value>hive/_HOST@TEST.MAMMUT.NETEASE.COM</value>
      <description>The service principal for the metastore thrift server. The special string _HOST will be replaced automatically with the correct host name.</description>
    </property>

    <property>
      <name>hive.metastore.kerberos.keytab.file</name>
      <value>/home/hzyaoqin/hive.service.keytab</value>
      <description>The path to the Kerberos Keytab file containing the metastore thrift server's service principal.</description>
    </property>

    <!-- +------------------------------------------+ -->
    <!-- |---------- Hadoop Impersonation ----------| -->
    <!-- +------------------------------------------+ -->

    <property>
        <name>hive.server2.enable.doAs</name>
        <value>true</value>
    </property>

    <property>
        <name>hadoop.proxyuser.hive.hosts</name>
        <value>*</value>
        <description>
            The superuser can connect only from host1 and host2 to impersonate a user
        </description>
    </property>

    <property>
        <name>hadoop.proxyuser.hive.groups</name>
        <value>*</value>
    </property>

    <property>
        <name>hadoop.proxyuser.hadoop.hosts</name>
        <value>*</value>
        <description>
            The superuser can connect only from host1 and host2 to impersonate a user
        </description>
    </property>

    <property>
        <name>hadoop.proxyuser.hadoop.groups</name>
        <value>*</value>
    </property>

    <!-- +------------------------------------------+ -->
    <!-- |------------   HiveServer2    ------------| -->
    <!-- +------------------------------------------+ -->

    <property>
        <name>hive.server2.thrift.port</name>
        <value>10000</value>
    </property>

    <property>
        <name>hive.server2.authentication</name>
        <value>KERBEROS</value>
    </property>

    <property>
        <name>hive.server2.authentication.kerberos.keytab</name>
        <value></value>
    </property>

    <property>
        <name>hive.server2.authentication.kerberos.principal</name>
        <value></value>
    </property>

    <property>
        <name>hive.security.authorization.createtable.owner.grants</name>
        <value>ALL</value>
        <description>
            the privileges automatically granted to the owner whenever a table gets created.
            An example like "select,drop" will grant select and drop privilege to the owner of the table
        </description>
    </property>

    <property>
        <name>hive.server2.thrift.sasl.qop</name>
        <value>auth</value>
    </property>

    <property>
        <name>hive.exec.scratchdir</name>
        <value>/tmp/hive/public</value>
    </property>

    <!-- Ranger -->
    <property>
        <name>hive.security.authorization.enabled</name>
        <value>true</value>
    </property>
    <property>
        <name>hive.security.authorization.manager</name>
        <value>org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory</value>
    </property>
    <property>
        <name>hive.security.authenticator.manager</name>
        <value>org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator</value>
    </property>
    <property>
        <name>hive.conf.restricted.list</name>
        <value>
            hive.security.authorization.enabled,hive.security.authorization.manager,hive.security.authenticator.manager
        </value>
    </property>

    <!-- Partition Support-->
    <property>
        <name>hive.exec.dynamic.partition</name>
        <value>true</value>
    </property>

    <property>
        <name>hive.exec.dynamic.partition.mode</name>
        <value>nonstrict</value>
    </property>

    <property>
        <name>hive.exec.max.dynamic.partitions.pernode</name>
        <value>10000</value>
    </property>

    <property>
        <name>hive.exec.max.dynamic.partitions</name>
        <value>10000</value>
    </property>

    <property>
        <name>hive.error.on.empty.partition</name>
        <value>true</value>
    </property>

    <!--HA -->
    <property>
        <name>hive.server2.support.dynamic.service.discovery</name>
        <value>false</value>
    </property>

    <property>
        <name>hive.server2.zookeeper.namespace</name>
        <value>spark-thriftserver</value>
    </property>

    <property>
        <name>hive.zookeeper.quorum</name>
        <value></value>
    </property>

    <!--Others -->
    <property>
        <name>datanucleus.fixedDataStore</name>
        <value>false</value>
    </property>

    <property>
        <name>datanecleus.autoCreateSchema</name>
        <value>false</value>
    </property>

    <property>
        <name>fs.hdfs.impl.disable.cache</name>
        <value>true</value>
    </property>

</configuration>
