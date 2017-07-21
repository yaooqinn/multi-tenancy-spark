<?xml version="1.0" encoding="UTF-8" standalone="no"?>
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

<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration xmlns:xi="http://www.w3.org/2001/XInclude">

    <!--  Properties whose name begin with "xasecure.audit.jpa." are used to configure JPA -->
    <property>
        <name>xasecure.audit.jpa.javax.persistence.jdbc.url</name>
        <value></value>
        <!-- jdbc:mysql://10.172.121.106/mengma -->
    </property>

    <property>
        <name>xasecure.audit.jpa.javax.persistence.jdbc.user</name>
        <value>user</value>
    </property>

    <property>
        <name>xasecure.audit.jpa.javax.persistence.jdbc.password</name>
        <value>password</value>
    </property>

    <property>
        <name>xasecure.audit.jpa.javax.persistence.jdbc.driver</name>
        <value>com.mysql.jdbc.Driver</value>
    </property>

    <!--property>
		<name>xasecure.audit.credential.provider.file</name>
		<value>jceks://file/etc/ranger/hive-cluster1/cred.jceks</value>
	</property-->

    <property>
        <name>xasecure.audit.is.enabled</name>
        <value>true</value>
    </property>

</configuration>
