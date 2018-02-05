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

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashMap
import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.common.`type`.HiveDecimal
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.serde2.io.{DateWritable, TimestampWritable}
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, WAREHOUSE_PATH}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


private[spark] object HiveUtils extends Logging {

  def withHiveExternalCatalog(sc: SparkContext): SparkContext = {
    sc.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
    sc
  }

  /** The version of hive used internally by Spark SQL. */
  val hiveExecutionVersion: String = "1.2.1"

  /**
   * The property key that is used to store the raw hive type string in the metadata of StructField.
   * For example, in the case where the Hive type is varchar, the type gets mapped to a string type
   * in Spark SQL, but we need to preserve the original type in order to invoke the correct object
   * inspector in Hive.
   */
  val hiveTypeString: String = "HIVE_TYPE_STRING"

  val CONVERT_METASTORE_PARQUET = SQLConfigBuilder("spark.sql.hive.convertMetastoreParquet")
    .doc("When set to false, Spark SQL will use the Hive SerDe for parquet tables instead of " +
      "the built in support.")
    .booleanConf
    .createWithDefault(true)

  val CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING =
    SQLConfigBuilder("spark.sql.hive.convertMetastoreParquet.mergeSchema")
      .doc("When true, also tries to merge possibly different but compatible Parquet schemas in " +
        "different Parquet data files. This configuration is only effective " +
        "when \"spark.sql.hive.convertMetastoreParquet\" is true.")
      .booleanConf
      .createWithDefault(false)

  val CONVERT_METASTORE_ORC = SQLConfigBuilder("spark.sql.hive.convertMetastoreOrc")
    .internal()
    .doc("When set to false, Spark SQL will use the Hive SerDe for ORC tables instead of " +
      "the built in support.")
    .booleanConf
    .createWithDefault(false)

  val HIVE_THRIFT_SERVER_ASYNC = SQLConfigBuilder("spark.sql.hive.thriftServer.async")
    .doc("When set to true, Hive Thrift server executes SQL queries in an asynchronous way.")
    .booleanConf
    .createWithDefault(true)

  /**
   * Configurations needed to create a [[HiveClient]].
   */
  private[hive] def hiveClientConfigurations(hadoopConf: Configuration): Map[String, String] = {
    // Hive 0.14.0 introduces timeout operations in HiveConf, and changes default values of a bunch
    // of time `ConfVar`s by adding time suffixes (`s`, `ms`, and `d` etc.).  This breaks backwards-
    // compatibility when users are trying to connecting to a Hive metastore of lower version,
    // because these options are expected to be integral values in lower versions of Hive.
    //
    // Here we enumerate all time `ConfVar`s and convert their values to numeric strings according
    // to their output time units.
    Seq(
      ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.METASTORE_CLIENT_SOCKET_LIFETIME -> TimeUnit.SECONDS,
      ConfVars.HMSHANDLERINTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_EVENT_DB_LISTENER_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_CLEAN_FREQ -> TimeUnit.SECONDS,
      ConfVars.METASTORE_EVENT_EXPIRY_DURATION -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_TTL -> TimeUnit.SECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.METASTORE_AGGREGATE_STATS_CACHE_MAX_READER_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVES_AUTO_PROGRESS_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOG_INCREMENTAL_PLAN_PROGRESS_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_STATS_JDBC_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_STATS_RETRIES_WAIT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES -> TimeUnit.SECONDS,
      ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_TXN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_WORKER_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CHECK_INTERVAL -> TimeUnit.SECONDS,
      ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_MAX_IDLE_TIME -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_ASYNC_EXEC_KEEPALIVE_TIME -> TimeUnit.SECONDS,
      ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_SESSION_CHECK_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_SESSION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.HIVE_SERVER2_IDLE_OPERATION_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SERVER_READ_SOCKET_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.HIVE_LOCALIZE_RESOURCE_WAIT_INTERVAL -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_CLIENT_FUTURE_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_JOB_MONITOR_TIMEOUT -> TimeUnit.SECONDS,
      ConfVars.SPARK_RPC_CLIENT_CONNECT_TIMEOUT -> TimeUnit.MILLISECONDS,
      ConfVars.SPARK_RPC_CLIENT_HANDSHAKE_TIMEOUT -> TimeUnit.MILLISECONDS
    ).map { case (confVar, unit) =>
      confVar.varname -> HiveConf.getTimeVar(hadoopConf, confVar, unit).toString
    }.toMap
  }

  /**
   * Create a [[HiveClient]] used to retrieve metadata from the Hive MetaStore.
   *
   * The version of the Hive client that is used here must match the metastore that is configured
   * in the hive-site.xml file.
   */
  protected[hive] def newClientForMetadata(
      conf: SparkConf,
      hadoopConf: Configuration,
      user: String = null): HiveClient = {
    val configurations = hiveClientConfigurations(hadoopConf)
    new HiveClientImpl(conf, hadoopConf, configurations, user)
  }

  /** Constructs a configuration for hive, where the metastore is located in a temp directory. */
  def newTemporaryConfiguration(useInMemoryDerby: Boolean): Map[String, String] = {
    val withInMemoryMode = if (useInMemoryDerby) "memory:" else ""

    val tempDir = Utils.createTempDir()
    val localMetastore = new File(tempDir, "metastore")
    val propMap: HashMap[String, String] = HashMap()
    // We have to mask all properties in hive-site.xml that relates to metastore data source
    // as we used a local metastore here.
    HiveConf.ConfVars.values().foreach { confvar =>
      if (confvar.varname.contains("datanucleus") || confvar.varname.contains("jdo")
        || confvar.varname.contains("hive.metastore.rawstore.impl")) {
        propMap.put(confvar.varname, confvar.getDefaultExpr)
      }
    }
    propMap.put(WAREHOUSE_PATH.key, localMetastore.toURI.toString)
    propMap.put(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname,
      s"jdbc:derby:$withInMemoryMode;databaseName=${localMetastore.getAbsolutePath};create=true")
    propMap.put("datanucleus.rdbms.datastoreAdapterClassName",
      "org.datanucleus.store.rdbms.adapter.DerbyAdapter")

    // SPARK-11783: When "hive.metastore.uris" is set, the metastore connection mode will be
    // remote (https://cwiki.apache.org/confluence/display/Hive/AdminManual+MetastoreAdmin
    // mentions that "If hive.metastore.uris is empty local mode is assumed, remote otherwise").
    // Remote means that the metastore server is running in its own process.
    // When the mode is remote, configurations like "javax.jdo.option.ConnectionURL" will not be
    // used (because they are used by remote metastore server that talks to the database).
    // Because execution Hive should always connects to an embedded derby metastore.
    // We have to remove the value of hive.metastore.uris. So, the execution Hive client connects
    // to the actual embedded derby metastore instead of the remote metastore.
    // You can search HiveConf.ConfVars.METASTOREURIS in the code of HiveConf (in Hive's repo).
    // Then, you will find that the local metastore mode is only set to true when
    // hive.metastore.uris is not set.
    propMap.put(ConfVars.METASTOREURIS.varname, "")

    // The execution client will generate garbage events, therefore the listeners that are generated
    // for the execution clients are useless. In order to not output garbage, we don't generate
    // these listeners.
    propMap.put(ConfVars.METASTORE_PRE_EVENT_LISTENERS.varname, "")
    propMap.put(ConfVars.METASTORE_EVENT_LISTENERS.varname, "")
    propMap.put(ConfVars.METASTORE_END_FUNCTION_LISTENERS.varname, "")

    // Copy any "spark.hadoop.foo=bar" system properties into conf as "foo=bar"
    sys.props.foreach { case (key, value) =>
      if (key.startsWith("spark.hadoop.")) {
        propMap.put(key.substring("spark.hadoop.".length), value)
      }
    }
    propMap.toMap
  }

  protected val primitiveTypes =
    Seq(StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, ByteType,
      ShortType, DateType, TimestampType, BinaryType)

  protected[sql] def toHiveString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "NULL"
    case (d: Int, DateType) => new DateWritable(d).toString
    case (t: Timestamp, TimestampType) => new TimestampWritable(t).toString
    case (bin: Array[Byte], BinaryType) => new String(bin, StandardCharsets.UTF_8)
    case (decimal: java.math.BigDecimal, DecimalType()) =>
      // Hive strips trailing zeros so use its toString
      HiveDecimal.create(decimal).toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }

  /** Hive outputs fields of structs slightly differently than top level attributes. */
  protected def toHiveStructString(a: (Any, DataType)): String = a match {
    case (struct: Row, StructType(fields)) =>
      struct.toSeq.zip(fields).map {
        case (v, t) => s""""${t.name}":${toHiveStructString(v, t.dataType)}"""
      }.mkString("{", ",", "}")
    case (seq: Seq[_], ArrayType(typ, _)) =>
      seq.map(v => (v, typ)).map(toHiveStructString).mkString("[", ",", "]")
    case (map: Map[_, _], MapType(kType, vType, _)) =>
      map.map {
        case (key, value) =>
          toHiveStructString((key, kType)) + ":" + toHiveStructString((value, vType))
      }.toSeq.sorted.mkString("{", ",", "}")
    case (null, _) => "null"
    case (s: String, StringType) => "\"" + s + "\""
    case (decimal, DecimalType()) => decimal.toString
    case (other, tpe) if primitiveTypes contains tpe => other.toString
  }
}
