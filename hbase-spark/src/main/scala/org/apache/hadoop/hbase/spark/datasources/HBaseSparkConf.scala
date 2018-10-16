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

package org.apache.hadoop.hbase.spark.datasources

import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is the hbase configuration. User can either set them in SparkConf, which
 * will take effect globally, or configure it per table, which will overwrite the value
 * set in SparkConf. If not set, the default value will take effect.
 */
@InterfaceAudience.Public
object HBaseSparkConf{
  /** Set to false to disable server-side caching of blocks for this scan,
   *  false by default, since full table scans generate too much BC churn.
   */
  val QUERY_CACHEBLOCKS = "hbase.spark.query.cacheblocks"
  val DEFAULT_QUERY_CACHEBLOCKS = false
  /** The number of rows for caching that will be passed to scan. */
  val QUERY_CACHEDROWS = "hbase.spark.query.cachedrows"
  /** Set the maximum number of values to return for each call to next() in scan. */
  val QUERY_BATCHSIZE = "hbase.spark.query.batchsize"
  /** The number of BulkGets send to HBase. */
  val BULKGET_SIZE = "hbase.spark.bulkget.size"
  val DEFAULT_BULKGET_SIZE = 1000
  /** Set to specify the location of hbase configuration file. */
  val HBASE_CONFIG_LOCATION = "hbase.spark.config.location"
  /** Set to specify whether create or use latest cached HBaseContext*/
  val USE_HBASECONTEXT = "hbase.spark.use.hbasecontext"
  val DEFAULT_USE_HBASECONTEXT = true
  /** Pushdown the filter to data source engine to increase the performance of queries. */
  val PUSHDOWN_COLUMN_FILTER = "hbase.spark.pushdown.columnfilter"
  val DEFAULT_PUSHDOWN_COLUMN_FILTER= true
  /** Class name of the encoder, which encode data types from Spark to HBase bytes. */
  val QUERY_ENCODER = "hbase.spark.query.encoder"
  val DEFAULT_QUERY_ENCODER = classOf[NaiveEncoder].getCanonicalName
  /** The timestamp used to filter columns with a specific timestamp. */
  val TIMESTAMP = "hbase.spark.query.timestamp"
  /** The starting timestamp used to filter columns with a specific range of versions. */
  val TIMERANGE_START = "hbase.spark.query.timerange.start"
  /** The ending timestamp used to filter columns with a specific range of versions. */
  val TIMERANGE_END =  "hbase.spark.query.timerange.end"
  /** The maximum number of version to return. */
  val MAX_VERSIONS = "hbase.spark.query.maxVersions"
  /** Delayed time to close hbase-spark connection when no reference to this connection, in milliseconds. */
  val DEFAULT_CONNECTION_CLOSE_DELAY = 10 * 60 * 1000
}
