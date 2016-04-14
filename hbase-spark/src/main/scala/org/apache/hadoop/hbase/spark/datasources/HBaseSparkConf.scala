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

object HBaseSparkConf{
  // This is the hbase configuration. User can either set them in SparkConf, which
  // will take effect globally, or configure it per table, which will overwrite the value
  // set in SparkConf. If not setted, the default value will take effect.
  val BLOCK_CACHE_ENABLE = "spark.hbase.blockcache.enable"
  // default block cache is set to true by default following hbase convention, but note that
  // this potentially may slow down the system
  val defaultBlockCacheEnable = true
  val CACHE_SIZE = "spark.hbase.cacheSize"
  val defaultCachingSize = 1000
  val BATCH_NUM = "spark.hbase.batchNum"
  val defaultBatchNum = 1000
  val BULKGET_SIZE = "spark.hbase.bulkGetSize"
  val defaultBulkGetSize = 1000

  val HBASE_CONFIG_RESOURCES_LOCATIONS = "hbase.config.resources"
  val USE_HBASE_CONTEXT = "hbase.use.hbase.context"
  val PUSH_DOWN_COLUMN_FILTER = "hbase.pushdown.column.filter"
  val defaultPushDownColumnFilter = true

  val TIMESTAMP = "hbase.spark.query.timestamp"
  val MIN_TIMESTAMP = "hbase.spark.query.minTimestamp"
  val MAX_TIMESTAMP = "hbase.spark.query.maxTimestamp"
  val MAX_VERSIONS = "hbase.spark.query.maxVersions"
}
