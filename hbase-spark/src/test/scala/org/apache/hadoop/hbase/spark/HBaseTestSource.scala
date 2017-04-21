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

package org.apache.hadoop.hbase.spark

import org.apache.hadoop.hbase.spark.datasources.HBaseSparkConf
import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class HBaseTestSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    DummyScan(
      parameters("cacheSize").toInt,
      parameters("batchNum").toInt,
      parameters("blockCacheingEnable").toBoolean,
      parameters("rowNum").toInt)(sqlContext)
  }
}

case class DummyScan(
     cacheSize: Int,
     batchNum: Int,
     blockCachingEnable: Boolean,
     rowNum: Int)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {
  private def sparkConf = SparkEnv.get.conf
  override def schema: StructType =
    StructType(StructField("i", IntegerType, nullable = false) :: Nil)

  override def buildScan(): RDD[Row] = sqlContext.sparkContext.parallelize(0 until rowNum)
    .map(Row(_))
    .map{ x =>
      if (sparkConf.getInt(HBaseSparkConf.QUERY_BATCHSIZE,
          -1) != batchNum ||
        sparkConf.getInt(HBaseSparkConf.QUERY_CACHEDROWS,
          -1) != cacheSize ||
        sparkConf.getBoolean(HBaseSparkConf.QUERY_CACHEBLOCKS,
          false) != blockCachingEnable) {
        throw new Exception("HBase Spark configuration cannot be set properly")
      }
      x
  }
}
