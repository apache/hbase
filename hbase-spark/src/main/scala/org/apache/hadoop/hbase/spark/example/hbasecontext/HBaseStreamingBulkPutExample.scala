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

package org.apache.hadoop.hbase.spark.example.hbasecontext

import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf

/**
 * This is a simple example of BulkPut with Spark Streaming
 */
object HBaseStreamingBulkPutExample {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("HBaseStreamingBulkPutExample " +
        "{host} {port} {tableName} {columnFamily}")
      return
    }

    val host = args(0)
    val port = args(1)
    val tableName = args(2)
    val columnFamily = args(3)

    val sparkConf = new SparkConf().setAppName("HBaseBulkPutTimestampExample " +
      tableName + " " + columnFamily)
    val sc = new SparkContext(sparkConf)
    try {
      val ssc = new StreamingContext(sc, Seconds(1))

      val lines = ssc.socketTextStream(host, port.toInt)

      val conf = HBaseConfiguration.create()

      val hbaseContext = new HBaseContext(sc, conf)

      hbaseContext.streamBulkPut[String](lines,
        TableName.valueOf(tableName),
        (putRecord) => {
          if (putRecord.length() > 0) {
            val put = new Put(Bytes.toBytes(putRecord))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("foo"), Bytes.toBytes("bar"))
            put
          } else {
            null
          }
        })
      ssc.start()
      ssc.awaitTerminationOrTimeout(60000)
    } finally {
      sc.stop()
    }
  }
}