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
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.SparkConf
import org.apache.yetus.audience.InterfaceAudience

/**
 * This is a simple example of putting records in HBase
 * with the bulkPut function.  In this example we are
 * also setting the timestamp in the put
 */
@InterfaceAudience.Private
object HBaseBulkPutTimestampExample {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.out.println("HBaseBulkPutTimestampExample {tableName} {columnFamily} are missing an argument")
      return
    }

    val tableName = args(0)
    val columnFamily = args(1)

    val sparkConf = new SparkConf().setAppName("HBaseBulkPutTimestampExample " +
      tableName + " " + columnFamily)
    val sc = new SparkContext(sparkConf)

    try {

      val rdd = sc.parallelize(Array(
        (Bytes.toBytes("6"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("1")))),
        (Bytes.toBytes("7"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("2")))),
        (Bytes.toBytes("8"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("3")))),
        (Bytes.toBytes("9"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("4")))),
        (Bytes.toBytes("10"),
          Array((Bytes.toBytes(columnFamily), Bytes.toBytes("1"), Bytes.toBytes("5"))))))

      val conf = HBaseConfiguration.create()

      val timeStamp = System.currentTimeMillis()

      val hbaseContext = new HBaseContext(sc, conf)
      hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
        TableName.valueOf(tableName),
        (putRecord) => {
          val put = new Put(putRecord._1)
          putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2,
            timeStamp, putValue._3))
          put
        })
    } finally {
      sc.stop()
    }
  }
}
