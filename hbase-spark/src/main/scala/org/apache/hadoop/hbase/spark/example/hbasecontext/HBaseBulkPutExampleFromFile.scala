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
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.spark.SparkConf

/**
 * This is a simple example of putting records in HBase
 * with the bulkPut function.  In this example we are
 * getting the put information from a file
 */
object HBaseBulkPutExampleFromFile {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("HBaseBulkPutExampleFromFile {tableName} {columnFamily} {inputFile}")
      return
    }

    val tableName = args(0)
    val columnFamily = args(1)
    val inputFile = args(2)

    val sparkConf = new SparkConf().setAppName("HBaseBulkPutExampleFromFile " +
      tableName + " " + columnFamily + " " + inputFile)
    val sc = new SparkContext(sparkConf)

    try {
      var rdd = sc.hadoopFile(
        inputFile,
        classOf[TextInputFormat],
        classOf[LongWritable],
        classOf[Text]).map(v => {
        System.out.println("reading-" + v._2.toString)
        v._2.toString
      })

      val conf = HBaseConfiguration.create()

      val hbaseContext = new HBaseContext(sc, conf)
      hbaseContext.bulkPut[String](rdd,
        TableName.valueOf(tableName),
        (putRecord) => {
          System.out.println("hbase-" + putRecord)
          val put = new Put(Bytes.toBytes("Value- " + putRecord))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("1"),
            Bytes.toBytes(putRecord.length()))
          put
        });
    } finally {
      sc.stop()
    }
  }
}
