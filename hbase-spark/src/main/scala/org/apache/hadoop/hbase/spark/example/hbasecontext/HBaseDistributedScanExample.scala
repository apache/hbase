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
import org.apache.hadoop.hbase.client.Scan
import org.apache.spark.SparkConf
/**
 * This is a simple example of scanning records from HBase
 * with the hbaseRDD function.
 */
object HBaseDistributedScanExample {
  def main(args: Array[String]) {
    if (args.length < 1) {
      println("GenerateGraphs {tableName}")
      return
    }

    val tableName = args(0)

    val sparkConf = new SparkConf().setAppName("HBaseDistributedScanExample " + tableName )
    val sc = new SparkContext(sparkConf)

    try {
      val conf = HBaseConfiguration.create()

      val hbaseContext = new HBaseContext(sc, conf)

      val scan = new Scan()
      scan.setCaching(100)

      val getRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)

      getRdd.foreach(v => println(Bytes.toString(v._1.get())))

      println("Length: " + getRdd.map(r => r._1.copyBytes()).collect().length);

        //.collect().foreach(v => println(Bytes.toString(v._1.get())))
    } finally {
      sc.stop()
    }
  }

}