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
package org.apache.hadoop.hbase.spark.example.hbasecontext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * This is a simple example of BulkPut with Spark Streaming
 */
final public class JavaHBaseStreamingBulkPutExample {

  private JavaHBaseStreamingBulkPutExample() {}

  public static void main(String[] args) {
    if (args.length < 4) {
      System.out.println("JavaHBaseBulkPutExample  " +
              "{host} {port} {tableName}");
      return;
    }

    String host = args[0];
    String port = args[1];
    String tableName = args[2];

    SparkConf sparkConf =
            new SparkConf().setAppName("JavaHBaseStreamingBulkPutExample " +
                    tableName + ":" + port + ":" + tableName);

    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      JavaStreamingContext jssc =
              new JavaStreamingContext(jsc, new Duration(1000));

      JavaReceiverInputDStream<String> javaDstream =
              jssc.socketTextStream(host, Integer.parseInt(port));

      Configuration conf = HBaseConfiguration.create();

      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

      hbaseContext.streamBulkPut(javaDstream,
              TableName.valueOf(tableName),
              new PutFunction());
    } finally {
      jsc.stop();
    }
  }

  public static class PutFunction implements Function<String, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
      String[] part = v.split(",");
      Put put = new Put(Bytes.toBytes(part[0]));

      put.addColumn(Bytes.toBytes(part[1]),
              Bytes.toBytes(part[2]),
              Bytes.toBytes(part[3]));
      return put;
    }

  }
}
