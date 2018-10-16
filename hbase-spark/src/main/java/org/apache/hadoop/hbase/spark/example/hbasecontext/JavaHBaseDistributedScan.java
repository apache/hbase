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

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.yetus.audience.InterfaceAudience;
import scala.Tuple2;

/**
 * This is a simple example of scanning records from HBase
 * with the hbaseRDD function.
 */
@InterfaceAudience.Private
final public class JavaHBaseDistributedScan {

  private JavaHBaseDistributedScan() {}

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("JavaHBaseDistributedScan {tableName}");
      return;
    }

    String tableName = args[0];

    SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseDistributedScan " + tableName);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      Configuration conf = HBaseConfiguration.create();

      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

      Scan scan = new Scan();
      scan.setCaching(100);

      JavaRDD<Tuple2<ImmutableBytesWritable, Result>> javaRdd =
              hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan);

      List<String> results = javaRdd.map(new ScanConvertFunction()).collect();

      System.out.println("Result Size: " + results.size());
    } finally {
      jsc.stop();
    }
  }

  private static class ScanConvertFunction implements
          Function<Tuple2<ImmutableBytesWritable, Result>, String> {
    @Override
    public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
      return Bytes.toString(v1._1().copyBytes());
    }
  }
}
