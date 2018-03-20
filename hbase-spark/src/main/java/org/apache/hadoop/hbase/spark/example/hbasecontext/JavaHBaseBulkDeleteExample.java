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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This is a simple example of deleting records in HBase
 * with the bulkDelete function.
 */
@InterfaceAudience.Private
final public class JavaHBaseBulkDeleteExample {

  private JavaHBaseBulkDeleteExample() {}

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("JavaHBaseBulkDeleteExample  {tableName}");
      return;
    }

    String tableName = args[0];

    SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkDeleteExample " + tableName);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      List<byte[]> list = new ArrayList<>(5);
      list.add(Bytes.toBytes("1"));
      list.add(Bytes.toBytes("2"));
      list.add(Bytes.toBytes("3"));
      list.add(Bytes.toBytes("4"));
      list.add(Bytes.toBytes("5"));

      JavaRDD<byte[]> rdd = jsc.parallelize(list);

      Configuration conf = HBaseConfiguration.create();

      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

      hbaseContext.bulkDelete(rdd,
              TableName.valueOf(tableName), new DeleteFunction(), 4);
    } finally {
      jsc.stop();
    }

  }

  public static class DeleteFunction implements Function<byte[], Delete> {
    private static final long serialVersionUID = 1L;
    public Delete call(byte[] v) throws Exception {
      return new Delete(v);
    }
  }
}
