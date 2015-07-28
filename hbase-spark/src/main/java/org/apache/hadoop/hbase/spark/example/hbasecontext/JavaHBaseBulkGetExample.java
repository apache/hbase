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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * This is a simple example of getting records in HBase
 * with the bulkGet function.
 */
final public class JavaHBaseBulkGetExample {

  private JavaHBaseBulkGetExample() {}

  public static void main(String[] args) {
    if (args.length < 1) {
      System.out.println("JavaHBaseBulkGetExample  {tableName}");
      return;
    }

    String tableName = args[0];

    SparkConf sparkConf = new SparkConf().setAppName("JavaHBaseBulkGetExample " + tableName);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      List<byte[]> list = new ArrayList<>();
      list.add(Bytes.toBytes("1"));
      list.add(Bytes.toBytes("2"));
      list.add(Bytes.toBytes("3"));
      list.add(Bytes.toBytes("4"));
      list.add(Bytes.toBytes("5"));

      JavaRDD<byte[]> rdd = jsc.parallelize(list);

      Configuration conf = HBaseConfiguration.create();

      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

      hbaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd, new GetFunction(),
              new ResultFunction());
    } finally {
      jsc.stop();
    }
  }

  public static class GetFunction implements Function<byte[], Get> {

    private static final long serialVersionUID = 1L;

    public Get call(byte[] v) throws Exception {
      return new Get(v);
    }
  }

  public static class ResultFunction implements Function<Result, String> {

    private static final long serialVersionUID = 1L;

    public String call(Result result) throws Exception {
      Iterator<Cell> it = result.listCells().iterator();
      StringBuilder b = new StringBuilder();

      b.append(Bytes.toString(result.getRow())).append(":");

      while (it.hasNext()) {
        Cell cell = it.next();
        String q = Bytes.toString(cell.getQualifierArray());
        if (q.equals("counter")) {
          b.append("(")
                  .append(Bytes.toString(cell.getQualifierArray()))
                  .append(",")
                  .append(Bytes.toLong(cell.getValueArray()))
                  .append(")");
        } else {
          b.append("(")
                  .append(Bytes.toString(cell.getQualifierArray()))
                  .append(",")
                  .append(Bytes.toString(cell.getValueArray()))
                  .append(")");
        }
      }
      return b.toString();
    }
  }
}