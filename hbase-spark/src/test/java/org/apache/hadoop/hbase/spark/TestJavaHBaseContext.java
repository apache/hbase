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
package org.apache.hadoop.hbase.spark;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.spark.example.hbasecontext.JavaHBaseBulkDeleteExample;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.junit.*;
import org.junit.experimental.categories.Category;

import scala.Tuple2;

import com.google.common.io.Files;

@Category({MiscTests.class, MediumTests.class})
public class TestJavaHBaseContext implements Serializable {
  private transient JavaSparkContext jsc;
  HBaseTestingUtility htu;
  protected static final Log LOG = LogFactory.getLog(TestJavaHBaseContext.class);


  byte[] tableName = Bytes.toBytes("t1");
  byte[] columnFamily = Bytes.toBytes("c");
  String columnFamilyStr = Bytes.toString(columnFamily);

  @Before
  public void setUp() {
    jsc = new JavaSparkContext("local", "JavaHBaseContextSuite");
    jsc.addJar("spark.jar");

    File tempDir = Files.createTempDir();
    tempDir.deleteOnExit();

    htu = HBaseTestingUtility.createLocalHTU();
    try {
      LOG.info("cleaning up test dir");

      htu.cleanupTestDir();

      LOG.info("starting minicluster");

      htu.startMiniZKCluster();
      htu.startMiniHBaseCluster(1, 1);

      LOG.info(" - minicluster started");

      try {
        htu.deleteTable(TableName.valueOf(tableName));
      } catch (Exception e) {
        LOG.info(" - no table " + Bytes.toString(tableName) + " found");
      }

      LOG.info(" - creating table " + Bytes.toString(tableName));
      htu.createTable(TableName.valueOf(tableName),
              columnFamily);
      LOG.info(" - created table");
    } catch (Exception e1) {
      throw new RuntimeException(e1);
    }
  }

  @After
  public void tearDown() {
    try {
      htu.deleteTable(TableName.valueOf(tableName));
      LOG.info("shuting down minicluster");
      htu.shutdownMiniHBaseCluster();
      htu.shutdownMiniZKCluster();
      LOG.info(" - minicluster shut down");
      htu.cleanupTestDir();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    jsc.stop();
    jsc = null;
  }

  @Test
  public void testBulkPut() throws IOException {

    List<String> list = new ArrayList<>();
    list.add("1," + columnFamilyStr + ",a,1");
    list.add("2," + columnFamilyStr + ",a,2");
    list.add("3," + columnFamilyStr + ",a,3");
    list.add("4," + columnFamilyStr + ",a,4");
    list.add("5," + columnFamilyStr + ",a,5");

    JavaRDD<String> rdd = jsc.parallelize(list);

    Configuration conf = htu.getConfiguration();

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    Connection conn = ConnectionFactory.createConnection(conf);
    Table table = conn.getTable(TableName.valueOf(tableName));

    try {
      List<Delete> deletes = new ArrayList<>();
      for (int i = 1; i < 6; i++) {
        deletes.add(new Delete(Bytes.toBytes(Integer.toString(i))));
      }
      table.delete(deletes);
    } finally {
      table.close();
    }

    hbaseContext.bulkPut(rdd,
            TableName.valueOf(tableName),
            new PutFunction());

    table = conn.getTable(TableName.valueOf(tableName));

    try {
      Result result1 = table.get(new Get(Bytes.toBytes("1")));
      Assert.assertNotNull("Row 1 should had been deleted", result1.getRow());

      Result result2 = table.get(new Get(Bytes.toBytes("2")));
      Assert.assertNotNull("Row 2 should had been deleted", result2.getRow());

      Result result3 = table.get(new Get(Bytes.toBytes("3")));
      Assert.assertNotNull("Row 3 should had been deleted", result3.getRow());

      Result result4 = table.get(new Get(Bytes.toBytes("4")));
      Assert.assertNotNull("Row 4 should had been deleted", result4.getRow());

      Result result5 = table.get(new Get(Bytes.toBytes("5")));
      Assert.assertNotNull("Row 5 should had been deleted", result5.getRow());
    } finally {
      table.close();
      conn.close();
    }
  }

  public static class PutFunction implements Function<String, Put> {

    private static final long serialVersionUID = 1L;

    public Put call(String v) throws Exception {
      String[] cells = v.split(",");
      Put put = new Put(Bytes.toBytes(cells[0]));

      put.addColumn(Bytes.toBytes(cells[1]), Bytes.toBytes(cells[2]),
              Bytes.toBytes(cells[3]));
      return put;
    }
  }

  @Test
  public void testBulkDelete() throws IOException {
    List<byte[]> list = new ArrayList<>();
    list.add(Bytes.toBytes("1"));
    list.add(Bytes.toBytes("2"));
    list.add(Bytes.toBytes("3"));

    JavaRDD<byte[]> rdd = jsc.parallelize(list);

    Configuration conf = htu.getConfiguration();

    populateTableWithMockData(conf, TableName.valueOf(tableName));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    hbaseContext.bulkDelete(rdd, TableName.valueOf(tableName),
            new JavaHBaseBulkDeleteExample.DeleteFunction(), 2);



    try (
            Connection conn = ConnectionFactory.createConnection(conf);
            Table table = conn.getTable(TableName.valueOf(tableName))
    ){
      Result result1 = table.get(new Get(Bytes.toBytes("1")));
      Assert.assertNull("Row 1 should had been deleted", result1.getRow());

      Result result2 = table.get(new Get(Bytes.toBytes("2")));
      Assert.assertNull("Row 2 should had been deleted", result2.getRow());

      Result result3 = table.get(new Get(Bytes.toBytes("3")));
      Assert.assertNull("Row 3 should had been deleted", result3.getRow());

      Result result4 = table.get(new Get(Bytes.toBytes("4")));
      Assert.assertNotNull("Row 4 should had been deleted", result4.getRow());

      Result result5 = table.get(new Get(Bytes.toBytes("5")));
      Assert.assertNotNull("Row 5 should had been deleted", result5.getRow());
    }
  }

  @Test
  public void testDistributedScan() throws IOException {
    Configuration conf = htu.getConfiguration();

    populateTableWithMockData(conf, TableName.valueOf(tableName));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    Scan scan = new Scan();
    scan.setCaching(100);

    JavaRDD<String> javaRdd =
            hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
                    .map(new ScanConvertFunction());

    List<String> results = javaRdd.collect();

    Assert.assertEquals(results.size(), 5);
  }

  private static class ScanConvertFunction implements
          Function<Tuple2<ImmutableBytesWritable, Result>, String> {
    @Override
    public String call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
      return Bytes.toString(v1._1().copyBytes());
    }
  }

  @Test
  public void testBulkGet() throws IOException {
    List<byte[]> list = new ArrayList<>();
    list.add(Bytes.toBytes("1"));
    list.add(Bytes.toBytes("2"));
    list.add(Bytes.toBytes("3"));
    list.add(Bytes.toBytes("4"));
    list.add(Bytes.toBytes("5"));

    JavaRDD<byte[]> rdd = jsc.parallelize(list);

    Configuration conf = htu.getConfiguration();

    populateTableWithMockData(conf, TableName.valueOf(tableName));

    JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf);

    final JavaRDD<String> stringJavaRDD =
            hbaseContext.bulkGet(TableName.valueOf(tableName), 2, rdd,
            new GetFunction(),
            new ResultFunction());

    Assert.assertEquals(stringJavaRDD.count(), 5);
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
        String q = Bytes.toString(CellUtil.cloneQualifier(cell));
        if ("counter".equals(q)) {
          b.append("(")
                  .append(q)
                  .append(",")
                  .append(Bytes.toLong(CellUtil.cloneValue(cell)))
                  .append(")");
        } else {
          b.append("(")
                  .append(q)
                  .append(",")
                  .append(Bytes.toString(CellUtil.cloneValue(cell)))
                  .append(")");
        }
      }
      return b.toString();
    }
  }

  private void populateTableWithMockData(Configuration conf, TableName tableName)
          throws IOException {
    try (
      Connection conn = ConnectionFactory.createConnection(conf);
      Table table = conn.getTable(tableName)) {

      List<Put> puts = new ArrayList<>();

      for (int i = 1; i < 6; i++) {
        Put put = new Put(Bytes.toBytes(Integer.toString(i)));
        put.addColumn(columnFamily, columnFamily, columnFamily);
        puts.add(put);
      }
      table.put(puts);
    }
  }

}