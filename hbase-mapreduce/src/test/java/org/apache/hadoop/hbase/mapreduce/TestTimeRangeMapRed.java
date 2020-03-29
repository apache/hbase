/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MapReduceTests.class, LargeTests.class})
public class TestTimeRangeMapRed {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimeRangeMapRed.class);

  private final static Logger log = LoggerFactory.getLogger(TestTimeRangeMapRed.class);
  private static final HBaseTestingUtility UTIL =
    new HBaseTestingUtility();
  private Admin admin;

  private static final byte [] KEY = Bytes.toBytes("row1");
  private static final NavigableMap<Long, Boolean> TIMESTAMP = new TreeMap<>();
  static {
    TIMESTAMP.put((long)1245620000, false);
    TIMESTAMP.put((long)1245620005, true); // include
    TIMESTAMP.put((long)1245620010, true); // include
    TIMESTAMP.put((long)1245620055, true); // include
    TIMESTAMP.put((long)1245620100, true); // include
    TIMESTAMP.put((long)1245620150, false);
    TIMESTAMP.put((long)1245620250, false);
  }
  static final long MINSTAMP = 1245620005;
  static final long MAXSTAMP = 1245620100 + 1; // maxStamp itself is excluded. so increment it.

  static final TableName TABLE_NAME = TableName.valueOf("table123");
  static final byte[] FAMILY_NAME = Bytes.toBytes("text");
  static final byte[] COLUMN_NAME = Bytes.toBytes("input");

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    this.admin = UTIL.getAdmin();
  }

  private static class ProcessTimeRangeMapper
  extends TableMapper<ImmutableBytesWritable, MapWritable>
  implements Configurable {

    private Configuration conf = null;
    private Table table = null;

    @Override
    public void map(ImmutableBytesWritable key, Result result,
        Context context)
    throws IOException {
      List<Long> tsList = new ArrayList<>();
      for (Cell kv : result.listCells()) {
        tsList.add(kv.getTimestamp());
      }

      List<Put> puts = new ArrayList<>();
      for (Long ts : tsList) {
        Put put = new Put(key.get());
        put.setDurability(Durability.SKIP_WAL);
        put.addColumn(FAMILY_NAME, COLUMN_NAME, ts, Bytes.toBytes(true));
        puts.add(put);
      }
      table.put(puts);
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration configuration) {
      this.conf = configuration;
      try {
        Connection connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TABLE_NAME);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testTimeRangeMapRed()
      throws IOException, InterruptedException, ClassNotFoundException {
    final TableDescriptorBuilder.ModifyableTableDescriptor tableDescriptor =
      new TableDescriptorBuilder.ModifyableTableDescriptor(TABLE_NAME);
    final ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor familyDescriptor =
      new ColumnFamilyDescriptorBuilder.ModifyableColumnFamilyDescriptor(FAMILY_NAME);
    familyDescriptor.setMaxVersions(Integer.MAX_VALUE);
    tableDescriptor.setColumnFamily(familyDescriptor);
    admin.createTable(tableDescriptor);
    List<Put> puts = new ArrayList<>();
    for (Map.Entry<Long, Boolean> entry : TIMESTAMP.entrySet()) {
      Put put = new Put(KEY);
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(FAMILY_NAME, COLUMN_NAME, entry.getKey(), Bytes.toBytes(false));
      puts.add(put);
    }
    Table table = UTIL.getConnection().getTable(tableDescriptor.getTableName());
    table.put(puts);
    runTestOnTable();
    verify(table);
    table.close();
  }

  private void runTestOnTable()
  throws IOException, InterruptedException, ClassNotFoundException {
    Job job = null;
    try {
      job = new Job(UTIL.getConfiguration(), "test123");
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setNumReduceTasks(0);
      Scan scan = new Scan();
      scan.addColumn(FAMILY_NAME, COLUMN_NAME);
      scan.setTimeRange(MINSTAMP, MAXSTAMP);
      scan.readAllVersions();
      TableMapReduceUtil.initTableMapperJob(TABLE_NAME,
        scan, ProcessTimeRangeMapper.class, Text.class, Text.class, job);
      job.waitForCompletion(true);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      if (job != null) {
        FileUtil.fullyDelete(
          new File(job.getConfiguration().get("hadoop.tmp.dir")));
      }
    }
  }

  private void verify(final Table table) throws IOException {
    Scan scan = new Scan();
    scan.addColumn(FAMILY_NAME, COLUMN_NAME);
    scan.setMaxVersions(1);
    ResultScanner scanner = table.getScanner(scan);
    for (Result r: scanner) {
      for (Cell kv : r.listCells()) {
        log.debug(Bytes.toString(r.getRow()) + "\t" + Bytes.toString(CellUtil.cloneFamily(kv))
            + "\t" + Bytes.toString(CellUtil.cloneQualifier(kv))
            + "\t" + kv.getTimestamp() + "\t" + Bytes.toBoolean(CellUtil.cloneValue(kv)));
        org.junit.Assert.assertEquals(TIMESTAMP.get(kv.getTimestamp()),
          Bytes.toBoolean(CellUtil.cloneValue(kv)));
      }
    }
    scanner.close();
  }

}

