/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test failure in ScanDeleteTracker.isDeleted when ROWCOL bloom filter
 * is used during a scan with a filter.
 */
@Category({ RegionServerTests.class, FilterTests.class, MediumTests.class })
public class TestIsDeleteFailure {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIsDeleteFailure.class);

  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 2);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testIsDeleteFailure() throws Exception {
    final HTableDescriptor table = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    final byte[] family = Bytes.toBytes("0");
    final byte[] c1 = Bytes.toBytes("C01");
    final byte[] c2 = Bytes.toBytes("C02");
    final byte[] c3 = Bytes.toBytes("C03");
    final byte[] c4 = Bytes.toBytes("C04");
    final byte[] c5 = Bytes.toBytes("C05");
    final byte[] c6 = Bytes.toBytes("C07");
    final byte[] c7 = Bytes.toBytes("C07");
    final byte[] c8 = Bytes.toBytes("C08");
    final byte[] c9 = Bytes.toBytes("C09");
    final byte[] c10 = Bytes.toBytes("C10");
    final byte[] c11 = Bytes.toBytes("C11");
    final byte[] c12 = Bytes.toBytes("C12");
    final byte[] c13 = Bytes.toBytes("C13");
    final byte[] c14 = Bytes.toBytes("C14");
    final byte[] c15 = Bytes.toBytes("C15");

    final byte[] val = Bytes.toBytes("foo");
    List<byte[]> fams = new ArrayList<>(1);
    fams.add(family);
    Table ht = TEST_UTIL
        .createTable(table, fams.toArray(new byte[0][]), null, BloomType.ROWCOL, 10000,
            new Configuration(TEST_UTIL.getConfiguration()));
    List<Mutation> pending = new ArrayList<Mutation>();
    for (int i = 0; i < 1000; i++) {
      byte[] row = Bytes.toBytes("key" + Integer.toString(i));
      Put put = new Put(row);
      put.addColumn(family, c3, val);
      put.addColumn(family, c4, val);
      put.addColumn(family, c5, val);
      put.addColumn(family, c6, val);
      put.addColumn(family, c7, val);
      put.addColumn(family, c8, val);
      put.addColumn(family, c12, val);
      put.addColumn(family, c13, val);
      put.addColumn(family, c15, val);
      pending.add(put);
      Delete del = new Delete(row);
      del.addColumns(family, c2);
      del.addColumns(family, c9);
      del.addColumns(family, c10);
      del.addColumns(family, c14);
      pending.add(del);
    }
    ht.batch(pending, new Object[pending.size()]);
    TEST_UTIL.flush();
    TEST_UTIL.compact(true);
    for (int i = 20; i < 300; i++) {
      byte[] row = Bytes.toBytes("key" + Integer.toString(i));
      Put put = new Put(row);
      put.addColumn(family, c3, val);
      put.addColumn(family, c4, val);
      put.addColumn(family, c5, val);
      put.addColumn(family, c6, val);
      put.addColumn(family, c7, val);
      put.addColumn(family, c8, val);
      put.addColumn(family, c12, val);
      put.addColumn(family, c13, val);
      put.addColumn(family, c15, val);
      pending.add(put);
      Delete del = new Delete(row);
      del.addColumns(family, c2);
      del.addColumns(family, c9);
      del.addColumns(family, c10);
      del.addColumns(family, c14);
      pending.add(del);
    }
    ht.batch(pending, new Object[pending.size()]);
    TEST_UTIL.flush();

    Scan scan = new Scan();
    scan.addColumn(family, c9);
    scan.addColumn(family, c15);
    SingleColumnValueFilter filter =
        new SingleColumnValueFilter(family, c15, CompareFilter.CompareOp.EQUAL,
            new BinaryComparator(c15));
    scan.setFilter(filter);
    //Trigger the scan for not existing row, so it will scan over all rows
    for (Result result : ht.getScanner(scan)) {
      result.advance();
    }
    ht.close();
  }
}
