/*
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
package org.apache.hadoop.hbase.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test if the FilterWrapper retains the same semantics defined in the
 * {@link org.apache.hadoop.hbase.filter.Filter}
 */
@Tag(FilterTests.TAG)
@Tag(MediumTests.TAG)
public class TestFilterWrapper {

  private static final Logger LOG = LoggerFactory.getLogger(TestFilterWrapper.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = null;
  private static Admin admin = null;
  private static TableName name = TableName.valueOf("test");
  private static Connection connection;

  @Test
  public void testFilterWrapper() {
    int kv_number = 0;
    int row_number = 0;
    try {
      Scan scan = new Scan();
      List<Filter> fs = new ArrayList<>();

      DependentColumnFilter f1 = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c5"),
        true, CompareOperator.EQUAL, new SubstringComparator("c5"));
      PageFilter f2 = new PageFilter(2);
      fs.add(f1);
      fs.add(f2);
      FilterList filter = new FilterList(fs);

      scan.setFilter(filter);
      Table table = connection.getTable(name);
      ResultScanner scanner = table.getScanner(scan);

      // row2 (c1-c4) and row3(c1-c4) are returned
      for (Result result : scanner) {
        row_number++;
        for (Cell kv : result.listCells()) {
          LOG.debug(kv_number + ". kv: " + kv);
          kv_number++;
          assertEquals("row" + (row_number + 1), new String(CellUtil.cloneRow(kv)),
            "Returned row is not correct");
        }
      }

      scanner.close();
      table.close();
    } catch (Exception e) {
      // no correct result is expected
      assertNull(e, "Exception happens in scan");
    }
    LOG.debug("check the fetched kv number");
    assertEquals(8, kv_number, "We should get 8 results returned.");
    assertEquals(2, row_number, "We should get 2 rows returned");
  }

  private static void prepareData() {
    try {
      Table table = connection.getTable(name);
      assertTrue(admin.tableExists(name), "Fail to create the table");
      List<Put> puts = new ArrayList<>();

      // row1 => <f1:c1, 1_c1, ts=1>, <f1:c2, 1_c2, ts=2>, <f1:c3, 1_c3,ts=3>,
      // <f1:c4,1_c4, ts=4>, <f1:c5, 1_c5, ts=5>
      // row2 => <f1:c1, 2_c1, ts=2>, <f1,c2, 2_c2, ts=2>, <f1:c3, 2_c3,ts=2>,
      // <f1:c4,2_c4, ts=2>, <f1:c5, 2_c5, ts=2>
      // row3 => <f1:c1, 3_c1, ts=3>, <f1:c2, 3_c2, ts=3>, <f1:c3, 3_c3,ts=2>,
      // <f1:c4,3_c4, ts=3>, <f1:c5, 3_c5, ts=3>
      for (int i = 1; i < 4; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        for (int j = 1; j < 6; j++) {
          long timestamp = j;
          if (i != 1) timestamp = i;
          put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c" + j), timestamp,
            Bytes.toBytes(i + "_c" + j));
        }
        puts.add(put);
      }

      table.put(puts);
      table.close();
    } catch (IOException e) {
      assertNull(e, "Exception found while putting data into table");
    }
  }

  private static void createTable() {
    assertNotNull(admin, "HBaseAdmin is not initialized successfully.");
    if (admin != null) {

      HTableDescriptor desc = new HTableDescriptor(name);
      HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("f1"));
      desc.addFamily(coldef);

      try {
        admin.createTable(desc);
        assertTrue(admin.tableExists(name), "Fail to create the table");
      } catch (IOException e) {
        assertNull(e, "Exception found while creating table");
      }
    }
  }

  private static void deleteTable() {
    if (admin != null) {
      try {
        admin.disableTable(name);
        admin.deleteTable(name);
      } catch (IOException e) {
        assertNull(e, "Exception found deleting the table");
      }
    }
  }

  private static void initialize(Configuration conf) {
    TestFilterWrapper.conf = HBaseConfiguration.create(conf);
    TestFilterWrapper.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    try {
      connection = ConnectionFactory.createConnection(TestFilterWrapper.conf);
      admin = TEST_UTIL.getAdmin();
    } catch (MasterNotRunningException e) {
      assertNull(e, "Master is not running");
    } catch (ZooKeeperConnectionException e) {
      assertNull(e, "Cannot connect to ZooKeeper");
    } catch (IOException e) {
      assertNull(e, "Caught IOException");
    }
    createTable();
    prepareData();
  }

  @BeforeAll
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    initialize(TEST_UTIL.getConfiguration());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    deleteTable();
    connection.close();
    TEST_UTIL.shutdownMiniCluster();
  }

}
