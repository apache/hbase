/*
 * Copyright The Apache Software Foundation
 *
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.junit.experimental.categories.Category;

/**
 * Test if the FilterWrapper retains the same semantics defined in the
 * {@link org.apache.hadoop.hbase.filter.Filter}
 */
@Category({FilterTests.class, MediumTests.class})
public class TestFilterWrapper {
  private static final Log LOG = LogFactory.getLog(TestFilterWrapper.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = null;
  private static HBaseAdmin admin = null;
  private static TableName name = TableName.valueOf("test");
  private static Connection connection;

  @Test
  public void testFilterWrapper() {
    int kv_number = 0;
    int row_number = 0;
    try {
      Scan scan = new Scan();
      List<Filter> fs = new ArrayList<Filter>();

      DependentColumnFilter f1 = new DependentColumnFilter(Bytes.toBytes("f1"),
          Bytes.toBytes("c5"), true, CompareFilter.CompareOp.EQUAL,
          new SubstringComparator("c5"));
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
          assertEquals("Returned row is not correct", new String(CellUtil.cloneRow(kv)),
              "row" + ( row_number + 1 ));
        }
      }

      scanner.close();
      table.close();
    } catch (Exception e) {
      // no correct result is expected
      assertNull("Exception happens in scan", e);
    }
    LOG.debug("check the fetched kv number");
    assertEquals("We should get 8 results returned.", 8, kv_number);
    assertEquals("We should get 2 rows returned", 2, row_number);
  }

  private static void prepareData() {
    try {
      Table table = connection.getTable(name);
      assertTrue("Fail to create the table", admin.tableExists(name));
      List<Put> puts = new ArrayList<Put>();

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
          if (i != 1)
            timestamp = i;
          put.add(Bytes.toBytes("f1"), Bytes.toBytes("c" + j), timestamp,
              Bytes.toBytes(i + "_c" + j));
        }
        puts.add(put);
      }

      table.put(puts);
      table.close();
    } catch (IOException e) {
      assertNull("Exception found while putting data into table", e);
    }
  }

  private static void createTable() {
    assertNotNull("HBaseAdmin is not initialized successfully.", admin);
    if (admin != null) {

      HTableDescriptor desc = new HTableDescriptor(name);
      HColumnDescriptor coldef = new HColumnDescriptor(Bytes.toBytes("f1"));
      desc.addFamily(coldef);

      try {
        admin.createTable(desc);
        assertTrue("Fail to create the table", admin.tableExists(name));
      } catch (IOException e) {
        assertNull("Exception found while creating table", e);
      }

    }
  }

  private static void deleteTable() {
    if (admin != null) {
      try {
        admin.disableTable(name);
        admin.deleteTable(name);
      } catch (IOException e) {
        assertNull("Exception found deleting the table", e);
      }
    }
  }

  private static void initialize(Configuration conf) {
    TestFilterWrapper.conf = HBaseConfiguration.create(conf);
    TestFilterWrapper.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    try {
      connection = ConnectionFactory.createConnection(TestFilterWrapper.conf);
      admin = TEST_UTIL.getHBaseAdmin();
    } catch (MasterNotRunningException e) {
      assertNull("Master is not running", e);
    } catch (ZooKeeperConnectionException e) {
      assertNull("Cannot connect to Zookeeper", e);
    } catch (IOException e) {
      assertNull("Caught IOException", e);
    }
    createTable();
    prepareData();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    initialize(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteTable();
    connection.close();
    TEST_UTIL.shutdownMiniCluster();
  }

}
