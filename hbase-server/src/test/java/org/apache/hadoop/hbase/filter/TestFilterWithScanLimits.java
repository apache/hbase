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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test if Filter is incompatible with scan-limits
 */
@Category(MediumTests.class)
public class TestFilterWithScanLimits {
  private static final Log LOG = LogFactory
      .getLog(TestFilterWithScanLimits.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = null;
  private static HBaseAdmin admin = null;
  private static byte[] name = Bytes.toBytes("test");

  @Test
  public void testScanWithLimit() {
    int kv_number = 0;
    try {
      Scan scan = new Scan();
      // set batch number as 2, which means each Result should contain 2 KVs at
      // most
      scan.setBatch(2);
      SingleColumnValueFilter filter = new SingleColumnValueFilter(
          Bytes.toBytes("f1"), Bytes.toBytes("c5"),
          CompareFilter.CompareOp.EQUAL, new SubstringComparator("2_c5"));

      // add filter after batch defined
      scan.setFilter(filter);
      HTable table = new HTable(conf, name);
      ResultScanner scanner = table.getScanner(scan);
      // Expect to get following row
      // row2 => <f1:c1, 2_c1>, <f1:c2, 2_c2>,
      // row2 => <f1:c3, 2_c3>, <f1:c4, 2_c4>,
      // row2 => <f1:c5, 2_c5>

      for (Result result : scanner) {
        for (Cell kv : result.listCells()) {
          kv_number++;
          LOG.debug(kv_number + ". kv: " + kv);
        }
      }

      scanner.close();
      table.close();
    } catch (Exception e) {
      // no correct result is expected
      assertNotNull("No IncompatibleFilterException catched", e);
    }
    LOG.debug("check the fetched kv number");
    assertEquals("We should not get result(s) returned.", 0, kv_number);
  }

  private static void prepareData() {
    try {
      HTable table = new HTable(TestFilterWithScanLimits.conf, name);
      assertTrue("Fail to create the table", admin.tableExists(name));
      List<Put> puts = new ArrayList<Put>();

      // row1 => <f1:c1, 1_c1>, <f1:c2, 1_c2>, <f1:c3, 1_c3>, <f1:c4,1_c4>,
      // <f1:c5, 1_c5>
      // row2 => <f1:c1, 2_c1>, <f1:c2, 2_c2>, <f1:c3, 2_c3>, <f1:c4,2_c4>,
      // <f1:c5, 2_c5>
      for (int i = 1; i < 4; i++) {
        Put put = new Put(Bytes.toBytes("row" + i));
        for (int j = 1; j < 6; j++) {
          put.add(Bytes.toBytes("f1"), Bytes.toBytes("c" + j),
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

      HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(name));
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
    TestFilterWithScanLimits.conf = HBaseConfiguration.create(conf);
    TestFilterWithScanLimits.conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    try {
      admin = new HBaseAdmin(conf);
    } catch (MasterNotRunningException e) {
      assertNull("Master is not running", e);
    } catch (ZooKeeperConnectionException e) {
      assertNull("Cannot connect to Zookeeper", e);
    } catch (IOException e) {
      assertNull("IOException", e);
    }
    createTable();
    prepareData();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    ((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    TEST_UTIL.startMiniCluster(1);
    initialize(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    deleteTable();
    TEST_UTIL.shutdownMiniCluster();
  }

}
