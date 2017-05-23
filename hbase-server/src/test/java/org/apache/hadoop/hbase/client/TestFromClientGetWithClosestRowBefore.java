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

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Random;

@Category({ MediumTests.class })
public class TestFromClientGetWithClosestRowBefore {

  private static final Logger LOG = Logger.getLogger(TestFromClientGetWithClosestRowBefore.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration CONF;
  private static final TableName TEST_TABLE = TableName.valueOf("test_table");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("f1");
  private static final Random RANDOM = new Random();

  @BeforeClass
  public static void setup() throws Exception {
    CONF = UTIL.getConfiguration();
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void teardown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY);
    htd.addFamily(hcd);

    UTIL.getHBaseAdmin().createTable(htd);
  }

  @After
  public void tearDown() throws Exception {
    for (HTableDescriptor htd : UTIL.getHBaseAdmin().listTables()) {
      UTIL.deleteTable(htd.getTableName());
    }
  }

  @Test
  public void testGetWithClosestRowBeforeWhenSplitRegion() throws Exception {
    Thread t = new Thread() {
      public void run() {
        try {
          Thread.sleep(100);
          UTIL.getHBaseAdmin().split(TEST_TABLE);
        } catch (Exception e) {
          LOG.error("split region failed: ", e);
        }
      }
    };

    try (Connection conn = ConnectionFactory.createConnection(CONF)) {
      try (Table table = conn.getTable(TEST_TABLE)) {
        for (int i = 0; i < 1000; i++) {
          byte[] data = Bytes.toBytes(String.format("%026d", i));
          Put put = new Put(data).addColumn(COLUMN_FAMILY, null, data);
          table.put(put);
        }
      }
      try (Table table = conn.getTable(TableName.META_TABLE_NAME)) {
        t.start();
        for (int i = 0; i < 10000; i++) {
          Get get = new Get(Bytes.toBytes(TEST_TABLE + ",,:")).addFamily(Bytes.toBytes("info"))
              .setClosestRowBefore(true);
          Result result = table.get(get);
          if (Result.getTotalSizeOfCells(result) == 0) {
            Assert.fail("Get with closestRowBefore return NONE result.");
          }
        }
      }
    }
  }

  @Test
  public void testClosestRowIsLatestPutRow() throws IOException {
    final int[] initialRowkeys = new int[] { 1, 1000 };

    Thread t = new Thread() {
      public void run() {
        try {
          // a huge value to slow down transaction committing.
          byte[] value = new byte[512 * 1024];
          for (int i = 0; i < value.length; i++) {
            value[i] = (byte) RANDOM.nextInt(256);
          }

          // Put rowKey= 2,3,4,...,(initialRowkeys[1]-1) into table, let the rowkey returned by a
          // Get with closestRowBefore to be exactly the latest put rowkey.
          try (Connection conn = ConnectionFactory.createConnection(CONF)) {
            try (Table table = conn.getTable(TEST_TABLE)) {
              for (int i = initialRowkeys[0] + 1; i < initialRowkeys[1]; i++) {
                byte[] data = Bytes.toBytes(String.format("%026d", i));
                Put put = new Put(data).addColumn(COLUMN_FAMILY, null, value);
                table.put(put);
              }
            }
          }
        } catch (Exception e) {
          LOG.error("Put huge value into table failed: ", e);
        }
      }
    };

    try (Connection conn = ConnectionFactory.createConnection(CONF)) {
      try (Table table = conn.getTable(TEST_TABLE)) {

        // Put the boundary into table firstly.
        for (int i = 0; i < initialRowkeys.length; i++) {
          byte[] rowKey = Bytes.toBytes(String.format("%026d", initialRowkeys[i]));
          Put put = new Put(rowKey).addColumn(COLUMN_FAMILY, null, rowKey);
          table.put(put);
        }

        t.start();
        byte[] rowKey = Bytes.toBytes(String.format("%026d", initialRowkeys[1] - 1));
        for (int i = 0; i < 1000; i++) {
          Get get = new Get(rowKey).addFamily(COLUMN_FAMILY).setClosestRowBefore(true);
          Result result = table.get(get);
          if (Result.getTotalSizeOfCells(result) == 0) {
            Assert.fail("Get with closestRowBefore return NONE result.");
          }
        }
      }
    }
  }
}
