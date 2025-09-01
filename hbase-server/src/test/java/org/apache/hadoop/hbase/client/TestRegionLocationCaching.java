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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestRegionLocationCaching {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionLocationCaching.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static int SLAVES = 1;
  private static TableName TABLE_NAME = TableName.valueOf("TestRegionLocationCaching");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
    TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY });
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCachingForHTableSinglePut() throws Exception {
    byte[] row = Bytes.toBytes("htable_single_put");
    byte[] value = Bytes.toBytes("value");

    Put put = new Put(row);
    put.addColumn(FAMILY, QUALIFIER, value);

    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      table.put(put);
    }

    checkRegionLocationIsCached(TABLE_NAME, TEST_UTIL.getConnection());
    checkExistence(TABLE_NAME, row, FAMILY, QUALIFIER);
  }

  @Test
  public void testCachingForHTableMultiPut() throws Exception {
    List<Put> multiput = new ArrayList<Put>();
    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes("htable_multi_put" + i));
      byte[] value = Bytes.toBytes("value_" + i);
      put.addColumn(FAMILY, QUALIFIER, value);
      multiput.add(put);
    }

    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      table.put(multiput);
    }
    checkRegionLocationIsCached(TABLE_NAME, TEST_UTIL.getConnection());
    for (int i = 0; i < 10; i++) {
      checkExistence(TABLE_NAME, Bytes.toBytes("htable_multi_put" + i), FAMILY, QUALIFIER);
    }
  }

  /**
   * Method to check whether the cached region location is non-empty for the given table. It repeats
   * the same check several times as clearing of cache by some async operations may not reflect
   * immediately.
   */
  private void checkRegionLocationIsCached(final TableName tableName, final Connection conn)
    throws InterruptedException, IOException {
    for (int count = 0; count < 50; count++) {
      int number = ((AsyncConnectionImpl) conn.toAsyncConnection()).getLocator()
        .getNumberOfCachedRegionLocations(tableName);
      assertNotEquals("Expected non-zero number of cached region locations", 0, number);
      Thread.sleep(100);
    }
  }

  /**
   * Method to check whether the cached region location is empty for the given table. It repeats the
   * same check several times as clearing of cache by some async operations may not reflect
   * immediately.
   */
  private void checkRegionLocationIsNotCached(final TableName tableName, final Connection conn)
    throws InterruptedException {
    for (int count = 0; count < 50; count++) {
      int number = ((AsyncConnectionImpl) conn.toAsyncConnection()).getLocator()
        .getNumberOfCachedRegionLocations(tableName);
      assertEquals("Expected zero number of cached region locations", 0, number);
      Thread.sleep(100);
    }
  }

  /**
   * Method to check whether the passed row exists in the given table
   */
  private static void checkExistence(final TableName tableName, final byte[] row,
    final byte[] family, final byte[] qualifier) throws Exception {
    // verify that the row exists
    Result r;
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    int nbTry = 0;
    try (Table table = TEST_UTIL.getConnection().getTable(tableName)) {
      do {
        assertTrue("Failed to get row after " + nbTry + " tries", nbTry < 50);
        nbTry++;
        Thread.sleep(100);
        r = table.get(get);
      } while (r == null || r.getValue(family, qualifier) == null);
    }
  }

  @Test
  public void testInvalidateMetaCache() throws Throwable {
    // There are 2 tables and 2 connections, both connection cached all region locations of all
    // tables,
    // after disable/delete one table using one connection, need invalidate the meta cache
    // of the table in other connections.
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build();

    TableName tbn1 = TableName.valueOf("testInvalidateMetaCache1");
    TableDescriptor tbd1 = TableDescriptorBuilder.newBuilder(tbn1).setColumnFamily(cfd).build();

    TableName tbn2 = TableName.valueOf("testInvalidateMetaCache2");
    TableDescriptor tbd2 = TableDescriptorBuilder.newBuilder(tbn2).setColumnFamily(cfd).build();

    Configuration conf1 = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf1.setLong("hbase.client.connection.metacache.invalidate-interval.ms", 5 * 1000);

    Connection conn1 = ConnectionFactory.createConnection(conf1);
    Connection conn2 = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());

    try {
      Admin admin1 = conn1.getAdmin();
      admin1.createTable(tbd1);
      admin1.createTable(tbd2);
      conn1.getRegionLocator(tbn1).getAllRegionLocations();
      conn1.getRegionLocator(tbn2).getAllRegionLocations();
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      Admin admin2 = conn2.getAdmin();
      admin2.disableTable(tbn1);
      // Sleep 10s to test whether the invalidateMetaCache task could execute regularly(the interval
      // is 5s).
      Threads.sleep(10 * 1000);
      checkRegionLocationIsNotCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      admin2.disableTable(tbn2);
      admin2.deleteTable(tbn2);
      Threads.sleep(10 * 1000);
      checkRegionLocationIsNotCached(tbn1, conn1);
      checkRegionLocationIsNotCached(tbn2, conn1);
    } finally {
      IOUtils.closeQuietly(conn1, conn2);
    }
  }

  @Test
  public void testDisableInvalidateMetaCache() throws Throwable {
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf")).build();

    TableName tbn1 = TableName.valueOf("testDisableInvalidateMetaCache1");
    TableDescriptor tbd1 = TableDescriptorBuilder.newBuilder(tbn1).setColumnFamily(cfd).build();

    TableName tbn2 = TableName.valueOf("testDisableInvalidateMetaCache2");
    TableDescriptor tbd2 = TableDescriptorBuilder.newBuilder(tbn2).setColumnFamily(cfd).build();

    Connection conn1 = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
    Connection conn2 = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());

    try {
      Admin admin1 = conn1.getAdmin();
      admin1.createTable(tbd1);
      admin1.createTable(tbd2);
      conn1.getRegionLocator(tbn1).getAllRegionLocations();
      conn1.getRegionLocator(tbn2).getAllRegionLocations();
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      Admin admin2 = conn2.getAdmin();
      admin2.disableTable(tbn1);
      Threads.sleep(10 * 1000);
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      admin2.disableTable(tbn2);
      admin2.deleteTable(tbn2);
      Threads.sleep(10 * 1000);
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);
    } finally {
      IOUtils.closeQuietly(conn1, conn2);
    }
  }
}
