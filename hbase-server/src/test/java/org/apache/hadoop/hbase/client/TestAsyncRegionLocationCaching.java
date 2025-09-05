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

import static org.apache.hadoop.hbase.client.ConnectionConfiguration.HBASE_CLIENT_META_CACHE_INVALIDATE_INTERVAL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
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
public class TestAsyncRegionLocationCaching {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncRegionLocationCaching.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static int SLAVES = 1;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
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
    conf1.setLong(HBASE_CLIENT_META_CACHE_INVALIDATE_INTERVAL, 5 * 1000);

    AsyncConnection conn1 = ConnectionFactory.createAsyncConnection(conf1).get();
    AsyncConnection conn2 =
      ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();

    try {
      AsyncAdmin admin1 = conn1.getAdmin();
      admin1.createTable(tbd1).get();
      admin1.createTable(tbd2).get();
      conn1.getRegionLocator(tbn1).getAllRegionLocations().get();
      conn1.getRegionLocator(tbn2).getAllRegionLocations().get();
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      AsyncAdmin admin2 = conn2.getAdmin();
      admin2.disableTable(tbn1).get();
      // Sleep 10s to test whether the invalidateMetaCache task could execute regularly(the interval
      // is 5s).
      Threads.sleep(10 * 1000);
      checkRegionLocationIsNotCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      admin2.disableTable(tbn2).get();
      admin2.deleteTable(tbn2).get();
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

    AsyncConnection conn1 =
      ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    AsyncConnection conn2 =
      ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();

    try {
      AsyncAdmin admin1 = conn1.getAdmin();
      admin1.createTable(tbd1).get();
      admin1.createTable(tbd2).get();
      conn1.getRegionLocator(tbn1).getAllRegionLocations().get();
      conn1.getRegionLocator(tbn2).getAllRegionLocations().get();
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      AsyncAdmin admin2 = conn2.getAdmin();
      admin2.disableTable(tbn1).get();
      Threads.sleep(10 * 1000);
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);

      admin2.disableTable(tbn2).get();
      admin2.deleteTable(tbn2).get();
      Threads.sleep(10 * 1000);
      checkRegionLocationIsCached(tbn1, conn1);
      checkRegionLocationIsCached(tbn2, conn1);
    } finally {
      IOUtils.closeQuietly(conn1, conn2);
    }
  }

  /**
   * Method to check whether the cached region location is non-empty for the given table. It repeats
   * the same check several times as clearing of cache by some async operations may not reflect
   * immediately.
   */
  private void checkRegionLocationIsCached(final TableName tableName, final AsyncConnection conn)
    throws InterruptedException, IOException {
    for (int count = 0; count < 50; count++) {
      int number =
        ((AsyncConnectionImpl) conn).getLocator().getNumberOfCachedRegionLocations(tableName);
      assertNotEquals("Expected non-zero number of cached region locations", 0, number);
      Thread.sleep(100);
    }
  }

  /**
   * Method to check whether the cached region location is empty for the given table. It repeats the
   * same check several times as clearing of cache by some async operations may not reflect
   * immediately.
   */
  private void checkRegionLocationIsNotCached(final TableName tableName, final AsyncConnection conn)
    throws InterruptedException {
    for (int count = 0; count < 50; count++) {
      int number =
        ((AsyncConnectionImpl) conn).getLocator().getNumberOfCachedRegionLocations(tableName);
      assertEquals("Expected zero number of cached region locations", 0, number);
      Thread.sleep(100);
    }
  }
}
