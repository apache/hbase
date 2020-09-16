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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that a coprocessor can open a connection and write to another table, inside a hook.
 */
@Category({CoprocessorTests.class, MediumTests.class})
public class TestOpenTableInCoprocessor {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestOpenTableInCoprocessor.class);

  private static final TableName otherTable = TableName.valueOf("otherTable");
  private static final TableName primaryTable = TableName.valueOf("primary");
  private static final byte[] family = new byte[] { 'f' };

  private static boolean[] completed = new boolean[1];
  /**
   * Custom coprocessor that just copies the write to another table.
   */
  public static class SendToOtherTableCoprocessor implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      try (Table table = e.getEnvironment().getConnection().
          getTable(otherTable)) {
        table.put(put);
        completed[0] = true;
      }
    }

  }

  private static boolean[] completedWithPool = new boolean[1];
  /**
   * Coprocessor that creates an HTable with a pool to write to another table
   */
  public static class CustomThreadPoolCoprocessor implements RegionCoprocessor, RegionObserver {

    /**
     * @return a pool that has one thread only at every time. A second action added to the pool (
     *         running concurrently), will cause an exception.
     */
    private ExecutorService getPool() {
      int maxThreads = 1;
      long keepAliveTime = 60;
      ThreadPoolExecutor pool = new ThreadPoolExecutor(1, maxThreads, keepAliveTime,
        TimeUnit.SECONDS, new SynchronousQueue<>(),
        new ThreadFactoryBuilder().setNameFormat("hbase-table-pool-%d").setDaemon(true)
          .setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
      pool.allowCoreThreadTimeOut(true);
      return pool;
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      try (Table table = e.getEnvironment().getConnection().getTable(otherTable, getPool())) {
        Put p = new Put(new byte[]{'a'});
        p.addColumn(family, null, new byte[]{'a'});
        try {
          table.batch(Collections.singletonList(put), null);
        } catch (InterruptedException e1) {
          throw new IOException(e1);
        }
        completedWithPool[0] = true;
      }
    }
  }

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster();
  }

  @After
  public void cleanupTestTable() throws Exception {
    UTIL.getAdmin().disableTable(primaryTable);
    UTIL.getAdmin().deleteTable(primaryTable);

    UTIL.getAdmin().disableTable(otherTable);
    UTIL.getAdmin().deleteTable(otherTable);

  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCoprocessorCanCreateConnectionToRemoteTable() throws Throwable {
    runCoprocessorConnectionToRemoteTable(SendToOtherTableCoprocessor.class, completed);
  }

  @Test
  public void testCoprocessorCanCreateConnectionToRemoteTableWithCustomPool() throws Throwable {
    runCoprocessorConnectionToRemoteTable(CustomThreadPoolCoprocessor.class, completedWithPool);
  }

  private void runCoprocessorConnectionToRemoteTable(Class clazz, boolean[] completeCheck)
      throws Throwable {
    // Check if given class implements RegionObserver.
    assert(RegionObserver.class.isAssignableFrom(clazz));
    HTableDescriptor primary = new HTableDescriptor(primaryTable);
    primary.addFamily(new HColumnDescriptor(family));
    // add our coprocessor
    primary.addCoprocessor(clazz.getName());

    HTableDescriptor other = new HTableDescriptor(otherTable);
    other.addFamily(new HColumnDescriptor(family));


    Admin admin = UTIL.getAdmin();
    admin.createTable(primary);
    admin.createTable(other);

    Table table = UTIL.getConnection().getTable(TableName.valueOf("primary"));
    Put p = new Put(new byte[] { 'a' });
    p.addColumn(family, null, new byte[]{'a'});
    table.put(p);
    table.close();

    Table target = UTIL.getConnection().getTable(otherTable);
    assertTrue("Didn't complete update to target table!", completeCheck[0]);
    assertEquals("Didn't find inserted row", 1, getKeyValueCount(target));
    target.close();
  }

  /**
   * Count the number of keyvalue in the table. Scans all possible versions
   * @param table table to scan
   * @return number of keyvalues over all rows in the table
   * @throws IOException
   */
  private int getKeyValueCount(Table table) throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(Integer.MAX_VALUE - 1);

    ResultScanner results = table.getScanner(scan);
    int count = 0;
    for (Result res : results) {
      count += res.listCells().size();
      System.out.println(count + ") " + res);
    }
    results.close();

    return count;
  }
}
