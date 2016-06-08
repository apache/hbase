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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.InterProcessLock;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the default table lock manager
 */
@Category({MasterTests.class, LargeTests.class})
public class TestTableLockManager {

  private static final Log LOG =
    LogFactory.getLog(TestTableLockManager.class);

  private static final TableName TABLE_NAME =
      TableName.valueOf("TestTableLevelLocks");

  private static final byte[] FAMILY = Bytes.toBytes("f1");

  private static final byte[] NEW_FAMILY = Bytes.toBytes("f2");

  private final HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();

  private static final CountDownLatch deleteColumn = new CountDownLatch(1);
  private static final CountDownLatch addColumn = new CountDownLatch(1);

  public void prepareMiniCluster() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY);
  }

  public void prepareMiniZkCluster() throws Exception {
    TEST_UTIL.startMiniZKCluster(1);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static class TestLockTimeoutExceptionMasterObserver extends BaseMasterObserver {
    @Override
    public void preDeleteColumnFamilyAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName, byte[] columnFamily) throws IOException {
      deleteColumn.countDown();
    }
    @Override
    public void postCompletedDeleteColumnFamilyAction(
        ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName, byte[] columnFamily) throws IOException {
      Threads.sleep(10000);
    }

    @Override
    public void preAddColumnFamilyAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName, HColumnDescriptor columnFamily) throws IOException {
      fail("Add column should have timeouted out for acquiring the table lock");
    }
  }

  @Test(timeout = 600000)
  public void testAlterAndDisable() throws Exception {
    prepareMiniCluster();
    // Send a request to alter a table, then sleep during
    // the alteration phase. In the mean time, from another
    // thread, send a request to disable, and then delete a table.

    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    master.getMasterCoprocessorHost().load(TestAlterAndDisableMasterObserver.class,
            0, TEST_UTIL.getConfiguration());

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<Object> alterTableFuture = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        Admin admin = TEST_UTIL.getHBaseAdmin();
        admin.addColumnFamily(TABLE_NAME, new HColumnDescriptor(NEW_FAMILY));
        LOG.info("Added new column family");
        HTableDescriptor tableDesc = admin.getTableDescriptor(TABLE_NAME);
        assertTrue(tableDesc.getFamiliesKeys().contains(NEW_FAMILY));
        return null;
      }
    });
    Future<Object> disableTableFuture = executor.submit(new Callable<Object>() {
      @Override
      public Object call() throws Exception {
        Admin admin = TEST_UTIL.getHBaseAdmin();
        admin.disableTable(TABLE_NAME);
        assertTrue(admin.isTableDisabled(TABLE_NAME));
        admin.deleteTable(TABLE_NAME);
        assertFalse(admin.tableExists(TABLE_NAME));
        return null;
      }
    });

    try {
      disableTableFuture.get();
      alterTableFuture.get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AssertionError) {
        throw (AssertionError) e.getCause();
      }
      throw e;
    }
  }

  public static class TestAlterAndDisableMasterObserver extends BaseMasterObserver {
    @Override
    public void preAddColumnFamilyAction(ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName, HColumnDescriptor columnFamily) throws IOException {
      LOG.debug("addColumn called");
      addColumn.countDown();
    }

    @Override
    public void postCompletedAddColumnFamilyAction(
        ObserverContext<MasterCoprocessorEnvironment> ctx,
        TableName tableName, HColumnDescriptor columnFamily) throws IOException {
      Threads.sleep(6000);
      try {
        ctx.getEnvironment().getMasterServices().checkTableModifiable(tableName);
      } catch(TableNotDisabledException expected) {
        //pass
        return;
      } catch(IOException ex) {
      }
      fail("was expecting the table to be enabled");
    }

    @Override
    public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                TableName tableName) throws IOException {
      try {
        LOG.debug("Waiting for addColumn to be processed first");
        //wait for addColumn to be processed first
        addColumn.await();
        LOG.debug("addColumn started, we can continue");
      } catch (InterruptedException ex) {
        LOG.warn("Sleep interrupted while waiting for addColumn countdown");
      }
    }

    @Override
    public void postDisableTableHandler(ObserverContext<MasterCoprocessorEnvironment> ctx,
                                        TableName tableName) throws IOException {
      Threads.sleep(3000);
    }
  }

  @Test(timeout = 600000)
  public void testDelete() throws Exception {
    prepareMiniCluster();

    Admin admin = TEST_UTIL.getHBaseAdmin();
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);

    //ensure that znode for the table node has been deleted
    final ZooKeeperWatcher zkWatcher = TEST_UTIL.getZooKeeperWatcher();
    final String znode = ZKUtil.joinZNode(zkWatcher.tableLockZNode, TABLE_NAME.getNameAsString());

    TEST_UTIL.waitFor(5000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        int ver = ZKUtil.checkExists(zkWatcher, znode);
        return ver < 0;
      }
    });
    int ver = ZKUtil.checkExists(zkWatcher,
      ZKUtil.joinZNode(zkWatcher.tableLockZNode, TABLE_NAME.getNameAsString()));
    assertTrue("Unexpected znode version " + ver, ver < 0);

  }

  public class TableLockCounter implements InterProcessLock.MetadataHandler {

    private int lockCount = 0;

    @Override
    public void handleMetadata(byte[] metadata) {
      lockCount++;
    }

    public void reset() {
      lockCount = 0;
    }

    public int getLockCount() {
      return lockCount;
    }
  }

  @Test(timeout = 600000)
  public void testReapAllTableLocks() throws Exception {
    prepareMiniZkCluster();
    ServerName serverName = ServerName.valueOf("localhost:10000", 0);
    final TableLockManager lockManager = TableLockManager.createTableLockManager(
        TEST_UTIL.getConfiguration(), TEST_UTIL.getZooKeeperWatcher(), serverName);

    String tables[] = {"table1", "table2", "table3", "table4"};
    ExecutorService executor = Executors.newFixedThreadPool(6);

    final CountDownLatch writeLocksObtained = new CountDownLatch(4);
    final CountDownLatch writeLocksAttempted = new CountDownLatch(10);
    //TODO: read lock tables

    //6 threads will be stuck waiting for the table lock
    for (int i = 0; i < tables.length; i++) {
      final String table = tables[i];
      for (int j = 0; j < i+1; j++) { //i+1 write locks attempted for table[i]
        executor.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            writeLocksAttempted.countDown();
            lockManager.writeLock(TableName.valueOf(table),
                    "testReapAllTableLocks").acquire();
            writeLocksObtained.countDown();
            return null;
          }
        });
      }
    }

    writeLocksObtained.await();
    writeLocksAttempted.await();

    TableLockCounter counter = new TableLockCounter();
    do {
      counter.reset();
      lockManager.visitAllLocks(counter);
      Thread.sleep(10);
    } while (counter.getLockCount() != 10);

    //now reap all table locks
    lockManager.reapWriteLocks();
    TEST_UTIL.getConfiguration().setInt(TableLockManager.TABLE_WRITE_LOCK_TIMEOUT_MS, 0);
    TableLockManager zeroTimeoutLockManager = TableLockManager.createTableLockManager(
          TEST_UTIL.getConfiguration(), TEST_UTIL.getZooKeeperWatcher(), serverName);

    //should not throw table lock timeout exception
    zeroTimeoutLockManager.writeLock(
        TableName.valueOf(tables[tables.length - 1]),
        "zero timeout")
      .acquire();

    executor.shutdownNow();
  }

  @Test(timeout = 600000)
  public void testTableReadLock() throws Exception {
    // test plan: write some data to the table. Continuously alter the table and
    // force splits
    // concurrently until we have 5 regions. verify the data just in case.
    // Every region should contain the same table descriptor
    // This is not an exact test
    prepareMiniCluster();
    LoadTestTool loadTool = new LoadTestTool();
    loadTool.setConf(TEST_UTIL.getConfiguration());
    int numKeys = 10000;
    final TableName tableName = TableName.valueOf("testTableReadLock");
    final Admin admin = TEST_UTIL.getHBaseAdmin();
    final HTableDescriptor desc = new HTableDescriptor(tableName);
    final byte[] family = Bytes.toBytes("test_cf");
    desc.addFamily(new HColumnDescriptor(family));
    admin.createTable(desc); // create with one region

    // write some data, not much
    int ret = loadTool.run(new String[] { "-tn", tableName.getNameAsString(), "-write",
        String.format("%d:%d:%d", 1, 10, 10), "-num_keys", String.valueOf(numKeys), "-skip_init" });
    if (0 != ret) {
      String errorMsg = "Load failed with error code " + ret;
      LOG.error(errorMsg);
      fail(errorMsg);
    }

    int familyValues = admin.getTableDescriptor(tableName).getFamily(family).getValues().size();
    StoppableImplementation stopper = new StoppableImplementation();
    final ChoreService choreService = new ChoreService("TEST_SERVER_NAME");

    //alter table every 10 sec
    ScheduledChore alterThread = new ScheduledChore("Alter Chore", stopper, 10000) {
      @Override
      protected void chore() {
        Random random = new Random();
        try {
          HTableDescriptor htd = admin.getTableDescriptor(tableName);
          String val = String.valueOf(random.nextInt());
          htd.getFamily(family).setValue(val, val);
          desc.getFamily(family).setValue(val, val); // save it for later
                                                     // control
          admin.modifyTable(tableName, htd);
        } catch (Exception ex) {
          LOG.warn("Caught exception", ex);
          fail(ex.getMessage());
        }
      }
    };

    //split table every 5 sec
    ScheduledChore splitThread = new ScheduledChore("Split thread", stopper, 5000) {
      @Override
      public void chore() {
        try {
          HRegion region = TEST_UTIL.getSplittableRegion(tableName, -1);
          if (region != null) {
            byte[] regionName = region.getRegionInfo().getRegionName();
            admin.flushRegion(regionName);
            admin.compactRegion(regionName);
            admin.splitRegion(regionName);
          } else {
            LOG.warn("Could not find suitable region for the table.  Possibly the " +
              "region got closed and the attempts got over before " +
              "the region could have got reassigned.");
          }
        } catch (NotServingRegionException nsre) {
          // the region may be in transition
          LOG.warn("Caught exception", nsre);
        } catch (Exception ex) {
          LOG.warn("Caught exception", ex);
          fail(ex.getMessage());
        }
      }
    };

    choreService.scheduleChore(alterThread);
    choreService.scheduleChore(splitThread);
    TEST_UTIL.waitTableEnabled(tableName);
    while (true) {
      List<HRegionInfo> regions = admin.getTableRegions(tableName);
      LOG.info(String.format("Table #regions: %d regions: %s:", regions.size(), regions));
      assertEquals(admin.getTableDescriptor(tableName), desc);
      for (HRegion region : TEST_UTIL.getMiniHBaseCluster().getRegions(tableName)) {
        HTableDescriptor regionTableDesc = region.getTableDesc();
        assertEquals(desc, regionTableDesc);
      }
      if (regions.size() >= 5) {
        break;
      }
      Threads.sleep(1000);
    }
    stopper.stop("test finished");

    int newFamilyValues = admin.getTableDescriptor(tableName).getFamily(family).getValues().size();
    LOG.info(String.format("Altered the table %d times", newFamilyValues - familyValues));
    assertTrue(newFamilyValues > familyValues); // at least one alter went
                                                // through

    ret = loadTool.run(new String[] { "-tn", tableName.getNameAsString(), "-read", "100:10",
        "-num_keys", String.valueOf(numKeys), "-skip_init" });
    if (0 != ret) {
      String errorMsg = "Verify failed with error code " + ret;
      LOG.error(errorMsg);
      fail(errorMsg);
    }

    admin.close();
    choreService.shutdown();
  }

}
