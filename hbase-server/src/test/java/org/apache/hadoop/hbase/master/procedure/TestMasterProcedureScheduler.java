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

package org.apache.hadoop.hbase.master.procedure;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterProcedureScheduler {
  private static final Log LOG = LogFactory.getLog(TestMasterProcedureScheduler.class);

  private MasterProcedureScheduler queue;
  private Configuration conf;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    queue = new MasterProcedureScheduler(conf, new TableLockManager.NullTableLockManager());
  }

  @After
  public void tearDown() throws IOException {
    assertEquals("proc-queue expected to be empty", 0, queue.size());
    queue.clear();
  }

  @Test
  public void testConcurrentCreateDelete() throws Exception {
    final MasterProcedureScheduler procQueue = queue;
    final TableName table = TableName.valueOf("testtb");
    final AtomicBoolean running = new AtomicBoolean(true);
    final AtomicBoolean failure = new AtomicBoolean(false);
    Thread createThread = new Thread() {
      @Override
      public void run() {
        try {
          TestTableProcedure proc = new TestTableProcedure(1, table,
              TableProcedureInterface.TableOperationType.CREATE);
          while (running.get() && !failure.get()) {
            if (procQueue.tryAcquireTableExclusiveLock(proc, table)) {
              procQueue.releaseTableExclusiveLock(proc, table);
            }
          }
        } catch (Throwable e) {
          LOG.error("create failed", e);
          failure.set(true);
        }
      }
    };

    Thread deleteThread = new Thread() {
      @Override
      public void run() {
        try {
          TestTableProcedure proc = new TestTableProcedure(2, table,
              TableProcedureInterface.TableOperationType.DELETE);
          while (running.get() && !failure.get()) {
            if (procQueue.tryAcquireTableExclusiveLock(proc, table)) {
              procQueue.releaseTableExclusiveLock(proc, table);
            }
            procQueue.markTableAsDeleted(table);
          }
        } catch (Throwable e) {
          LOG.error("delete failed", e);
          failure.set(true);
        }
      }
    };

    createThread.start();
    deleteThread.start();
    for (int i = 0; i < 100 && running.get() && !failure.get(); ++i) {
      Thread.sleep(100);
    }
    running.set(false);
    createThread.join();
    deleteThread.join();
    assertEquals(false, failure.get());
  }

  /**
   * Verify simple create/insert/fetch/delete of the table queue.
   */
  @Test
  public void testSimpleTableOpsQueues() throws Exception {
    final int NUM_TABLES = 10;
    final int NUM_ITEMS = 10;

    int count = 0;
    for (int i = 1; i <= NUM_TABLES; ++i) {
      TableName tableName = TableName.valueOf(String.format("test-%04d", i));
      // insert items
      for (int j = 1; j <= NUM_ITEMS; ++j) {
        queue.addBack(new TestTableProcedure(i * 1000 + j, tableName,
          TableProcedureInterface.TableOperationType.EDIT));
        assertEquals(++count, queue.size());
      }
    }
    assertEquals(NUM_TABLES * NUM_ITEMS, queue.size());

    for (int j = 1; j <= NUM_ITEMS; ++j) {
      for (int i = 1; i <= NUM_TABLES; ++i) {
        Procedure proc = queue.poll();
        assertTrue(proc != null);
        TableName tableName = ((TestTableProcedure)proc).getTableName();
        queue.tryAcquireTableExclusiveLock(proc, tableName);
        queue.releaseTableExclusiveLock(proc, tableName);
        queue.completionCleanup(proc);
        assertEquals(--count, queue.size());
        assertEquals(i * 1000 + j, proc.getProcId());
      }
    }
    assertEquals(0, queue.size());

    for (int i = 1; i <= NUM_TABLES; ++i) {
      TableName tableName = TableName.valueOf(String.format("test-%04d", i));
      // complete the table deletion
      assertTrue(queue.markTableAsDeleted(tableName));
    }
  }

  /**
   * Check that the table queue is not deletable until every procedure
   * in-progress is completed (this is a special case for write-locks).
   */
  @Test
  public void testCreateDeleteTableOperationsWithWriteLock() throws Exception {
    TableName tableName = TableName.valueOf("testtb");

    queue.addBack(new TestTableProcedure(1, tableName,
          TableProcedureInterface.TableOperationType.EDIT));

    // table can't be deleted because one item is in the queue
    assertFalse(queue.markTableAsDeleted(tableName));

    // fetch item and take a lock
    Procedure proc = queue.poll();
    assertEquals(1, proc.getProcId());
    // take the xlock
    assertTrue(queue.tryAcquireTableExclusiveLock(proc, tableName));
    // table can't be deleted because we have the lock
    assertEquals(0, queue.size());
    assertFalse(queue.markTableAsDeleted(tableName));
    // release the xlock
    queue.releaseTableExclusiveLock(proc, tableName);
    // complete the table deletion
    assertTrue(queue.markTableAsDeleted(tableName));
  }

  /**
   * Check that the table queue is not deletable until every procedure
   * in-progress is completed (this is a special case for read-locks).
   */
  @Test
  public void testCreateDeleteTableOperationsWithReadLock() throws Exception {
    final TableName tableName = TableName.valueOf("testtb");
    final int nitems = 2;

    for (int i = 1; i <= nitems; ++i) {
      queue.addBack(new TestTableProcedure(i, tableName,
            TableProcedureInterface.TableOperationType.READ));
    }

    // table can't be deleted because one item is in the queue
    assertFalse(queue.markTableAsDeleted(tableName));

    Procedure[] procs = new Procedure[nitems];
    for (int i = 0; i < nitems; ++i) {
      // fetch item and take a lock
      Procedure proc = procs[i] = queue.poll();
      assertEquals(i + 1, proc.getProcId());
      // take the rlock
      assertTrue(queue.tryAcquireTableSharedLock(proc, tableName));
      // table can't be deleted because we have locks and/or items in the queue
      assertFalse(queue.markTableAsDeleted(tableName));
    }

    for (int i = 0; i < nitems; ++i) {
      // table can't be deleted because we have locks
      assertFalse(queue.markTableAsDeleted(tableName));
      // release the rlock
      queue.releaseTableSharedLock(procs[i], tableName);
    }

    // there are no items and no lock in the queeu
    assertEquals(0, queue.size());
    // complete the table deletion
    assertTrue(queue.markTableAsDeleted(tableName));
  }

  /**
   * Verify the correct logic of RWLocks on the queue
   */
  @Test
  public void testVerifyRwLocks() throws Exception {
    TableName tableName = TableName.valueOf("testtb");
    queue.addBack(new TestTableProcedure(1, tableName,
          TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestTableProcedure(2, tableName,
          TableProcedureInterface.TableOperationType.READ));
    queue.addBack(new TestTableProcedure(3, tableName,
          TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestTableProcedure(4, tableName,
          TableProcedureInterface.TableOperationType.READ));
    queue.addBack(new TestTableProcedure(5, tableName,
          TableProcedureInterface.TableOperationType.READ));

    // Fetch the 1st item and take the write lock
    Procedure proc = queue.poll();
    assertEquals(1, proc.getProcId());
    assertEquals(true, queue.tryAcquireTableExclusiveLock(proc, tableName));

    // Fetch the 2nd item and verify that the lock can't be acquired
    assertEquals(null, queue.poll(0));

    // Release the write lock and acquire the read lock
    queue.releaseTableExclusiveLock(proc, tableName);

    // Fetch the 2nd item and take the read lock
    Procedure rdProc = queue.poll();
    assertEquals(2, rdProc.getProcId());
    assertEquals(true, queue.tryAcquireTableSharedLock(rdProc, tableName));

    // Fetch the 3rd item and verify that the lock can't be acquired
    assertEquals(null, queue.poll(0));

    // release the rdlock of item 2 and take the wrlock for the 3d item
    queue.releaseTableSharedLock(rdProc, tableName);

    // Fetch the 3rd item and take the write lock
    Procedure wrProc = queue.poll();
    assertEquals(true, queue.tryAcquireTableExclusiveLock(wrProc, tableName));

    // Fetch 4th item and verify that the lock can't be acquired
    assertEquals(null, queue.poll(0));

    // Release the write lock and acquire the read lock
    queue.releaseTableExclusiveLock(wrProc, tableName);

    // Fetch the 4th item and take the read lock
    rdProc = queue.poll();
    assertEquals(4, rdProc.getProcId());
    assertEquals(true, queue.tryAcquireTableSharedLock(rdProc, tableName));

    // Fetch the 4th item and take the read lock
    Procedure rdProc2 = queue.poll();
    assertEquals(5, rdProc2.getProcId());
    assertEquals(true, queue.tryAcquireTableSharedLock(rdProc2, tableName));

    // Release 4th and 5th read-lock
    queue.releaseTableSharedLock(rdProc, tableName);
    queue.releaseTableSharedLock(rdProc2, tableName);

    // remove table queue
    assertEquals(0, queue.size());
    assertTrue("queue should be deleted", queue.markTableAsDeleted(tableName));
  }

  @Test
  public void testVerifyNamespaceRwLocks() throws Exception {
    String nsName1 = "ns1";
    String nsName2 = "ns2";
    TableName tableName1 = TableName.valueOf(nsName1, "testtb");
    TableName tableName2 = TableName.valueOf(nsName2, "testtb");
    queue.addBack(new TestNamespaceProcedure(1, nsName1,
          TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestTableProcedure(2, tableName1,
          TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestTableProcedure(3, tableName2,
          TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestNamespaceProcedure(4, nsName2,
          TableProcedureInterface.TableOperationType.EDIT));

    // Fetch the 1st item and take the write lock
    Procedure procNs1 = queue.poll();
    assertEquals(1, procNs1.getProcId());
    assertEquals(true, queue.tryAcquireNamespaceExclusiveLock(procNs1, nsName1));

    // System tables have 2 as default priority
    Procedure procNs2 = queue.poll();
    assertEquals(4, procNs2.getProcId());
    assertEquals(true, queue.tryAcquireNamespaceExclusiveLock(procNs2, nsName2));
    queue.releaseNamespaceExclusiveLock(procNs2, nsName2);
    queue.yield(procNs2);

    // table on ns1 is locked, so we get table on ns2
    procNs2 = queue.poll();
    assertEquals(3, procNs2.getProcId());
    assertEquals(true, queue.tryAcquireTableExclusiveLock(procNs2, tableName2));

    // ns2 is not available (TODO we may avoid this one)
    Procedure procNs2b = queue.poll();
    assertEquals(4, procNs2b.getProcId());
    assertEquals(false, queue.tryAcquireNamespaceExclusiveLock(procNs2b, nsName2));
    queue.yield(procNs2b);

    // release the ns1 lock
    queue.releaseNamespaceExclusiveLock(procNs1, nsName1);

    // we are now able to execute table of ns1
    long procId = queue.poll().getProcId();
    assertEquals(2, procId);

    queue.releaseTableExclusiveLock(procNs2, tableName2);

    // we are now able to execute ns2
    procId = queue.poll().getProcId();
    assertEquals(4, procId);
  }

  @Test
  public void testSharedZkLock() throws Exception {
    final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    final String dir = TEST_UTIL.getDataTestDir("TestSharedZkLock").toString();
    MiniZooKeeperCluster zkCluster = new MiniZooKeeperCluster(conf);
    int zkPort = zkCluster.startup(new File(dir));

    try {
      conf.set("hbase.zookeeper.quorum", "localhost:" + zkPort);

      ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "testSchedWithZkLock", null, false);
      ServerName mockName = ServerName.valueOf("localhost", 60000, 1);
      MasterProcedureScheduler procQueue = new MasterProcedureScheduler(
        conf,
        TableLockManager.createTableLockManager(conf, zkw, mockName));

      final TableName tableName = TableName.valueOf("testtb");
      TestTableProcedure procA =
          new TestTableProcedure(1, tableName, TableProcedureInterface.TableOperationType.READ);
      TestTableProcedure procB =
          new TestTableProcedure(2, tableName, TableProcedureInterface.TableOperationType.READ);

      assertTrue(procQueue.tryAcquireTableSharedLock(procA, tableName));
      assertTrue(procQueue.tryAcquireTableSharedLock(procB, tableName));

      procQueue.releaseTableSharedLock(procA, tableName);
      procQueue.releaseTableSharedLock(procB, tableName);
    } finally {
      zkCluster.shutdown();
    }
  }

  @Test
  public void testXLockWaitingForExecutingSharedLockToRelease() {
    final TableName tableName = TableName.valueOf("testtb");
    final HRegionInfo regionA = new HRegionInfo(tableName, Bytes.toBytes("a"), Bytes.toBytes("b"));

    queue.addBack(new TestRegionProcedure(1, tableName,
        TableProcedureInterface.TableOperationType.ASSIGN, regionA));
    queue.addBack(new TestTableProcedure(2, tableName,
          TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestRegionProcedure(3, tableName,
        TableProcedureInterface.TableOperationType.UNASSIGN, regionA));

    // Fetch the 1st item and take the shared lock
    Procedure proc = queue.poll();
    assertEquals(1, proc.getProcId());
    assertEquals(false, queue.waitRegion(proc, regionA));

    // the xlock operation in the queue can't be executed
    assertEquals(null, queue.poll(0));

    // release the shared lock
    queue.wakeRegion(proc, regionA);

    // Fetch the 2nd item and take the xlock
    proc = queue.poll();
    assertEquals(2, proc.getProcId());
    assertEquals(true, queue.tryAcquireTableExclusiveLock(proc, tableName));

    // everything is locked by the table operation
    assertEquals(null, queue.poll(0));

    // release the table xlock
    queue.releaseTableExclusiveLock(proc, tableName);

    // grab the last item in the queue
    proc = queue.poll();
    assertEquals(3, proc.getProcId());

    // lock and unlock the region
    assertEquals(false, queue.waitRegion(proc, regionA));
    assertEquals(null, queue.poll(0));
    queue.wakeRegion(proc, regionA);
  }

  @Test
  public void testVerifyRegionLocks() throws Exception {
    final TableName tableName = TableName.valueOf("testtb");
    final HRegionInfo regionA = new HRegionInfo(tableName, Bytes.toBytes("a"), Bytes.toBytes("b"));
    final HRegionInfo regionB = new HRegionInfo(tableName, Bytes.toBytes("b"), Bytes.toBytes("c"));
    final HRegionInfo regionC = new HRegionInfo(tableName, Bytes.toBytes("c"), Bytes.toBytes("d"));

    queue.addBack(new TestTableProcedure(1, tableName,
          TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestRegionProcedure(2, tableName,
        TableProcedureInterface.TableOperationType.MERGE, regionA, regionB));
    queue.addBack(new TestRegionProcedure(3, tableName,
        TableProcedureInterface.TableOperationType.SPLIT, regionA));
    queue.addBack(new TestRegionProcedure(4, tableName,
        TableProcedureInterface.TableOperationType.SPLIT, regionB));
    queue.addBack(new TestRegionProcedure(5, tableName,
        TableProcedureInterface.TableOperationType.UNASSIGN, regionC));

    // Fetch the 1st item and take the write lock
    Procedure proc = queue.poll();
    assertEquals(1, proc.getProcId());
    assertEquals(true, queue.tryAcquireTableExclusiveLock(proc, tableName));

    // everything is locked by the table operation
    assertEquals(null, queue.poll(0));

    // release the table lock
    queue.releaseTableExclusiveLock(proc, tableName);

    // Fetch the 2nd item and the the lock on regionA and regionB
    Procedure mergeProc = queue.poll();
    assertEquals(2, mergeProc.getProcId());
    assertEquals(false, queue.waitRegions(mergeProc, tableName, regionA, regionB));

    // Fetch the 3rd item and the try to lock region A which will fail
    // because already locked. this procedure will go in waiting.
    // (this stuff will be explicit until we get rid of the zk-lock)
    Procedure procA = queue.poll();
    assertEquals(3, procA.getProcId());
    assertEquals(true, queue.waitRegions(procA, tableName, regionA));

    // Fetch the 4th item, same story as the 3rd
    Procedure procB = queue.poll();
    assertEquals(4, procB.getProcId());
    assertEquals(true, queue.waitRegions(procB, tableName, regionB));

    // Fetch the 5th item, since it is a non-locked region we are able to execute it
    Procedure procC = queue.poll();
    assertEquals(5, procC.getProcId());
    assertEquals(false, queue.waitRegions(procC, tableName, regionC));

    // 3rd and 4th are in the region suspended queue
    assertEquals(null, queue.poll(0));

    // Release region A-B from merge operation (procId=2)
    queue.wakeRegions(mergeProc, tableName, regionA, regionB);

    // Fetch the 3rd item, now the lock on the region is available
    procA = queue.poll();
    assertEquals(3, procA.getProcId());
    assertEquals(false, queue.waitRegions(procA, tableName, regionA));

    // Fetch the 4th item, now the lock on the region is available
    procB = queue.poll();
    assertEquals(4, procB.getProcId());
    assertEquals(false, queue.waitRegions(procB, tableName, regionB));

    // release the locks on the regions
    queue.wakeRegions(procA, tableName, regionA);
    queue.wakeRegions(procB, tableName, regionB);
    queue.wakeRegions(procC, tableName, regionC);
  }

  @Test
  public void testVerifySubProcRegionLocks() throws Exception {
    final TableName tableName = TableName.valueOf("testVerifySubProcRegionLocks");
    final HRegionInfo regionA = new HRegionInfo(tableName, Bytes.toBytes("a"), Bytes.toBytes("b"));
    final HRegionInfo regionB = new HRegionInfo(tableName, Bytes.toBytes("b"), Bytes.toBytes("c"));
    final HRegionInfo regionC = new HRegionInfo(tableName, Bytes.toBytes("c"), Bytes.toBytes("d"));

    queue.addBack(new TestTableProcedure(1, tableName,
        TableProcedureInterface.TableOperationType.ENABLE));

    // Fetch the 1st item from the queue, "the root procedure" and take the table lock
    Procedure rootProc = queue.poll();
    assertEquals(1, rootProc.getProcId());
    assertEquals(true, queue.tryAcquireTableExclusiveLock(rootProc, tableName));
    assertEquals(null, queue.poll(0));

    // Execute the 1st step of the root-proc.
    // we should get 3 sub-proc back, one for each region.
    // (this step is done by the executor/rootProc, we are simulating it)
    Procedure[] subProcs = new Procedure[] {
      new TestRegionProcedure(1, 2, tableName,
        TableProcedureInterface.TableOperationType.ASSIGN, regionA),
      new TestRegionProcedure(1, 3, tableName,
        TableProcedureInterface.TableOperationType.ASSIGN, regionB),
      new TestRegionProcedure(1, 4, tableName,
        TableProcedureInterface.TableOperationType.ASSIGN, regionC),
    };

    // at this point the rootProc is going in a waiting state
    // and the sub-procedures will be added in the queue.
    // (this step is done by the executor, we are simulating it)
    for (int i = subProcs.length - 1; i >= 0; --i) {
      queue.addFront(subProcs[i]);
    }
    assertEquals(subProcs.length, queue.size());

    // we should be able to fetch and execute all the sub-procs,
    // since they are operating on different regions
    for (int i = 0; i < subProcs.length; ++i) {
      TestRegionProcedure regionProc = (TestRegionProcedure)queue.poll(0);
      assertEquals(subProcs[i].getProcId(), regionProc.getProcId());
      assertEquals(false, queue.waitRegions(regionProc, tableName, regionProc.getRegionInfo()));
    }

    // nothing else in the queue
    assertEquals(null, queue.poll(0));

    // release all the region locks
    for (int i = 0; i < subProcs.length; ++i) {
      TestRegionProcedure regionProc = (TestRegionProcedure)subProcs[i];
      queue.wakeRegions(regionProc, tableName, regionProc.getRegionInfo());
    }

    // nothing else in the queue
    assertEquals(null, queue.poll(0));

    // release the table lock (for the root procedure)
    queue.releaseTableExclusiveLock(rootProc, tableName);
  }

  @Test
  public void testSuspendedTableQueue() throws Exception {
    final TableName tableName = TableName.valueOf("testSuspendedQueue");

    queue.addBack(new TestTableProcedure(1, tableName,
        TableProcedureInterface.TableOperationType.EDIT));
    queue.addBack(new TestTableProcedure(2, tableName,
        TableProcedureInterface.TableOperationType.EDIT));

    Procedure proc = queue.poll();
    assertEquals(1, proc.getProcId());
    assertTrue(queue.tryAcquireTableExclusiveLock(proc, tableName));

    // Suspend
    // TODO: If we want to keep the zk-lock we need to retain the lock on suspend
    ProcedureEvent event = new ProcedureEvent("testSuspendedTableQueueEvent");
    assertEquals(true, queue.waitEvent(event, proc, true));
    queue.releaseTableExclusiveLock(proc, tableName);
    assertEquals(null, queue.poll(0));

    // Resume
    queue.wakeEvent(event);

    proc = queue.poll();
    assertTrue(queue.tryAcquireTableExclusiveLock(proc, tableName));
    assertEquals(1, proc.getProcId());
    queue.releaseTableExclusiveLock(proc, tableName);

    proc = queue.poll();
    assertTrue(queue.tryAcquireTableExclusiveLock(proc, tableName));
    assertEquals(2, proc.getProcId());
    queue.releaseTableExclusiveLock(proc, tableName);
  }

  @Test
  public void testSuspendedProcedure() throws Exception {
    final TableName tableName = TableName.valueOf("testSuspendedProcedure");

    queue.addBack(new TestTableProcedure(1, tableName,
        TableProcedureInterface.TableOperationType.READ));
    queue.addBack(new TestTableProcedure(2, tableName,
        TableProcedureInterface.TableOperationType.READ));

    Procedure proc = queue.poll();
    assertEquals(1, proc.getProcId());

    // suspend
    ProcedureEvent event = new ProcedureEvent("testSuspendedProcedureEvent");
    assertEquals(true, queue.waitEvent(event, proc));

    proc = queue.poll();
    assertEquals(2, proc.getProcId());
    assertEquals(null, queue.poll(0));

    // resume
    queue.wakeEvent(event);

    proc = queue.poll();
    assertEquals(1, proc.getProcId());
    assertEquals(null, queue.poll(0));
  }

  /**
   * Verify that "write" operations for a single table are serialized,
   * but different tables can be executed in parallel.
   */
  @Test(timeout=90000)
  public void testConcurrentWriteOps() throws Exception {
    final TestTableProcSet procSet = new TestTableProcSet(queue);

    final int NUM_ITEMS = 10;
    final int NUM_TABLES = 4;
    final AtomicInteger opsCount = new AtomicInteger(0);
    for (int i = 0; i < NUM_TABLES; ++i) {
      TableName tableName = TableName.valueOf(String.format("testtb-%04d", i));
      for (int j = 1; j < NUM_ITEMS; ++j) {
        procSet.addBack(new TestTableProcedure(i * 100 + j, tableName,
          TableProcedureInterface.TableOperationType.EDIT));
        opsCount.incrementAndGet();
      }
    }
    assertEquals(opsCount.get(), queue.size());

    final Thread[] threads = new Thread[NUM_TABLES * 2];
    final HashSet<TableName> concurrentTables = new HashSet<TableName>();
    final ArrayList<String> failures = new ArrayList<String>();
    final AtomicInteger concurrentCount = new AtomicInteger(0);
    for (int i = 0; i < threads.length; ++i) {
      threads[i] = new Thread() {
        @Override
        public void run() {
          while (opsCount.get() > 0) {
            try {
              Procedure proc = procSet.acquire();
              if (proc == null) {
                queue.signalAll();
                if (opsCount.get() > 0) {
                  continue;
                }
                break;
              }

              TableName tableId = procSet.getTableName(proc);
              synchronized (concurrentTables) {
                assertTrue("unexpected concurrency on " + tableId, concurrentTables.add(tableId));
              }
              assertTrue(opsCount.decrementAndGet() >= 0);
              try {
                long procId = proc.getProcId();
                int concurrent = concurrentCount.incrementAndGet();
                assertTrue("inc-concurrent="+ concurrent +" 1 <= concurrent <= "+ NUM_TABLES,
                  concurrent >= 1 && concurrent <= NUM_TABLES);
                LOG.debug("[S] tableId="+ tableId +" procId="+ procId +" concurrent="+ concurrent);
                Thread.sleep(2000);
                concurrent = concurrentCount.decrementAndGet();
                LOG.debug("[E] tableId="+ tableId +" procId="+ procId +" concurrent="+ concurrent);
                assertTrue("dec-concurrent=" + concurrent, concurrent < NUM_TABLES);
              } finally {
                synchronized (concurrentTables) {
                  assertTrue(concurrentTables.remove(tableId));
                }
                procSet.release(proc);
              }
            } catch (Throwable e) {
              LOG.error("Failed " + e.getMessage(), e);
              synchronized (failures) {
                failures.add(e.getMessage());
              }
            } finally {
              queue.signalAll();
            }
          }
        }
      };
      threads[i].start();
    }
    for (int i = 0; i < threads.length; ++i) {
      threads[i].join();
    }
    assertTrue(failures.toString(), failures.isEmpty());
    assertEquals(0, opsCount.get());
    assertEquals(0, queue.size());

    for (int i = 1; i <= NUM_TABLES; ++i) {
      TableName table = TableName.valueOf(String.format("testtb-%04d", i));
      assertTrue("queue should be deleted, table=" + table, queue.markTableAsDeleted(table));
    }
  }

  public static class TestTableProcSet {
    private final MasterProcedureScheduler queue;

    public TestTableProcSet(final MasterProcedureScheduler queue) {
      this.queue = queue;
    }

    public void addBack(Procedure proc) {
      queue.addBack(proc);
    }

    public void addFront(Procedure proc) {
      queue.addFront(proc);
    }

    public Procedure acquire() {
      Procedure proc = null;
      boolean avail = false;
      while (!avail) {
        proc = queue.poll();
        if (proc == null) break;
        switch (getTableOperationType(proc)) {
          case CREATE:
          case DELETE:
          case EDIT:
            avail = queue.tryAcquireTableExclusiveLock(proc, getTableName(proc));
            break;
          case READ:
            avail = queue.tryAcquireTableSharedLock(proc, getTableName(proc));
            break;
          default:
            throw new UnsupportedOperationException();
        }
        if (!avail) {
          addFront(proc);
          LOG.debug("yield procId=" + proc);
        }
      }
      return proc;
    }

    public void release(Procedure proc) {
      switch (getTableOperationType(proc)) {
        case CREATE:
        case DELETE:
        case EDIT:
          queue.releaseTableExclusiveLock(proc, getTableName(proc));
          break;
        case READ:
          queue.releaseTableSharedLock(proc, getTableName(proc));
          break;
      }
    }

    public TableName getTableName(Procedure proc) {
      return ((TableProcedureInterface)proc).getTableName();
    }

    public TableProcedureInterface.TableOperationType getTableOperationType(Procedure proc) {
      return ((TableProcedureInterface)proc).getTableOperationType();
    }
  }

  public static class TestTableProcedure extends TestProcedure
      implements TableProcedureInterface {
    private final TableOperationType opType;
    private final TableName tableName;

    public TestTableProcedure() {
      throw new UnsupportedOperationException("recovery should not be triggered here");
    }

    public TestTableProcedure(long procId, TableName tableName, TableOperationType opType) {
      this(-1, procId, tableName, opType);
    }

    public TestTableProcedure(long parentProcId, long procId, TableName tableName,
        TableOperationType opType) {
      super(procId, parentProcId);
      this.tableName = tableName;
      this.opType = opType;
    }
    @Override
    public TableName getTableName() {
      return tableName;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return opType;
    }

    @Override
    public void toStringClassDetails(final StringBuilder sb) {
      sb.append(getClass().getSimpleName());
      sb.append(" (table=");
      sb.append(getTableName());
      sb.append(")");
    }
  }

  public static class TestRegionProcedure extends TestTableProcedure {
    private final HRegionInfo[] regionInfo;

    public TestRegionProcedure() {
      throw new UnsupportedOperationException("recovery should not be triggered here");
    }

    public TestRegionProcedure(long procId, TableName tableName, TableOperationType opType,
        HRegionInfo... regionInfo) {
      this(-1, procId, tableName, opType, regionInfo);
    }

    public TestRegionProcedure(long parentProcId, long procId, TableName tableName,
        TableOperationType opType, HRegionInfo... regionInfo) {
      super(parentProcId, procId, tableName, opType);
      this.regionInfo = regionInfo;
    }

    public HRegionInfo[] getRegionInfo() {
      return regionInfo;
    }

    @Override
    public void toStringClassDetails(final StringBuilder sb) {
      sb.append(getClass().getSimpleName());
      sb.append(" (region=");
      sb.append(getRegionInfo());
      sb.append(")");
    }
  }

  public static class TestNamespaceProcedure extends TestProcedure
      implements TableProcedureInterface {
    private final TableOperationType opType;
    private final String nsName;

    public TestNamespaceProcedure() {
      throw new UnsupportedOperationException("recovery should not be triggered here");
    }

    public TestNamespaceProcedure(long procId, String nsName, TableOperationType opType) {
      super(procId);
      this.nsName = nsName;
      this.opType = opType;
    }

    @Override
    public TableName getTableName() {
      return TableName.NAMESPACE_TABLE_NAME;
    }

    @Override
    public TableOperationType getTableOperationType() {
      return opType;
    }
  }
}
