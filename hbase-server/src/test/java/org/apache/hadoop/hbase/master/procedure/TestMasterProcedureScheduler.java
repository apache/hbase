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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
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
    assertEquals(0, queue.size());
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
          while (running.get() && !failure.get()) {
            if (procQueue.tryAcquireTableExclusiveLock(table, "create")) {
              procQueue.releaseTableExclusiveLock(table);
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
          while (running.get() && !failure.get()) {
            if (procQueue.tryAcquireTableExclusiveLock(table, "delete")) {
              procQueue.releaseTableExclusiveLock(table);
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
        queue.tryAcquireTableExclusiveLock(tableName, "test");
        queue.releaseTableExclusiveLock(tableName);
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
    assertEquals(1, queue.poll().getProcId());
    // take the xlock
    assertTrue(queue.tryAcquireTableExclusiveLock(tableName, "write"));
    // table can't be deleted because we have the lock
    assertEquals(0, queue.size());
    assertFalse(queue.markTableAsDeleted(tableName));
    // release the xlock
    queue.releaseTableExclusiveLock(tableName);
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

    for (int i = 1; i <= nitems; ++i) {
      // fetch item and take a lock
      assertEquals(i, queue.poll().getProcId());
      // take the rlock
      assertTrue(queue.tryAcquireTableSharedLock(tableName, "read " + i));
      // table can't be deleted because we have locks and/or items in the queue
      assertFalse(queue.markTableAsDeleted(tableName));
    }

    for (int i = 1; i <= nitems; ++i) {
      // table can't be deleted because we have locks
      assertFalse(queue.markTableAsDeleted(tableName));
      // release the rlock
      queue.releaseTableSharedLock(tableName);
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
    long procId = queue.poll().getProcId();
    assertEquals(1, procId);
    assertEquals(true, queue.tryAcquireTableExclusiveLock(tableName, "write " + procId));

    // Fetch the 2nd item and verify that the lock can't be acquired
    assertEquals(null, queue.poll(0));

    // Release the write lock and acquire the read lock
    queue.releaseTableExclusiveLock(tableName);

    // Fetch the 2nd item and take the read lock
    procId = queue.poll().getProcId();
    assertEquals(2, procId);
    assertEquals(true, queue.tryAcquireTableSharedLock(tableName, "read " + procId));

    // Fetch the 3rd item and verify that the lock can't be acquired
    procId = queue.poll().getProcId();
    assertEquals(3, procId);
    assertEquals(false, queue.tryAcquireTableExclusiveLock(tableName, "write " + procId));

    // release the rdlock of item 2 and take the wrlock for the 3d item
    queue.releaseTableSharedLock(tableName);
    assertEquals(true, queue.tryAcquireTableExclusiveLock(tableName, "write " + procId));

    // Fetch 4th item and verify that the lock can't be acquired
    assertEquals(null, queue.poll(0));

    // Release the write lock and acquire the read lock
    queue.releaseTableExclusiveLock(tableName);

    // Fetch the 4th item and take the read lock
    procId = queue.poll().getProcId();
    assertEquals(4, procId);
    assertEquals(true, queue.tryAcquireTableSharedLock(tableName, "read " + procId));

    // Fetch the 4th item and take the read lock
    procId = queue.poll().getProcId();
    assertEquals(5, procId);
    assertEquals(true, queue.tryAcquireTableSharedLock(tableName, "read " + procId));

    // Release 4th and 5th read-lock
    queue.releaseTableSharedLock(tableName);
    queue.releaseTableSharedLock(tableName);

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
    long procId = queue.poll().getProcId();
    assertEquals(1, procId);
    assertEquals(true, queue.tryAcquireNamespaceExclusiveLock(nsName1));

    // System tables have 2 as default priority
    Procedure proc = queue.poll();
    assertEquals(4, proc.getProcId());
    assertEquals(true, queue.tryAcquireNamespaceExclusiveLock(nsName2));
    queue.releaseNamespaceExclusiveLock(nsName2);
    queue.yield(proc);

    // table on ns1 is locked, so we get table on ns2
    procId = queue.poll().getProcId();
    assertEquals(3, procId);
    assertEquals(true, queue.tryAcquireTableExclusiveLock(tableName2, "lock " + procId));

    // ns2 is not available (TODO we may avoid this one)
    proc = queue.poll();
    assertEquals(4, proc.getProcId());
    assertEquals(false, queue.tryAcquireNamespaceExclusiveLock(nsName2));
    queue.yield(proc);

    // release the ns1 lock
    queue.releaseNamespaceExclusiveLock(nsName1);

    // we are now able to execute table of ns1
    procId = queue.poll().getProcId();
    assertEquals(2, procId);

    queue.releaseTableExclusiveLock(tableName2);

    // we are now able to execute ns2
    procId = queue.poll().getProcId();
    assertEquals(4, procId);
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
              TableProcedureInterface proc = procSet.acquire();
              if (proc == null) {
                queue.signalAll();
                if (opsCount.get() > 0) {
                  continue;
                }
                break;
              }
              synchronized (concurrentTables) {
                assertTrue("unexpected concurrency on " + proc.getTableName(),
                  concurrentTables.add(proc.getTableName()));
              }
              assertTrue(opsCount.decrementAndGet() >= 0);
              try {
                long procId = ((Procedure)proc).getProcId();
                TableName tableId = proc.getTableName();
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
                  assertTrue(concurrentTables.remove(proc.getTableName()));
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
    private Map<Long, TableProcedureInterface> procsMap =
      new ConcurrentHashMap<Long, TableProcedureInterface>();

    public TestTableProcSet(final MasterProcedureScheduler queue) {
      this.queue = queue;
    }

    public void addBack(TableProcedureInterface tableProc) {
      Procedure proc = (Procedure)tableProc;
      procsMap.put(proc.getProcId(), tableProc);
      queue.addBack(proc);
    }

    public void addFront(TableProcedureInterface tableProc) {
      Procedure proc = (Procedure)tableProc;
      procsMap.put(proc.getProcId(), tableProc);
      queue.addFront(proc);
    }

    public TableProcedureInterface acquire() {
      TableProcedureInterface proc = null;
      boolean avail = false;
      while (!avail) {
        Procedure xProc = queue.poll();
        proc = xProc != null ? procsMap.remove(xProc.getProcId()) : null;
        if (proc == null) break;
        switch (proc.getTableOperationType()) {
          case CREATE:
          case DELETE:
          case EDIT:
            avail = queue.tryAcquireTableExclusiveLock(proc.getTableName(),
              "op="+ proc.getTableOperationType());
            break;
          case READ:
            avail = queue.tryAcquireTableSharedLock(proc.getTableName(),
              "op="+ proc.getTableOperationType());
            break;
        }
        if (!avail) {
          addFront(proc);
          LOG.debug("yield procId=" + proc);
        }
      }
      return proc;
    }

    public void release(TableProcedureInterface proc) {
      switch (proc.getTableOperationType()) {
        case CREATE:
        case DELETE:
        case EDIT:
          queue.releaseTableExclusiveLock(proc.getTableName());
          break;
        case READ:
          queue.releaseTableSharedLock(proc.getTableName());
          break;
      }
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
      super(procId);
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
