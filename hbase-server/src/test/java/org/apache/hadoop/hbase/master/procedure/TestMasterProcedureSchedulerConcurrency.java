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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.procedure.TestMasterProcedureScheduler.TestTableProcedure;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterProcedureSchedulerConcurrency {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterProcedureSchedulerConcurrency.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestMasterProcedureSchedulerConcurrency.class);

  private MasterProcedureScheduler queue;

  @Before
  public void setUp() throws IOException {
    queue = new MasterProcedureScheduler(pid -> null);
    queue.start();
  }

  @After
  public void tearDown() throws IOException {
    assertEquals("proc-queue expected to be empty", 0, queue.size());
    queue.stop();
    queue.clear();
  }

  /**
   * Verify that "write" operations for a single table are serialized,
   * but different tables can be executed in parallel.
   */
  @Test
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
    final HashSet<TableName> concurrentTables = new HashSet<>();
    final ArrayList<String> failures = new ArrayList<>();
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
      final TableName table = TableName.valueOf(String.format("testtb-%04d", i));
      final TestTableProcedure dummyProc = new TestTableProcedure(100, table,
        TableProcedureInterface.TableOperationType.DELETE);
      assertTrue("queue should be deleted, table=" + table,
        queue.markTableAsDeleted(table, dummyProc));
    }
  }

  @Test
  public void testMasterProcedureSchedulerPerformanceEvaluation() throws Exception {
    // Make sure the tool does not get stuck
    MasterProcedureSchedulerPerformanceEvaluation.main(new String[] {
      "-num_ops", "1000"
    });
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
      boolean waiting = true;
      while (waiting && queue.size() > 0) {
        proc = queue.poll(100000000L);
        if (proc == null) continue;
        switch (getTableOperationType(proc)) {
          case CREATE:
          case DELETE:
          case EDIT:
            waiting = queue.waitTableExclusiveLock(proc, getTableName(proc));
            break;
          case READ:
            waiting = queue.waitTableSharedLock(proc, getTableName(proc));
            break;
          default:
            throw new UnsupportedOperationException();
        }
      }
      return proc;
    }

    public void release(Procedure proc) {
      switch (getTableOperationType(proc)) {
        case CREATE:
        case DELETE:
        case EDIT:
          queue.wakeTableExclusiveLock(proc, getTableName(proc));
          break;
        case READ:
          queue.wakeTableSharedLock(proc, getTableName(proc));
          break;
        default:
          throw new UnsupportedOperationException();
      }
    }

    public TableName getTableName(Procedure proc) {
      return ((TableProcedureInterface)proc).getTableName();
    }

    public TableProcedureInterface.TableOperationType getTableOperationType(Procedure proc) {
      return ((TableProcedureInterface)proc).getTableOperationType();
    }
  }
}
