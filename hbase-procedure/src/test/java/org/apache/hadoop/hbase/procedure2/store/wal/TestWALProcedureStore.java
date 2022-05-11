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
package org.apache.hadoop.hbase.procedure2.store.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.LoadCounter;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.SequentialProcedure;
import org.apache.hadoop.hbase.procedure2.store.LeaseRecovery;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Int64Value;

@Category({ MasterTests.class, SmallTests.class })
public class TestWALProcedureStore {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALProcedureStore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestWALProcedureStore.class);

  private static final int PROCEDURE_STORE_SLOTS = 1;

  private WALProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  private void setupConfig(final Configuration conf) {
    conf.setBoolean(WALProcedureStore.EXEC_WAL_CLEANUP_ON_LOAD_CONF_KEY, true);
  }

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    testDir = htu.getDataTestDir();
    htu.getConfiguration().set(HConstants.HBASE_DIR, testDir.toString());
    fs = testDir.getFileSystem(htu.getConfiguration());
    htu.getConfiguration().set(HConstants.HBASE_DIR, testDir.toString());
    assertTrue(testDir.depth() > 1);

    setupConfig(htu.getConfiguration());
    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), logDir);
    procStore.start(PROCEDURE_STORE_SLOTS);
    procStore.recoverLease();
    procStore.load(new LoadCounter());
  }

  @After
  public void tearDown() throws IOException {
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  private void storeRestart(ProcedureStore.ProcedureLoader loader) throws Exception {
    ProcedureTestingUtility.storeRestart(procStore, loader);
  }

  @Test
  public void testEmptyRoll() throws Exception {
    for (int i = 0; i < 10; ++i) {
      procStore.periodicRollForTesting();
    }
    assertEquals(1, procStore.getActiveLogs().size());
    FileStatus[] status = fs.listStatus(logDir);
    assertEquals(1, status.length);
  }

  @Test
  public void testRestartWithoutData() throws Exception {
    for (int i = 0; i < 10; ++i) {
      final LoadCounter loader = new LoadCounter();
      storeRestart(loader);
    }
    LOG.info("ACTIVE WALs " + procStore.getActiveLogs());
    assertEquals(1, procStore.getActiveLogs().size());
    FileStatus[] status = fs.listStatus(logDir);
    assertEquals(1, status.length);
  }

  /**
   * Tests that tracker for all old logs are loaded back after procedure store is restarted.
   */
  @Test
  public void trackersLoadedForAllOldLogs() throws Exception {
    for (int i = 0; i <= 20; ++i) {
      procStore.insert(new TestProcedure(i), null);
      if (i > 0 && (i % 5) == 0) {
        LoadCounter loader = new LoadCounter();
        storeRestart(loader);
      }
    }
    assertEquals(5, procStore.getActiveLogs().size());
    for (int i = 0; i < procStore.getActiveLogs().size() - 1; ++i) {
      ProcedureStoreTracker tracker = procStore.getActiveLogs().get(i).getTracker();
      assertTrue(tracker != null && !tracker.isEmpty());
    }
  }

  @Test
  public void testWalCleanerSequentialClean() throws Exception {
    final Procedure<?>[] procs = new Procedure[5];
    ArrayList<ProcedureWALFile> logs = null;

    // Insert procedures and roll wal after every insert.
    for (int i = 0; i < procs.length; i++) {
      procs[i] = new TestSequentialProcedure();
      procStore.insert(procs[i], null);
      procStore.rollWriterForTesting();
      logs = procStore.getActiveLogs();
      assertEquals(logs.size(), i + 2); // Extra 1 for current ongoing wal.
    }

    // Delete procedures in sequential order make sure that only the corresponding wal is deleted
    // from logs list.
    final int[] deleteOrder = new int[] { 0, 1, 2, 3, 4 };
    for (int i = 0; i < deleteOrder.length; i++) {
      procStore.delete(procs[deleteOrder[i]].getProcId());
      procStore.removeInactiveLogsForTesting();
      assertFalse(logs.get(deleteOrder[i]).toString(),
        procStore.getActiveLogs().contains(logs.get(deleteOrder[i])));
      assertEquals(procStore.getActiveLogs().size(), procs.length - i);
    }
  }

  // Test that wal cleaner doesn't create holes in wal files list i.e. it only deletes files if
  // they are in the starting of the list.
  @Test
  public void testWalCleanerNoHoles() throws Exception {
    final Procedure<?>[] procs = new Procedure[5];
    ArrayList<ProcedureWALFile> logs = null;
    // Insert procedures and roll wal after every insert.
    for (int i = 0; i < procs.length; i++) {
      procs[i] = new TestSequentialProcedure();
      procStore.insert(procs[i], null);
      procStore.rollWriterForTesting();
      logs = procStore.getActiveLogs();
      assertEquals(i + 2, logs.size()); // Extra 1 for current ongoing wal.
    }

    for (int i = 1; i < procs.length; i++) {
      procStore.delete(procs[i].getProcId());
    }
    assertEquals(procs.length + 1, procStore.getActiveLogs().size());
    procStore.delete(procs[0].getProcId());
    assertEquals(1, procStore.getActiveLogs().size());
  }

  @Test
  public void testWalCleanerUpdates() throws Exception {
    TestSequentialProcedure p1 = new TestSequentialProcedure();
    TestSequentialProcedure p2 = new TestSequentialProcedure();
    procStore.insert(p1, null);
    procStore.insert(p2, null);
    procStore.rollWriterForTesting();
    ProcedureWALFile firstLog = procStore.getActiveLogs().get(0);
    procStore.update(p1);
    procStore.rollWriterForTesting();
    procStore.update(p2);
    procStore.rollWriterForTesting();
    procStore.removeInactiveLogsForTesting();
    assertFalse(procStore.getActiveLogs().contains(firstLog));
  }

  @Test
  public void testWalCleanerUpdatesDontLeaveHoles() throws Exception {
    TestSequentialProcedure p1 = new TestSequentialProcedure();
    TestSequentialProcedure p2 = new TestSequentialProcedure();
    procStore.insert(p1, null);
    procStore.insert(p2, null);
    procStore.rollWriterForTesting(); // generates first log with p1 + p2
    ProcedureWALFile log1 = procStore.getActiveLogs().get(0);
    procStore.update(p2);
    procStore.rollWriterForTesting(); // generates second log with p2
    ProcedureWALFile log2 = procStore.getActiveLogs().get(1);
    procStore.update(p2);
    procStore.rollWriterForTesting(); // generates third log with p2
    procStore.removeInactiveLogsForTesting(); // Shouldn't remove 2nd log.
    assertEquals(4, procStore.getActiveLogs().size());
    procStore.update(p1);
    procStore.rollWriterForTesting(); // generates fourth log with p1
    procStore.removeInactiveLogsForTesting(); // Should remove first two logs.
    assertEquals(3, procStore.getActiveLogs().size());
    assertFalse(procStore.getActiveLogs().contains(log1));
    assertFalse(procStore.getActiveLogs().contains(log2));
  }

  @Test
  public void testWalCleanerWithEmptyRolls() throws Exception {
    final Procedure<?>[] procs = new Procedure[3];
    for (int i = 0; i < procs.length; ++i) {
      procs[i] = new TestSequentialProcedure();
      procStore.insert(procs[i], null);
    }
    assertEquals(1, procStore.getActiveLogs().size());
    procStore.rollWriterForTesting();
    assertEquals(2, procStore.getActiveLogs().size());
    procStore.rollWriterForTesting();
    assertEquals(3, procStore.getActiveLogs().size());

    for (int i = 0; i < procs.length; ++i) {
      procStore.update(procs[i]);
      procStore.rollWriterForTesting();
      procStore.rollWriterForTesting();
      if (i < (procs.length - 1)) {
        assertEquals(3 + ((i + 1) * 2), procStore.getActiveLogs().size());
      }
    }
    assertEquals(7, procStore.getActiveLogs().size());

    for (int i = 0; i < procs.length; ++i) {
      procStore.delete(procs[i].getProcId());
      assertEquals(7 - ((i + 1) * 2), procStore.getActiveLogs().size());
    }
    assertEquals(1, procStore.getActiveLogs().size());
  }

  @Test
  public void testEmptyLogLoad() throws Exception {
    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(0, loader.getMaxProcId());
    assertEquals(0, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  @Test
  public void testLoad() throws Exception {
    Set<Long> procIds = new HashSet<>();

    // Insert something in the log
    Procedure<?> proc1 = new TestSequentialProcedure();
    procIds.add(proc1.getProcId());
    procStore.insert(proc1, null);

    Procedure<?> proc2 = new TestSequentialProcedure();
    Procedure<?>[] child2 = new Procedure[2];
    child2[0] = new TestSequentialProcedure();
    child2[1] = new TestSequentialProcedure();

    procIds.add(proc2.getProcId());
    procIds.add(child2[0].getProcId());
    procIds.add(child2[1].getProcId());
    procStore.insert(proc2, child2);

    // Verify that everything is there
    verifyProcIdsOnRestart(procIds);

    // Update and delete something
    procStore.update(proc1);
    procStore.update(child2[1]);
    procStore.delete(child2[1].getProcId());
    procIds.remove(child2[1].getProcId());

    // Verify that everything is there
    verifyProcIdsOnRestart(procIds);

    // Remove 4 byte from the trailers
    procStore.stop(false);
    FileStatus[] logs = fs.listStatus(logDir);
    assertEquals(3, logs.length);
    for (int i = 0; i < logs.length; ++i) {
      corruptLog(logs[i], 4);
    }
    verifyProcIdsOnRestart(procIds);
  }

  @Test
  public void testNoTrailerDoubleRestart() throws Exception {
    // log-0001: proc 0, 1 and 2 are inserted
    Procedure<?> proc0 = new TestSequentialProcedure();
    procStore.insert(proc0, null);
    Procedure<?> proc1 = new TestSequentialProcedure();
    procStore.insert(proc1, null);
    Procedure<?> proc2 = new TestSequentialProcedure();
    procStore.insert(proc2, null);
    procStore.rollWriterForTesting();

    // log-0002: proc 1 deleted
    procStore.delete(proc1.getProcId());
    procStore.rollWriterForTesting();

    // log-0003: proc 2 is update
    procStore.update(proc2);
    procStore.rollWriterForTesting();

    // log-0004: proc 2 deleted
    procStore.delete(proc2.getProcId());

    // stop the store and remove the trailer
    procStore.stop(false);
    FileStatus[] logs = fs.listStatus(logDir);
    assertEquals(4, logs.length);
    for (int i = 0; i < logs.length; ++i) {
      corruptLog(logs[i], 4);
    }

    // Test Load 1
    // Restart the store (avoid cleaning up the files, to check the rebuilded trackers)
    htu.getConfiguration().setBoolean(WALProcedureStore.EXEC_WAL_CLEANUP_ON_LOAD_CONF_KEY, false);
    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(1, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());

    // Test Load 2
    assertEquals(5, fs.listStatus(logDir).length);
    loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(1, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());

    // remove proc-0
    procStore.delete(proc0.getProcId());
    procStore.periodicRollForTesting();
    assertEquals(1, fs.listStatus(logDir).length);
    storeRestart(loader);
  }

  @Test
  public void testProcIdHoles() throws Exception {
    // Insert
    for (int i = 0; i < 100; i += 2) {
      procStore.insert(new TestProcedure(i), null);
      if (i > 0 && (i % 10) == 0) {
        LoadCounter loader = new LoadCounter();
        storeRestart(loader);
        assertEquals(0, loader.getCorruptedCount());
        assertEquals((i / 2) + 1, loader.getLoadedCount());
      }
    }
    assertEquals(10, procStore.getActiveLogs().size());

    // Delete
    for (int i = 0; i < 100; i += 2) {
      procStore.delete(i);
    }
    assertEquals(1, procStore.getActiveLogs().size());

    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(0, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  @Test
  public void testCorruptedTrailer() throws Exception {
    // Insert something
    for (int i = 0; i < 100; ++i) {
      procStore.insert(new TestSequentialProcedure(), null);
    }

    // Stop the store
    procStore.stop(false);

    // Remove 4 byte from the trailer
    FileStatus[] logs = fs.listStatus(logDir);
    assertEquals(1, logs.length);
    corruptLog(logs[0], 4);

    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(100, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  private static void assertUpdated(final ProcedureStoreTracker tracker, final Procedure<?>[] procs,
    final int[] updatedProcs, final int[] nonUpdatedProcs) {
    for (int index : updatedProcs) {
      long procId = procs[index].getProcId();
      assertTrue("Procedure id : " + procId, tracker.isModified(procId));
    }
    for (int index : nonUpdatedProcs) {
      long procId = procs[index].getProcId();
      assertFalse("Procedure id : " + procId, tracker.isModified(procId));
    }
  }

  private static void assertDeleted(final ProcedureStoreTracker tracker, final Procedure<?>[] procs,
    final int[] deletedProcs, final int[] nonDeletedProcs) {
    for (int index : deletedProcs) {
      long procId = procs[index].getProcId();
      assertEquals("Procedure id : " + procId, ProcedureStoreTracker.DeleteState.YES,
        tracker.isDeleted(procId));
    }
    for (int index : nonDeletedProcs) {
      long procId = procs[index].getProcId();
      assertEquals("Procedure id : " + procId, ProcedureStoreTracker.DeleteState.NO,
        tracker.isDeleted(procId));
    }
  }

  @Test
  public void testCorruptedTrailersRebuild() throws Exception {
    final Procedure<?>[] procs = new Procedure[6];
    for (int i = 0; i < procs.length; ++i) {
      procs[i] = new TestSequentialProcedure();
    }
    // Log State (I=insert, U=updated, D=delete)
    // | log 1 | log 2 | log 3 |
    // 0 | I, D | | |
    // 1 | I | | |
    // 2 | I | D | |
    // 3 | I | U | |
    // 4 | | I | D |
    // 5 | | | I |
    procStore.insert(procs[0], null);
    procStore.insert(procs[1], null);
    procStore.insert(procs[2], null);
    procStore.insert(procs[3], null);
    procStore.delete(procs[0].getProcId());
    procStore.rollWriterForTesting();
    procStore.delete(procs[2].getProcId());
    procStore.update(procs[3]);
    procStore.insert(procs[4], null);
    procStore.rollWriterForTesting();
    procStore.delete(procs[4].getProcId());
    procStore.insert(procs[5], null);

    // Stop the store
    procStore.stop(false);

    // Remove 4 byte from the trailers
    final FileStatus[] logs = fs.listStatus(logDir);
    assertEquals(3, logs.length);
    for (int i = 0; i < logs.length; ++i) {
      corruptLog(logs[i], 4);
    }

    // Restart the store (avoid cleaning up the files, to check the rebuilded trackers)
    htu.getConfiguration().setBoolean(WALProcedureStore.EXEC_WAL_CLEANUP_ON_LOAD_CONF_KEY, false);
    final LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(3, loader.getLoadedCount()); // procs 1, 3 and 5
    assertEquals(0, loader.getCorruptedCount());

    // Check the Trackers
    final ArrayList<ProcedureWALFile> walFiles = procStore.getActiveLogs();
    LOG.info("WALs " + walFiles);
    assertEquals(4, walFiles.size());
    LOG.info("Checking wal " + walFiles.get(0));
    assertUpdated(walFiles.get(0).getTracker(), procs, new int[] { 0, 1, 2, 3 },
      new int[] { 4, 5 });
    LOG.info("Checking wal " + walFiles.get(1));
    assertUpdated(walFiles.get(1).getTracker(), procs, new int[] { 2, 3, 4 },
      new int[] { 0, 1, 5 });
    LOG.info("Checking wal " + walFiles.get(2));
    assertUpdated(walFiles.get(2).getTracker(), procs, new int[] { 4, 5 },
      new int[] { 0, 1, 2, 3 });
    LOG.info("Checking global tracker ");
    assertDeleted(procStore.getStoreTracker(), procs, new int[] { 0, 2, 4 }, new int[] { 1, 3, 5 });
  }

  @Test
  public void testCorruptedEntries() throws Exception {
    // Insert something
    for (int i = 0; i < 100; ++i) {
      procStore.insert(new TestSequentialProcedure(), null);
    }

    // Stop the store
    procStore.stop(false);

    // Remove some byte from the log
    // (enough to cut the trailer and corrupt some entries)
    FileStatus[] logs = fs.listStatus(logDir);
    assertEquals(1, logs.length);
    corruptLog(logs[0], 1823);

    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertTrue(procStore.getCorruptedLogs() != null);
    assertEquals(1, procStore.getCorruptedLogs().size());
    assertEquals(87, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  @Test
  public void testCorruptedProcedures() throws Exception {
    // Insert root-procedures
    TestProcedure[] rootProcs = new TestProcedure[10];
    for (int i = 1; i <= rootProcs.length; i++) {
      rootProcs[i - 1] = new TestProcedure(i, 0);
      procStore.insert(rootProcs[i - 1], null);
      rootProcs[i - 1].addStackId(0);
      procStore.update(rootProcs[i - 1]);
    }
    // insert root-child txn
    procStore.rollWriterForTesting();
    for (int i = 1; i <= rootProcs.length; i++) {
      TestProcedure b = new TestProcedure(rootProcs.length + i, i);
      rootProcs[i - 1].addStackId(1);
      procStore.insert(rootProcs[i - 1], new Procedure[] { b });
    }
    // insert child updates
    procStore.rollWriterForTesting();
    for (int i = 1; i <= rootProcs.length; i++) {
      procStore.update(new TestProcedure(rootProcs.length + i, i));
    }

    // Stop the store
    procStore.stop(false);

    // the first log was removed,
    // we have insert-txn and updates in the others so everything is fine
    FileStatus[] logs = fs.listStatus(logDir);
    assertEquals(Arrays.toString(logs), 2, logs.length);
    Arrays.sort(logs, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        return o1.getPath().getName().compareTo(o2.getPath().getName());
      }
    });

    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(rootProcs.length * 2, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());

    // Remove the second log, we have lost all the root/parent references
    fs.delete(logs[0].getPath(), false);
    loader.reset();
    storeRestart(loader);
    assertEquals(0, loader.getLoadedCount());
    assertEquals(rootProcs.length, loader.getCorruptedCount());
    for (Procedure<?> proc : loader.getCorrupted()) {
      assertTrue(proc.toString(), proc.getParentProcId() <= rootProcs.length);
      assertTrue(proc.toString(),
        proc.getProcId() > rootProcs.length && proc.getProcId() <= (rootProcs.length * 2));
    }
  }

  @Test
  public void testRollAndRemove() throws IOException {
    // Insert something in the log
    Procedure<?> proc1 = new TestSequentialProcedure();
    procStore.insert(proc1, null);

    Procedure<?> proc2 = new TestSequentialProcedure();
    procStore.insert(proc2, null);

    // roll the log, now we have 2
    procStore.rollWriterForTesting();
    assertEquals(2, procStore.getActiveLogs().size());

    // everything will be up to date in the second log
    // so we can remove the first one
    procStore.update(proc1);
    procStore.update(proc2);
    assertEquals(1, procStore.getActiveLogs().size());

    // roll the log, now we have 2
    procStore.rollWriterForTesting();
    assertEquals(2, procStore.getActiveLogs().size());

    // remove everything active
    // so we can remove all the logs
    procStore.delete(proc1.getProcId());
    procStore.delete(proc2.getProcId());
    assertEquals(1, procStore.getActiveLogs().size());
  }

  @Test
  public void testFileNotFoundDuringLeaseRecovery() throws IOException {
    final TestProcedure[] procs = new TestProcedure[3];
    for (int i = 0; i < procs.length; ++i) {
      procs[i] = new TestProcedure(i + 1, 0);
      procStore.insert(procs[i], null);
    }
    procStore.rollWriterForTesting();
    for (int i = 0; i < procs.length; ++i) {
      procStore.update(procs[i]);
      procStore.rollWriterForTesting();
    }
    procStore.stop(false);

    FileStatus[] status = fs.listStatus(logDir);
    assertEquals(procs.length + 1, status.length);

    // simulate another active master removing the wals
    procStore = new WALProcedureStore(htu.getConfiguration(), logDir, null, new LeaseRecovery() {
      private int count = 0;

      @Override
      public void recoverFileLease(FileSystem fs, Path path) throws IOException {
        if (++count <= 2) {
          fs.delete(path, false);
          LOG.debug("Simulate FileNotFound at count=" + count + " for " + path);
          throw new FileNotFoundException("test file not found " + path);
        }
        LOG.debug("Simulate recoverFileLease() at count=" + count + " for " + path);
      }
    });

    final LoadCounter loader = new LoadCounter();
    procStore.start(PROCEDURE_STORE_SLOTS);
    procStore.recoverLease();
    procStore.load(loader);
    assertEquals(procs.length, loader.getMaxProcId());
    assertEquals(1, loader.getRunnableCount());
    assertEquals(0, loader.getCompletedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  @Test
  public void testLogFileAlreadyExists() throws IOException {
    final boolean[] tested = { false };
    WALProcedureStore mStore = Mockito.spy(procStore);

    Answer<Boolean> ans = new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
        long logId = ((Long) invocationOnMock.getArgument(0)).longValue();
        switch ((int) logId) {
          case 2:
            // Create a file so that real rollWriter() runs into file exists condition
            Path logFilePath = mStore.getLogFilePath(logId);
            mStore.getFileSystem().create(logFilePath);
            break;
          case 3:
            // Success only when we retry with logId 3
            tested[0] = true;
          default:
            break;
        }
        return (Boolean) invocationOnMock.callRealMethod();
      }
    };

    // First time Store has one log file, next id will be 2
    Mockito.doAnswer(ans).when(mStore).rollWriter(2);
    // next time its 3
    Mockito.doAnswer(ans).when(mStore).rollWriter(3);

    mStore.recoverLease();
    assertTrue(tested[0]);
  }

  @Test
  public void testLoadChildren() throws Exception {
    TestProcedure a = new TestProcedure(1, 0);
    TestProcedure b = new TestProcedure(2, 1);
    TestProcedure c = new TestProcedure(3, 1);

    // INIT
    procStore.insert(a, null);

    // Run A first step
    a.addStackId(0);
    procStore.update(a);

    // Run A second step
    a.addStackId(1);
    procStore.insert(a, new Procedure[] { b, c });

    // Run B first step
    b.addStackId(2);
    procStore.update(b);

    // Run C first and last step
    c.addStackId(3);
    procStore.update(c);

    // Run B second setp
    b.addStackId(4);
    procStore.update(b);

    // back to A
    a.addStackId(5);
    a.setSuccessState();
    procStore.delete(a, new long[] { b.getProcId(), c.getProcId() });
    restartAndAssert(3, 0, 1, 0);
  }

  @Test
  public void testBatchDelete() throws Exception {
    for (int i = 1; i < 10; ++i) {
      procStore.insert(new TestProcedure(i), null);
    }

    // delete nothing
    long[] toDelete = new long[] { 1, 2, 3, 4 };
    procStore.delete(toDelete, 2, 0);
    LoadCounter loader = restartAndAssert(9, 9, 0, 0);
    for (int i = 1; i < 10; ++i) {
      assertEquals(true, loader.isRunnable(i));
    }

    // delete the full "toDelete" array (2, 4, 6, 8)
    toDelete = new long[] { 2, 4, 6, 8 };
    procStore.delete(toDelete, 0, toDelete.length);
    loader = restartAndAssert(9, 5, 0, 0);
    for (int i = 1; i < 10; ++i) {
      assertEquals(i % 2 != 0, loader.isRunnable(i));
    }

    // delete a slice of "toDelete" (1, 3)
    toDelete = new long[] { 5, 7, 1, 3, 9 };
    procStore.delete(toDelete, 2, 2);
    loader = restartAndAssert(9, 3, 0, 0);
    for (int i = 1; i < 10; ++i) {
      assertEquals(i > 3 && i % 2 != 0, loader.isRunnable(i));
    }

    // delete a single item (5)
    toDelete = new long[] { 5 };
    procStore.delete(toDelete, 0, 1);
    loader = restartAndAssert(9, 2, 0, 0);
    for (int i = 1; i < 10; ++i) {
      assertEquals(i > 5 && i % 2 != 0, loader.isRunnable(i));
    }

    // delete remaining using a slice of "toDelete" (7, 9)
    toDelete = new long[] { 0, 7, 9 };
    procStore.delete(toDelete, 1, 2);
    loader = restartAndAssert(0, 0, 0, 0);
    for (int i = 1; i < 10; ++i) {
      assertEquals(false, loader.isRunnable(i));
    }
  }

  @Test
  public void testBatchInsert() throws Exception {
    final int count = 10;
    final TestProcedure[] procs = new TestProcedure[count];
    for (int i = 0; i < procs.length; ++i) {
      procs[i] = new TestProcedure(i + 1);
    }
    procStore.insert(procs);
    restartAndAssert(count, count, 0, 0);

    for (int i = 0; i < procs.length; ++i) {
      final long procId = procs[i].getProcId();
      procStore.delete(procId);
      restartAndAssert(procId != count ? count : 0, count - (i + 1), 0, 0);
    }
    procStore.removeInactiveLogsForTesting();
    assertEquals("WALs=" + procStore.getActiveLogs(), 1, procStore.getActiveLogs().size());
  }

  @Test
  public void testWALDirAndWALArchiveDir() throws IOException {
    Configuration conf = htu.getConfiguration();
    procStore = createWALProcedureStore(conf);
    assertEquals(procStore.getFileSystem(), procStore.getWalArchiveDir().getFileSystem(conf));
  }

  private WALProcedureStore createWALProcedureStore(Configuration conf) throws IOException {
    return new WALProcedureStore(conf, new LeaseRecovery() {
      @Override
      public void recoverFileLease(FileSystem fs, Path path) throws IOException {
        // no-op
      }
    });
  }

  private LoadCounter restartAndAssert(long maxProcId, long runnableCount, int completedCount,
    int corruptedCount) throws Exception {
    return ProcedureTestingUtility.storeRestartAndAssert(procStore, maxProcId, runnableCount,
      completedCount, corruptedCount);
  }

  private void corruptLog(final FileStatus logFile, final long dropBytes) throws IOException {
    assertTrue(logFile.getLen() > dropBytes);
    LOG.debug(
      "corrupt log " + logFile.getPath() + " size=" + logFile.getLen() + " drop=" + dropBytes);
    Path tmpPath = new Path(testDir, "corrupted.log");
    InputStream in = fs.open(logFile.getPath());
    OutputStream out = fs.create(tmpPath);
    IOUtils.copyBytes(in, out, logFile.getLen() - dropBytes, true);
    if (!fs.rename(tmpPath, logFile.getPath())) {
      throw new IOException("Unable to rename");
    }
  }

  private void verifyProcIdsOnRestart(final Set<Long> procIds) throws Exception {
    LOG.debug("expected: " + procIds);
    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(procIds.size(), loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  public static class TestSequentialProcedure extends SequentialProcedure<Void> {
    private static long seqid = 0;

    public TestSequentialProcedure() {
      setProcId(++seqid);
    }

    @Override
    protected Procedure<Void>[] execute(Void env) {
      return null;
    }

    @Override
    protected void rollback(Void env) {
    }

    @Override
    protected boolean abort(Void env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
      long procId = getProcId();
      if (procId % 2 == 0) {
        Int64Value.Builder builder = Int64Value.newBuilder().setValue(procId);
        serializer.serialize(builder.build());
      }
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
      long procId = getProcId();
      if (procId % 2 == 0) {
        Int64Value value = serializer.deserialize(Int64Value.class);
        assertEquals(procId, value.getValue());
      }
    }
  }
}
