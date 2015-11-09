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

package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility.TestProcedure;
import org.apache.hadoop.hbase.procedure2.SequentialProcedure;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStore.ProcedureIterator;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MasterTests.class, SmallTests.class})
public class TestWALProcedureStore {
  private static final Log LOG = LogFactory.getLog(TestWALProcedureStore.class);

  private static final int PROCEDURE_STORE_SLOTS = 1;
  private static final Procedure NULL_PROC = null;

  private WALProcedureStore procStore;

  private HBaseCommonTestingUtility htu;
  private FileSystem fs;
  private Path testDir;
  private Path logDir;

  @Before
  public void setUp() throws IOException {
    htu = new HBaseCommonTestingUtility();
    testDir = htu.getDataTestDir();
    fs = testDir.getFileSystem(htu.getConfiguration());
    assertTrue(testDir.depth() > 1);

    logDir = new Path(testDir, "proc-logs");
    procStore = ProcedureTestingUtility.createWalStore(htu.getConfiguration(), fs, logDir);
    procStore.start(PROCEDURE_STORE_SLOTS);
    procStore.recoverLease();
  }

  @After
  public void tearDown() throws IOException {
    procStore.stop(false);
    fs.delete(logDir, true);
  }

  private void storeRestart(ProcedureStore.ProcedureLoader loader) throws Exception {
    procStore.stop(false);
    procStore.start(PROCEDURE_STORE_SLOTS);
    procStore.recoverLease();
    procStore.load(loader);
  }

  @Test
  public void testEmptyRoll() throws Exception {
    for (int i = 0; i < 10; ++i) {
      procStore.periodicRoll();
    }
    FileStatus[] status = fs.listStatus(logDir);
    assertEquals(1, status.length);
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
    Procedure proc1 = new TestSequentialProcedure();
    procIds.add(proc1.getProcId());
    procStore.insert(proc1, null);

    Procedure proc2 = new TestSequentialProcedure();
    Procedure[] child2 = new Procedure[2];
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
    assertEquals(85, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  @Test
  public void testCorruptedProcedures() throws Exception {
    // Insert root-procedures
    TestProcedure[] rootProcs = new TestProcedure[10];
    for (int i = 1; i <= rootProcs.length; i++) {
      rootProcs[i-1] = new TestProcedure(i, 0);
      procStore.insert(rootProcs[i-1], null);
      rootProcs[i-1].addStackId(0);
      procStore.update(rootProcs[i-1]);
    }
    // insert root-child txn
    procStore.rollWriter();
    for (int i = 1; i <= rootProcs.length; i++) {
      TestProcedure b = new TestProcedure(rootProcs.length + i, i);
      rootProcs[i-1].addStackId(1);
      procStore.insert(rootProcs[i-1], new Procedure[] { b });
    }
    // insert child updates
    procStore.rollWriter();
    for (int i = 1; i <= rootProcs.length; i++) {
      procStore.update(new TestProcedure(rootProcs.length + i, i));
    }

    // Stop the store
    procStore.stop(false);

    // Remove 4 byte from the trailer
    FileStatus[] logs = fs.listStatus(logDir);
    assertEquals(3, logs.length);
    Arrays.sort(logs, new Comparator<FileStatus>() {
      @Override
      public int compare(FileStatus o1, FileStatus o2) {
        return o1.getPath().getName().compareTo(o2.getPath().getName());
      }
    });

    // Remove the first log, we have insert-txn and updates in the others so everything is fine.
    fs.delete(logs[0].getPath(), false);
    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(rootProcs.length * 2, loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());

    // Remove the second log, we have lost any root/parent references
    fs.delete(logs[1].getPath(), false);
    loader.reset();
    storeRestart(loader);
    assertEquals(0, loader.getLoadedCount());
    assertEquals(rootProcs.length, loader.getCorruptedCount());
    for (Procedure proc: loader.getCorrupted()) {
      assertTrue(proc.toString(), proc.getParentProcId() <= rootProcs.length);
      assertTrue(proc.toString(),
                  proc.getProcId() > rootProcs.length &&
                  proc.getProcId() <= (rootProcs.length * 2));
    }
  }

  @Test(timeout=60000)
  public void testWalReplayOrder_AB_A() throws Exception {
    /*
     * | A B | -> | A |
     */
    TestProcedure a = new TestProcedure(1, 0);
    TestProcedure b = new TestProcedure(2, 1);

    procStore.insert(a, null);
    a.addStackId(0);
    procStore.update(a);

    procStore.insert(a, new Procedure[] { b });
    b.addStackId(1);
    procStore.update(b);

    procStore.rollWriter();

    a.addStackId(2);
    procStore.update(a);

    storeRestart(new ProcedureStore.ProcedureLoader() {
      @Override
      public void setMaxProcId(long maxProcId) {
        assertEquals(2, maxProcId);
      }

      @Override
      public void load(ProcedureIterator procIter) throws IOException {
        assertTrue(procIter.hasNext());
        assertEquals(1, procIter.next().getProcId());
        assertTrue(procIter.hasNext());
        assertEquals(2, procIter.next().getProcId());
        assertFalse(procIter.hasNext());
      }

      @Override
      public void handleCorrupted(ProcedureIterator procIter) throws IOException {
        assertFalse(procIter.hasNext());
      }
    });
  }

  @Test(timeout=60000)
  public void testWalReplayOrder_ABC_BAD() throws Exception {
    /*
     * | A B C | -> | B A D |
     */
    TestProcedure a = new TestProcedure(1, 0);
    TestProcedure b = new TestProcedure(2, 1);
    TestProcedure c = new TestProcedure(3, 2);
    TestProcedure d = new TestProcedure(4, 0);

    procStore.insert(a, null);
    a.addStackId(0);
    procStore.update(a);

    procStore.insert(a, new Procedure[] { b });
    b.addStackId(1);
    procStore.update(b);

    procStore.insert(b, new Procedure[] { c });
    b.addStackId(2);
    procStore.update(b);

    procStore.rollWriter();

    b.addStackId(3);
    procStore.update(b);

    a.addStackId(4);
    procStore.update(a);

    procStore.insert(d, null);
    d.addStackId(0);
    procStore.update(d);

    storeRestart(new ProcedureStore.ProcedureLoader() {
      @Override
      public void setMaxProcId(long maxProcId) {
        assertEquals(4, maxProcId);
      }

      @Override
      public void load(ProcedureIterator procIter) throws IOException {
        assertTrue(procIter.hasNext());
        assertEquals(4, procIter.next().getProcId());
        // TODO: This will be multiple call once we do fast-start
        //assertFalse(procIter.hasNext());

        assertTrue(procIter.hasNext());
        assertEquals(1, procIter.next().getProcId());
        assertTrue(procIter.hasNext());
        assertEquals(2, procIter.next().getProcId());
        assertTrue(procIter.hasNext());
        assertEquals(3, procIter.next().getProcId());
        assertFalse(procIter.hasNext());
      }

      @Override
      public void handleCorrupted(ProcedureIterator procIter) throws IOException {
        assertFalse(procIter.hasNext());
      }
    });
  }

  @Test
  public void testInsertUpdateDelete() throws Exception {
    final int NTHREAD = 2;

    procStore.stop(false);
    fs.delete(logDir, true);

    org.apache.hadoop.conf.Configuration conf =
      new org.apache.hadoop.conf.Configuration(htu.getConfiguration());
    conf.setBoolean("hbase.procedure.store.wal.use.hsync", false);
    conf.setInt("hbase.procedure.store.wal.periodic.roll.msec", 10000);
    conf.setInt("hbase.procedure.store.wal.roll.threshold", 128 * 1024);

    fs.mkdirs(logDir);
    procStore = ProcedureTestingUtility.createWalStore(conf, fs, logDir);
    procStore.start(NTHREAD);
    procStore.recoverLease();

    final long LAST_PROC_ID = 9999;
    final Thread[] thread = new Thread[NTHREAD];
    final AtomicLong procCounter = new AtomicLong((long)Math.round(Math.random() * 100));
    for (int i = 0; i < thread.length; ++i) {
      thread[i] = new Thread() {
        @Override
        public void run() {
          Random rand = new Random();
          TestProcedure proc;
          do {
            proc = new TestProcedure(procCounter.addAndGet(1));
            // Insert
            procStore.insert(proc, null);
            // Update
            for (int i = 0, nupdates = rand.nextInt(10); i <= nupdates; ++i) {
              try { Thread.sleep(0, rand.nextInt(15)); } catch (InterruptedException e) {}
              procStore.update(proc);
            }
            // Delete
            procStore.delete(proc.getProcId());
          } while (proc.getProcId() < LAST_PROC_ID);
        }
      };
      thread[i].start();
    }

    for (int i = 0; i < thread.length; ++i) {
      thread[i].join();
    }

    procStore.getStoreTracker().dump();
    assertTrue(procCounter.get() >= LAST_PROC_ID);
    assertTrue(procStore.getStoreTracker().isEmpty());
    assertEquals(1, procStore.getActiveLogs().size());
  }

  private void corruptLog(final FileStatus logFile, final long dropBytes)
      throws IOException {
    assertTrue(logFile.getLen() > dropBytes);
    LOG.debug("corrupt log " + logFile.getPath() +
              " size=" + logFile.getLen() + " drop=" + dropBytes);
    Path tmpPath = new Path(testDir, "corrupted.log");
    InputStream in = fs.open(logFile.getPath());
    OutputStream out =  fs.create(tmpPath);
    IOUtils.copyBytes(in, out, logFile.getLen() - dropBytes, true);
    fs.rename(tmpPath, logFile.getPath());
  }

  private void verifyProcIdsOnRestart(final Set<Long> procIds) throws Exception {
    LOG.debug("expected: " + procIds);
    LoadCounter loader = new LoadCounter();
    storeRestart(loader);
    assertEquals(procIds.size(), loader.getLoadedCount());
    assertEquals(0, loader.getCorruptedCount());
  }

  private void assertEmptyLogDir() {
    try {
      FileStatus[] status = fs.listStatus(logDir);
      assertTrue("expected empty state-log dir", status == null || status.length == 0);
    } catch (FileNotFoundException e) {
      fail("expected the state-log dir to be present: " + logDir);
    } catch (IOException e) {
      fail("got en exception on state-log dir list: " + e.getMessage());
    }
  }

  public static class TestSequentialProcedure extends SequentialProcedure<Void> {
    private static long seqid = 0;

    public TestSequentialProcedure() {
      setProcId(++seqid);
    }

    @Override
    protected Procedure[] execute(Void env) { return null; }

    @Override
    protected void rollback(Void env) { }

    @Override
    protected boolean abort(Void env) { return false; }

    @Override
    protected void serializeStateData(final OutputStream stream) throws IOException {
      long procId = getProcId();
      if (procId % 2 == 0) {
        stream.write(Bytes.toBytes(procId));
      }
    }

    @Override
    protected void deserializeStateData(InputStream stream) throws IOException {
      long procId = getProcId();
      if (procId % 2 == 0) {
        byte[] bProcId = new byte[8];
        assertEquals(8, stream.read(bProcId));
        assertEquals(procId, Bytes.toLong(bProcId));
      } else {
        assertEquals(0, stream.available());
      }
    }
  }

  private class LoadCounter implements ProcedureStore.ProcedureLoader {
    private final ArrayList<Procedure> corrupted = new ArrayList<Procedure>();
    private final ArrayList<Procedure> loaded = new ArrayList<Procedure>();

    private Set<Long> procIds;
    private long maxProcId = 0;

    public LoadCounter() {
      this(null);
    }

    public LoadCounter(final Set<Long> procIds) {
      this.procIds = procIds;
    }

    public void reset() {
      reset(null);
    }

    public void reset(final Set<Long> procIds) {
      corrupted.clear();
      loaded.clear();
      this.procIds = procIds;
      this.maxProcId = 0;
    }

    public long getMaxProcId() {
      return maxProcId;
    }

    public ArrayList<Procedure> getLoaded() {
      return loaded;
    }

    public int getLoadedCount() {
      return loaded.size();
    }

    public ArrayList<Procedure> getCorrupted() {
      return corrupted;
    }

    public int getCorruptedCount() {
      return corrupted.size();
    }

    @Override
    public void setMaxProcId(long maxProcId) {
      maxProcId = maxProcId;
    }

    @Override
    public void load(ProcedureIterator procIter) throws IOException {
      while (procIter.hasNext()) {
        Procedure proc = procIter.next();
        LOG.debug("loading procId=" + proc.getProcId() + ": " + proc);
        if (procIds != null) {
          assertTrue("procId=" + proc.getProcId() + " unexpected",
                     procIds.contains(proc.getProcId()));
        }
        loaded.add(proc);
      }
    }

    @Override
    public void handleCorrupted(ProcedureIterator procIter) throws IOException {
      while (procIter.hasNext()) {
        Procedure proc = procIter.next();
        LOG.debug("corrupted procId=" + proc.getProcId() + ": " + proc);
        corrupted.add(proc);
      }
    }
  }
}