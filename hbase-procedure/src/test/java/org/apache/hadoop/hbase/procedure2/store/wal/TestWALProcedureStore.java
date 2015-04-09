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
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.SequentialProcedure;
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

  private Iterator<Procedure> storeRestart() throws Exception {
    procStore.stop(false);
    procStore.start(PROCEDURE_STORE_SLOTS);
    procStore.recoverLease();
    return procStore.load();
  }

  @Test
  public void testEmptyLogLoad() throws Exception {
    Iterator<Procedure> loader = storeRestart();
    assertEquals(0, countProcedures(loader));
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

    int count = countProcedures(storeRestart());
    assertEquals(100, count);
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

    int count = countProcedures(storeRestart());
    assertTrue(procStore.getCorruptedLogs() != null);
    assertEquals(1, procStore.getCorruptedLogs().size());
    assertEquals(85, count);
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
    int count = 0;
    Iterator<Procedure> loader = storeRestart();
    while (loader.hasNext()) {
      Procedure proc = loader.next();
      LOG.debug("loading procId=" + proc.getProcId());
      assertTrue("procId=" + proc.getProcId() + " unexpected", procIds.contains(proc.getProcId()));
      count++;
    }
    assertEquals(procIds.size(), count);
  }

  private void assertIsEmpty(Iterator<Procedure> iterator) {
    assertEquals(0, countProcedures(iterator));
  }

  private int countProcedures(Iterator<Procedure> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      Procedure proc = iterator.next();
      LOG.trace("loading procId=" + proc.getProcId());
      count++;
    }
    return count;
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
}