/**
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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtility.START_KEY_BYTES;
import static org.apache.hadoop.hbase.HBaseTestingUtility.fam1;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestCase.HRegionIncommon;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * Test compaction framework and common functions
 */
@Category(MediumTests.class)
public class TestCompaction {
  @Rule public TestName name = new TestName();
  static final Log LOG = LogFactory.getLog(TestCompaction.class.getName());
  private static final HBaseTestingUtility UTIL = HBaseTestingUtility.createLocalHTU();
  protected Configuration conf = UTIL.getConfiguration();
  
  private HRegion r = null;
  private HTableDescriptor htd = null;
  private static final byte [] COLUMN_FAMILY = fam1;
  private final byte [] STARTROW = Bytes.toBytes(START_KEY);
  private static final byte [] COLUMN_FAMILY_TEXT = COLUMN_FAMILY;
  private int compactionThreshold;
  private byte[] secondRowBytes, thirdRowBytes;
  private static final long MAX_FILES_TO_COMPACT = 10;

  /** constructor */
  public TestCompaction() {
    super();

    // Set cache flush size to 1MB
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);
    conf.setInt(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, 100);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      NoLimitCompactionThroughputController.class.getName());
    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);

    secondRowBytes = START_KEY_BYTES.clone();
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes = START_KEY_BYTES.clone();
    thirdRowBytes[START_KEY_BYTES.length - 1] += 2;
  }

  @Before
  public void setUp() throws Exception {
    this.htd = UTIL.createTableDescriptor(name.getMethodName());
    this.r = UTIL.createLocalHRegion(htd, null, null);
  }

  @After
  public void tearDown() throws Exception {
    HLog hlog = r.getLog();
    this.r.close();
    hlog.closeAndDelete();
  }

  /**
   * Verify that you can stop a long-running compaction
   * (used during RS shutdown)
   * @throws Exception
   */
  @Test
  public void testInterruptCompaction() throws Exception {
    assertEquals(0, count());

    // lower the polling interval for this test
    int origWI = HStore.closeCheckInterval;
    HStore.closeCheckInterval = 10*1000; // 10 KB

    try {
      // Create a couple store files w/ 15KB (over 10KB interval)
      int jmax = (int) Math.ceil(15.0/compactionThreshold);
      byte [] pad = new byte[1000]; // 1 KB chunk
      for (int i = 0; i < compactionThreshold; i++) {
        HRegionIncommon loader = new HRegionIncommon(r);
        Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(i)));
        p.setDurability(Durability.SKIP_WAL);
        for (int j = 0; j < jmax; j++) {
          p.add(COLUMN_FAMILY, Bytes.toBytes(j), pad);
        }
        HBaseTestCase.addContent(loader, Bytes.toString(COLUMN_FAMILY));
        loader.put(p);
        loader.flushcache();
      }

      HRegion spyR = spy(r);
      doAnswer(new Answer() {
        public Object answer(InvocationOnMock invocation) throws Throwable {
          r.writestate.writesEnabled = false;
          return invocation.callRealMethod();
        }
      }).when(spyR).doRegionCompactionPrep();

      // force a minor compaction, but not before requesting a stop
      spyR.compactStores();

      // ensure that the compaction stopped, all old files are intact,
      Store s = r.stores.get(COLUMN_FAMILY);
      assertEquals(compactionThreshold, s.getStorefilesCount());
      assertTrue(s.getStorefilesSize() > 15*1000);
      // and no new store files persisted past compactStores()
      FileStatus[] ls = r.getFilesystem().listStatus(r.getRegionFileSystem().getTempDir());
      assertEquals(0, ls.length);

    } finally {
      // don't mess up future tests
      r.writestate.writesEnabled = true;
      HStore.closeCheckInterval = origWI;

      // Delete all Store information once done using
      for (int i = 0; i < compactionThreshold; i++) {
        Delete delete = new Delete(Bytes.add(STARTROW, Bytes.toBytes(i)));
        byte [][] famAndQf = {COLUMN_FAMILY, null};
        delete.deleteFamily(famAndQf[0]);
        r.delete(delete);
      }
      r.flushcache();

      // Multiple versions allowed for an entry, so the delete isn't enough
      // Lower TTL and expire to ensure that all our entries have been wiped
      final int ttl = 1000;
      for (Store hstore: this.r.stores.values()) {
        HStore store = (HStore)hstore;
        ScanInfo old = store.getScanInfo();
        ScanInfo si = new ScanInfo(old.getFamily(),
            old.getMinVersions(), old.getMaxVersions(), ttl,
            old.getKeepDeletedCells(), 0, old.getComparator());
        store.setScanInfo(si);
      }
      Thread.sleep(ttl);

      r.compactStores(true);
      assertEquals(0, count());
    }
  }

  private int count() throws IOException {
    int count = 0;
    for (StoreFile f: this.r.stores.
        get(COLUMN_FAMILY_TEXT).getStorefiles()) {
      HFileScanner scanner = f.getReader().getScanner(false, false);
      if (!scanner.seekTo()) {
        continue;
      }
      do {
        count++;
      } while(scanner.next());
    }
    return count;
  }

  private void createStoreFile(final HRegion region) throws IOException {
    createStoreFile(region, Bytes.toString(COLUMN_FAMILY));
  }

  private void createStoreFile(final HRegion region, String family) throws IOException {
    HRegionIncommon loader = new HRegionIncommon(region);
    HBaseTestCase.addContent(loader, family);
    loader.flushcache();
  }

  @Test
  public void testCompactionWithCorruptResult() throws Exception {
    int nfiles = 10;
    for (int i = 0; i < nfiles; i++) {
      createStoreFile(r);
    }
    HStore store = (HStore) r.getStore(COLUMN_FAMILY);

    Collection<StoreFile> storeFiles = store.getStorefiles();
    DefaultCompactor tool = (DefaultCompactor)store.storeEngine.getCompactor();
    tool.compactForTesting(storeFiles, false);

    // Now lets corrupt the compacted file.
    FileSystem fs = store.getFileSystem();
    // default compaction policy created one and only one new compacted file
    Path dstPath = store.getRegionFileSystem().createTempName();
    FSDataOutputStream stream = fs.create(dstPath, null, true, 512, (short)3, (long)1024, null);
    stream.writeChars("CORRUPT FILE!!!!");
    stream.close();
    Path origPath = store.getRegionFileSystem().commitStoreFile(
      Bytes.toString(COLUMN_FAMILY), dstPath);

    try {
      ((HStore)store).moveFileIntoPlace(origPath);
    } catch (Exception e) {
      // The complete compaction should fail and the corrupt file should remain
      // in the 'tmp' directory;
      assert (fs.exists(origPath));
      assert (!fs.exists(dstPath));
      System.out.println("testCompactionWithCorruptResult Passed");
      return;
    }
    fail("testCompactionWithCorruptResult failed since no exception was" +
        "thrown while completing a corrupt file");
  }

  /**
   * Create a custom compaction request and be sure that we can track it through the queue, knowing
   * when the compaction is completed.
   */
  @Test
  public void testTrackingCompactionRequest() throws Exception {
    // setup a compact/split thread on a mock server
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    CompactSplitThread thread = new CompactSplitThread(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    Store store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < MAX_FILES_TO_COMPACT + 1; i++) {
      createStoreFile(r);
    }

    CountDownLatch latch = new CountDownLatch(1);
    TrackableCompactionRequest request = new TrackableCompactionRequest(latch);
    thread.requestCompaction(r, store, "test custom comapction", Store.PRIORITY_USER, request,null);
    // wait for the latch to complete.
    latch.await();

    thread.interruptIfNecessary();
  }

  /**
   * HBASE-7947: Regression test to ensure adding to the correct list in the
   * {@link CompactSplitThread}
   * @throws Exception on failure
   */
  @Test
  public void testMultipleCustomCompactionRequests() throws Exception {
    // setup a compact/split thread on a mock server
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    CompactSplitThread thread = new CompactSplitThread(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    int numStores = r.getStores().size();
    List<Pair<CompactionRequest, Store>> requests =
        new ArrayList<Pair<CompactionRequest, Store>>(numStores);
    CountDownLatch latch = new CountDownLatch(numStores);
    // create some store files and setup requests for each store on which we want to do a
    // compaction
    for (Store store : r.getStores().values()) {
      createStoreFile(r, store.getColumnFamilyName());
      createStoreFile(r, store.getColumnFamilyName());
      createStoreFile(r, store.getColumnFamilyName());
      requests
          .add(new Pair<CompactionRequest, Store>(new TrackableCompactionRequest(latch), store));
    }

    thread.requestCompaction(r, "test mulitple custom comapctions", Store.PRIORITY_USER,
      Collections.unmodifiableList(requests), null);

    // wait for the latch to complete.
    latch.await();

    thread.interruptIfNecessary();
  }

  private class StoreMockMaker extends StatefulStoreMockMaker {
    public ArrayList<StoreFile> compacting = new ArrayList<StoreFile>();
    public ArrayList<StoreFile> notCompacting = new ArrayList<StoreFile>();
    private ArrayList<Integer> results;

    public StoreMockMaker(ArrayList<Integer> results) {
      this.results = results;
    }

    public class TestCompactionContext extends CompactionContext {
      private List<StoreFile> selectedFiles;
      public TestCompactionContext(List<StoreFile> selectedFiles) {
        super();
        this.selectedFiles = selectedFiles;
      }

      @Override
      public List<StoreFile> preSelect(List<StoreFile> filesCompacting) {
        return new ArrayList<StoreFile>();
      }

      @Override
      public boolean select(List<StoreFile> filesCompacting, boolean isUserCompaction,
          boolean mayUseOffPeak, boolean forceMajor) throws IOException {
        this.request = new CompactionRequest(selectedFiles);
        this.request.setPriority(getPriority());
        return true;
      }

      @Override
      public List<Path> compact(CompactionThroughputController throughputController)
          throws IOException {
        finishCompaction(this.selectedFiles);
        return new ArrayList<Path>();
      }
    }

    @Override
    public synchronized CompactionContext selectCompaction() {
      CompactionContext ctx = new TestCompactionContext(new ArrayList<StoreFile>(notCompacting));
      compacting.addAll(notCompacting);
      notCompacting.clear();
      try {
        ctx.select(null, false, false, false);
      } catch (IOException ex) {
        fail("Shouldn't happen");
      }
      return ctx;
    }

    @Override
    public synchronized void cancelCompaction(Object object) {
      TestCompactionContext ctx = (TestCompactionContext)object;
      compacting.removeAll(ctx.selectedFiles);
      notCompacting.addAll(ctx.selectedFiles);
    }

    public synchronized void finishCompaction(List<StoreFile> sfs) {
      if (sfs.isEmpty()) return;
      synchronized (results) {
        results.add(sfs.size());
      }
      compacting.removeAll(sfs);
    }

    @Override
    public int getPriority() {
      return 7 - compacting.size() - notCompacting.size();
    }
  }

  public class BlockingStoreMockMaker extends StatefulStoreMockMaker {
    BlockingCompactionContext blocked = null;

    public class BlockingCompactionContext extends CompactionContext {
      public volatile boolean isInCompact = false;

      public void unblock() {
        synchronized (this) { this.notifyAll(); }
      }

      @Override
      public List<Path> compact(CompactionThroughputController throughputController)
          throws IOException {
        try {
          isInCompact = true;
          synchronized (this) {
            this.wait();
          }
        } catch (InterruptedException e) {
          Assume.assumeNoException(e);
        }
        return new ArrayList<Path>();
      }

      @Override
      public List<StoreFile> preSelect(List<StoreFile> filesCompacting) {
        return new ArrayList<StoreFile>();
      }

      @Override
      public boolean select(List<StoreFile> f, boolean i, boolean m, boolean e)
          throws IOException {
        this.request = new CompactionRequest(new ArrayList<StoreFile>());
        return true;
      }
    }

    @Override
    public CompactionContext selectCompaction() {
      this.blocked = new BlockingCompactionContext();
      try {
        this.blocked.select(null, false, false, false);
      } catch (IOException ex) {
        fail("Shouldn't happen");
      }
      return this.blocked;
    }

    @Override
    public void cancelCompaction(Object object) {}

    public int getPriority() {
      return Integer.MIN_VALUE; // some invalid value, see createStoreMock
    }

    public BlockingCompactionContext waitForBlocking() {
      while (this.blocked == null || !this.blocked.isInCompact) {
        Threads.sleepWithoutInterrupt(50);
      }
      BlockingCompactionContext ctx = this.blocked;
      this.blocked = null;
      return ctx;
    }

    @Override
    public Store createStoreMock(String name) throws Exception {
      return createStoreMock(Integer.MIN_VALUE, name);
    }

    public Store createStoreMock(int priority, String name) throws Exception {
      // Override the mock to always return the specified priority.
      Store s = super.createStoreMock(name);
      when(s.getCompactPriority()).thenReturn(priority);
      return s;
    }
  }

  /** Test compaction priority management and multiple compactions per store (HBASE-8665). */
  @Test
  public void testCompactionQueuePriorities() throws Exception {
    // Setup a compact/split thread on a mock server.
    final Configuration conf = HBaseConfiguration.create();
    HRegionServer mockServer = mock(HRegionServer.class);
    when(mockServer.isStopped()).thenReturn(false);
    when(mockServer.getConfiguration()).thenReturn(conf);
    CompactSplitThread cst = new CompactSplitThread(mockServer);
    when(mockServer.getCompactSplitThread()).thenReturn(cst);

    // Set up the region mock that redirects compactions.
    HRegion r = mock(HRegion.class);
    when(
      r.compact(any(CompactionContext.class), any(Store.class),
        any(CompactionThroughputController.class))).then(new Answer<Boolean>() {
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        ((CompactionContext)invocation.getArguments()[0]).compact(
          (CompactionThroughputController)invocation.getArguments()[2]);
        return true;
      }
    });

    // Set up store mocks for 2 "real" stores and the one we use for blocking CST.
    ArrayList<Integer> results = new ArrayList<Integer>();
    StoreMockMaker sm = new StoreMockMaker(results), sm2 = new StoreMockMaker(results);
    Store store = sm.createStoreMock("store1"), store2 = sm2.createStoreMock("store2");
    BlockingStoreMockMaker blocker = new BlockingStoreMockMaker();

    // First, block the compaction thread so that we could muck with queue.
    cst.requestSystemCompaction(r, blocker.createStoreMock(1, "b-pri1"), "b-pri1");
    BlockingStoreMockMaker.BlockingCompactionContext currentBlock = blocker.waitForBlocking();

    // Add 4 files to store1, 3 to store2, and queue compactions; pri 3 and 4 respectively.
    for (int i = 0; i < 4; ++i) {
      sm.notCompacting.add(createFile());
    }
    cst.requestSystemCompaction(r, store, "s1-pri3");
    for (int i = 0; i < 3; ++i) {
      sm2.notCompacting.add(createFile());
    }
    cst.requestSystemCompaction(r, store2, "s2-pri4");
    // Now add 2 more files to store1 and queue compaction - pri 1.
    for (int i = 0; i < 2; ++i) {
      sm.notCompacting.add(createFile());
    }
    cst.requestSystemCompaction(r, store, "s1-pri1");
    // Finally add blocking compaction with priority 2.
    cst.requestSystemCompaction(r, blocker.createStoreMock(2, "b-pri2"), "b-pri2");

    // Unblock the blocking compaction; we should run pri1 and become block again in pri2.
    currentBlock.unblock();
    currentBlock = blocker.waitForBlocking();
    // Pri1 should have "compacted" all 6 files.
    assertEquals(1, results.size());
    assertEquals(6, results.get(0).intValue());
    // Add 2 files to store 1 (it has 2 files now).
    for (int i = 0; i < 2; ++i) {
      sm.notCompacting.add(createFile());
    }
    // Now we have pri4 for store 2 in queue, and pri3 for store1; store1's current priority
    // is 5, however, so it must not preempt store 2. Add blocking compaction at the end.
    cst.requestSystemCompaction(r, blocker.createStoreMock(7, "b-pri7"), "b-pri7");
    currentBlock.unblock();
    currentBlock = blocker.waitForBlocking();
    assertEquals(3, results.size());
    assertEquals(3, results.get(1).intValue()); // 3 files should go before 2 files.
    assertEquals(2, results.get(2).intValue());

    currentBlock.unblock();
    cst.interruptIfNecessary();
  }

  private static StoreFile createFile() throws Exception {
    StoreFile sf = mock(StoreFile.class);
    when(sf.getPath()).thenReturn(new Path("file"));
    StoreFile.Reader r = mock(StoreFile.Reader.class);
    when(r.length()).thenReturn(10L);
    when(sf.getReader()).thenReturn(r);
    return sf;
  }

  /**
   * Simple {@link CompactionRequest} on which you can wait until the requested compaction finishes.
   */
  public static class TrackableCompactionRequest extends CompactionRequest {
    private CountDownLatch done;

    /**
     * Constructor for a custom compaction. Uses the setXXX methods to update the state of the
     * compaction before being used.
     */
    public TrackableCompactionRequest(CountDownLatch finished) {
      super();
      this.done = finished;
    }

    @Override
    public void afterExecute() {
      super.afterExecute();
      this.done.countDown();
    }
  }
}
