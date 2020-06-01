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
import static org.junit.Assert.assertNotNull;
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
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestCase.HRegionIncommon;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
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
  private final byte[] FAMILY = Bytes.toBytes("cf");

  /** constructor */
  public TestCompaction() {
    super();

    // Set cache flush size to 1MB
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);
    conf.setInt(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, 100);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      NoLimitThroughputController.class.getName());
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
    if (name.getMethodName().equals("testCompactionSeqId")) {
      UTIL.getConfiguration().set("hbase.hstore.compaction.kv.max", "10");
      UTIL.getConfiguration().set(
          DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY,
          DummyCompactor.class.getName());
      HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
      hcd.setMaxVersions(65536);
      this.htd.addFamily(hcd);
    }
    this.r = UTIL.createLocalHRegion(htd, null, null);
  }

  @After
  public void tearDown() throws Exception {
    WAL wal = r.getWAL();
    this.r.close();
    wal.close();
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
        @Override
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
      // only one empty dir exists in temp dir
      FileStatus[] ls = r.getFilesystem().listStatus(r.getRegionFileSystem().getTempDir());
      assertEquals(1, ls.length);
      Path storeTempDir = new Path(r.getRegionFileSystem().getTempDir(), Bytes.toString(COLUMN_FAMILY));
      assertTrue(r.getFilesystem().exists(storeTempDir));
      ls = r.getFilesystem().listStatus(storeTempDir);
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
      r.flush(true);

      // Multiple versions allowed for an entry, so the delete isn't enough
      // Lower TTL and expire to ensure that all our entries have been wiped
      final int ttl = 1000;
      for (Store hstore: this.r.stores.values()) {
        HStore store = (HStore)hstore;
        ScanInfo old = store.getScanInfo();
        ScanInfo si = new ScanInfo(old.getConfiguration(), old.getFamily(),
            old.getMinVersions(), old.getMaxVersions(), ttl,
            old.getKeepDeletedCells(), 0, old.getComparator());
        store.setScanInfo(si);
      }
      Thread.sleep(ttl);

      r.compact(true);
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

  @Test
  public void testCompactionFailure() throws Exception {
    // setup a compact/split thread on a mock server
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    CompactSplitThread thread = new CompactSplitThread(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    Store store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < HStore.DEFAULT_BLOCKING_STOREFILE_COUNT - 1; i++) {
      createStoreFile(r);
    }

    HRegion mockRegion = Mockito.spy(r);
    Mockito.when(mockRegion.checkSplit()).thenThrow(new IndexOutOfBoundsException());

    MetricsRegionWrapper metricsWrapper = new MetricsRegionWrapperImpl(r);

    long preCompletedCount = metricsWrapper.getNumCompactionsCompleted();
    long preFailedCount = metricsWrapper.getNumCompactionsFailed();

    CountDownLatch latch = new CountDownLatch(1);
    TrackableCompactionRequest request = new TrackableCompactionRequest(latch);
    thread.requestCompaction(mockRegion, store, "test custom comapction", Store.PRIORITY_USER,
        request, null);
    // wait for the latch to complete.
    latch.await(120, TimeUnit.SECONDS);

    // compaction should have completed and been marked as failed due to error in split request
    long postCompletedCount = metricsWrapper.getNumCompactionsCompleted();
    long postFailedCount = metricsWrapper.getNumCompactionsFailed();

    assertTrue("Completed count should have increased (pre=" + preCompletedCount +
        ", post="+postCompletedCount+")",
        postCompletedCount > preCompletedCount);
    assertTrue("Failed count should have increased (pre=" + preFailedCount +
        ", post=" + postFailedCount + ")",
        postFailedCount > preFailedCount);
  }

  /**
   * Test no new Compaction requests are generated after calling stop compactions
   */
  @Test
  public void testStopStartCompaction() throws IOException {
    // setup a compact/split thread on a mock server
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    final CompactSplitThread thread = new CompactSplitThread(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);
    // setup a region/store with some files
    Store store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < HStore.DEFAULT_BLOCKING_STOREFILE_COUNT - 1; i++) {
      createStoreFile(r);
    }
    thread.switchCompaction(false);
    thread.requestCompaction(r, store, "test", Store.PRIORITY_USER, new CompactionRequest(), null);
    assertEquals(false, thread.isCompactionsEnabled());
    assertEquals(0, thread.getLongCompactions().getActiveCount() + thread.getShortCompactions()
      .getActiveCount());
    thread.switchCompaction(true);
    assertEquals(true, thread.isCompactionsEnabled());
    // Make sure no compactions have run.
    assertEquals(0, thread.getLongCompactions().getCompletedTaskCount() +
        thread.getShortCompactions().getCompletedTaskCount());
    // Request a compaction and make sure it is submitted successfully.
    assertNotNull(thread.requestCompaction(r, store, "test", Store.PRIORITY_USER,
        new CompactionRequest(), null));
    // Wait until the compaction finishes.
    Waiter.waitFor(UTIL.getConfiguration(), 5000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return thread.getLongCompactions().getCompletedTaskCount() +
            thread.getShortCompactions().getCompletedTaskCount() == 1;
      }
    });
    // Make sure there are no compactions running.
    assertEquals(0, thread.getLongCompactions().getActiveCount()
        + thread.getShortCompactions().getActiveCount());
  }

  @Test
  public void testInterruptingRunningCompactions() throws Exception {
    // setup a compact/split thread on a mock server
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      WaitThroughPutController.class.getName());
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    CompactSplitThread thread = new CompactSplitThread(mockServer);

    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    Store store = r.getStore(COLUMN_FAMILY);
    int jmax = (int) Math.ceil(15.0 / compactionThreshold);
    byte[] pad = new byte[1000]; // 1 KB chunk
    for (int i = 0; i < compactionThreshold; i++) {
      HRegionIncommon loader = new HRegionIncommon(r);
      Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(i)));
      p.setDurability(Durability.SKIP_WAL);
      for (int j = 0; j < jmax; j++) {
        p.addColumn(COLUMN_FAMILY, Bytes.toBytes(j), pad);
      }
      HBaseTestCase.addContent(loader, Bytes.toString(COLUMN_FAMILY));
      loader.put(p);
      r.flush(true);
    }
    Store s = r.getStore(COLUMN_FAMILY);
    int initialFiles = s.getStorefilesCount();

    thread.requestCompaction(r, store, "test custom comapction", Store.PRIORITY_USER,
      new CompactionRequest(), null);

    Thread.sleep(3000);
    thread.switchCompaction(false);
    assertEquals(initialFiles, s.getStorefilesCount());
    //don't mess up future tests
    thread.switchCompaction(true);
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
    for (Store store : r.getStores()) {
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
      public List<Path> compact(ThroughputController throughputController, User user)
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
      public List<Path> compact(ThroughputController throughputController, User user)
          throws IOException {
        try {
          isInCompact = true;
          synchronized (this) {
            // FIXME: This wait may spuriously wake up, so this test is likely to be flaky
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

    @Override
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
    when(mockServer.getChoreService()).thenReturn(new ChoreService("test"));
    CompactSplitThread cst = new CompactSplitThread(mockServer);
    when(mockServer.getCompactSplitThread()).thenReturn(cst);
    //prevent large compaction thread pool stealing job from small compaction queue.
    cst.shutdownLongCompactions();
    // Set up the region mock that redirects compactions.
    HRegion r = mock(HRegion.class);
    when(
      r.compact(any(CompactionContext.class), any(Store.class),
        any(ThroughputController.class), any(User.class))).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        invocation.getArgumentAt(0, CompactionContext.class).compact(
          invocation.getArgumentAt(2, ThroughputController.class), null);
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

  /**
   * Firstly write 10 cells (with different time stamp) to a qualifier and flush
   * to hfile1, then write 10 cells (with different time stamp) to the same
   * qualifier and flush to hfile2. The latest cell (cell-A) in hfile1 and the
   * oldest cell (cell-B) in hfile2 are with the same time stamp but different
   * sequence id, and will get scanned successively during compaction.
   * <p/>
   * We set compaction.kv.max to 10 so compaction will scan 10 versions each
   * round, meanwhile we set keepSeqIdPeriod=0 in {@link DummyCompactor} so all
   * 10 versions of hfile2 will be written out with seqId cleaned (set to 0)
   * including cell-B, then when scanner goes to cell-A it will cause a scan
   * out-of-order assertion error before HBASE-16931
   *
   * @throws Exception
   *           if error occurs during the test
   */
  @Test
  public void testCompactionSeqId() throws Exception {
    final byte[] ROW = Bytes.toBytes("row");
    final byte[] QUALIFIER = Bytes.toBytes("qualifier");

    long timestamp = 10000;

    // row1/cf:a/10009/Put/vlen=2/seqid=11 V: v9
    // row1/cf:a/10008/Put/vlen=2/seqid=10 V: v8
    // row1/cf:a/10007/Put/vlen=2/seqid=9 V: v7
    // row1/cf:a/10006/Put/vlen=2/seqid=8 V: v6
    // row1/cf:a/10005/Put/vlen=2/seqid=7 V: v5
    // row1/cf:a/10004/Put/vlen=2/seqid=6 V: v4
    // row1/cf:a/10003/Put/vlen=2/seqid=5 V: v3
    // row1/cf:a/10002/Put/vlen=2/seqid=4 V: v2
    // row1/cf:a/10001/Put/vlen=2/seqid=3 V: v1
    // row1/cf:a/10000/Put/vlen=2/seqid=2 V: v0
    for (int i = 0; i < 10; i++) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, timestamp + i, Bytes.toBytes("v" + i));
      r.put(put);
    }
    r.flush(true);

    // row1/cf:a/10018/Put/vlen=3/seqid=16 V: v18
    // row1/cf:a/10017/Put/vlen=3/seqid=17 V: v17
    // row1/cf:a/10016/Put/vlen=3/seqid=18 V: v16
    // row1/cf:a/10015/Put/vlen=3/seqid=19 V: v15
    // row1/cf:a/10014/Put/vlen=3/seqid=20 V: v14
    // row1/cf:a/10013/Put/vlen=3/seqid=21 V: v13
    // row1/cf:a/10012/Put/vlen=3/seqid=22 V: v12
    // row1/cf:a/10011/Put/vlen=3/seqid=23 V: v11
    // row1/cf:a/10010/Put/vlen=3/seqid=24 V: v10
    // row1/cf:a/10009/Put/vlen=2/seqid=25 V: v9
    for (int i = 18; i > 8; i--) {
      Put put = new Put(ROW);
      put.addColumn(FAMILY, QUALIFIER, timestamp + i, Bytes.toBytes("v" + i));
      r.put(put);
    }
    r.flush(true);
    r.compact(true);
  }

  public static class DummyCompactor extends DefaultCompactor {
    public DummyCompactor(Configuration conf, Store store) {
      super(conf, store);
      this.keepSeqIdPeriod = 0;
    }
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

  /**
   * Simple {@link CompactionLifeCycleTracker} on which you can wait until the requested compaction
   * finishes.
   */
  public static class WaitThroughPutController extends NoLimitThroughputController{

    public WaitThroughPutController() {
    }

    @Override
    public long control(String compactionName, long size) throws InterruptedException {
      Thread.sleep(6000000);
      return 6000000;
    }
  }
}
