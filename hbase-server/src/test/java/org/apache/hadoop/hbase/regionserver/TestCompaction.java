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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HBaseTestingUtil.START_KEY;
import static org.apache.hadoop.hbase.HBaseTestingUtil.START_KEY_BYTES;
import static org.apache.hadoop.hbase.HBaseTestingUtil.fam1;
import static org.apache.hadoop.hbase.regionserver.Store.PRIORITY_USER;
import static org.apache.hadoop.hbase.regionserver.compactions.CloseChecker.SIZE_LIMIT_KEY;
import static org.apache.hadoop.hbase.regionserver.compactions.CloseChecker.TIME_LIMIT_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequestImpl;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

/**
 * Test compaction framework and common functions
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestCompaction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompaction.class);

  @Rule
  public TestName name = new TestName();
  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  protected Configuration conf = UTIL.getConfiguration();

  private HRegion r = null;
  private TableDescriptor tableDescriptor = null;
  private static final byte[] COLUMN_FAMILY = fam1;
  private final byte[] STARTROW = Bytes.toBytes(START_KEY);
  private static final byte[] COLUMN_FAMILY_TEXT = COLUMN_FAMILY;
  private int compactionThreshold;
  private byte[] secondRowBytes, thirdRowBytes;
  private static final long MAX_FILES_TO_COMPACT = 10;
  private final byte[] FAMILY = Bytes.toBytes("cf");

  /** constructor */
  public TestCompaction() {
    // Set cache flush size to 1MB
    conf.setInt(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, 1024 * 1024);
    conf.setInt(HConstants.HREGION_MEMSTORE_BLOCK_MULTIPLIER, 100);
    conf.setLong(HConstants.COMPACTION_SCANNER_SIZE_MAX, 10L);
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      NoLimitThroughputController.class.getName());
    compactionThreshold = conf.getInt("hbase.hstore.compactionThreshold", 3);

    secondRowBytes = START_KEY_BYTES.clone();
    // Increment the least significant character so we get to next row.
    secondRowBytes[START_KEY_BYTES.length - 1]++;
    thirdRowBytes = START_KEY_BYTES.clone();
    thirdRowBytes[START_KEY_BYTES.length - 1] =
      (byte) (thirdRowBytes[START_KEY_BYTES.length - 1] + 2);
  }

  @Before
  public void setUp() throws Exception {
    TableDescriptorBuilder builder = UTIL.createModifyableTableDescriptor(name.getMethodName());
    if (name.getMethodName().equals("testCompactionSeqId")) {
      UTIL.getConfiguration().set("hbase.hstore.compaction.kv.max", "10");
      UTIL.getConfiguration().set(DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY,
        DummyCompactor.class.getName());
      ColumnFamilyDescriptor familyDescriptor =
        ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(65536).build();
      builder.setColumnFamily(familyDescriptor);
    }
    if (name.getMethodName().equals("testCompactionWithCorruptBlock")) {
      UTIL.getConfiguration().setBoolean("hbase.hstore.validate.read_fully", true);
      ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(FAMILY)
        .setCompressionType(Compression.Algorithm.GZ).build();
      builder.setColumnFamily(familyDescriptor);
    }
    this.tableDescriptor = builder.build();
    this.r = UTIL.createLocalHRegion(tableDescriptor, null, null);
  }

  @After
  public void tearDown() throws Exception {
    WAL wal = r.getWAL();
    this.r.close();
    wal.close();
  }

  /**
   * Verify that you can stop a long-running compaction (used during RS shutdown)
   */
  @Test
  public void testInterruptCompactionBySize() throws Exception {
    assertEquals(0, count());

    // lower the polling interval for this test
    conf.setInt(SIZE_LIMIT_KEY, 10 * 1000 /* 10 KB */);

    try {
      // Create a couple store files w/ 15KB (over 10KB interval)
      int jmax = (int) Math.ceil(15.0 / compactionThreshold);
      byte[] pad = new byte[1000]; // 1 KB chunk
      for (int i = 0; i < compactionThreshold; i++) {
        Table loader = new RegionAsTable(r);
        Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(i)));
        p.setDurability(Durability.SKIP_WAL);
        for (int j = 0; j < jmax; j++) {
          p.addColumn(COLUMN_FAMILY, Bytes.toBytes(j), pad);
        }
        HTestConst.addContent(loader, Bytes.toString(COLUMN_FAMILY));
        loader.put(p);
        r.flush(true);
      }

      HRegion spyR = spy(r);
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          r.writestate.writesEnabled = false;
          return invocation.callRealMethod();
        }
      }).when(spyR).doRegionCompactionPrep();

      // force a minor compaction, but not before requesting a stop
      spyR.compactStores();

      // ensure that the compaction stopped, all old files are intact,
      HStore s = r.getStore(COLUMN_FAMILY);
      assertEquals(compactionThreshold, s.getStorefilesCount());
      assertTrue(s.getStorefilesSize() > 15 * 1000);
      // and no new store files persisted past compactStores()
      // only one empty dir exists in temp dir
      FileStatus[] ls = r.getFilesystem().listStatus(r.getRegionFileSystem().getTempDir());
      assertEquals(1, ls.length);
      Path storeTempDir =
        new Path(r.getRegionFileSystem().getTempDir(), Bytes.toString(COLUMN_FAMILY));
      assertTrue(r.getFilesystem().exists(storeTempDir));
      ls = r.getFilesystem().listStatus(storeTempDir);
      assertEquals(0, ls.length);
    } finally {
      // don't mess up future tests
      r.writestate.writesEnabled = true;
      conf.setInt(SIZE_LIMIT_KEY, 10 * 1000 * 1000 /* 10 MB */);

      // Delete all Store information once done using
      for (int i = 0; i < compactionThreshold; i++) {
        Delete delete = new Delete(Bytes.add(STARTROW, Bytes.toBytes(i)));
        byte[][] famAndQf = { COLUMN_FAMILY, null };
        delete.addFamily(famAndQf[0]);
        r.delete(delete);
      }
      r.flush(true);

      // Multiple versions allowed for an entry, so the delete isn't enough
      // Lower TTL and expire to ensure that all our entries have been wiped
      final int ttl = 1000;
      for (HStore store : this.r.stores.values()) {
        ScanInfo old = store.getScanInfo();
        ScanInfo si = old.customize(old.getMaxVersions(), ttl, old.getKeepDeletedCells());
        store.setScanInfo(si);
      }
      Thread.sleep(ttl);

      r.compact(true);
      assertEquals(0, count());
    }
  }

  @Test
  public void testInterruptCompactionByTime() throws Exception {
    assertEquals(0, count());

    // lower the polling interval for this test
    conf.setLong(TIME_LIMIT_KEY, 1 /* 1ms */);

    try {
      // Create a couple store files w/ 15KB (over 10KB interval)
      int jmax = (int) Math.ceil(15.0 / compactionThreshold);
      byte[] pad = new byte[1000]; // 1 KB chunk
      for (int i = 0; i < compactionThreshold; i++) {
        Table loader = new RegionAsTable(r);
        Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(i)));
        p.setDurability(Durability.SKIP_WAL);
        for (int j = 0; j < jmax; j++) {
          p.addColumn(COLUMN_FAMILY, Bytes.toBytes(j), pad);
        }
        HTestConst.addContent(loader, Bytes.toString(COLUMN_FAMILY));
        loader.put(p);
        r.flush(true);
      }

      HRegion spyR = spy(r);
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          r.writestate.writesEnabled = false;
          return invocation.callRealMethod();
        }
      }).when(spyR).doRegionCompactionPrep();

      // force a minor compaction, but not before requesting a stop
      spyR.compactStores();

      // ensure that the compaction stopped, all old files are intact,
      HStore s = r.getStore(COLUMN_FAMILY);
      assertEquals(compactionThreshold, s.getStorefilesCount());
      assertTrue(s.getStorefilesSize() > 15 * 1000);
      // and no new store files persisted past compactStores()
      // only one empty dir exists in temp dir
      FileStatus[] ls = r.getFilesystem().listStatus(r.getRegionFileSystem().getTempDir());
      assertEquals(1, ls.length);
      Path storeTempDir =
        new Path(r.getRegionFileSystem().getTempDir(), Bytes.toString(COLUMN_FAMILY));
      assertTrue(r.getFilesystem().exists(storeTempDir));
      ls = r.getFilesystem().listStatus(storeTempDir);
      assertEquals(0, ls.length);
    } finally {
      // don't mess up future tests
      r.writestate.writesEnabled = true;
      conf.setLong(TIME_LIMIT_KEY, 10 * 1000L /* 10 s */);

      // Delete all Store information once done using
      for (int i = 0; i < compactionThreshold; i++) {
        Delete delete = new Delete(Bytes.add(STARTROW, Bytes.toBytes(i)));
        byte[][] famAndQf = { COLUMN_FAMILY, null };
        delete.addFamily(famAndQf[0]);
        r.delete(delete);
      }
      r.flush(true);

      // Multiple versions allowed for an entry, so the delete isn't enough
      // Lower TTL and expire to ensure that all our entries have been wiped
      final int ttl = 1000;
      for (HStore store : this.r.stores.values()) {
        ScanInfo old = store.getScanInfo();
        ScanInfo si = old.customize(old.getMaxVersions(), ttl, old.getKeepDeletedCells());
        store.setScanInfo(si);
      }
      Thread.sleep(ttl);

      r.compact(true);
      assertEquals(0, count());
    }
  }

  private int count() throws IOException {
    int count = 0;
    for (HStoreFile f : this.r.stores.get(COLUMN_FAMILY_TEXT).getStorefiles()) {
      f.initReader();
      try (StoreFileScanner scanner = f.getPreadScanner(false, Long.MAX_VALUE, 0, false)) {
        scanner.seek(KeyValue.LOWESTKEY);
        while (scanner.next() != null) {
          count++;
        }
      }
    }
    return count;
  }

  private void createStoreFile(final HRegion region) throws IOException {
    createStoreFile(region, Bytes.toString(COLUMN_FAMILY));
  }

  private void createStoreFile(final HRegion region, String family) throws IOException {
    Table loader = new RegionAsTable(region);
    HTestConst.addContent(loader, family);
    region.flush(true);
  }

  @Test
  public void testCompactionWithCorruptResult() throws Exception {
    int nfiles = 10;
    for (int i = 0; i < nfiles; i++) {
      createStoreFile(r);
    }
    HStore store = r.getStore(COLUMN_FAMILY);

    Collection<HStoreFile> storeFiles = store.getStorefiles();
    DefaultCompactor tool = (DefaultCompactor) store.storeEngine.getCompactor();
    CompactionRequestImpl request = new CompactionRequestImpl(storeFiles);
    tool.compact(request, NoLimitThroughputController.INSTANCE, null);

    // Now lets corrupt the compacted file.
    FileSystem fs = store.getFileSystem();
    // default compaction policy created one and only one new compacted file
    Path tmpPath = store.getRegionFileSystem().createTempName();
    try (FSDataOutputStream stream = fs.create(tmpPath, null, true, 512, (short) 3, 1024L, null)) {
      stream.writeChars("CORRUPT FILE!!!!");
    }

    // The complete compaction should fail and the corrupt file should remain
    // in the 'tmp' directory;
    assertThrows(IOException.class, () -> store.doCompaction(null, null, null,
      EnvironmentEdgeManager.currentTime(), Collections.singletonList(tmpPath)));
    assertTrue(fs.exists(tmpPath));
  }

  /**
   * This test uses a hand-modified HFile, which is loaded in from the resources' path. That file
   * was generated from the test support code in this class and then edited to corrupt the
   * GZ-encoded block by zeroing-out the first two bytes of the GZip header, the "standard
   * declaration" of {@code 1f 8b}, found at offset 33 in the file. I'm not sure why, but it seems
   * that in this test context we do not enforce CRC checksums. Thus, this corruption manifests in
   * the Decompressor rather than in the reader when it loads the block bytes and compares vs. the
   * header.
   */
  @Test
  public void testCompactionWithCorruptBlock() throws Exception {
    createStoreFile(r, Bytes.toString(FAMILY));
    createStoreFile(r, Bytes.toString(FAMILY));
    HStore store = r.getStore(FAMILY);

    Collection<HStoreFile> storeFiles = store.getStorefiles();
    DefaultCompactor tool = (DefaultCompactor) store.storeEngine.getCompactor();
    CompactionRequestImpl request = new CompactionRequestImpl(storeFiles);
    tool.compact(request, NoLimitThroughputController.INSTANCE, null);

    // insert the hfile with a corrupted data block into the region's tmp directory, where
    // compaction output is collected.
    FileSystem fs = store.getFileSystem();
    Path tmpPath = store.getRegionFileSystem().createTempName();
    try (
      InputStream inputStream =
        getClass().getResourceAsStream("TestCompaction_HFileWithCorruptBlock.gz");
      GZIPInputStream gzipInputStream = new GZIPInputStream(Objects.requireNonNull(inputStream));
      OutputStream outputStream = fs.create(tmpPath, null, true, 512, (short) 3, 1024L, null)) {
      assertThat(gzipInputStream, notNullValue());
      assertThat(outputStream, notNullValue());
      IOUtils.copyBytes(gzipInputStream, outputStream, 512);
    }
    LoggerFactory.getLogger(TestCompaction.class).info("Wrote corrupted HFile to {}", tmpPath);

    // The complete compaction should fail and the corrupt file should remain
    // in the 'tmp' directory;
    try {
      store.doCompaction(request, storeFiles, null, EnvironmentEdgeManager.currentTime(),
        Collections.singletonList(tmpPath));
    } catch (IOException e) {
      Throwable rootCause = e;
      while (rootCause.getCause() != null) {
        rootCause = rootCause.getCause();
      }
      assertThat(rootCause, allOf(instanceOf(IOException.class),
        hasProperty("message", containsString("not a gzip file"))));
      assertTrue(fs.exists(tmpPath));
      return;
    }
    fail("Compaction should have failed due to corrupt block");
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
    CompactSplit thread = new CompactSplit(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    HStore store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < MAX_FILES_TO_COMPACT + 1; i++) {
      createStoreFile(r);
    }

    CountDownLatch latch = new CountDownLatch(1);
    Tracker tracker = new Tracker(latch);
    thread.requestCompaction(r, store, "test custom comapction", PRIORITY_USER, tracker, null);
    // wait for the latch to complete.
    latch.await();

    thread.interruptIfNecessary();
  }

  @Test
  public void testCompactionFailure() throws Exception {
    // setup a compact/split thread on a mock server
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    CompactSplit thread = new CompactSplit(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    HStore store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < HStore.DEFAULT_BLOCKING_STOREFILE_COUNT - 1; i++) {
      createStoreFile(r);
    }

    HRegion mockRegion = Mockito.spy(r);
    Mockito.when(mockRegion.checkSplit())
      .thenThrow(new RuntimeException("Thrown intentionally by test!"));

    try (MetricsRegionWrapperImpl metricsWrapper = new MetricsRegionWrapperImpl(r)) {

      long preCompletedCount = metricsWrapper.getNumCompactionsCompleted();
      long preFailedCount = metricsWrapper.getNumCompactionsFailed();

      CountDownLatch latch = new CountDownLatch(1);
      Tracker tracker = new Tracker(latch);
      thread.requestCompaction(mockRegion, store, "test custom comapction", PRIORITY_USER, tracker,
        null);
      // wait for the latch to complete.
      latch.await(120, TimeUnit.SECONDS);

      // compaction should have completed and been marked as failed due to error in split request
      long postCompletedCount = metricsWrapper.getNumCompactionsCompleted();
      long postFailedCount = metricsWrapper.getNumCompactionsFailed();

      assertTrue("Completed count should have increased (pre=" + preCompletedCount + ", post="
        + postCompletedCount + ")", postCompletedCount > preCompletedCount);
      assertTrue("Failed count should have increased (pre=" + preFailedCount + ", post="
        + postFailedCount + ")", postFailedCount > preFailedCount);
    }
  }

  /**
   * Test no new Compaction requests are generated after calling stop compactions
   */
  @Test
  public void testStopStartCompaction() throws IOException {
    // setup a compact/split thread on a mock server
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    final CompactSplit thread = new CompactSplit(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);
    // setup a region/store with some files
    HStore store = r.getStore(COLUMN_FAMILY);
    createStoreFile(r);
    for (int i = 0; i < HStore.DEFAULT_BLOCKING_STOREFILE_COUNT - 1; i++) {
      createStoreFile(r);
    }
    thread.switchCompaction(false);
    thread.requestCompaction(r, store, "test", Store.PRIORITY_USER,
      CompactionLifeCycleTracker.DUMMY, null);
    assertFalse(thread.isCompactionsEnabled());
    int longCompactions = thread.getLongCompactions().getActiveCount();
    int shortCompactions = thread.getShortCompactions().getActiveCount();
    assertEquals(
      "longCompactions=" + longCompactions + "," + "shortCompactions=" + shortCompactions, 0,
      longCompactions + shortCompactions);
    thread.switchCompaction(true);
    assertTrue(thread.isCompactionsEnabled());
    // Make sure no compactions have run.
    assertEquals(0, thread.getLongCompactions().getCompletedTaskCount()
      + thread.getShortCompactions().getCompletedTaskCount());
    // Request a compaction and make sure it is submitted successfully.
    thread.requestCompaction(r, store, "test", Store.PRIORITY_USER,
      CompactionLifeCycleTracker.DUMMY, null);
    // Wait until the compaction finishes.
    Waiter.waitFor(UTIL.getConfiguration(), 5000,
      (Waiter.Predicate<Exception>) () -> thread.getLongCompactions().getCompletedTaskCount()
        + thread.getShortCompactions().getCompletedTaskCount() == 1);
    // Make sure there are no compactions running.
    assertEquals(0,
      thread.getLongCompactions().getActiveCount() + thread.getShortCompactions().getActiveCount());
  }

  @Test
  public void testInterruptingRunningCompactions() throws Exception {
    // setup a compact/split thread on a mock server
    conf.set(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      WaitThroughPutController.class.getName());
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    CompactSplit thread = new CompactSplit(mockServer);

    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    HStore store = r.getStore(COLUMN_FAMILY);
    int jmax = (int) Math.ceil(15.0 / compactionThreshold);
    byte[] pad = new byte[1000]; // 1 KB chunk
    for (int i = 0; i < compactionThreshold; i++) {
      Table loader = new RegionAsTable(r);
      Put p = new Put(Bytes.add(STARTROW, Bytes.toBytes(i)));
      p.setDurability(Durability.SKIP_WAL);
      for (int j = 0; j < jmax; j++) {
        p.addColumn(COLUMN_FAMILY, Bytes.toBytes(j), pad);
      }
      HTestConst.addContent(loader, Bytes.toString(COLUMN_FAMILY));
      loader.put(p);
      r.flush(true);
    }
    HStore s = r.getStore(COLUMN_FAMILY);
    int initialFiles = s.getStorefilesCount();

    thread.requestCompaction(r, store, "test custom comapction", PRIORITY_USER,
      CompactionLifeCycleTracker.DUMMY, null);

    Thread.sleep(3000);
    thread.switchCompaction(false);
    assertEquals(initialFiles, s.getStorefilesCount());
    // don't mess up future tests
    thread.switchCompaction(true);
  }

  /**
   * HBASE-7947: Regression test to ensure adding to the correct list in the {@link CompactSplit}
   * @throws Exception on failure
   */
  @Test
  public void testMultipleCustomCompactionRequests() throws Exception {
    // setup a compact/split thread on a mock server
    HRegionServer mockServer = Mockito.mock(HRegionServer.class);
    Mockito.when(mockServer.getConfiguration()).thenReturn(r.getBaseConf());
    CompactSplit thread = new CompactSplit(mockServer);
    Mockito.when(mockServer.getCompactSplitThread()).thenReturn(thread);

    // setup a region/store with some files
    int numStores = r.getStores().size();
    CountDownLatch latch = new CountDownLatch(numStores);
    Tracker tracker = new Tracker(latch);
    // create some store files and setup requests for each store on which we want to do a
    // compaction
    for (HStore store : r.getStores()) {
      createStoreFile(r, store.getColumnFamilyName());
      createStoreFile(r, store.getColumnFamilyName());
      createStoreFile(r, store.getColumnFamilyName());
      thread.requestCompaction(r, store, "test mulitple custom comapctions", PRIORITY_USER, tracker,
        null);
    }
    // wait for the latch to complete.
    latch.await();

    thread.interruptIfNecessary();
  }

  class StoreMockMaker extends StatefulStoreMockMaker {
    public ArrayList<HStoreFile> compacting = new ArrayList<>();
    public ArrayList<HStoreFile> notCompacting = new ArrayList<>();
    private final ArrayList<Integer> results;

    public StoreMockMaker(ArrayList<Integer> results) {
      this.results = results;
    }

    public class TestCompactionContext extends CompactionContext {

      private List<HStoreFile> selectedFiles;

      public TestCompactionContext(List<HStoreFile> selectedFiles) {
        super();
        this.selectedFiles = selectedFiles;
      }

      @Override
      public List<HStoreFile> preSelect(List<HStoreFile> filesCompacting) {
        return new ArrayList<>();
      }

      @Override
      public boolean select(List<HStoreFile> filesCompacting, boolean isUserCompaction,
        boolean mayUseOffPeak, boolean forceMajor) throws IOException {
        this.request = new CompactionRequestImpl(selectedFiles);
        this.request.setPriority(getPriority());
        return true;
      }

      @Override
      public List<Path> compact(ThroughputController throughputController, User user)
        throws IOException {
        finishCompaction(this.selectedFiles);
        return new ArrayList<>();
      }
    }

    @Override
    public synchronized Optional<CompactionContext> selectCompaction() {
      CompactionContext ctx = new TestCompactionContext(new ArrayList<>(notCompacting));
      compacting.addAll(notCompacting);
      notCompacting.clear();
      try {
        ctx.select(null, false, false, false);
      } catch (IOException ex) {
        fail("Shouldn't happen");
      }
      return Optional.of(ctx);
    }

    @Override
    public synchronized void cancelCompaction(Object object) {
      TestCompactionContext ctx = (TestCompactionContext) object;
      compacting.removeAll(ctx.selectedFiles);
      notCompacting.addAll(ctx.selectedFiles);
    }

    public synchronized void finishCompaction(List<HStoreFile> sfs) {
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
        synchronized (this) {
          this.notifyAll();
        }
      }

      @Override
      public List<Path> compact(ThroughputController throughputController, User user)
        throws IOException {
        try {
          isInCompact = true;
          synchronized (this) {
            this.wait();
          }
        } catch (InterruptedException e) {
          Assume.assumeNoException(e);
        }
        return new ArrayList<>();
      }

      @Override
      public List<HStoreFile> preSelect(List<HStoreFile> filesCompacting) {
        return new ArrayList<>();
      }

      @Override
      public boolean select(List<HStoreFile> f, boolean i, boolean m, boolean e)
        throws IOException {
        this.request = new CompactionRequestImpl(new ArrayList<>());
        return true;
      }
    }

    @Override
    public Optional<CompactionContext> selectCompaction() {
      this.blocked = new BlockingCompactionContext();
      try {
        this.blocked.select(null, false, false, false);
      } catch (IOException ex) {
        fail("Shouldn't happen");
      }
      return Optional.of(blocked);
    }

    @Override
    public void cancelCompaction(Object object) {
    }

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
    public HStore createStoreMock(String name) throws Exception {
      return createStoreMock(Integer.MIN_VALUE, name);
    }

    public HStore createStoreMock(int priority, String name) throws Exception {
      // Override the mock to always return the specified priority.
      HStore s = super.createStoreMock(name);
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
    CompactSplit cst = new CompactSplit(mockServer);
    when(mockServer.getCompactSplitThread()).thenReturn(cst);
    // prevent large compaction thread pool stealing job from small compaction queue.
    cst.shutdownLongCompactions();
    // Set up the region mock that redirects compactions.
    HRegion r = mock(HRegion.class);
    when(r.compact(any(), any(), any(), any())).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        invocation.<CompactionContext> getArgument(0).compact(invocation.getArgument(2), null);
        return true;
      }
    });

    // Set up store mocks for 2 "real" stores and the one we use for blocking CST.
    ArrayList<Integer> results = new ArrayList<>();
    StoreMockMaker sm = new StoreMockMaker(results), sm2 = new StoreMockMaker(results);
    HStore store = sm.createStoreMock("store1");
    HStore store2 = sm2.createStoreMock("store2");
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
   * Firstly write 10 cells (with different time stamp) to a qualifier and flush to hfile1, then
   * write 10 cells (with different time stamp) to the same qualifier and flush to hfile2. The
   * latest cell (cell-A) in hfile1 and the oldest cell (cell-B) in hfile2 are with the same time
   * stamp but different sequence id, and will get scanned successively during compaction.
   * <p/>
   * We set compaction.kv.max to 10 so compaction will scan 10 versions each round, meanwhile we set
   * keepSeqIdPeriod=0 in {@link DummyCompactor} so all 10 versions of hfile2 will be written out
   * with seqId cleaned (set to 0) including cell-B, then when scanner goes to cell-A it will cause
   * a scan out-of-order assertion error before HBASE-16931 if error occurs during the test
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
    public DummyCompactor(Configuration conf, HStore store) {
      super(conf, store);
      this.keepSeqIdPeriod = 0;
    }
  }

  private static HStoreFile createFile() throws Exception {
    HStoreFile sf = mock(HStoreFile.class);
    when(sf.getPath()).thenReturn(new Path("file"));
    StoreFileReader r = mock(StoreFileReader.class);
    when(r.length()).thenReturn(10L);
    when(sf.getReader()).thenReturn(r);
    return sf;
  }

  /**
   * Simple {@link CompactionLifeCycleTracker} on which you can wait until the requested compaction
   * finishes.
   */
  public static class Tracker implements CompactionLifeCycleTracker {

    private final CountDownLatch done;

    public Tracker(CountDownLatch done) {
      this.done = done;
    }

    @Override
    public void afterExecution(Store store) {
      done.countDown();
    }
  }

  /**
   * Simple {@link CompactionLifeCycleTracker} on which you can wait until the requested compaction
   * finishes.
   */
  public static class WaitThroughPutController extends NoLimitThroughputController {

    public WaitThroughPutController() {
    }

    @Override
    public long control(String compactionName, long size) throws InterruptedException {
      Thread.sleep(6000000);
      return 6000000;
    }
  }
}
