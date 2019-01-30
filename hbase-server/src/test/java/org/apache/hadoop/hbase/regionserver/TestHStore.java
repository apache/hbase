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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MemoryCompactionPolicy;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.exceptions.IllegalArgumentIOException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Test class for the HStore
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestHStore {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHStore.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestHStore.class);
  @Rule
  public TestName name = new TestName();

  HRegion region;
  HStore store;
  byte [] table = Bytes.toBytes("table");
  byte [] family = Bytes.toBytes("family");

  byte [] row = Bytes.toBytes("row");
  byte [] row2 = Bytes.toBytes("row2");
  byte [] qf1 = Bytes.toBytes("qf1");
  byte [] qf2 = Bytes.toBytes("qf2");
  byte [] qf3 = Bytes.toBytes("qf3");
  byte [] qf4 = Bytes.toBytes("qf4");
  byte [] qf5 = Bytes.toBytes("qf5");
  byte [] qf6 = Bytes.toBytes("qf6");

  NavigableSet<byte[]> qualifiers = new ConcurrentSkipListSet<>(Bytes.BYTES_COMPARATOR);

  List<Cell> expected = new ArrayList<>();
  List<Cell> result = new ArrayList<>();

  long id = System.currentTimeMillis();
  Get get = new Get(row);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR = TEST_UTIL.getDataTestDir("TestStore").toString();


  /**
   * Setup
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    qualifiers.add(qf1);
    qualifiers.add(qf3);
    qualifiers.add(qf5);

    Iterator<byte[]> iter = qualifiers.iterator();
    while(iter.hasNext()){
      byte [] next = iter.next();
      expected.add(new KeyValue(row, family, next, 1, (byte[])null));
      get.addColumn(family, next);
    }
  }

  private void init(String methodName) throws IOException {
    init(methodName, TEST_UTIL.getConfiguration());
  }

  private HStore init(String methodName, Configuration conf) throws IOException {
    // some of the tests write 4 versions and then flush
    // (with HBASE-4241, lower versions are collected on flush)
    return init(methodName, conf,
      ColumnFamilyDescriptorBuilder.newBuilder(family).setMaxVersions(4).build());
  }

  private HStore init(String methodName, Configuration conf, ColumnFamilyDescriptor hcd)
      throws IOException {
    return init(methodName, conf, TableDescriptorBuilder.newBuilder(TableName.valueOf(table)), hcd);
  }

  private HStore init(String methodName, Configuration conf, TableDescriptorBuilder builder,
      ColumnFamilyDescriptor hcd) throws IOException {
    return init(methodName, conf, builder, hcd, null);
  }

  private HStore init(String methodName, Configuration conf, TableDescriptorBuilder builder,
      ColumnFamilyDescriptor hcd, MyStoreHook hook) throws IOException {
    return init(methodName, conf, builder, hcd, hook, false);
  }

  private void initHRegion(String methodName, Configuration conf, TableDescriptorBuilder builder,
      ColumnFamilyDescriptor hcd, MyStoreHook hook, boolean switchToPread) throws IOException {
    TableDescriptor htd = builder.setColumnFamily(hcd).build();
    Path basedir = new Path(DIR + methodName);
    Path tableDir = FSUtils.getTableDir(basedir, htd.getTableName());
    final Path logdir = new Path(basedir, AbstractFSWALProvider.getWALDirectoryName(methodName));

    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);
    ChunkCreator.initialize(MemStoreLABImpl.CHUNK_SIZE_DEFAULT, false,
      MemStoreLABImpl.CHUNK_SIZE_DEFAULT, 1, 0, null);
    RegionInfo info = RegionInfoBuilder.newBuilder(htd.getTableName()).build();
    Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, basedir);
    WALFactory wals = new WALFactory(walConf, methodName);
    region = new HRegion(new HRegionFileSystem(conf, fs, tableDir, info), wals.getWAL(info), conf,
        htd, null);
    region.regionServicesForStores = Mockito.spy(region.regionServicesForStores);
    ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(1);
    Mockito.when(region.regionServicesForStores.getInMemoryCompactionPool()).thenReturn(pool);
  }

  private HStore init(String methodName, Configuration conf, TableDescriptorBuilder builder,
      ColumnFamilyDescriptor hcd, MyStoreHook hook, boolean switchToPread) throws IOException {
    initHRegion(methodName, conf, builder, hcd, hook, switchToPread);
    if (hook == null) {
      store = new HStore(region, hcd, conf);
    } else {
      store = new MyStore(region, hcd, conf, hook, switchToPread);
    }
    return store;
  }

  /**
   * Test we do not lose data if we fail a flush and then close.
   * Part of HBase-10466
   * @throws Exception
   */
  @Test
  public void testFlushSizeSizing() throws Exception {
    LOG.info("Setting up a faulty file system that cannot write in " + this.name.getMethodName());
    final Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    // Only retry once.
    conf.setInt("hbase.hstore.flush.retries.number", 1);
    User user = User.createUserForTesting(conf, this.name.getMethodName(),
      new String[]{"foo"});
    // Inject our faulty LocalFileSystem
    conf.setClass("fs.file.impl", FaultyFileSystem.class, FileSystem.class);
    user.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        // Make sure it worked (above is sensitive to caching details in hadoop core)
        FileSystem fs = FileSystem.get(conf);
        assertEquals(FaultyFileSystem.class, fs.getClass());
        FaultyFileSystem ffs = (FaultyFileSystem)fs;

        // Initialize region
        init(name.getMethodName(), conf);

        MemStoreSize mss = store.memstore.getFlushableSize();
        assertEquals(0, mss.getDataSize());
        LOG.info("Adding some data");
        MemStoreSizing kvSize = new NonThreadSafeMemStoreSizing();
        store.add(new KeyValue(row, family, qf1, 1, (byte[]) null), kvSize);
        // add the heap size of active (mutable) segment
        kvSize.incMemStoreSize(0, MutableSegment.DEEP_OVERHEAD, 0, 0);
        mss = store.memstore.getFlushableSize();
        assertEquals(kvSize.getMemStoreSize(), mss);
        // Flush.  Bug #1 from HBASE-10466.  Make sure size calculation on failed flush is right.
        try {
          LOG.info("Flushing");
          flushStore(store, id++);
          fail("Didn't bubble up IOE!");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains("Fault injected"));
        }
        // due to snapshot, change mutable to immutable segment
        kvSize.incMemStoreSize(0,
          CSLMImmutableSegment.DEEP_OVERHEAD_CSLM - MutableSegment.DEEP_OVERHEAD, 0, 0);
        mss = store.memstore.getFlushableSize();
        assertEquals(kvSize.getMemStoreSize(), mss);
        MemStoreSizing kvSize2 = new NonThreadSafeMemStoreSizing();
        store.add(new KeyValue(row, family, qf2, 2, (byte[]) null), kvSize2);
        kvSize2.incMemStoreSize(0, MutableSegment.DEEP_OVERHEAD, 0, 0);
        // Even though we add a new kv, we expect the flushable size to be 'same' since we have
        // not yet cleared the snapshot -- the above flush failed.
        assertEquals(kvSize.getMemStoreSize(), mss);
        ffs.fault.set(false);
        flushStore(store, id++);
        mss = store.memstore.getFlushableSize();
        // Size should be the foreground kv size.
        assertEquals(kvSize2.getMemStoreSize(), mss);
        flushStore(store, id++);
        mss = store.memstore.getFlushableSize();
        assertEquals(0, mss.getDataSize());
        assertEquals(MutableSegment.DEEP_OVERHEAD, mss.getHeapSize());
        return null;
      }
    });
  }

  /**
   * Verify that compression and data block encoding are respected by the
   * Store.createWriterInTmp() method, used on store flush.
   */
  @Test
  public void testCreateWriter() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);

    ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setCompressionType(Compression.Algorithm.GZ).setDataBlockEncoding(DataBlockEncoding.DIFF)
        .build();
    init(name.getMethodName(), conf, hcd);

    // Test createWriterInTmp()
    StoreFileWriter writer =
        store.createWriterInTmp(4, hcd.getCompressionType(), false, true, false, false);
    Path path = writer.getPath();
    writer.append(new KeyValue(row, family, qf1, Bytes.toBytes(1)));
    writer.append(new KeyValue(row, family, qf2, Bytes.toBytes(2)));
    writer.append(new KeyValue(row2, family, qf1, Bytes.toBytes(3)));
    writer.append(new KeyValue(row2, family, qf2, Bytes.toBytes(4)));
    writer.close();

    // Verify that compression and encoding settings are respected
    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf), true, conf);
    assertEquals(hcd.getCompressionType(), reader.getCompressionAlgorithm());
    assertEquals(hcd.getDataBlockEncoding(), reader.getDataBlockEncoding());
    reader.close();
  }

  @Test
  public void testDeleteExpiredStoreFiles() throws Exception {
    testDeleteExpiredStoreFiles(0);
    testDeleteExpiredStoreFiles(1);
  }

  /*
   * @param minVersions the MIN_VERSIONS for the column family
   */
  public void testDeleteExpiredStoreFiles(int minVersions) throws Exception {
    int storeFileNum = 4;
    int ttl = 4;
    IncrementingEnvironmentEdge edge = new IncrementingEnvironmentEdge();
    EnvironmentEdgeManagerTestHelper.injectEdge(edge);

    Configuration conf = HBaseConfiguration.create();
    // Enable the expired store file deletion
    conf.setBoolean("hbase.store.delete.expired.storefile", true);
    // Set the compaction threshold higher to avoid normal compactions.
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 5);

    init(name.getMethodName() + "-" + minVersions, conf, ColumnFamilyDescriptorBuilder
        .newBuilder(family).setMinVersions(minVersions).setTimeToLive(ttl).build());

    long storeTtl = this.store.getScanInfo().getTtl();
    long sleepTime = storeTtl / storeFileNum;
    long timeStamp;
    // There are 4 store files and the max time stamp difference among these
    // store files will be (this.store.ttl / storeFileNum)
    for (int i = 1; i <= storeFileNum; i++) {
      LOG.info("Adding some data for the store file #" + i);
      timeStamp = EnvironmentEdgeManager.currentTime();
      this.store.add(new KeyValue(row, family, qf1, timeStamp, (byte[]) null), null);
      this.store.add(new KeyValue(row, family, qf2, timeStamp, (byte[]) null), null);
      this.store.add(new KeyValue(row, family, qf3, timeStamp, (byte[]) null), null);
      flush(i);
      edge.incrementTime(sleepTime);
    }

    // Verify the total number of store files
    assertEquals(storeFileNum, this.store.getStorefiles().size());

     // Each call will find one expired store file and delete it before compaction happens.
     // There will be no compaction due to threshold above. Last file will not be replaced.
    for (int i = 1; i <= storeFileNum - 1; i++) {
      // verify the expired store file.
      assertFalse(this.store.requestCompaction().isPresent());
      Collection<HStoreFile> sfs = this.store.getStorefiles();
      // Ensure i files are gone.
      if (minVersions == 0) {
        assertEquals(storeFileNum - i, sfs.size());
        // Ensure only non-expired files remain.
        for (HStoreFile sf : sfs) {
          assertTrue(sf.getReader().getMaxTimestamp() >= (edge.currentTime() - storeTtl));
        }
      } else {
        assertEquals(storeFileNum, sfs.size());
      }
      // Let the next store file expired.
      edge.incrementTime(sleepTime);
    }
    assertFalse(this.store.requestCompaction().isPresent());

    Collection<HStoreFile> sfs = this.store.getStorefiles();
    // Assert the last expired file is not removed.
    if (minVersions == 0) {
      assertEquals(1, sfs.size());
    }
    long ts = sfs.iterator().next().getReader().getMaxTimestamp();
    assertTrue(ts < (edge.currentTime() - storeTtl));

    for (HStoreFile sf : sfs) {
      sf.closeStoreFile(true);
    }
  }

  @Test
  public void testLowestModificationTime() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    // Initialize region
    init(name.getMethodName(), conf);

    int storeFileNum = 4;
    for (int i = 1; i <= storeFileNum; i++) {
      LOG.info("Adding some data for the store file #"+i);
      this.store.add(new KeyValue(row, family, qf1, i, (byte[])null), null);
      this.store.add(new KeyValue(row, family, qf2, i, (byte[])null), null);
      this.store.add(new KeyValue(row, family, qf3, i, (byte[])null), null);
      flush(i);
    }
    // after flush; check the lowest time stamp
    long lowestTimeStampFromManager = StoreUtils.getLowestTimestamp(store.getStorefiles());
    long lowestTimeStampFromFS = getLowestTimeStampFromFS(fs, store.getStorefiles());
    assertEquals(lowestTimeStampFromManager,lowestTimeStampFromFS);

    // after compact; check the lowest time stamp
    store.compact(store.requestCompaction().get(), NoLimitThroughputController.INSTANCE, null);
    lowestTimeStampFromManager = StoreUtils.getLowestTimestamp(store.getStorefiles());
    lowestTimeStampFromFS = getLowestTimeStampFromFS(fs, store.getStorefiles());
    assertEquals(lowestTimeStampFromManager, lowestTimeStampFromFS);
  }

  private static long getLowestTimeStampFromFS(FileSystem fs,
      final Collection<HStoreFile> candidates) throws IOException {
    long minTs = Long.MAX_VALUE;
    if (candidates.isEmpty()) {
      return minTs;
    }
    Path[] p = new Path[candidates.size()];
    int i = 0;
    for (HStoreFile sf : candidates) {
      p[i] = sf.getPath();
      ++i;
    }

    FileStatus[] stats = fs.listStatus(p);
    if (stats == null || stats.length == 0) {
      return minTs;
    }
    for (FileStatus s : stats) {
      minTs = Math.min(minTs, s.getModificationTime());
    }
    return minTs;
  }

  //////////////////////////////////////////////////////////////////////////////
  // Get tests
  //////////////////////////////////////////////////////////////////////////////

  private static final int BLOCKSIZE_SMALL = 8192;
  /**
   * Test for hbase-1686.
   * @throws IOException
   */
  @Test
  public void testEmptyStoreFile() throws IOException {
    init(this.name.getMethodName());
    // Write a store file.
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), null);
    flush(1);
    // Now put in place an empty store file.  Its a little tricky.  Have to
    // do manually with hacked in sequence id.
    HStoreFile f = this.store.getStorefiles().iterator().next();
    Path storedir = f.getPath().getParent();
    long seqid = f.getMaxSequenceId();
    Configuration c = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(c);
    HFileContext meta = new HFileContextBuilder().withBlockSize(BLOCKSIZE_SMALL).build();
    StoreFileWriter w = new StoreFileWriter.Builder(c, new CacheConfig(c),
        fs)
            .withOutputDir(storedir)
            .withFileContext(meta)
            .build();
    w.appendMetadata(seqid + 1, false);
    w.close();
    this.store.close();
    // Reopen it... should pick up two files
    this.store = new HStore(this.store.getHRegion(), this.store.getColumnFamilyDescriptor(), c);
    assertEquals(2, this.store.getStorefilesCount());

    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(),
        qualifiers);
    assertEquals(1, result.size());
  }

  /**
   * Getting data from memstore only
   * @throws IOException
   */
  @Test
  public void testGet_FromMemStoreOnly() throws IOException {
    init(this.name.getMethodName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null), null);

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(), qualifiers);

    //Compare
    assertCheck();
  }

  @Test
  public void testTimeRangeIfSomeCellsAreDroppedInFlush() throws IOException {
    testTimeRangeIfSomeCellsAreDroppedInFlush(1);
    testTimeRangeIfSomeCellsAreDroppedInFlush(3);
    testTimeRangeIfSomeCellsAreDroppedInFlush(5);
  }

  private void testTimeRangeIfSomeCellsAreDroppedInFlush(int maxVersion) throws IOException {
    init(this.name.getMethodName(), TEST_UTIL.getConfiguration(),
    ColumnFamilyDescriptorBuilder.newBuilder(family).setMaxVersions(maxVersion).build());
    long currentTs = 100;
    long minTs = currentTs;
    // the extra cell won't be flushed to disk,
    // so the min of timerange will be different between memStore and hfile.
    for (int i = 0; i != (maxVersion + 1); ++i) {
      this.store.add(new KeyValue(row, family, qf1, ++currentTs, (byte[])null), null);
      if (i == 1) {
        minTs = currentTs;
      }
    }
    flushStore(store, id++);

    Collection<HStoreFile> files = store.getStorefiles();
    assertEquals(1, files.size());
    HStoreFile f = files.iterator().next();
    f.initReader();
    StoreFileReader reader = f.getReader();
    assertEquals(minTs, reader.timeRange.getMin());
    assertEquals(currentTs, reader.timeRange.getMax());
  }

  /**
   * Getting data from files only
   * @throws IOException
   */
  @Test
  public void testGet_FromFilesOnly() throws IOException {
    init(this.name.getMethodName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), null);
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null), null);
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null), null);
    //flush
    flush(3);

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(),
        qualifiers);
    //this.store.get(get, qualifiers, result);

    //Need to sort the result since multiple files
    Collections.sort(result, CellComparatorImpl.COMPARATOR);

    //Compare
    assertCheck();
  }

  /**
   * Getting data from memstore and files
   * @throws IOException
   */
  @Test
  public void testGet_FromMemStoreAndFiles() throws IOException {
    init(this.name.getMethodName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), null);
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null), null);
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null), null);
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null), null);

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(), qualifiers);

    //Need to sort the result since multiple files
    Collections.sort(result, CellComparatorImpl.COMPARATOR);

    //Compare
    assertCheck();
  }

  private void flush(int storeFilessize) throws IOException{
    this.store.snapshot();
    flushStore(store, id++);
    assertEquals(storeFilessize, this.store.getStorefiles().size());
    assertEquals(0, ((AbstractMemStore)this.store.memstore).getActive().getCellsCount());
  }

  private void assertCheck() {
    assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i));
    }
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
    if (store != null) {
      try {
        store.close();
      } catch (IOException e) {
      }
      store = null;
    }
    if (region != null) {
      region.close();
      region = null;
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    TEST_UTIL.cleanupTestDir();
  }

  @Test
  public void testHandleErrorsInFlush() throws Exception {
    LOG.info("Setting up a faulty file system that cannot write");

    final Configuration conf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    User user = User.createUserForTesting(conf,
        "testhandleerrorsinflush", new String[]{"foo"});
    // Inject our faulty LocalFileSystem
    conf.setClass("fs.file.impl", FaultyFileSystem.class,
        FileSystem.class);
    user.runAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        // Make sure it worked (above is sensitive to caching details in hadoop core)
        FileSystem fs = FileSystem.get(conf);
        assertEquals(FaultyFileSystem.class, fs.getClass());

        // Initialize region
        init(name.getMethodName(), conf);

        LOG.info("Adding some data");
        store.add(new KeyValue(row, family, qf1, 1, (byte[])null), null);
        store.add(new KeyValue(row, family, qf2, 1, (byte[])null), null);
        store.add(new KeyValue(row, family, qf3, 1, (byte[])null), null);

        LOG.info("Before flush, we should have no files");

        Collection<StoreFileInfo> files =
          store.getRegionFileSystem().getStoreFiles(store.getColumnFamilyName());
        assertEquals(0, files != null ? files.size() : 0);

        //flush
        try {
          LOG.info("Flushing");
          flush(1);
          fail("Didn't bubble up IOE!");
        } catch (IOException ioe) {
          assertTrue(ioe.getMessage().contains("Fault injected"));
        }

        LOG.info("After failed flush, we should still have no files!");
        files = store.getRegionFileSystem().getStoreFiles(store.getColumnFamilyName());
        assertEquals(0, files != null ? files.size() : 0);
        store.getHRegion().getWAL().close();
        return null;
      }
    });
    FileSystem.closeAllForUGI(user.getUGI());
  }

  /**
   * Faulty file system that will fail if you write past its fault position the FIRST TIME
   * only; thereafter it will succeed.  Used by {@link TestHRegion} too.
   */
  static class FaultyFileSystem extends FilterFileSystem {
    List<SoftReference<FaultyOutputStream>> outStreams = new ArrayList<>();
    private long faultPos = 200;
    AtomicBoolean fault = new AtomicBoolean(true);

    public FaultyFileSystem() {
      super(new LocalFileSystem());
      System.err.println("Creating faulty!");
    }

    @Override
    public FSDataOutputStream create(Path p) throws IOException {
      return new FaultyOutputStream(super.create(p), faultPos, fault);
    }

    @Override
    public FSDataOutputStream create(Path f, FsPermission permission,
        boolean overwrite, int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return new FaultyOutputStream(super.create(f, permission,
          overwrite, bufferSize, replication, blockSize, progress), faultPos, fault);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f, boolean overwrite,
        int bufferSize, short replication, long blockSize, Progressable progress)
    throws IOException {
      // Fake it.  Call create instead.  The default implementation throws an IOE
      // that this is not supported.
      return create(f, overwrite, bufferSize, replication, blockSize, progress);
    }
  }

  static class FaultyOutputStream extends FSDataOutputStream {
    volatile long faultPos = Long.MAX_VALUE;
    private final AtomicBoolean fault;

    public FaultyOutputStream(FSDataOutputStream out, long faultPos, final AtomicBoolean fault)
    throws IOException {
      super(out, null);
      this.faultPos = faultPos;
      this.fault = fault;
    }

    @Override
    public synchronized void write(byte[] buf, int offset, int length) throws IOException {
      System.err.println("faulty stream write at pos " + getPos());
      injectFault();
      super.write(buf, offset, length);
    }

    private void injectFault() throws IOException {
      if (this.fault.get() && getPos() >= faultPos) {
        throw new IOException("Fault injected");
      }
    }
  }

  private static void flushStore(HStore store, long id) throws IOException {
    StoreFlushContext storeFlushCtx = store.createFlushContext(id, FlushLifeCycleTracker.DUMMY);
    storeFlushCtx.prepare();
    storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
    storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));
  }

  /**
   * Generate a list of KeyValues for testing based on given parameters
   * @param timestamps
   * @param numRows
   * @param qualifier
   * @param family
   * @return
   */
  List<Cell> getKeyValueSet(long[] timestamps, int numRows,
      byte[] qualifier, byte[] family) {
    List<Cell> kvList = new ArrayList<>();
    for (int i=1;i<=numRows;i++) {
      byte[] b = Bytes.toBytes(i);
      for (long timestamp: timestamps) {
        kvList.add(new KeyValue(b, family, qualifier, timestamp, b));
      }
    }
    return kvList;
  }

  /**
   * Test to ensure correctness when using Stores with multiple timestamps
   * @throws IOException
   */
  @Test
  public void testMultipleTimestamps() throws IOException {
    int numRows = 1;
    long[] timestamps1 = new long[] {1,5,10,20};
    long[] timestamps2 = new long[] {30,80};

    init(this.name.getMethodName());

    List<Cell> kvList1 = getKeyValueSet(timestamps1,numRows, qf1, family);
    for (Cell kv : kvList1) {
      this.store.add(kv, null);
    }

    this.store.snapshot();
    flushStore(store, id++);

    List<Cell> kvList2 = getKeyValueSet(timestamps2,numRows, qf1, family);
    for(Cell kv : kvList2) {
      this.store.add(kv, null);
    }

    List<Cell> result;
    Get get = new Get(Bytes.toBytes(1));
    get.addColumn(family,qf1);

    get.setTimeRange(0,15);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(40,90);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(10,45);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(80,145);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(1,2);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()>0);

    get.setTimeRange(90,200);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    assertTrue(result.size()==0);
  }

  /**
   * Test for HBASE-3492 - Test split on empty colfam (no store files).
   *
   * @throws IOException When the IO operations fail.
   */
  @Test
  public void testSplitWithEmptyColFam() throws IOException {
    init(this.name.getMethodName());
    assertFalse(store.getSplitPoint().isPresent());
    store.getHRegion().forceSplit(null);
    assertFalse(store.getSplitPoint().isPresent());
    store.getHRegion().clearSplit();
  }

  @Test
  public void testStoreUsesConfigurationFromHcdAndHtd() throws Exception {
    final String CONFIG_KEY = "hbase.regionserver.thread.compaction.throttle";
    long anyValue = 10;

    // We'll check that it uses correct config and propagates it appropriately by going thru
    // the simplest "real" path I can find - "throttleCompaction", which just checks whether
    // a number we pass in is higher than some config value, inside compactionPolicy.
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CONFIG_KEY, anyValue);
    init(name.getMethodName() + "-xml", conf);
    assertTrue(store.throttleCompaction(anyValue + 1));
    assertFalse(store.throttleCompaction(anyValue));

    // HTD overrides XML.
    --anyValue;
    init(name.getMethodName() + "-htd", conf, TableDescriptorBuilder
        .newBuilder(TableName.valueOf(table)).setValue(CONFIG_KEY, Long.toString(anyValue)),
      ColumnFamilyDescriptorBuilder.of(family));
    assertTrue(store.throttleCompaction(anyValue + 1));
    assertFalse(store.throttleCompaction(anyValue));

    // HCD overrides them both.
    --anyValue;
    init(name.getMethodName() + "-hcd", conf,
      TableDescriptorBuilder.newBuilder(TableName.valueOf(table)).setValue(CONFIG_KEY,
        Long.toString(anyValue)),
      ColumnFamilyDescriptorBuilder.newBuilder(family).setValue(CONFIG_KEY, Long.toString(anyValue))
          .build());
    assertTrue(store.throttleCompaction(anyValue + 1));
    assertFalse(store.throttleCompaction(anyValue));
  }

  public static class DummyStoreEngine extends DefaultStoreEngine {
    public static DefaultCompactor lastCreatedCompactor = null;

    @Override
    protected void createComponents(Configuration conf, HStore store, CellComparator comparator)
        throws IOException {
      super.createComponents(conf, store, comparator);
      lastCreatedCompactor = this.compactor;
    }
  }

  @Test
  public void testStoreUsesSearchEngineOverride() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DummyStoreEngine.class.getName());
    init(this.name.getMethodName(), conf);
    assertEquals(DummyStoreEngine.lastCreatedCompactor,
      this.store.storeEngine.getCompactor());
  }

  private void addStoreFile() throws IOException {
    HStoreFile f = this.store.getStorefiles().iterator().next();
    Path storedir = f.getPath().getParent();
    long seqid = this.store.getMaxSequenceId().orElse(0L);
    Configuration c = TEST_UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(c);
    HFileContext fileContext = new HFileContextBuilder().withBlockSize(BLOCKSIZE_SMALL).build();
    StoreFileWriter w = new StoreFileWriter.Builder(c, new CacheConfig(c),
        fs)
            .withOutputDir(storedir)
            .withFileContext(fileContext)
            .build();
    w.appendMetadata(seqid + 1, false);
    w.close();
    LOG.info("Added store file:" + w.getPath());
  }

  private void archiveStoreFile(int index) throws IOException {
    Collection<HStoreFile> files = this.store.getStorefiles();
    HStoreFile sf = null;
    Iterator<HStoreFile> it = files.iterator();
    for (int i = 0; i <= index; i++) {
      sf = it.next();
    }
    store.getRegionFileSystem().removeStoreFiles(store.getColumnFamilyName(), Lists.newArrayList(sf));
  }

  private void closeCompactedFile(int index) throws IOException {
    Collection<HStoreFile> files =
        this.store.getStoreEngine().getStoreFileManager().getCompactedfiles();
    HStoreFile sf = null;
    Iterator<HStoreFile> it = files.iterator();
    for (int i = 0; i <= index; i++) {
      sf = it.next();
    }
    sf.closeStoreFile(true);
    store.getStoreEngine().getStoreFileManager().removeCompactedFiles(Lists.newArrayList(sf));
  }

  @Test
  public void testRefreshStoreFiles() throws Exception {
    init(name.getMethodName());

    assertEquals(0, this.store.getStorefilesCount());

    // Test refreshing store files when no store files are there
    store.refreshStoreFiles();
    assertEquals(0, this.store.getStorefilesCount());

    // add some data, flush
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), null);
    flush(1);
    assertEquals(1, this.store.getStorefilesCount());

    // add one more file
    addStoreFile();

    assertEquals(1, this.store.getStorefilesCount());
    store.refreshStoreFiles();
    assertEquals(2, this.store.getStorefilesCount());

    // add three more files
    addStoreFile();
    addStoreFile();
    addStoreFile();

    assertEquals(2, this.store.getStorefilesCount());
    store.refreshStoreFiles();
    assertEquals(5, this.store.getStorefilesCount());

    closeCompactedFile(0);
    archiveStoreFile(0);

    assertEquals(5, this.store.getStorefilesCount());
    store.refreshStoreFiles();
    assertEquals(4, this.store.getStorefilesCount());

    archiveStoreFile(0);
    archiveStoreFile(1);
    archiveStoreFile(2);

    assertEquals(4, this.store.getStorefilesCount());
    store.refreshStoreFiles();
    assertEquals(1, this.store.getStorefilesCount());

    archiveStoreFile(0);
    store.refreshStoreFiles();
    assertEquals(0, this.store.getStorefilesCount());
  }

  @Test
  public void testRefreshStoreFilesNotChanged() throws IOException {
    init(name.getMethodName());

    assertEquals(0, this.store.getStorefilesCount());

    // add some data, flush
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), null);
    flush(1);
    // add one more file
    addStoreFile();

    HStore spiedStore = spy(store);

    // call first time after files changed
    spiedStore.refreshStoreFiles();
    assertEquals(2, this.store.getStorefilesCount());
    verify(spiedStore, times(1)).replaceStoreFiles(any(), any());

    // call second time
    spiedStore.refreshStoreFiles();

    //ensure that replaceStoreFiles is not called if files are not refreshed
    verify(spiedStore, times(0)).replaceStoreFiles(null, null);
  }

  private long countMemStoreScanner(StoreScanner scanner) {
    if (scanner.currentScanners == null) {
      return 0;
    }
    return scanner.currentScanners.stream()
            .filter(s -> !s.isFileScanner())
            .count();
  }

  @Test
  public void testNumberOfMemStoreScannersAfterFlush() throws IOException {
    long seqId = 100;
    long timestamp = System.currentTimeMillis();
    Cell cell0 = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(row).setFamily(family)
        .setQualifier(qf1).setTimestamp(timestamp).setType(Cell.Type.Put)
        .setValue(qf1).build();
    PrivateCellUtil.setSequenceId(cell0, seqId);
    testNumberOfMemStoreScannersAfterFlush(Arrays.asList(cell0), Collections.emptyList());

    Cell cell1 = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(row).setFamily(family)
        .setQualifier(qf2).setTimestamp(timestamp).setType(Cell.Type.Put)
        .setValue(qf1).build();
    PrivateCellUtil.setSequenceId(cell1, seqId);
    testNumberOfMemStoreScannersAfterFlush(Arrays.asList(cell0), Arrays.asList(cell1));

    seqId = 101;
    timestamp = System.currentTimeMillis();
    Cell cell2 = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(row2).setFamily(family)
        .setQualifier(qf2).setTimestamp(timestamp).setType(Cell.Type.Put)
        .setValue(qf1).build();
    PrivateCellUtil.setSequenceId(cell2, seqId);
    testNumberOfMemStoreScannersAfterFlush(Arrays.asList(cell0), Arrays.asList(cell1, cell2));
  }

  private void testNumberOfMemStoreScannersAfterFlush(List<Cell> inputCellsBeforeSnapshot,
      List<Cell> inputCellsAfterSnapshot) throws IOException {
    init(this.name.getMethodName() + "-" + inputCellsBeforeSnapshot.size());
    TreeSet<byte[]> quals = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    long seqId = Long.MIN_VALUE;
    for (Cell c : inputCellsBeforeSnapshot) {
      quals.add(CellUtil.cloneQualifier(c));
      seqId = Math.max(seqId, c.getSequenceId());
    }
    for (Cell c : inputCellsAfterSnapshot) {
      quals.add(CellUtil.cloneQualifier(c));
      seqId = Math.max(seqId, c.getSequenceId());
    }
    inputCellsBeforeSnapshot.forEach(c -> store.add(c, null));
    StoreFlushContext storeFlushCtx = store.createFlushContext(id++, FlushLifeCycleTracker.DUMMY);
    storeFlushCtx.prepare();
    inputCellsAfterSnapshot.forEach(c -> store.add(c, null));
    int numberOfMemScannersBeforeFlush = inputCellsAfterSnapshot.isEmpty() ? 1 : 2;
    try (StoreScanner s = (StoreScanner) store.getScanner(new Scan(), quals, seqId)) {
      // snapshot + active (if inputCellsAfterSnapshot isn't empty)
      assertEquals(numberOfMemScannersBeforeFlush, countMemStoreScanner(s));
      storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
      storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));
      // snapshot has no data after flush
      int numberOfMemScannersAfterFlush = inputCellsAfterSnapshot.isEmpty() ? 0 : 1;
      boolean more;
      int cellCount = 0;
      do {
        List<Cell> cells = new ArrayList<>();
        more = s.next(cells);
        cellCount += cells.size();
        assertEquals(more ? numberOfMemScannersAfterFlush : 0, countMemStoreScanner(s));
      } while (more);
      assertEquals("The number of cells added before snapshot is " + inputCellsBeforeSnapshot.size()
          + ", The number of cells added after snapshot is " + inputCellsAfterSnapshot.size(),
          inputCellsBeforeSnapshot.size() + inputCellsAfterSnapshot.size(), cellCount);
      // the current scanners is cleared
      assertEquals(0, countMemStoreScanner(s));
    }
  }

  private Cell createCell(byte[] qualifier, long ts, long sequenceId, byte[] value)
      throws IOException {
    return createCell(row, qualifier, ts, sequenceId, value);
  }

  private Cell createCell(byte[] row, byte[] qualifier, long ts, long sequenceId, byte[] value)
      throws IOException {
    Cell c = CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(row).setFamily(family)
        .setQualifier(qualifier).setTimestamp(ts).setType(Cell.Type.Put)
        .setValue(value).build();
    PrivateCellUtil.setSequenceId(c, sequenceId);
    return c;
  }

  @Test
  public void testFlushBeforeCompletingScanWoFilter() throws IOException, InterruptedException {
    final AtomicBoolean timeToGoNextRow = new AtomicBoolean(false);
    final int expectedSize = 3;
    testFlushBeforeCompletingScan(new MyListHook() {
      @Override
      public void hook(int currentSize) {
        if (currentSize == expectedSize - 1) {
          try {
            flushStore(store, id++);
            timeToGoNextRow.set(true);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }, new FilterBase() {
      @Override
      public Filter.ReturnCode filterCell(final Cell c) throws IOException {
        return ReturnCode.INCLUDE;
      }
    }, expectedSize);
  }

  @Test
  public void testFlushBeforeCompletingScanWithFilter() throws IOException, InterruptedException {
    final AtomicBoolean timeToGoNextRow = new AtomicBoolean(false);
    final int expectedSize = 2;
    testFlushBeforeCompletingScan(new MyListHook() {
      @Override
      public void hook(int currentSize) {
        if (currentSize == expectedSize - 1) {
          try {
            flushStore(store, id++);
            timeToGoNextRow.set(true);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }, new FilterBase() {
      @Override
      public Filter.ReturnCode filterCell(final Cell c) throws IOException {
        if (timeToGoNextRow.get()) {
          timeToGoNextRow.set(false);
          return ReturnCode.NEXT_ROW;
        } else {
          return ReturnCode.INCLUDE;
        }
      }
    }, expectedSize);
  }

  @Test
  public void testFlushBeforeCompletingScanWithFilterHint() throws IOException,
      InterruptedException {
    final AtomicBoolean timeToGetHint = new AtomicBoolean(false);
    final int expectedSize = 2;
    testFlushBeforeCompletingScan(new MyListHook() {
      @Override
      public void hook(int currentSize) {
        if (currentSize == expectedSize - 1) {
          try {
            flushStore(store, id++);
            timeToGetHint.set(true);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }, new FilterBase() {
      @Override
      public Filter.ReturnCode filterCell(final Cell c) throws IOException {
        if (timeToGetHint.get()) {
          timeToGetHint.set(false);
          return Filter.ReturnCode.SEEK_NEXT_USING_HINT;
        } else {
          return Filter.ReturnCode.INCLUDE;
        }
      }
      @Override
      public Cell getNextCellHint(Cell currentCell) throws IOException {
        return currentCell;
      }
    }, expectedSize);
  }

  private void testFlushBeforeCompletingScan(MyListHook hook, Filter filter, int expectedSize)
          throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    byte[] r0 = Bytes.toBytes("row0");
    byte[] r1 = Bytes.toBytes("row1");
    byte[] r2 = Bytes.toBytes("row2");
    byte[] value0 = Bytes.toBytes("value0");
    byte[] value1 = Bytes.toBytes("value1");
    byte[] value2 = Bytes.toBytes("value2");
    MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
    long ts = EnvironmentEdgeManager.currentTime();
    long seqId = 100;
    init(name.getMethodName(), conf, TableDescriptorBuilder.newBuilder(TableName.valueOf(table)),
      ColumnFamilyDescriptorBuilder.newBuilder(family).setMaxVersions(1).build(),
      new MyStoreHook() {
        @Override
        public long getSmallestReadPoint(HStore store) {
          return seqId + 3;
        }
      });
    // The cells having the value0 won't be flushed to disk because the value of max version is 1
    store.add(createCell(r0, qf1, ts, seqId, value0), memStoreSizing);
    store.add(createCell(r0, qf2, ts, seqId, value0), memStoreSizing);
    store.add(createCell(r0, qf3, ts, seqId, value0), memStoreSizing);
    store.add(createCell(r1, qf1, ts + 1, seqId + 1, value1), memStoreSizing);
    store.add(createCell(r1, qf2, ts + 1, seqId + 1, value1), memStoreSizing);
    store.add(createCell(r1, qf3, ts + 1, seqId + 1, value1), memStoreSizing);
    store.add(createCell(r2, qf1, ts + 2, seqId + 2, value2), memStoreSizing);
    store.add(createCell(r2, qf2, ts + 2, seqId + 2, value2), memStoreSizing);
    store.add(createCell(r2, qf3, ts + 2, seqId + 2, value2), memStoreSizing);
    store.add(createCell(r1, qf1, ts + 3, seqId + 3, value1), memStoreSizing);
    store.add(createCell(r1, qf2, ts + 3, seqId + 3, value1), memStoreSizing);
    store.add(createCell(r1, qf3, ts + 3, seqId + 3, value1), memStoreSizing);
    List<Cell> myList = new MyList<>(hook);
    Scan scan = new Scan()
            .withStartRow(r1)
            .setFilter(filter);
    try (InternalScanner scanner = (InternalScanner) store.getScanner(
          scan, null, seqId + 3)){
      // r1
      scanner.next(myList);
      assertEquals(expectedSize, myList.size());
      for (Cell c : myList) {
        byte[] actualValue = CellUtil.cloneValue(c);
        assertTrue("expected:" + Bytes.toStringBinary(value1)
          + ", actual:" + Bytes.toStringBinary(actualValue)
          , Bytes.equals(actualValue, value1));
      }
      List<Cell> normalList = new ArrayList<>(3);
      // r2
      scanner.next(normalList);
      assertEquals(3, normalList.size());
      for (Cell c : normalList) {
        byte[] actualValue = CellUtil.cloneValue(c);
        assertTrue("expected:" + Bytes.toStringBinary(value2)
          + ", actual:" + Bytes.toStringBinary(actualValue)
          , Bytes.equals(actualValue, value2));
      }
    }
  }

  @Test
  public void testCreateScannerAndSnapshotConcurrently() throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HStore.MEMSTORE_CLASS_NAME, MyCompactingMemStore.class.getName());
    init(name.getMethodName(), conf, ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setInMemoryCompaction(MemoryCompactionPolicy.BASIC).build());
    byte[] value = Bytes.toBytes("value");
    MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
    long ts = EnvironmentEdgeManager.currentTime();
    long seqId = 100;
    // older data whihc shouldn't be "seen" by client
    store.add(createCell(qf1, ts, seqId, value), memStoreSizing);
    store.add(createCell(qf2, ts, seqId, value), memStoreSizing);
    store.add(createCell(qf3, ts, seqId, value), memStoreSizing);
    TreeSet<byte[]> quals = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    quals.add(qf1);
    quals.add(qf2);
    quals.add(qf3);
    StoreFlushContext storeFlushCtx = store.createFlushContext(id++, FlushLifeCycleTracker.DUMMY);
    MyCompactingMemStore.START_TEST.set(true);
    Runnable flush = () -> {
      // this is blocked until we create first scanner from pipeline and snapshot -- phase (1/5)
      // recreate the active memstore -- phase (4/5)
      storeFlushCtx.prepare();
    };
    ExecutorService service = Executors.newSingleThreadExecutor();
    service.submit(flush);
    // we get scanner from pipeline and snapshot but they are empty. -- phase (2/5)
    // this is blocked until we recreate the active memstore -- phase (3/5)
    // we get scanner from active memstore but it is empty -- phase (5/5)
    InternalScanner scanner = (InternalScanner) store.getScanner(
          new Scan(new Get(row)), quals, seqId + 1);
    service.shutdown();
    service.awaitTermination(20, TimeUnit.SECONDS);
    try {
      try {
        List<Cell> results = new ArrayList<>();
        scanner.next(results);
        assertEquals(3, results.size());
        for (Cell c : results) {
          byte[] actualValue = CellUtil.cloneValue(c);
          assertTrue("expected:" + Bytes.toStringBinary(value)
            + ", actual:" + Bytes.toStringBinary(actualValue)
            , Bytes.equals(actualValue, value));
        }
      } finally {
        scanner.close();
      }
    } finally {
      MyCompactingMemStore.START_TEST.set(false);
      storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
      storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));
    }
  }

  @Test
  public void testScanWithDoubleFlush() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // Initialize region
    MyStore myStore = initMyStore(name.getMethodName(), conf, new MyStoreHook(){
      @Override
      public void getScanners(MyStore store) throws IOException {
        final long tmpId = id++;
        ExecutorService s = Executors.newSingleThreadExecutor();
        s.submit(() -> {
          try {
            // flush the store before storescanner updates the scanners from store.
            // The current data will be flushed into files, and the memstore will
            // be clear.
            // -- phase (4/4)
            flushStore(store, tmpId);
          }catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        });
        s.shutdown();
        try {
          // wait for the flush, the thread will be blocked in HStore#notifyChangedReadersObservers.
          s.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
        }
      }
    });
    byte[] oldValue = Bytes.toBytes("oldValue");
    byte[] currentValue = Bytes.toBytes("currentValue");
    MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
    long ts = EnvironmentEdgeManager.currentTime();
    long seqId = 100;
    // older data whihc shouldn't be "seen" by client
    myStore.add(createCell(qf1, ts, seqId, oldValue), memStoreSizing);
    myStore.add(createCell(qf2, ts, seqId, oldValue), memStoreSizing);
    myStore.add(createCell(qf3, ts, seqId, oldValue), memStoreSizing);
    long snapshotId = id++;
    // push older data into snapshot -- phase (1/4)
    StoreFlushContext storeFlushCtx = store.createFlushContext(snapshotId, FlushLifeCycleTracker
        .DUMMY);
    storeFlushCtx.prepare();

    // insert current data into active -- phase (2/4)
    myStore.add(createCell(qf1, ts + 1, seqId + 1, currentValue), memStoreSizing);
    myStore.add(createCell(qf2, ts + 1, seqId + 1, currentValue), memStoreSizing);
    myStore.add(createCell(qf3, ts + 1, seqId + 1, currentValue), memStoreSizing);
    TreeSet<byte[]> quals = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    quals.add(qf1);
    quals.add(qf2);
    quals.add(qf3);
    try (InternalScanner scanner = (InternalScanner) myStore.getScanner(
        new Scan(new Get(row)), quals, seqId + 1)) {
      // complete the flush -- phase (3/4)
      storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
      storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));

      List<Cell> results = new ArrayList<>();
      scanner.next(results);
      assertEquals(3, results.size());
      for (Cell c : results) {
        byte[] actualValue = CellUtil.cloneValue(c);
        assertTrue("expected:" + Bytes.toStringBinary(currentValue)
          + ", actual:" + Bytes.toStringBinary(actualValue)
          , Bytes.equals(actualValue, currentValue));
      }
    }
  }

  @Test
  public void testReclaimChunkWhenScaning() throws IOException {
    init("testReclaimChunkWhenScaning");
    long ts = EnvironmentEdgeManager.currentTime();
    long seqId = 100;
    byte[] value = Bytes.toBytes("value");
    // older data whihc shouldn't be "seen" by client
    store.add(createCell(qf1, ts, seqId, value), null);
    store.add(createCell(qf2, ts, seqId, value), null);
    store.add(createCell(qf3, ts, seqId, value), null);
    TreeSet<byte[]> quals = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    quals.add(qf1);
    quals.add(qf2);
    quals.add(qf3);
    try (InternalScanner scanner = (InternalScanner) store.getScanner(
        new Scan(new Get(row)), quals, seqId)) {
      List<Cell> results = new MyList<>(size -> {
        switch (size) {
          // 1) we get the first cell (qf1)
          // 2) flush the data to have StoreScanner update inner scanners
          // 3) the chunk will be reclaimed after updaing
          case 1:
            try {
              flushStore(store, id++);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            break;
          // 1) we get the second cell (qf2)
          // 2) add some cell to fill some byte into the chunk (we have only one chunk)
          case 2:
            try {
              byte[] newValue = Bytes.toBytes("newValue");
              // older data whihc shouldn't be "seen" by client
              store.add(createCell(qf1, ts + 1, seqId + 1, newValue), null);
              store.add(createCell(qf2, ts + 1, seqId + 1, newValue), null);
              store.add(createCell(qf3, ts + 1, seqId + 1, newValue), null);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            break;
          default:
            break;
        }
      });
      scanner.next(results);
      assertEquals(3, results.size());
      for (Cell c : results) {
        byte[] actualValue = CellUtil.cloneValue(c);
        assertTrue("expected:" + Bytes.toStringBinary(value)
          + ", actual:" + Bytes.toStringBinary(actualValue)
          , Bytes.equals(actualValue, value));
      }
    }
  }

  /**
   * If there are two running InMemoryFlushRunnable, the later InMemoryFlushRunnable
   * may change the versionedList. And the first InMemoryFlushRunnable will use the chagned
   * versionedList to remove the corresponding segments.
   * In short, there will be some segements which isn't in merge are removed.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRunDoubleMemStoreCompactors() throws IOException, InterruptedException {
    int flushSize = 500;
    Configuration conf = HBaseConfiguration.create();
    conf.set(HStore.MEMSTORE_CLASS_NAME, MyCompactingMemStoreWithCustomCompactor.class.getName());
    conf.setDouble(CompactingMemStore.IN_MEMORY_FLUSH_THRESHOLD_FACTOR_KEY, 0.25);
    MyCompactingMemStoreWithCustomCompactor.RUNNER_COUNT.set(0);
    conf.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, String.valueOf(flushSize));
    // Set the lower threshold to invoke the "MERGE" policy
    conf.set(MemStoreCompactionStrategy.COMPACTING_MEMSTORE_THRESHOLD_KEY, String.valueOf(0));
    init(name.getMethodName(), conf, ColumnFamilyDescriptorBuilder.newBuilder(family)
        .setInMemoryCompaction(MemoryCompactionPolicy.BASIC).build());
    byte[] value = Bytes.toBytes("thisisavarylargevalue");
    MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
    long ts = EnvironmentEdgeManager.currentTime();
    long seqId = 100;
    // older data whihc shouldn't be "seen" by client
    store.add(createCell(qf1, ts, seqId, value), memStoreSizing);
    store.add(createCell(qf2, ts, seqId, value), memStoreSizing);
    store.add(createCell(qf3, ts, seqId, value), memStoreSizing);
    assertEquals(1, MyCompactingMemStoreWithCustomCompactor.RUNNER_COUNT.get());
    StoreFlushContext storeFlushCtx = store.createFlushContext(id++, FlushLifeCycleTracker.DUMMY);
    storeFlushCtx.prepare();
    // This shouldn't invoke another in-memory flush because the first compactor thread
    // hasn't accomplished the in-memory compaction.
    store.add(createCell(qf1, ts + 1, seqId + 1, value), memStoreSizing);
    store.add(createCell(qf1, ts + 1, seqId + 1, value), memStoreSizing);
    store.add(createCell(qf1, ts + 1, seqId + 1, value), memStoreSizing);
    assertEquals(1, MyCompactingMemStoreWithCustomCompactor.RUNNER_COUNT.get());
    //okay. Let the compaction be completed
    MyMemStoreCompactor.START_COMPACTOR_LATCH.countDown();
    CompactingMemStore mem = (CompactingMemStore) ((HStore)store).memstore;
    while (mem.isMemStoreFlushingInMemory()) {
      TimeUnit.SECONDS.sleep(1);
    }
    // This should invoke another in-memory flush.
    store.add(createCell(qf1, ts + 2, seqId + 2, value), memStoreSizing);
    store.add(createCell(qf1, ts + 2, seqId + 2, value), memStoreSizing);
    store.add(createCell(qf1, ts + 2, seqId + 2, value), memStoreSizing);
    assertEquals(2, MyCompactingMemStoreWithCustomCompactor.RUNNER_COUNT.get());
    conf.set(HConstants.HREGION_MEMSTORE_FLUSH_SIZE,
      String.valueOf(TableDescriptorBuilder.DEFAULT_MEMSTORE_FLUSH_SIZE));
    storeFlushCtx.flushCache(Mockito.mock(MonitoredTask.class));
    storeFlushCtx.commit(Mockito.mock(MonitoredTask.class));
  }

  @Test
  public void testAge() throws IOException {
    long currentTime = System.currentTimeMillis();
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    edge.setValue(currentTime);
    EnvironmentEdgeManager.injectEdge(edge);
    Configuration conf = TEST_UTIL.getConfiguration();
    ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.of(family);
    initHRegion(name.getMethodName(), conf,
      TableDescriptorBuilder.newBuilder(TableName.valueOf(table)), hcd, null, false);
    HStore store = new HStore(region, hcd, conf) {

      @Override
      protected StoreEngine<?, ?, ?, ?> createStoreEngine(HStore store, Configuration conf,
          CellComparator kvComparator) throws IOException {
        List<HStoreFile> storefiles =
            Arrays.asList(mockStoreFile(currentTime - 10), mockStoreFile(currentTime - 100),
              mockStoreFile(currentTime - 1000), mockStoreFile(currentTime - 10000));
        StoreFileManager sfm = mock(StoreFileManager.class);
        when(sfm.getStorefiles()).thenReturn(storefiles);
        StoreEngine<?, ?, ?, ?> storeEngine = mock(StoreEngine.class);
        when(storeEngine.getStoreFileManager()).thenReturn(sfm);
        return storeEngine;
      }
    };
    assertEquals(10L, store.getMinStoreFileAge().getAsLong());
    assertEquals(10000L, store.getMaxStoreFileAge().getAsLong());
    assertEquals((10 + 100 + 1000 + 10000) / 4.0, store.getAvgStoreFileAge().getAsDouble(), 1E-4);
  }

  private HStoreFile mockStoreFile(long createdTime) {
    StoreFileInfo info = mock(StoreFileInfo.class);
    when(info.getCreatedTimestamp()).thenReturn(createdTime);
    HStoreFile sf = mock(HStoreFile.class);
    when(sf.getReader()).thenReturn(mock(StoreFileReader.class));
    when(sf.isHFile()).thenReturn(true);
    when(sf.getFileInfo()).thenReturn(info);
    return sf;
  }

  private MyStore initMyStore(String methodName, Configuration conf, MyStoreHook hook)
      throws IOException {
    return (MyStore) init(methodName, conf,
      TableDescriptorBuilder.newBuilder(TableName.valueOf(table)),
      ColumnFamilyDescriptorBuilder.newBuilder(family).setMaxVersions(5).build(), hook);
  }

  private static class MyStore extends HStore {
    private final MyStoreHook hook;

    MyStore(final HRegion region, final ColumnFamilyDescriptor family, final Configuration
        confParam, MyStoreHook hook, boolean switchToPread) throws IOException {
      super(region, family, confParam);
      this.hook = hook;
    }

    @Override
    public List<KeyValueScanner> getScanners(List<HStoreFile> files, boolean cacheBlocks,
        boolean usePread, boolean isCompaction, ScanQueryMatcher matcher, byte[] startRow,
        boolean includeStartRow, byte[] stopRow, boolean includeStopRow, long readPt,
        boolean includeMemstoreScanner) throws IOException {
      hook.getScanners(this);
      return super.getScanners(files, cacheBlocks, usePread, isCompaction, matcher, startRow, true,
        stopRow, false, readPt, includeMemstoreScanner);
    }

    @Override
    public long getSmallestReadPoint() {
      return hook.getSmallestReadPoint(this);
    }
  }

  private abstract static class MyStoreHook {

    void getScanners(MyStore store) throws IOException {
    }

    long getSmallestReadPoint(HStore store) {
      return store.getHRegion().getSmallestReadPoint();
    }
  }

  @Test
  public void testSwitchingPreadtoStreamParallelyWithCompactionDischarger() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.hstore.engine.class", DummyStoreEngine.class.getName());
    conf.setLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, 0);
    // Set the lower threshold to invoke the "MERGE" policy
    MyStore store = initMyStore(name.getMethodName(), conf, new MyStoreHook() {});
    MemStoreSizing memStoreSizing = new NonThreadSafeMemStoreSizing();
    long ts = System.currentTimeMillis();
    long seqID = 1L;
    // Add some data to the region and do some flushes
    for (int i = 1; i < 10; i++) {
      store.add(createCell(Bytes.toBytes("row" + i), qf1, ts, seqID++, Bytes.toBytes("")),
        memStoreSizing);
    }
    // flush them
    flushStore(store, seqID);
    for (int i = 11; i < 20; i++) {
      store.add(createCell(Bytes.toBytes("row" + i), qf1, ts, seqID++, Bytes.toBytes("")),
        memStoreSizing);
    }
    // flush them
    flushStore(store, seqID);
    for (int i = 21; i < 30; i++) {
      store.add(createCell(Bytes.toBytes("row" + i), qf1, ts, seqID++, Bytes.toBytes("")),
        memStoreSizing);
    }
    // flush them
    flushStore(store, seqID);

    assertEquals(3, store.getStorefilesCount());
    Scan scan = new Scan();
    scan.addFamily(family);
    Collection<HStoreFile> storefiles2 = store.getStorefiles();
    ArrayList<HStoreFile> actualStorefiles = Lists.newArrayList(storefiles2);
    StoreScanner storeScanner =
        (StoreScanner) store.getScanner(scan, scan.getFamilyMap().get(family), Long.MAX_VALUE);
    // get the current heap
    KeyValueHeap heap = storeScanner.heap;
    // create more store files
    for (int i = 31; i < 40; i++) {
      store.add(createCell(Bytes.toBytes("row" + i), qf1, ts, seqID++, Bytes.toBytes("")),
        memStoreSizing);
    }
    // flush them
    flushStore(store, seqID);

    for (int i = 41; i < 50; i++) {
      store.add(createCell(Bytes.toBytes("row" + i), qf1, ts, seqID++, Bytes.toBytes("")),
        memStoreSizing);
    }
    // flush them
    flushStore(store, seqID);
    storefiles2 = store.getStorefiles();
    ArrayList<HStoreFile> actualStorefiles1 = Lists.newArrayList(storefiles2);
    actualStorefiles1.removeAll(actualStorefiles);
    // Do compaction
    MyThread thread = new MyThread(storeScanner);
    thread.start();
    store.replaceStoreFiles(actualStorefiles, actualStorefiles1);
    thread.join();
    KeyValueHeap heap2 = thread.getHeap();
    assertFalse(heap.equals(heap2));
  }

  private static class MyThread extends Thread {
    private StoreScanner scanner;
    private KeyValueHeap heap;

    public MyThread(StoreScanner scanner) {
      this.scanner = scanner;
    }

    public KeyValueHeap getHeap() {
      return this.heap;
    }

    @Override
    public void run() {
      scanner.trySwitchToStreamRead();
      heap = scanner.heap;
    }
  }

  private static class MyMemStoreCompactor extends MemStoreCompactor {
    private static final AtomicInteger RUNNER_COUNT = new AtomicInteger(0);
    private static final CountDownLatch START_COMPACTOR_LATCH = new CountDownLatch(1);
    public MyMemStoreCompactor(CompactingMemStore compactingMemStore, MemoryCompactionPolicy
        compactionPolicy) throws IllegalArgumentIOException {
      super(compactingMemStore, compactionPolicy);
    }

    @Override
    public boolean start() throws IOException {
      boolean isFirst = RUNNER_COUNT.getAndIncrement() == 0;
      if (isFirst) {
        try {
          START_COMPACTOR_LATCH.await();
          return super.start();
        } catch (InterruptedException ex) {
          throw new RuntimeException(ex);
        }
      }
      return super.start();
    }
  }

  public static class MyCompactingMemStoreWithCustomCompactor extends CompactingMemStore {
    private static final AtomicInteger RUNNER_COUNT = new AtomicInteger(0);
    public MyCompactingMemStoreWithCustomCompactor(Configuration conf, CellComparatorImpl c,
        HStore store, RegionServicesForStores regionServices,
        MemoryCompactionPolicy compactionPolicy) throws IOException {
      super(conf, c, store, regionServices, compactionPolicy);
    }

    @Override
    protected MemStoreCompactor createMemStoreCompactor(MemoryCompactionPolicy compactionPolicy)
        throws IllegalArgumentIOException {
      return new MyMemStoreCompactor(this, compactionPolicy);
    }

    @Override
    protected boolean setInMemoryCompactionFlag() {
      boolean rval = super.setInMemoryCompactionFlag();
      if (rval) {
        RUNNER_COUNT.incrementAndGet();
        if (LOG.isDebugEnabled()) {
          LOG.debug("runner count: " + RUNNER_COUNT.get());
        }
      }
      return rval;
    }
  }

  public static class MyCompactingMemStore extends CompactingMemStore {
    private static final AtomicBoolean START_TEST = new AtomicBoolean(false);
    private final CountDownLatch getScannerLatch = new CountDownLatch(1);
    private final CountDownLatch snapshotLatch = new CountDownLatch(1);
    public MyCompactingMemStore(Configuration conf, CellComparatorImpl c,
        HStore store, RegionServicesForStores regionServices,
        MemoryCompactionPolicy compactionPolicy) throws IOException {
      super(conf, c, store, regionServices, compactionPolicy);
    }

    @Override
    protected List<KeyValueScanner> createList(int capacity) {
      if (START_TEST.get()) {
        try {
          getScannerLatch.countDown();
          snapshotLatch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      return new ArrayList<>(capacity);
    }
    @Override
    protected void pushActiveToPipeline(MutableSegment active) {
      if (START_TEST.get()) {
        try {
          getScannerLatch.await();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      super.pushActiveToPipeline(active);
      if (START_TEST.get()) {
        snapshotLatch.countDown();
      }
    }
  }

  interface MyListHook {
    void hook(int currentSize);
  }

  private static class MyList<T> implements List<T> {
    private final List<T> delegatee = new ArrayList<>();
    private final MyListHook hookAtAdd;
    MyList(final MyListHook hookAtAdd) {
      this.hookAtAdd = hookAtAdd;
    }
    @Override
    public int size() {return delegatee.size();}

    @Override
    public boolean isEmpty() {return delegatee.isEmpty();}

    @Override
    public boolean contains(Object o) {return delegatee.contains(o);}

    @Override
    public Iterator<T> iterator() {return delegatee.iterator();}

    @Override
    public Object[] toArray() {return delegatee.toArray();}

    @Override
    public <R> R[] toArray(R[] a) {return delegatee.toArray(a);}

    @Override
    public boolean add(T e) {
      hookAtAdd.hook(size());
      return delegatee.add(e);
    }

    @Override
    public boolean remove(Object o) {return delegatee.remove(o);}

    @Override
    public boolean containsAll(Collection<?> c) {return delegatee.containsAll(c);}

    @Override
    public boolean addAll(Collection<? extends T> c) {return delegatee.addAll(c);}

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {return delegatee.addAll(index, c);}

    @Override
    public boolean removeAll(Collection<?> c) {return delegatee.removeAll(c);}

    @Override
    public boolean retainAll(Collection<?> c) {return delegatee.retainAll(c);}

    @Override
    public void clear() {delegatee.clear();}

    @Override
    public T get(int index) {return delegatee.get(index);}

    @Override
    public T set(int index, T element) {return delegatee.set(index, element);}

    @Override
    public void add(int index, T element) {delegatee.add(index, element);}

    @Override
    public T remove(int index) {return delegatee.remove(index);}

    @Override
    public int indexOf(Object o) {return delegatee.indexOf(o);}

    @Override
    public int lastIndexOf(Object o) {return delegatee.lastIndexOf(o);}

    @Override
    public ListIterator<T> listIterator() {return delegatee.listIterator();}

    @Override
    public ListIterator<T> listIterator(int index) {return delegatee.listIterator(index);}

    @Override
    public List<T> subList(int fromIndex, int toIndex) {return delegatee.subList(fromIndex, toIndex);}
  }
}
