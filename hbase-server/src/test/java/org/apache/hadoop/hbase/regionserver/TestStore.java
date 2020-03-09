/*
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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Lists;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import static org.apache.hadoop.hbase.regionserver.MemStoreChunkPool.CHUNK_POOL_MAXSIZE_KEY;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

/**
 * Test class for the Store
 */
@Category(MediumTests.class)
public class TestStore {
  private static final Log LOG = LogFactory.getLog(TestStore.class);
  @Rule public TestName name = new TestName();

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

  NavigableSet<byte[]> qualifiers =
    new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);

  List<Cell> expected = new ArrayList<Cell>();
  List<Cell> result = new ArrayList<Cell>();

  long id = System.currentTimeMillis();
  Get get = new Get(row);

  private HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final String DIR = TEST_UTIL.getDataTestDir("TestStore").toString();


  /**
   * Setup
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    qualifiers.clear();
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

  private void init(String methodName, Configuration conf)
  throws IOException {
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    // some of the tests write 4 versions and then flush
    // (with HBASE-4241, lower versions are collected on flush)
    hcd.setMaxVersions(4);
    init(methodName, conf, hcd);
  }

  private void init(String methodName, Configuration conf,
      HColumnDescriptor hcd) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    init(methodName, conf, htd, hcd);
  }

  private Store init(String methodName, Configuration conf, HTableDescriptor htd,
      HColumnDescriptor hcd) throws IOException {
    return init(methodName, conf, htd, hcd, null);
  }

  @SuppressWarnings("deprecation")
  private Store init(String methodName, Configuration conf, HTableDescriptor htd,
      HColumnDescriptor hcd, MyStoreHook hook) throws IOException {
    //Setting up a Store
    Path basedir = new Path(DIR+methodName);
    Path tableDir = FSUtils.getTableDir(basedir, htd.getTableName());
    final Path logdir = new Path(basedir, DefaultWALProvider.getWALDirectoryName(methodName));

    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);

    if (htd.hasFamily(hcd.getName())) {
      htd.modifyFamily(hcd);
    } else {
      htd.addFamily(hcd);
    }
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);
    final Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, basedir);
    final WALFactory wals = new WALFactory(walConf, null, methodName);
    HRegion region = new HRegion(tableDir, wals.getWAL(info.getEncodedNameAsBytes(),
            info.getTable().getNamespace()), fs, conf, info, htd, null);
    if (hook == null) {
      store = new HStore(region, hcd, conf);
    } else {
      store = new MyStore(region, hcd, conf, hook);
    }
    return store;
  }

  /**
   * Test we do not lose data if we fail a flush and then close.
   * Part of HBase-10466
   * @throws Exception
   */
  @Test
  public void testFlushSizeAccounting() throws Exception {
    LOG.info("Setting up a faulty file system that cannot write in " +
      this.name.getMethodName());
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
        Assert.assertEquals(FaultyFileSystem.class, fs.getClass());
        FaultyFileSystem ffs = (FaultyFileSystem)fs;

        // Initialize region
        init(name.getMethodName(), conf);

        long size = store.memstore.getFlushableSize();
        Assert.assertEquals(0, size);
        LOG.info("Adding some data");
        long kvSize = store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
        size = store.memstore.getFlushableSize();
        Assert.assertEquals(kvSize, size);
        // Flush.  Bug #1 from HBASE-10466.  Make sure size calculation on failed flush is right.
        try {
          LOG.info("Flushing");
          flushStore(store, id++);
          Assert.fail("Didn't bubble up IOE!");
        } catch (IOException ioe) {
          Assert.assertTrue(ioe.getMessage().contains("Fault injected"));
        }
        size = store.memstore.getFlushableSize();
        Assert.assertEquals(kvSize, size);
        store.add(new KeyValue(row, family, qf2, 2, (byte[])null));
        // Even though we add a new kv, we expect the flushable size to be 'same' since we have
        // not yet cleared the snapshot -- the above flush failed.
        Assert.assertEquals(kvSize, size);
        ffs.fault.set(false);
        flushStore(store, id++);
        size = store.memstore.getFlushableSize();
        // Size should be the foreground kv size.
        Assert.assertEquals(kvSize, size);
        flushStore(store, id++);
        size = store.memstore.getFlushableSize();
        Assert.assertEquals(0, size);
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

    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setCompressionType(Compression.Algorithm.GZ);
    hcd.setDataBlockEncoding(DataBlockEncoding.DIFF);
    init(name.getMethodName(), conf, hcd);

    // Test createWriterInTmp()
    StoreFile.Writer writer = store.createWriterInTmp(4, hcd.getCompression(), false, true, false);
    Path path = writer.getPath();
    writer.append(new KeyValue(row, family, qf1, Bytes.toBytes(1)));
    writer.append(new KeyValue(row, family, qf2, Bytes.toBytes(2)));
    writer.append(new KeyValue(row2, family, qf1, Bytes.toBytes(3)));
    writer.append(new KeyValue(row2, family, qf2, Bytes.toBytes(4)));
    writer.close();

    // Verify that compression and encoding settings are respected
    HFile.Reader reader = HFile.createReader(fs, path, new CacheConfig(conf), conf);
    Assert.assertEquals(hcd.getCompressionType(), reader.getCompressionAlgorithm());
    Assert.assertEquals(hcd.getDataBlockEncoding(), reader.getDataBlockEncoding());
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

    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMinVersions(minVersions);
    hcd.setTimeToLive(ttl);
    init(name.getMethodName() + "-" + minVersions, conf, hcd);

    long storeTtl = this.store.getScanInfo().getTtl();
    long sleepTime = storeTtl / storeFileNum;
    long timeStamp;
    // There are 4 store files and the max time stamp difference among these
    // store files will be (this.store.ttl / storeFileNum)
    for (int i = 1; i <= storeFileNum; i++) {
      LOG.info("Adding some data for the store file #" + i);
      timeStamp = EnvironmentEdgeManager.currentTime();
      this.store.add(new KeyValue(row, family, qf1, timeStamp, (byte[]) null));
      this.store.add(new KeyValue(row, family, qf2, timeStamp, (byte[]) null));
      this.store.add(new KeyValue(row, family, qf3, timeStamp, (byte[]) null));
      flush(i);
      edge.incrementTime(sleepTime);
    }

    // Verify the total number of store files
    Assert.assertEquals(storeFileNum, this.store.getStorefiles().size());

     // Each call will find one expired store file and delete it before compaction happens.
     // There will be no compaction due to threshold above. Last file will not be replaced.
    for (int i = 1; i <= storeFileNum - 1; i++) {
      // verify the expired store file.
      assertNull(this.store.requestCompaction());
      Collection<StoreFile> sfs = this.store.getStorefiles();
      // Ensure i files are gone.
      if (minVersions == 0) {
        assertEquals(storeFileNum - i, sfs.size());
        // Ensure only non-expired files remain.
        for (StoreFile sf : sfs) {
          assertTrue(sf.getReader().getMaxTimestamp() >= (edge.currentTime() - storeTtl));
        }
      } else {
        assertEquals(storeFileNum, sfs.size());
      }
      // Let the next store file expired.
      edge.incrementTime(sleepTime);
    }
    assertNull(this.store.requestCompaction());

    Collection<StoreFile> sfs = this.store.getStorefiles();
    // Assert the last expired file is not removed.
    if (minVersions == 0) {
      assertEquals(1, sfs.size());
    }
    long ts = sfs.iterator().next().getReader().getMaxTimestamp();
    assertTrue(ts < (edge.currentTime() - storeTtl));

    for (StoreFile sf : sfs) {
      sf.closeReader(true);
    }
  }

  @Test
  public void testRollback() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    // Initialize region
    init(name.getMethodName(), conf);
    Cell cell = CellUtil.createCell(row, family, qf1);
    int len = KeyValueUtil.length(cell);
    int offset = 77;
    byte[] buf = new byte[offset + len];
    KeyValueUtil.appendToByteArray(cell, buf, offset);
    KeyValue newKv = new KeyValue(buf, offset, len);
    newKv.setSequenceId(cell.getSequenceId());
    List<Cell> testCells = Arrays.asList(cell, cell, newKv);
    for (Cell c : testCells) {
      long sizeBeforeRollback = store.heapSize();
      store.add(cell);
      store.rollback(cell);
      long sizeAeforeRollback = store.heapSize();
      assertEquals(sizeBeforeRollback, sizeAeforeRollback);
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
      this.store.add(new KeyValue(row, family, qf1, i, (byte[])null));
      this.store.add(new KeyValue(row, family, qf2, i, (byte[])null));
      this.store.add(new KeyValue(row, family, qf3, i, (byte[])null));
      flush(i);
    }
    // after flush; check the lowest time stamp
    long lowestTimeStampFromManager = StoreUtils.getLowestTimestamp(store.getStorefiles());
    long lowestTimeStampFromFS = getLowestTimeStampFromFS(fs, store.getStorefiles());
    Assert.assertEquals(lowestTimeStampFromManager,lowestTimeStampFromFS);

    // after compact; check the lowest time stamp
    store.compact(store.requestCompaction(), NoLimitThroughputController.INSTANCE);
    lowestTimeStampFromManager = StoreUtils.getLowestTimestamp(store.getStorefiles());
    lowestTimeStampFromFS = getLowestTimeStampFromFS(fs, store.getStorefiles());
    Assert.assertEquals(lowestTimeStampFromManager, lowestTimeStampFromFS);
  }

  private static long getLowestTimeStampFromFS(FileSystem fs,
      final Collection<StoreFile> candidates) throws IOException {
    long minTs = Long.MAX_VALUE;
    if (candidates.isEmpty()) {
      return minTs;
    }
    Path[] p = new Path[candidates.size()];
    int i = 0;
    for (StoreFile sf : candidates) {
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
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    flush(1);
    // Now put in place an empty store file.  Its a little tricky.  Have to
    // do manually with hacked in sequence id.
    StoreFile f = this.store.getStorefiles().iterator().next();
    Path storedir = f.getPath().getParent();
    long seqid = f.getMaxSequenceId();
    Configuration c = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(c);
    HFileContext meta = new HFileContextBuilder().withBlockSize(BLOCKSIZE_SMALL).build();
    StoreFile.Writer w = new StoreFile.WriterBuilder(c, new CacheConfig(c),
        fs)
            .withOutputDir(storedir)
            .withFileContext(meta)
            .build();
    w.appendMetadata(seqid + 1, false);
    w.close();
    this.store.close();
    // Reopen it... should pick up two files
    this.store = new HStore(this.store.getHRegion(), this.store.getFamily(), c);
    Assert.assertEquals(2, this.store.getStorefilesCount());

    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(),
        qualifiers);
    Assert.assertEquals(1, result.size());
  }

  /**
   * Getting data from memstore only
   * @throws IOException
   */
  @Test
  public void testGet_FromMemStoreOnly() throws IOException {
    init(this.name.getMethodName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null));

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(), qualifiers);

    //Compare
    assertCheck();
  }

  /**
   * Getting data from files only
   * @throws IOException
   */
  @Test
  public void testGet_FromFilesOnly() throws IOException {
    init(this.name.getMethodName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null));
    //flush
    flush(3);

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(),
        qualifiers);
    //this.store.get(get, qualifiers, result);

    //Need to sort the result since multiple files
    Collections.sort(result, KeyValue.COMPARATOR);

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
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null));
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null));
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null));

    //Get
    result = HBaseTestingUtility.getFromStoreFile(store,
        get.getRow(), qualifiers);

    //Need to sort the result since multiple files
    Collections.sort(result, KeyValue.COMPARATOR);

    //Compare
    assertCheck();
  }

  private void flush(int storeFilessize) throws IOException{
    this.store.snapshot();
    flushStore(store, id++);
    Assert.assertEquals(storeFilessize, this.store.getStorefiles().size());
    Assert.assertEquals(0, ((DefaultMemStore)this.store.memstore)
      .activeSection.getCellSkipListSet().sizeForTests());
  }

  private void assertCheck() {
    Assert.assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++) {
      Assert.assertEquals(expected.get(i), result.get(i));
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // IncrementColumnValue tests
  //////////////////////////////////////////////////////////////////////////////
  /*
   * test the internal details of how ICV works, especially during a flush scenario.
   */
  @Test
  public void testIncrementColumnValue_ICVDuringFlush()
      throws IOException, InterruptedException {
    init(this.name.getMethodName());

    long oldValue = 1L;
    long newValue = 3L;
    this.store.add(new KeyValue(row, family, qf1,
        System.currentTimeMillis(),
        Bytes.toBytes(oldValue)));

    // snapshot the store.
    this.store.snapshot();

    // add other things:
    this.store.add(new KeyValue(row, family, qf2,
        System.currentTimeMillis(),
        Bytes.toBytes(oldValue)));

    // update during the snapshot.
    long ret = this.store.updateColumnValue(row, family, qf1, newValue);

    // memstore should have grown by some amount.
    Assert.assertTrue(ret > 0);

    // then flush.
    flushStore(store, id++);
    Assert.assertEquals(1, this.store.getStorefiles().size());
    // from the one we inserted up there, and a new one
    Assert.assertEquals(2, ((DefaultMemStore)this.store.memstore)
      .activeSection.getCellSkipListSet().sizeForTests());

    // how many key/values for this row are there?
    Get get = new Get(row);
    get.addColumn(family, qf1);
    get.setMaxVersions(); // all versions.
    List<Cell> results = new ArrayList<Cell>();

    results = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertEquals(2, results.size());

    long ts1 = results.get(0).getTimestamp();
    long ts2 = results.get(1).getTimestamp();

    Assert.assertTrue(ts1 > ts2);

    Assert.assertEquals(newValue, Bytes.toLong(CellUtil.cloneValue(results.get(0))));
    Assert.assertEquals(oldValue, Bytes.toLong(CellUtil.cloneValue(results.get(1))));
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManagerTestHelper.reset();
  }

  @Test
  public void testICV_negMemstoreSize()  throws IOException {
      init(this.name.getMethodName());

    long time = 100;
    ManualEnvironmentEdge ee = new ManualEnvironmentEdge();
    ee.setValue(time);
    EnvironmentEdgeManagerTestHelper.injectEdge(ee);
    long newValue = 3L;
    long size = 0;


    size += this.store.add(new KeyValue(Bytes.toBytes("200909091000"), family, qf1,
        System.currentTimeMillis(), Bytes.toBytes(newValue)));
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091200"), family, qf1,
        System.currentTimeMillis(), Bytes.toBytes(newValue)));
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091300"), family, qf1,
        System.currentTimeMillis(), Bytes.toBytes(newValue)));
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091400"), family, qf1,
        System.currentTimeMillis(), Bytes.toBytes(newValue)));
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091500"), family, qf1,
        System.currentTimeMillis(), Bytes.toBytes(newValue)));


    for ( int i = 0 ; i < 10000 ; ++i) {
      newValue++;

      long ret = this.store.updateColumnValue(row, family, qf1, newValue);
      long ret2 = this.store.updateColumnValue(row2, family, qf1, newValue);

      if (ret != 0) System.out.println("ret: " + ret);
      if (ret2 != 0) System.out.println("ret2: " + ret2);

      Assert.assertTrue("ret: " + ret, ret >= 0);
      size += ret;
      Assert.assertTrue("ret2: " + ret2, ret2 >= 0);
      size += ret2;


      if (i % 1000 == 0)
        ee.setValue(++time);
    }

    long computedSize=0;
    for (Cell cell : ((DefaultMemStore)this.store.memstore).activeSection.getCellSkipListSet()) {
      long kvsize = DefaultMemStore.heapSizeChange(cell, true);
      //System.out.println(kv + " size= " + kvsize + " kvsize= " + kv.heapSize());
      computedSize += kvsize;
    }
    Assert.assertEquals(computedSize, size);
  }

  @Test
  public void testIncrementColumnValue_SnapshotFlushCombo() throws Exception {
    ManualEnvironmentEdge mee = new ManualEnvironmentEdge();
    EnvironmentEdgeManagerTestHelper.injectEdge(mee);
    init(this.name.getMethodName());

    long oldValue = 1L;
    long newValue = 3L;
    this.store.add(new KeyValue(row, family, qf1,
        EnvironmentEdgeManager.currentTime(),
        Bytes.toBytes(oldValue)));

    // snapshot the store.
    this.store.snapshot();

    // update during the snapshot, the exact same TS as the Put (lololol)
    long ret = this.store.updateColumnValue(row, family, qf1, newValue);

    // memstore should have grown by some amount.
    Assert.assertTrue(ret > 0);

    // then flush.
    flushStore(store, id++);
    Assert.assertEquals(1, this.store.getStorefiles().size());
    Assert.assertEquals(1, ((DefaultMemStore)this.store.memstore)
      .activeSection.getCellSkipListSet().sizeForTests());

    // now increment again:
    newValue += 1;
    this.store.updateColumnValue(row, family, qf1, newValue);

    // at this point we have a TS=1 in snapshot, and a TS=2 in kvset, so increment again:
    newValue += 1;
    this.store.updateColumnValue(row, family, qf1, newValue);

    // the second TS should be TS=2 or higher., even though 'time=1' right now.


    // how many key/values for this row are there?
    Get get = new Get(row);
    get.addColumn(family, qf1);
    get.setMaxVersions(); // all versions.
    List<Cell> results = new ArrayList<Cell>();

    results = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertEquals(2, results.size());

    long ts1 = results.get(0).getTimestamp();
    long ts2 = results.get(1).getTimestamp();

    Assert.assertTrue(ts1 > ts2);
    Assert.assertEquals(newValue, Bytes.toLong(CellUtil.cloneValue(results.get(0))));
    Assert.assertEquals(oldValue, Bytes.toLong(CellUtil.cloneValue(results.get(1))));

    mee.setValue(2); // time goes up slightly
    newValue += 1;
    this.store.updateColumnValue(row, family, qf1, newValue);

    results = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertEquals(2, results.size());

    ts1 = results.get(0).getTimestamp();
    ts2 = results.get(1).getTimestamp();

    Assert.assertTrue(ts1 > ts2);
    Assert.assertEquals(newValue, Bytes.toLong(CellUtil.cloneValue(results.get(0))));
    Assert.assertEquals(oldValue, Bytes.toLong(CellUtil.cloneValue(results.get(1))));
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
        Assert.assertEquals(FaultyFileSystem.class, fs.getClass());

        // Initialize region
        init(name.getMethodName(), conf);

        LOG.info("Adding some data");
        store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
        store.add(new KeyValue(row, family, qf2, 1, (byte[])null));
        store.add(new KeyValue(row, family, qf3, 1, (byte[])null));

        LOG.info("Before flush, we should have no files");

        Collection<StoreFileInfo> files =
          store.getRegionFileSystem().getStoreFiles(store.getColumnFamilyName());
        Assert.assertEquals(0, files != null ? files.size() : 0);

        //flush
        try {
          LOG.info("Flushing");
          flush(1);
          Assert.fail("Didn't bubble up IOE!");
        } catch (IOException ioe) {
          Assert.assertTrue(ioe.getMessage().contains("Fault injected"));
        }

        LOG.info("After failed flush, we should still have no files!");
        files = store.getRegionFileSystem().getStoreFiles(store.getColumnFamilyName());
        Assert.assertEquals(0, files != null ? files.size() : 0);
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
    List<SoftReference<FaultyOutputStream>> outStreams =
      new ArrayList<SoftReference<FaultyOutputStream>>();
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
    public void write(byte[] buf, int offset, int length) throws IOException {
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
    StoreFlushContext storeFlushCtx = store.createFlushContext(id);
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
    List<Cell> kvList = new ArrayList<Cell>();
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
      this.store.add(KeyValueUtil.ensureKeyValue(kv));
    }

    this.store.snapshot();
    flushStore(store, id++);

    List<Cell> kvList2 = getKeyValueSet(timestamps2,numRows, qf1, family);
    for(Cell kv : kvList2) {
      this.store.add(KeyValueUtil.ensureKeyValue(kv));
    }

    List<Cell> result;
    Get get = new Get(Bytes.toBytes(1));
    get.addColumn(family,qf1);

    get.setTimeRange(0,15);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertTrue(result.size()>0);

    get.setTimeRange(40,90);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertTrue(result.size()>0);

    get.setTimeRange(10,45);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertTrue(result.size()>0);

    get.setTimeRange(80,145);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertTrue(result.size()>0);

    get.setTimeRange(1,2);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertTrue(result.size()>0);

    get.setTimeRange(90,200);
    result = HBaseTestingUtility.getFromStoreFile(store, get);
    Assert.assertTrue(result.size()==0);
  }

  /**
   * Test for HBASE-3492 - Test split on empty colfam (no store files).
   *
   * @throws IOException When the IO operations fail.
   */
  @Test
  public void testSplitWithEmptyColFam() throws IOException {
    init(this.name.getMethodName());
    Assert.assertNull(store.getSplitPoint());
    store.getHRegion().forceSplit(null);
    Assert.assertNull(store.getSplitPoint());
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
    Assert.assertTrue(store.throttleCompaction(anyValue + 1));
    Assert.assertFalse(store.throttleCompaction(anyValue));

    // HTD overrides XML.
    --anyValue;
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    htd.setConfiguration(CONFIG_KEY, Long.toString(anyValue));
    init(name.getMethodName() + "-htd", conf, htd, hcd);
    Assert.assertTrue(store.throttleCompaction(anyValue + 1));
    Assert.assertFalse(store.throttleCompaction(anyValue));

    // HCD overrides them both.
    --anyValue;
    hcd.setConfiguration(CONFIG_KEY, Long.toString(anyValue));
    init(name.getMethodName() + "-hcd", conf, htd, hcd);
    Assert.assertTrue(store.throttleCompaction(anyValue + 1));
    Assert.assertFalse(store.throttleCompaction(anyValue));
  }

  public static class DummyStoreEngine extends DefaultStoreEngine {
    public static DefaultCompactor lastCreatedCompactor = null;
    @Override
    protected void createComponents(
        Configuration conf, Store store, KVComparator comparator) throws IOException {
      super.createComponents(conf, store, comparator);
      lastCreatedCompactor = this.compactor;
    }
  }

  @Test
  public void testStoreUsesSearchEngineOverride() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, DummyStoreEngine.class.getName());
    init(this.name.getMethodName(), conf);
    Assert.assertEquals(DummyStoreEngine.lastCreatedCompactor,
      this.store.storeEngine.getCompactor());
  }

  private void addStoreFile() throws IOException {
    StoreFile f = this.store.getStorefiles().iterator().next();
    Path storedir = f.getPath().getParent();
    long seqid = this.store.getMaxSequenceId();
    Configuration c = TEST_UTIL.getConfiguration();
    FileSystem fs = FileSystem.get(c);
    HFileContext fileContext = new HFileContextBuilder().withBlockSize(BLOCKSIZE_SMALL).build();
    StoreFile.Writer w = new StoreFile.WriterBuilder(c, new CacheConfig(c),
        fs)
            .withOutputDir(storedir)
            .withFileContext(fileContext)
            .build();
    w.appendMetadata(seqid + 1, false);
    w.close();
    LOG.info("Added store file:" + w.getPath());
  }

  private void archiveStoreFile(int index) throws IOException {
    Collection<StoreFile> files = this.store.getStorefiles();
    StoreFile sf = null;
    Iterator<StoreFile> it = files.iterator();
    for (int i = 0; i <= index; i++) {
      sf = it.next();
    }
    store.getRegionFileSystem().removeStoreFiles(store.getColumnFamilyName(), Lists.newArrayList(sf));
  }

  private void closeCompactedFile(int index) throws IOException {
    Collection<StoreFile> files =
        this.store.getStoreEngine().getStoreFileManager().getCompactedfiles();
    StoreFile sf = null;
    Iterator<StoreFile> it = files.iterator();
    for (int i = 0; i <= index; i++) {
      sf = it.next();
    }
    sf.closeReader(true);
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
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
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

  @SuppressWarnings("unchecked")
  @Test
  public void testRefreshStoreFilesNotChanged() throws IOException {
    init(name.getMethodName());

    assertEquals(0, this.store.getStorefilesCount());

    // add some data, flush
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null));
    flush(1);
    // add one more file
    addStoreFile();

    HStore spiedStore = spy(store);

    // call first time after files changed
    spiedStore.refreshStoreFiles();
    assertEquals(2, this.store.getStorefilesCount());
    verify(spiedStore, times(1)).replaceStoreFiles(any(Collection.class), any(Collection.class));

    // call second time
    spiedStore.refreshStoreFiles();

    //ensure that replaceStoreFiles is not called if files are not refreshed
    verify(spiedStore, times(0)).replaceStoreFiles(null, null);
  }

  private Cell createCell(byte[] qualifier, long ts, long sequenceId, byte[] value) throws IOException {
    return createCell(row, qualifier, ts, sequenceId, value);
  }

  private Cell createCell(byte[] row, byte[] qualifier, long ts, long sequenceId, byte[] value) throws IOException {
    Cell c = CellUtil.createCell(row, family, qualifier, ts, KeyValue.Type.Put.getCode(), value);
    CellUtil.setSequenceId(c, sequenceId);
    return c;
  }

  @Test
  public void testScanWithDoubleFlush() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // Initialize region
    MyStore myStore = initMyStore(name.getMethodName(), conf, new MyStoreHook() {
      @Override
      public void getScanners(final MyStore store) throws IOException {
        final long tmpId = id++;
        ExecutorService s = Executors.newSingleThreadExecutor();
        s.submit(new Runnable() {
          @Override
          public void run() {
            try {
              // flush the store before storescanner updates the scanners from store.
              // The current data will be flushed into files and the memstore will
              // be clear.
              // -- phase (4/4)
              flushStore(store, tmpId);
            } catch (IOException ex) {
              throw new RuntimeException(ex);
            }
          }
        });
        s.shutdown();
        try {
          // wait for the flush, the thread will be blocked in HStore#notifyChangedReadersObservers.
          s.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
        }
      }
    });
    byte[] oldValue = Bytes.toBytes("oldValue");
    byte[] currentValue = Bytes.toBytes("currentValue");
    long ts = EnvironmentEdgeManager.currentTime();
    long seqId = 100;
    // older data whihc shouldn't be "seen" by client
    myStore.add(createCell(qf1, ts, seqId, oldValue));
    myStore.add(createCell(qf2, ts, seqId, oldValue));
    myStore.add(createCell(qf3, ts, seqId, oldValue));
    long snapshotId = id++;
    // push older data into snapshot -- phase (1/4)
    StoreFlushContext storeFlushCtx = store.createFlushContext(snapshotId);
    storeFlushCtx.prepare();

    // insert current data into active -- phase (2/4)
    myStore.add(createCell(qf1, ts + 1, seqId + 1, currentValue));
    myStore.add(createCell(qf2, ts + 1, seqId + 1, currentValue));
    myStore.add(createCell(qf3, ts + 1, seqId + 1, currentValue));
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
                + ", actual:" + Bytes.toStringBinary(actualValue),
                 Bytes.equals(actualValue, currentValue));
      }
    }

  }

  @Test
  public void testFlushBeforeCompletingScanWoFilter() throws IOException, InterruptedException {
    final AtomicBoolean timeToGoNextRow = new AtomicBoolean(false);
    testFlushBeforeCompletingScan(new MyListHook() {
      @Override
      public void hook(int currentSize) {
        if (currentSize == 2) {
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
      public Filter.ReturnCode filterKeyValue(Cell v) throws IOException {
        return Filter.ReturnCode.INCLUDE;
      }
    });
  }

  @Test
  public void testFlushBeforeCompletingScanWithFilter() throws IOException, InterruptedException {
    final AtomicBoolean timeToGoNextRow = new AtomicBoolean(false);
    testFlushBeforeCompletingScan(new MyListHook() {
      @Override
      public void hook(int currentSize) {
        if (currentSize == 2) {
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
      public Filter.ReturnCode filterKeyValue(Cell v) throws IOException {
        if (timeToGoNextRow.get()) {
          timeToGoNextRow.set(false);
          return Filter.ReturnCode.NEXT_ROW;
        } else {
          return Filter.ReturnCode.INCLUDE;
        }
      }
    });
  }

  @Test
  public void testFlushBeforeCompletingScanWithFilterHint() throws IOException, InterruptedException {
    final AtomicBoolean timeToGetHint = new AtomicBoolean(false);
    testFlushBeforeCompletingScan(new MyListHook() {
      @Override
      public void hook(int currentSize) {
        if (currentSize == 2) {
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
      public Filter.ReturnCode filterKeyValue(Cell v) throws IOException {
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
    });
  }

  private void testFlushBeforeCompletingScan(MyListHook hook, Filter filter)
          throws IOException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMaxVersions(1);
    byte[] r0 = Bytes.toBytes("row0");
    byte[] r1 = Bytes.toBytes("row1");
    byte[] r2 = Bytes.toBytes("row2");
    byte[] value0 = Bytes.toBytes("value0");
    byte[] value1 = Bytes.toBytes("value1");
    byte[] value2 = Bytes.toBytes("value2");
    long ts = EnvironmentEdgeManager.currentTime();
    final long seqId = 100;
    init(name.getMethodName(), conf, new HTableDescriptor(TableName.valueOf(table)), hcd, new MyStoreHook() {
      @Override
      public long getSmallestReadPoint(HStore store) {
        return seqId + 3;
      }
    });
    // The cells having the value0 won't be flushed to disk because the value of max version is 1
    store.add(createCell(r0, qf1, ts, seqId, value0));
    store.add(createCell(r0, qf2, ts, seqId, value0));
    store.add(createCell(r0, qf3, ts, seqId, value0));
    store.add(createCell(r1, qf1, ts + 1, seqId + 1, value1));
    store.add(createCell(r1, qf2, ts + 1, seqId + 1, value1));
    store.add(createCell(r1, qf3, ts + 1, seqId + 1, value1));
    store.add(createCell(r2, qf1, ts + 2, seqId + 2, value2));
    store.add(createCell(r2, qf2, ts + 2, seqId + 2, value2));
    store.add(createCell(r2, qf3, ts + 2, seqId + 2, value2));
    store.add(createCell(r1, qf1, ts + 3, seqId + 3, value1));
    store.add(createCell(r1, qf2, ts + 3, seqId + 3, value1));
    store.add(createCell(r1, qf3, ts + 3, seqId + 3, value1));
    List<Cell> myList = new MyList<>(hook);
    Scan scan = new Scan()
            .withStartRow(r1)
            .setFilter(filter);
    try (InternalScanner scanner = (InternalScanner) store.getScanner(
          scan, null, seqId + 3)){
      // r1
      scanner.next(myList);
      assertEquals(3, myList.size());
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
  public void testReclaimChunkWhenScaning() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.setFloat(CHUNK_POOL_MAXSIZE_KEY, 1);
    init("testReclaimChunkWhenScaning", conf);
    final long ts = EnvironmentEdgeManager.currentTime();
    final long seqId = 100;
    byte[] value = Bytes.toBytes("value");
    // older data whihc shouldn't be "seen" by client
    store.add(createCell(qf1, ts, seqId, value));
    store.add(createCell(qf2, ts, seqId, value));
    store.add(createCell(qf3, ts, seqId, value));
    TreeSet<byte[]> quals = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    quals.add(qf1);
    quals.add(qf2);
    quals.add(qf3);
    try (InternalScanner scanner = (InternalScanner) store.getScanner(
        new Scan(new Get(row)), quals, seqId)) {
      List<Cell> results = new MyList<>(new MyListHook() {
        @Override
        public void hook(int size) {
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
                store.add(createCell(qf1, ts + 1, seqId + 1, newValue));
                store.add(createCell(qf2, ts + 1, seqId + 1, newValue));
                store.add(createCell(qf3, ts + 1, seqId + 1, newValue));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              break;
            default:
              break;
          }
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

  @Test
  public void testHFileContextSetWithCFAndTable() throws Exception {
    init(this.name.getMethodName());
    StoreFile.Writer writer = store.createWriterInTmp(10000L,
        Compression.Algorithm.NONE, false, true, false, true, -1);
    HFileContext hFileContext = writer.getHFileWriter().getFileContext();
    assertArrayEquals(family, hFileContext.getColumnFamily());
    assertArrayEquals(table, hFileContext.getTableName());
  }

  private MyStore initMyStore(String methodName, Configuration conf, MyStoreHook hook) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table));
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    hcd.setMaxVersions(5);
    return (MyStore) init(methodName, conf, htd, hcd, hook);
  }

  private static class MyStore extends HStore {

    private final MyStoreHook hook;

    MyStore(final HRegion region, final HColumnDescriptor family,
            final Configuration confParam, MyStoreHook hook) throws IOException {
      super(region, family, confParam);
      this.hook = hook;
    }

    @Override
    public List<KeyValueScanner> getScanners(List<StoreFile> files, boolean cacheBlocks,
            boolean isGet, boolean usePread, boolean isCompaction, ScanQueryMatcher matcher,
            byte[] startRow, byte[] stopRow, long readPt, boolean includeMemstoreScanner) throws IOException {
      hook.getScanners(this);
      return super.getScanners(files, cacheBlocks, isGet, usePread,
              isCompaction, matcher, startRow, stopRow, readPt, includeMemstoreScanner);
    }

    @Override
    public long getSmallestReadPoint() {
      return hook.getSmallestReadPoint(this);
    }
  }

  private abstract class MyStoreHook {
    void getScanners(MyStore store) throws IOException {
    }
    long getSmallestReadPoint(HStore store) {
      return store.getHRegion().getSmallestReadPoint();
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
    public <T> T[] toArray(T[] a) {return delegatee.toArray(a);}

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
