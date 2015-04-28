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

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;
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
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import com.google.common.collect.Lists;

/**
 * Test class for the Store
 */
@Category({RegionServerTests.class, MediumTests.class})
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

  @SuppressWarnings("deprecation")
  private Store init(String methodName, Configuration conf, HTableDescriptor htd,
      HColumnDescriptor hcd) throws IOException {
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
    HRegion region = new HRegion(tableDir, wals.getWAL(info.getEncodedNameAsBytes()), fs, conf,
        info, htd, null);

    store = new HStore(region, hcd, conf);
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
    final Configuration conf = HBaseConfiguration.create();
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
        long kvSize = store.add(new KeyValue(row, family, qf1, 1, (byte[])null)).getFirst();
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
    init(name.getMethodName(), conf, hcd);

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
    store.compact(store.requestCompaction(), NoLimitCompactionThroughputController.INSTANCE);
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
    Assert.assertEquals(0, ((DefaultMemStore)this.store.memstore).cellSet.size());
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
    Assert.assertEquals(2, ((DefaultMemStore)this.store.memstore).cellSet.size());

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
        System.currentTimeMillis(),
        Bytes.toBytes(newValue))).getFirst();
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091200"), family, qf1,
        System.currentTimeMillis(),
        Bytes.toBytes(newValue))).getFirst();
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091300"), family, qf1,
        System.currentTimeMillis(),
        Bytes.toBytes(newValue))).getFirst();
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091400"), family, qf1,
        System.currentTimeMillis(),
        Bytes.toBytes(newValue))).getFirst();
    size += this.store.add(new KeyValue(Bytes.toBytes("200909091500"), family, qf1,
        System.currentTimeMillis(),
        Bytes.toBytes(newValue))).getFirst();


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
    for (Cell cell : ((DefaultMemStore)this.store.memstore).cellSet) {
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
    Assert.assertEquals(1, ((DefaultMemStore)this.store.memstore).cellSet.size());

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

    final Configuration conf = HBaseConfiguration.create();
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

  @Test
  public void testRefreshStoreFiles() throws Exception {
    init(name.getMethodName());

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
}
