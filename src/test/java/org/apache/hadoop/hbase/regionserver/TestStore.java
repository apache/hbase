/*
 * Copyright 2009 The Apache Software Foundation
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

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.util.Progressable;
import org.mockito.Mockito;

import com.google.common.base.Joiner;

/**
 * Test class for the Store
 */
public class TestStore extends TestCase {
  public static final Log LOG = LogFactory.getLog(TestStore.class);

  Store store;
  byte [] table = Bytes.toBytes("table");
  byte [] family = Bytes.toBytes("family");

  byte [] row = Bytes.toBytes("row");
  byte [] qf1 = Bytes.toBytes("qf1");
  byte [] qf2 = Bytes.toBytes("qf2");
  byte [] qf3 = Bytes.toBytes("qf3");
  byte [] qf4 = Bytes.toBytes("qf4");
  byte [] qf5 = Bytes.toBytes("qf5");
  byte [] qf6 = Bytes.toBytes("qf6");

  NavigableSet<byte[]> qualifiers =
    new ConcurrentSkipListSet<byte[]>(Bytes.BYTES_COMPARATOR);

  List<KeyValue> expected = new ArrayList<KeyValue>();
  List<KeyValue> result = new ArrayList<KeyValue>();

  long id = System.currentTimeMillis();
  Get get = new Get(row);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final String DIR = TEST_UTIL.getTestDir() + "/TestStore/";

  private static final int MAX_VERSION = 4;

  /**
   * Setup
   * @throws IOException
   */
  @Override
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
    init(methodName, HBaseConfiguration.create());
  }

  private void init(String methodName, Configuration conf)
  throws IOException {
    //Setting up a Store
    Path basedir = new Path(DIR+methodName);
    Path logdir = new Path(DIR+methodName+"/logs");
    Path oldLogDir = new Path(basedir, HConstants.HREGION_OLDLOGDIR_NAME);
    HColumnDescriptor hcd = new HColumnDescriptor(family);
    // with HBASE-4241, lower versions are collected on flush
    hcd.setMaxVersions(TestStore.MAX_VERSION);
    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);

    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd, null, null, false);
    HLog hlog = new HLog(fs, logdir, oldLogDir, conf, null);
    HRegion region = new HRegion(basedir, hlog, fs, conf, info, null);

    store = new Store(basedir, region, hcd, fs, conf);
  }

  public void testDeleteExpiredStoreFiles() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // Enable the expired store file deletion
    conf.setBoolean("hbase.store.delete.expired.storefile", true);
    init(getName(), conf);

    this.store.ttl = 1000;
    int storeFileNum = 4;
    long sleepTime = this.store.ttl / storeFileNum;
    long timeStamp;

    // There are 4 store files and the max time stamp difference among these
    // store files will be (this.store.ttl / storeFileNum)
    for (int i = 1; i <= storeFileNum; i++) {
      LOG.info("Adding some data for the store file #" + i);
      timeStamp = System.currentTimeMillis();
      this.store.add(new KeyValue(row, family, qf1, timeStamp, (byte[]) null), -1L);
      this.store.add(new KeyValue(row, family, qf2, timeStamp, (byte[]) null), -1L);
      this.store.add(new KeyValue(row, family, qf3, timeStamp, (byte[]) null), -1L);
      flush(i);
      Thread.sleep(sleepTime);
    }

    // Verify the total number of store files
    assertEquals(storeFileNum, this.store.getStorefiles().size());

    // Advanced the max sequence ID by flushing one more file
    this.store.add(new KeyValue(row, family, qf3, System.currentTimeMillis(), (byte[]) null), -1L);
    flush(storeFileNum + 1);
    
    // Each compaction request will find one expired store file and delete it
    // by the compaction.
    for (int i = 1; i <= storeFileNum; i++) {
      // verify the expired store file.
      CompactionRequest cr = this.store.requestCompaction();
      
      assertEquals(1, cr.getFiles().size());
      assertTrue(cr.getFiles().get(0).getReader().getMaxTimestamp() <
          (System.currentTimeMillis() - store.ttl));
      
      // Verify that the expired store file is compacted to an empty store file.
      this.store.compact(cr);
      // It is an empty store file.
      assertEquals(storeFileNum + 1 - i, this.store.getStorefiles().size());
      // Let the next store file expired.
      Thread.sleep(sleepTime);
    }
  }

  public void testLowestModificationTime() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(conf);
    // Initialize region
    init(getName(), conf);

    int storeFileNum = 4;
    for (int i = 1; i <= storeFileNum; i++) {
      LOG.info("Adding some data for the store file #"+i);
      this.store.add(new KeyValue(row, family, qf1, i, (byte[])null), -1L);
      this.store.add(new KeyValue(row, family, qf2, i, (byte[])null), -1L);
      this.store.add(new KeyValue(row, family, qf3, i, (byte[])null), -1L);
      flush(i);
    }
    // after flush; check the lowest time stamp
    long lowestTimestampFromManager = CompactionManager.getLowestTimestamp(store.getStorefiles());
    long lowestTimeStampFromFS = getLowestTimeStampFromFS(fs,store.getStorefiles());
    assertEquals(lowestTimestampFromManager, lowestTimeStampFromFS);

    // after compact; check the lowest time stamp
    store.compact(store.requestCompaction());
    lowestTimestampFromManager = CompactionManager.getLowestTimestamp(store.getStorefiles());
    lowestTimeStampFromFS = getLowestTimeStampFromFS(fs,store.getStorefiles());
    assertEquals(lowestTimestampFromManager,lowestTimeStampFromFS);
  }

  private static long getLowestTimeStampFromFS(FileSystem fs,
      final List<StoreFile> candidates) throws IOException {
    long minTs = Long.MAX_VALUE;
    if (candidates.isEmpty()) {
      return minTs;
    }
    Path[] p = new Path[candidates.size()];
    for (int i = 0; i < candidates.size(); ++i) {
      p[i] = candidates.get(i).getPath();
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

  /**
   * Test for hbase-1686.
   * @throws IOException
   */
  public void testEmptyStoreFile() throws IOException {
    init(this.getName());
    // Write a store file.
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), -1L);
    flush(1);
    // Now put in place an empty store file.  Its a little tricky.  Have to
    // do manually with hacked in sequence id.
    StoreFile f = this.store.getStorefiles().get(0);
    Path storedir = f.getPath().getParent();
    long seqid = f.getMaxSequenceId();
    Configuration c = HBaseConfiguration.create();
    FileSystem fs = FileSystem.get(c);
    StoreFile.Writer w = new StoreFile.WriterBuilder(c, new CacheConfig(c),
        fs, StoreFile.DEFAULT_BLOCKSIZE_SMALL)
            .withOutputDir(storedir)
            .build();
    w.appendMetadata(seqid + 1, false);
    w.close();
    this.store.close();
    // Reopen it... should pick up two files
    this.store = new Store(storedir.getParent().getParent(),
      this.store.getHRegion(),
      this.store.getFamily(), fs, c);
    System.out.println(this.store.getHRegionInfo().getEncodedName());
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
  public void testGet_FromMemStoreOnly() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null), -1L);

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
  public void testGet_FromFilesOnly() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), -1L);
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null), -1L);
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null), -1L);
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
  public void testGet_FromMemStoreAndFiles() throws IOException {
    init(this.getName());

    //Put data in memstore
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), -1L);
    //flush
    flush(1);

    //Add more data
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf4, 1, (byte[])null), -1L);
    //flush
    flush(2);

    //Add more data
    this.store.add(new KeyValue(row, family, qf5, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf6, 1, (byte[])null), -1L);

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
    assertEquals(storeFilessize, this.store.getStorefiles().size());
    assertEquals(0, this.store.memstore.kvset.size());
  }

  private void assertCheck() {
    assertEquals(expected.size(), result.size());
    for(int i=0; i<expected.size(); i++) {
      assertEquals(expected.get(i), result.get(i));
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // IncrementColumnValue tests
  //////////////////////////////////////////////////////////////////////////////
  /*
   * test the internal details of how ICV works, especially during a flush scenario.
   */
  public void testIncrementColumnValue_ICVDuringFlush()
      throws IOException, InterruptedException {
    init(this.getName());

    long oldValue = 1L;
    long newValue = 3L;
    this.store.add(new KeyValue(row, family, qf1,
        System.currentTimeMillis(),
        Bytes.toBytes(oldValue)),
        -1L);

    // snapshot the store.
    this.store.snapshot();

    // add other things:
    this.store.add(new KeyValue(row, family, qf2,
        System.currentTimeMillis(),
        Bytes.toBytes(oldValue)),
        -1L);

    // sleep 2 ms to space out the increments.
    Thread.sleep(2);

    // update during the snapshot.
    long ret = this.store.updateColumnValue(row, family, qf1, newValue, -1L);

    // memstore should have grown by some amount.
    assertTrue(ret > 0);

    // then flush.
    flushStore(store, id++);
    assertEquals(1, this.store.getStorefiles().size());
    // from the one we inserted up there, and a new one
    assertEquals(2, this.store.memstore.kvset.size());

    // how many key/values for this row are there?
    Get get = new Get(row);
    get.addColumn(family, qf1);
    get.setMaxVersions(); // all versions.
    List<KeyValue> results = new ArrayList<KeyValue>();

    NavigableSet<byte[]> cols = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    cols.add(qf1);

    results = HBaseTestingUtility.getFromStoreFile(store, get);
    assertEquals(2, results.size());

    long ts1 = results.get(0).getTimestamp();
    long ts2 = results.get(1).getTimestamp();

    assertTrue(ts1 > ts2);

    assertEquals(newValue, Bytes.toLong(results.get(0).getValue()));
    assertEquals(oldValue, Bytes.toLong(results.get(1).getValue()));

  }

  public void testHandleErrorsInFlush() throws Exception {
    LOG.info("Setting up a faulty file system that cannot write");

    Configuration conf = HBaseConfiguration.create();
    // Set a different UGI so we don't get the same cached LocalFS instance
    conf.set(UnixUserGroupInformation.UGI_PROPERTY_NAME,
        "testhandleerrorsinflush,foo");
    // Inject our faulty LocalFileSystem
    conf.setClass("fs.file.impl", FaultyFileSystem.class,
        FileSystem.class);
    // Make sure it worked (above is sensitive to caching details in hadoop core)
    FileSystem fs = FileSystem.get(conf);
    assertEquals(FaultyFileSystem.class, fs.getClass());

    // Initialize region
    init(getName(), conf);

    LOG.info("Adding some data");
    this.store.add(new KeyValue(row, family, qf1, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf2, 1, (byte[])null), -1L);
    this.store.add(new KeyValue(row, family, qf3, 1, (byte[])null), -1L);

    LOG.info("Before flush, we should have no files");
    FileStatus[] files = fs.listStatus(store.getHomedir());
    Path[] paths = FileUtil.stat2Paths(files);
    System.err.println("Got paths: " + Joiner.on(",").join(paths));
    assertEquals(0, paths.length);

    //flush
    try {
      LOG.info("Flushing");
      flush(1);
      fail("Didn't bubble up IOE!");
    } catch (IOException ioe) {
      assertTrue(ioe.getMessage().contains("Fault injected"));
    }

    LOG.info("After failed flush, we should still have no files!");
    files = fs.listStatus(store.getHomedir());
    paths = FileUtil.stat2Paths(files);
    System.err.println("Got paths: " + Joiner.on(",").join(paths));
    assertEquals(0, paths.length);
  }


  static class FaultyFileSystem extends FilterFileSystem {
    List<SoftReference<FaultyOutputStream>> outStreams =
      new ArrayList<SoftReference<FaultyOutputStream>>();
    private long faultPos = 200;

    public FaultyFileSystem() throws IOException {
      super(new LocalFileSystem());
      System.err.println("Creating faulty!");
    }

    @Override
    public FSDataOutputStream create(Path p,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        int bytesPerChecksum,
        Progressable progress) throws IOException {
      return new FaultyOutputStream(super.create(p,
          permission, overwrite, bufferSize, replication,
          blockSize, bytesPerChecksum, progress), faultPos);
    }

    @Override
    public FSDataOutputStream createNonRecursive(Path f,
        FsPermission permission, boolean overwrite,
        int bufferSize, short replication, long blockSize,
        Progressable progress) throws IOException {
      return create(f, permission, overwrite, bufferSize,
          replication, blockSize, progress);
    }
  }

  static class FaultyOutputStream extends FSDataOutputStream {
    volatile long faultPos = Long.MAX_VALUE;

    public FaultyOutputStream(FSDataOutputStream out,
        long faultPos) throws IOException {
      super(out, null);
      this.faultPos = faultPos;
    }

    @Override
    public void write(byte[] buf, int offset, int length) throws IOException {
      System.err.println("faulty stream write at pos " + getPos());
      injectFault();
      super.write(buf, offset, length);
    }

    private void injectFault() throws IOException {
      if (getPos() >= faultPos) {
        throw new IOException("Fault injected");
      }
    }
  }



  private static void flushStore(Store store, long id) throws IOException {
    StoreFlusher storeFlusher = store.getStoreFlusher(id);
    storeFlusher.prepare();
    storeFlusher.flushCache(Mockito.mock(MonitoredTask.class));
    storeFlusher.commit();
  }



  /**
   * Generate a list of KeyValues for testing based on given parameters
   * @param timestamps
   * @param numRows
   * @param qualifier
   * @param family
   * @return
   */
  List<KeyValue> getKeyValueSet(long[] timestamps, int numRows,
      byte[] qualifier, byte[] family) {
    List<KeyValue> kvList = new ArrayList<KeyValue>();
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
  public void testMultipleTimestamps() throws IOException {
    int numRows = 1;
    long[] timestamps1 = new long[] {1,5,10,20};
    long[] timestamps2 = new long[] {30,80};

    init(this.getName());

    List<KeyValue> kvList1 = getKeyValueSet(timestamps1,numRows, qf1, family);
    for (KeyValue kv : kvList1) {
      this.store.add(kv, -1L);
    }

    this.store.snapshot();
    flushStore(store, id++);

    List<KeyValue> kvList2 = getKeyValueSet(timestamps2,numRows, qf1, family);
    for(KeyValue kv : kvList2) {
      this.store.add(kv, -1L);
    }

    assertEquals(Math.max(timestamps1.length, timestamps2.length),
        TestStore.MAX_VERSION);
    List<KeyValue> result;
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

  public void testIsPeakTime() throws IOException {
    String methodName = "/testIsPeakTime";
    final int[][] testcases = new int[][] { { 11, 15 }, { 23, 3 }, { 23, 0 },
        { 0, 0 }, { 0, 23 } };
    final int [][] tests = new int[][] {{ 10, 13, 16 }, { 0, 1, 4 },
      { 22, 1, 23 }, { 1, 0, 2 }, { 0, 23, 16 }};
    final boolean[][] answers = new boolean[][] {{false, true, false},
      {true, true, false}, {false, false, true}, {false, false , false},
      {true, false, true}};
    assertEquals(testcases.length, tests.length);
    assertEquals(tests.length, answers.length);
    for (int i = 0; i < testcases.length; i++) {
      int s = testcases[i][0];
      int e = testcases[i][1];
      Configuration conf = HBaseConfiguration.create();
      conf.setInt("hbase.peak.start.hour", s);
      conf.setInt("hbase.peak.end.hour", e);
      assertEquals(tests[i].length, answers[i].length);
      for (int j = 0; j < tests[i].length; j++) {
        String method = methodName + i + "_" + j;
        init(method, conf);
        assertEquals(answers[i][j], this.store.isPeakTime(tests[i][j]));
      }
    }
  }

  /**
   * Checks whether compaction hook is updated in the configuration when we call
   * {@link Store#notifyOnChange(Configuration)}
   *
   * @throws IOException
   */
  public void testCompactionHookOnlineConfigurationChange() throws IOException {
    final String compactionHookExample = "org.apache.hadoop.hbase.regionserver.compactionhook.LowerToUpperCompactionHook";
    final String compactionHookExample2 =  "org.apache.hadoop.hbase.regionserver.compactionhook.SkipCompactionHook";
    Configuration conf = HBaseConfiguration.create();
    init(getName());
    // compaction hook should be null by default
    assertNull(this.store.getCompactHook());
    assertNull(conf.get(HConstants.COMPACTION_HOOK));
    conf.set(HConstants.COMPACTION_HOOK, compactionHookExample);
    // confirm it is null before we call notifyOnChange
    assertNull(this.store.getCompactHook());
    this.store.notifyOnChange(conf);
    assertTrue(this.store.getCompactHook().getClass().getName()
        .equals(compactionHookExample));
    // change it to another implmentation of compaction hook
    conf.set(HConstants.COMPACTION_HOOK, compactionHookExample2);
    this.store.notifyOnChange(conf);
    // confirm that store has picked up the new compaction hook
    assertTrue(this.store.getCompactHook().getClass().getName().equals(compactionHookExample2));
    //set it to empty
    conf.set(HConstants.COMPACTION_HOOK, "");
    this.store.notifyOnChange(conf);
    assertNull(this.store.getCompactHook());;
  }
}
