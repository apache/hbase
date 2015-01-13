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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category({RegionServerTests.class, SmallTests.class})
public class TestDefaultCompactSelection extends TestCase {
  private final static Log LOG = LogFactory.getLog(TestDefaultCompactSelection.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected Configuration conf;
  protected HStore store;
  private static final String DIR=
    TEST_UTIL.getDataTestDir(TestDefaultCompactSelection.class.getSimpleName()).toString();
  private static Path TEST_FILE;

  protected static final int minFiles = 3;
  protected static final int maxFiles = 5;

  protected static final long minSize = 10;
  protected static final long maxSize = 2100;

  private WALFactory wals;
  private HRegion region;

  @Override
  public void setUp() throws Exception {
    // setup config values necessary for store
    this.conf = TEST_UTIL.getConfiguration();
    this.conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 0);
    this.conf.setInt("hbase.hstore.compaction.min", minFiles);
    this.conf.setInt("hbase.hstore.compaction.max", maxFiles);
    this.conf.setLong(HConstants.HREGION_MEMSTORE_FLUSH_SIZE, minSize);
    this.conf.setLong("hbase.hstore.compaction.max.size", maxSize);
    this.conf.setFloat("hbase.hstore.compaction.ratio", 1.0F);
    // Test depends on this not being set to pass.  Default breaks test.  TODO: Revisit.
    this.conf.unset("hbase.hstore.compaction.min.size");

    //Setting up a Store
    final String id = TestDefaultCompactSelection.class.getName();
    Path basedir = new Path(DIR);
    final Path logdir = new Path(basedir, DefaultWALProvider.getWALDirectoryName(id));
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("family"));
    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("table")));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

    final Configuration walConf = new Configuration(conf);
    FSUtils.setRootDir(walConf, basedir);
    wals = new WALFactory(walConf, null, id);
    region = HBaseTestingUtility.createRegionAndWAL(info, basedir, conf, htd);
    HBaseTestingUtility.closeRegionAndWAL(region);
    Path tableDir = FSUtils.getTableDir(basedir, htd.getTableName());
    region = new HRegion(tableDir, wals.getWAL(info.getEncodedNameAsBytes()), fs, conf, info, htd,
        null);

    store = new HStore(region, hcd, conf);

    TEST_FILE = region.getRegionFileSystem().createTempName();
    fs.createNewFile(TEST_FILE);
  }

  @After
  public void tearDown() throws IOException {
    IOException ex = null;
    try {
      region.close();
    } catch (IOException e) {
      LOG.warn("Caught Exception", e);
      ex = e;
    }
    try {
      wals.close();
    } catch (IOException e) {
      LOG.warn("Caught Exception", e);
      ex = e;
    }
    if (ex != null) {
      throw ex;
    }
  }

  ArrayList<Long> toArrayList(long... numbers) {
    ArrayList<Long> result = new ArrayList<Long>();
    for (long i : numbers) {
      result.add(i);
    }
    return result;
  }

  List<StoreFile> sfCreate(long... sizes) throws IOException {
    ArrayList<Long> ageInDisk = new ArrayList<Long>();
    for (int i = 0; i < sizes.length; i++) {
      ageInDisk.add(0L);
    }
    return sfCreate(toArrayList(sizes), ageInDisk);
  }

  List<StoreFile> sfCreate(ArrayList<Long> sizes, ArrayList<Long> ageInDisk)
    throws IOException {
    return sfCreate(false, sizes, ageInDisk);
  }

  List<StoreFile> sfCreate(boolean isReference, long... sizes) throws IOException {
    ArrayList<Long> ageInDisk = new ArrayList<Long>(sizes.length);
    for (int i = 0; i < sizes.length; i++) {
      ageInDisk.add(0L);
    }
    return sfCreate(isReference, toArrayList(sizes), ageInDisk);
  }

  List<StoreFile> sfCreate(boolean isReference, ArrayList<Long> sizes, ArrayList<Long> ageInDisk)
      throws IOException {
    List<StoreFile> ret = Lists.newArrayList();
    for (int i = 0; i < sizes.size(); i++) {
      ret.add(new MockStoreFile(TEST_UTIL, TEST_FILE,
          sizes.get(i), ageInDisk.get(i), isReference, i));
    }
    return ret;
  }

  long[] getSizes(List<StoreFile> sfList) {
    long[] aNums = new long[sfList.size()];
    for (int i = 0; i < sfList.size(); ++i) {
      aNums[i] = sfList.get(i).getReader().length();
    }
    return aNums;
  }

  void compactEquals(List<StoreFile> candidates, long... expected)
    throws IOException {
    compactEquals(candidates, false, false, expected);
  }

  void compactEquals(List<StoreFile> candidates, boolean forcemajor, long... expected)
    throws IOException {
    compactEquals(candidates, forcemajor, false, expected);
  }

  void compactEquals(List<StoreFile> candidates, boolean forcemajor, boolean isOffPeak,
      long ... expected)
  throws IOException {
    store.forceMajor = forcemajor;
    //Test Default compactions
    CompactionRequest result = ((RatioBasedCompactionPolicy)store.storeEngine.getCompactionPolicy())
        .selectCompaction(candidates, new ArrayList<StoreFile>(), false, isOffPeak, forcemajor);
    List<StoreFile> actual = new ArrayList<StoreFile>(result.getFiles());
    if (isOffPeak && !forcemajor) {
      assertTrue(result.isOffPeak());
    }
    assertEquals(Arrays.toString(expected), Arrays.toString(getSizes(actual)));
    store.forceMajor = false;
  }

  public void testCompactionRatio() throws IOException {
    /**
     * NOTE: these tests are specific to describe the implementation of the
     * current compaction algorithm.  Developed to ensure that refactoring
     * doesn't implicitly alter this.
     */
    long tooBig = maxSize + 1;

    // default case. preserve user ratio on size
    compactEquals(sfCreate(100,50,23,12,12), 23, 12, 12);
    // less than compact threshold = don't compact
    compactEquals(sfCreate(100,50,25,12,12) /* empty */);
    // greater than compact size = skip those
    compactEquals(sfCreate(tooBig, tooBig, 700, 700, 700), 700, 700, 700);
    // big size + threshold
    compactEquals(sfCreate(tooBig, tooBig, 700,700) /* empty */);
    // small files = don't care about ratio
    compactEquals(sfCreate(7,1,1), 7,1,1);

    // don't exceed max file compact threshold
    // note:  file selection starts with largest to smallest.
    compactEquals(sfCreate(7, 6, 5, 4, 3, 2, 1), 5, 4, 3, 2, 1);

    compactEquals(sfCreate(50, 10, 10 ,10, 10), 10, 10, 10, 10);

    compactEquals(sfCreate(10, 10, 10, 10, 50), 10, 10, 10, 10);

    compactEquals(sfCreate(251, 253, 251, maxSize -1), 251, 253, 251);

    compactEquals(sfCreate(maxSize -1,maxSize -1,maxSize -1) /* empty */);

    // Always try and compact something to get below blocking storefile count
    this.conf.setLong("hbase.hstore.compaction.min.size", 1);
    store.storeEngine.getCompactionPolicy().setConf(conf);
    compactEquals(sfCreate(512,256,128,64,32,16,8,4,2,1), 4,2,1);
    this.conf.setLong("hbase.hstore.compaction.min.size", minSize);
    store.storeEngine.getCompactionPolicy().setConf(conf);

    /* MAJOR COMPACTION */
    // if a major compaction has been forced, then compact everything
    compactEquals(sfCreate(50,25,12,12), true, 50, 25, 12, 12);
    // also choose files < threshold on major compaction
    compactEquals(sfCreate(12,12), true, 12, 12);
    // even if one of those files is too big
    compactEquals(sfCreate(tooBig, 12,12), true, tooBig, 12, 12);
    // don't exceed max file compact threshold, even with major compaction
    store.forceMajor = true;
    compactEquals(sfCreate(7, 6, 5, 4, 3, 2, 1), 5, 4, 3, 2, 1);
    store.forceMajor = false;
    // if we exceed maxCompactSize, downgrade to minor
    // if not, it creates a 'snowball effect' when files >> maxCompactSize:
    // the last file in compaction is the aggregate of all previous compactions
    compactEquals(sfCreate(100,50,23,12,12), true, 23, 12, 12);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 1);
    conf.setFloat("hbase.hregion.majorcompaction.jitter", 0);
    store.storeEngine.getCompactionPolicy().setConf(conf);
    try {
      // trigger an aged major compaction
      compactEquals(sfCreate(50,25,12,12), 50, 25, 12, 12);
      // major sure exceeding maxCompactSize also downgrades aged minors
      compactEquals(sfCreate(100,50,23,12,12), 23, 12, 12);
    } finally {
      conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 1000*60*60*24);
      conf.setFloat("hbase.hregion.majorcompaction.jitter", 0.20F);
    }

    /* REFERENCES == file is from a region that was split */
    // treat storefiles that have references like a major compaction
    compactEquals(sfCreate(true, 100,50,25,12,12), 100, 50, 25, 12, 12);
    // reference files shouldn't obey max threshold
    compactEquals(sfCreate(true, tooBig, 12,12), tooBig, 12, 12);
    // reference files should obey max file compact to avoid OOM
    compactEquals(sfCreate(true, 7, 6, 5, 4, 3, 2, 1), 7, 6, 5, 4, 3);

    // empty case
    compactEquals(new ArrayList<StoreFile>() /* empty */);
    // empty case (because all files are too big)
   compactEquals(sfCreate(tooBig, tooBig) /* empty */);
  }

  public void testOffPeakCompactionRatio() throws IOException {
    /*
     * NOTE: these tests are specific to describe the implementation of the
     * current compaction algorithm.  Developed to ensure that refactoring
     * doesn't implicitly alter this.
     */
    // set an off-peak compaction threshold
    this.conf.setFloat("hbase.hstore.compaction.ratio.offpeak", 5.0F);
    store.storeEngine.getCompactionPolicy().setConf(this.conf);
    // Test with and without the flag.
    compactEquals(sfCreate(999, 50, 12, 12, 1), false, true, 50, 12, 12, 1);
    compactEquals(sfCreate(999, 50, 12, 12, 1), 12, 12, 1);
  }

  public void testStuckStoreCompaction() throws IOException {
    // Select the smallest compaction if the store is stuck.
    compactEquals(sfCreate(99,99,99,99,99,99, 30,30,30,30), 30, 30, 30);
    // If not stuck, standard policy applies.
    compactEquals(sfCreate(99,99,99,99,99, 30,30,30,30), 99, 30, 30, 30, 30);

    // Add sufficiently small files to compaction, though
    compactEquals(sfCreate(99,99,99,99,99,99, 30,30,30,15), 30, 30, 30, 15);
    // Prefer earlier compaction to latter if the benefit is not significant
    compactEquals(sfCreate(99,99,99,99, 30,26,26,29,25,25), 30, 26, 26);
    // Prefer later compaction if the benefit is significant.
    compactEquals(sfCreate(99,99,99,99, 27,27,27,20,20,20), 20, 20, 20);
  }

  public void testCompactionEmptyHFile() throws IOException {
    // Set TTL
    ScanInfo oldScanInfo = store.getScanInfo();
    ScanInfo newScanInfo = new ScanInfo(oldScanInfo.getFamily(),
        oldScanInfo.getMinVersions(), oldScanInfo.getMaxVersions(), 600,
        oldScanInfo.getKeepDeletedCells(), oldScanInfo.getTimeToPurgeDeletes(),
        oldScanInfo.getComparator());
    store.setScanInfo(newScanInfo);
    // Do not compact empty store file
    List<StoreFile> candidates = sfCreate(0);
    for (StoreFile file : candidates) {
      if (file instanceof MockStoreFile) {
        MockStoreFile mockFile = (MockStoreFile) file;
        mockFile.setTimeRangeTracker(new TimeRangeTracker(-1, -1));
        mockFile.setEntries(0);
      }
    }
    // Test Default compactions
    CompactionRequest result = ((RatioBasedCompactionPolicy) store.storeEngine
        .getCompactionPolicy()).selectCompaction(candidates,
        new ArrayList<StoreFile>(), false, false, false);
    assertTrue(result.getFiles().size() == 0);
    store.setScanInfo(oldScanInfo);
  }
}
