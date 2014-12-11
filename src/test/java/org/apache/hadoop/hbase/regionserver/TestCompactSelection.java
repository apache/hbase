/**
 * Copyright 2010 The Apache Software Foundation
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
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
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.NoOpDataBlockEncoder;
import org.apache.hadoop.hbase.regionserver.compactions.CompactSelection;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestCompactSelection extends TestCase {
  private final static Log LOG = LogFactory.getLog(TestCompactSelection.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private Configuration conf;
  private Store store;
  private static final String DIR=
    TEST_UTIL.getDataTestDir("TestCompactSelection").toString();
  private static Path TEST_FILE;

  private static final int minFiles = 3;
  private static final int maxFiles = 5;

  private static final long minSize = 10;
  private static final long maxSize = 1000;


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

    //Setting up a Store
    Path basedir = new Path(DIR);
    Path logdir = new Path(DIR+"/logs");
    Path oldLogDir = new Path(basedir, HConstants.HREGION_OLDLOGDIR_NAME);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("family"));
    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);

    HTableDescriptor htd = new HTableDescriptor(Bytes.toBytes("table"));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getName(), null, null, false);

    HLog hlog = new HLog(fs, logdir, oldLogDir, conf);
    HRegion region = HRegion.createHRegion(info, basedir, conf, htd);
    HRegion.closeHRegion(region);
    Path tableDir = new Path(basedir, Bytes.toString(htd.getName()));
    region = new HRegion(tableDir, hlog, fs, conf, info, htd, null);

    store = new Store(basedir, region, hcd, fs, conf);
    TEST_FILE = StoreFile.getRandomFilename(fs, store.getHomedir());
    fs.create(TEST_FILE);
  }

  // used so our tests don't deal with actual StoreFiles
  static class MockStoreFile extends StoreFile {
    long length = 0;
    boolean isRef = false;
    TimeRangeTracker timeRangeTracker;
    long entryCount;

    MockStoreFile(long length, boolean isRef) throws IOException {
      super(TEST_UTIL.getTestFileSystem(), TEST_FILE,
            TEST_UTIL.getConfiguration(),
            new CacheConfig(TEST_UTIL.getConfiguration()), BloomType.NONE,
            NoOpDataBlockEncoder.INSTANCE);
      this.length = length;
      this.isRef  = isRef;
    }

    void setLength(long newLen) {
      this.length = newLen;
    }

    @Override
    boolean isMajorCompaction() {
      return false;
    }

    @Override
    boolean isReference() {
      return this.isRef;
    }

    void setTimeRangeTracker(TimeRangeTracker timeRangeTracker) {
      this.timeRangeTracker = timeRangeTracker;
    }

    void setEntries(long entryCount) {
      this.entryCount = entryCount;
    }

    @Override
    public StoreFile.Reader getReader() {
      final long len = this.length;
      final TimeRangeTracker timeRange = this.timeRangeTracker;
      final long entries = this.entryCount;
      return new StoreFile.Reader() {
        @Override
        public long length() {
          return len;
        }

        @Override
        public long getMaxTimestamp() {
          return timeRange == null ? Long.MAX_VALUE
              : timeRange.maximumTimestamp;
        }

        @Override
        public long getEntries() {
          return entries;
        }
      };
    }
  }

  List<StoreFile> sfCreate(long ... sizes) throws IOException {
    return sfCreate(false, sizes);
  }

  List<StoreFile> sfCreate(boolean isReference, long ... sizes)
  throws IOException {
    List<StoreFile> ret = Lists.newArrayList();
    for (long i : sizes) {
      ret.add(new MockStoreFile(i, isReference));
    }
    return ret;
  }

  long[] getSizes(List<StoreFile> sfList) {
    long[] aNums = new long[sfList.size()];
    for (int i=0; i <sfList.size(); ++i) {
      aNums[i] = sfList.get(i).getReader().length();
    }
    return aNums;
  }
  
  void compactEquals(List<StoreFile> candidates, long ... expected) 
  throws IOException {
    compactEquals(candidates, false, expected);
  }

  void compactEquals(List<StoreFile> candidates, boolean forcemajor, 
      long ... expected)
  throws IOException {
    store.forceMajor = forcemajor;
    List<StoreFile> actual = store.compactSelection(candidates).getFilesToCompact();
    store.forceMajor = false;
    assertEquals(Arrays.toString(expected), Arrays.toString(getSizes(actual)));
  }

  public void testCompactionRatio() throws IOException {
    /*
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
    compactEquals(sfCreate(8,3,1), 8,3,1);
    /* TODO: add sorting + unit test back in when HBASE-2856 is fixed 
    // sort first so you don't include huge file the tail end
    // happens with HFileOutputFormat bulk migration
    compactEquals(sfCreate(100,50,23,12,12, 500), 23, 12, 12);
     */
    // don't exceed max file compact threshold
    assertEquals(maxFiles,
        store.compactSelection(sfCreate(7,6,5,4,3,2,1)).getFilesToCompact().size());
    // note:  file selection starts with largest to smallest.
    compactEquals(sfCreate(7, 6, 5, 4, 3, 2, 1), 7, 6, 5, 4, 3);
    
    /* MAJOR COMPACTION */
    // if a major compaction has been forced, then compact everything
    compactEquals(sfCreate(50,25,12,12), true, 50, 25, 12, 12);
    // also choose files < threshold on major compaction
    compactEquals(sfCreate(12,12), true, 12, 12);
    // even if one of those files is too big
    compactEquals(sfCreate(tooBig, 12,12), true, tooBig, 12, 12);
    // don't exceed max file compact threshold, even with major compaction
    store.forceMajor = true;
    compactEquals(sfCreate(7, 6, 5, 4, 3, 2, 1), 7, 6, 5, 4, 3);
    store.forceMajor = false;

    // if we exceed maxCompactSize, downgrade to minor
    // if not, it creates a 'snowball effect' when files >> maxCompactSize:
    // the last file in compaction is the aggregate of all previous compactions
    compactEquals(sfCreate(100,50,23,12,12), true, 23, 12, 12);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 1);
    conf.setFloat("hbase.hregion.majorcompaction.jitter", 0);
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
    assertEquals(maxFiles,
        store.compactSelection(sfCreate(true, 7,6,5,4,3,2,1)).getFilesToCompact().size());
    // reference compaction
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
    long tooBig = maxSize + 1;

    Calendar calendar = new GregorianCalendar();
    int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
    LOG.debug("Hour of day = " + hourOfDay);
    int hourPlusOne = ((hourOfDay+1+24)%24);
    int hourMinusOne = ((hourOfDay-1+24)%24);
    int hourMinusTwo = ((hourOfDay-2+24)%24);

    // check compact selection without peak hour setting
    LOG.debug("Testing compact selection without off-peak settings...");
    compactEquals(sfCreate(999,50,12,12,1), 12, 12, 1);

    // set an off-peak compaction threshold
    this.conf.setFloat("hbase.hstore.compaction.ratio.offpeak", 5.0F);

    // set peak hour to current time and check compact selection
    this.conf.setLong("hbase.offpeak.start.hour", hourMinusOne);
    this.conf.setLong("hbase.offpeak.end.hour", hourPlusOne);
    LOG.debug("Testing compact selection with off-peak settings (" +
        hourMinusOne + ", " + hourPlusOne + ")");
    compactEquals(sfCreate(999,50,12,12, 1), 50, 12, 12, 1);

    // set peak hour outside current selection and check compact selection
    this.conf.setLong("hbase.offpeak.start.hour", hourMinusTwo);
    this.conf.setLong("hbase.offpeak.end.hour", hourMinusOne);
    LOG.debug("Testing compact selection with off-peak settings (" +
        hourMinusTwo + ", " + hourMinusOne + ")");
    compactEquals(sfCreate(999,50,12,12, 1), 12, 12, 1);
  }

  public void testCompactionEmptyHFile() throws IOException {
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
    CompactSelection compactSelection = new CompactSelection(conf, candidates);
    CompactSelection result = compactSelection
        .selectExpiredStoreFilesToCompact(600L);
    assertTrue(result == null);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

