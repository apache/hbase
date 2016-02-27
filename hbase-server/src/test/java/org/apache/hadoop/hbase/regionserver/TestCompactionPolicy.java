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

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCompactionPolicy {
  private final static Log LOG = LogFactory.getLog(TestDefaultCompactSelection.class);
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  protected Configuration conf;
  protected HStore store;
  private static final String DIR = TEST_UTIL.getDataTestDir(
    TestDefaultCompactSelection.class.getSimpleName()).toString();
  protected static Path TEST_FILE;
  protected static final int minFiles = 3;
  protected static final int maxFiles = 5;

  protected static final long minSize = 10;
  protected static final long maxSize = 2100;

  private FSHLog hlog;
  private HRegion region;

  @Before
  public void setUp() throws Exception {
    config();
    initialize();
  }

  /**
   * setup config values necessary for store
   */
  protected void config() {
    this.conf = TEST_UTIL.getConfiguration();
    this.conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 0);
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, minFiles);
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, maxFiles);
    this.conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, minSize);
    this.conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, maxSize);
    this.conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.0F);
  }

  /**
   * Setting up a Store
   * @throws IOException with error
   */
  protected void initialize() throws IOException {
    Path basedir = new Path(DIR);
    String logName = "logs";
    Path logdir = new Path(DIR, logName);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("family"));
    FileSystem fs = FileSystem.get(conf);

    fs.delete(logdir, true);

    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("table")));
    htd.addFamily(hcd);
    HRegionInfo info = new HRegionInfo(htd.getTableName(), null, null, false);

    hlog = new FSHLog(fs, basedir, logName, conf);
    region = HRegion.createHRegion(info, basedir, conf, htd, hlog);
    region.close();
    Path tableDir = FSUtils.getTableDir(basedir, htd.getTableName());
    region = new HRegion(tableDir, hlog, fs, conf, info, htd, null);

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
      hlog.close();
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

  List<StoreFile> sfCreate(ArrayList<Long> sizes, ArrayList<Long> ageInDisk) throws IOException {
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
      ret.add(new MockStoreFile(TEST_UTIL, TEST_FILE, sizes.get(i), ageInDisk.get(i), isReference,
          i));
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

  void compactEquals(List<StoreFile> candidates, long... expected) throws IOException {
    compactEquals(candidates, false, false, expected);
  }

  void compactEquals(List<StoreFile> candidates, boolean forcemajor, long... expected)
      throws IOException {
    compactEquals(candidates, forcemajor, false, expected);
  }

  void compactEquals(List<StoreFile> candidates, boolean forcemajor, boolean isOffPeak,
      long... expected) throws IOException {
    store.forceMajor = forcemajor;
    // Test Default compactions
    CompactionRequest result =
        ((RatioBasedCompactionPolicy) store.storeEngine.getCompactionPolicy()).selectCompaction(
          candidates, new ArrayList<StoreFile>(), false, isOffPeak, forcemajor);
    List<StoreFile> actual = new ArrayList<StoreFile>(result.getFiles());
    if (isOffPeak && !forcemajor) {
      Assert.assertTrue(result.isOffPeak());
    }
    Assert.assertEquals(Arrays.toString(expected), Arrays.toString(getSizes(actual)));
    store.forceMajor = false;
  }
}
