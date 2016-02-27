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
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDefaultCompactSelection extends TestCompactionPolicy {

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testCompactionEmptyHFile() throws IOException {
    // Set TTL
    ScanInfo oldScanInfo = store.getScanInfo();
    ScanInfo newScanInfo = new ScanInfo(oldScanInfo.getConfiguration(), oldScanInfo.getFamily(),
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
    Assert.assertTrue(result.getFiles().size() == 0);
    store.setScanInfo(oldScanInfo);
  }
}
