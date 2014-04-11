/**
 * Copyright 2014 The Apache Software Foundation
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
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.Test;

/**
 * Testcases for MemStoreFlusher
 */
public class TestMemStoreFlusher {
  private static class HRegionServerMock implements HRegionServerIf {
    RegionServerMetrics metrics;
    public HRegionServerMock(Configuration conf) {
      metrics = new RegionServerMetrics(conf);
    }

    AtomicLong globalMemstoreSize = new AtomicLong(0);
    @Override
    public String getRSThreadName() {
      return "RSThread";
    }

    @Override
    public void checkFileSystem() {
    }

    @Override
    public boolean requestSplit(HRegionIf r) {
      return false;
    }

    @Override
    public RegionServerMetrics getMetrics() {
      return metrics;
    }

    @Override
    public AtomicLong getGlobalMemstoreSize() {
      return globalMemstoreSize;
    }

    @Override
    public SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
      return null;
    }

    @Override
    public void requestCompaction(HRegionIf r, String why) {
    }

  }

  private static class HRegionMock implements HRegionIf {
    private final HRegionInfo info;
    final AtomicInteger flushedCount = new AtomicInteger();
    /**
     * The number of checking maxStoreFilesCount times
     */
    final AtomicInteger checkingCount = new AtomicInteger();
    final int maxStoreFilesCount;

    public HRegionMock(StringBytes tableName, int maxStoreFilesCount) {
      info = new HRegionInfo(new HTableDescriptor(tableName.getBytes()),
          null, null);
      this.maxStoreFilesCount = maxStoreFilesCount;
    }

    @Override
    public boolean flushMemstoreShapshot(boolean selectiveFlushRequest)
        throws IOException {
      flushedCount.addAndGet(1);
      return true;
    }

    @Override
    public List<Pair<Long, Long>> getRecentFlushInfo() {
      return new ArrayList<>();
    }

    @Override
    public HRegionInfo getRegionInfo() {
      return info;
    }

    @Override
    public boolean hasReferences() {
      return false;
    }

    @Override
    public int maxStoreFilesCount() {
      checkingCount.addAndGet(1);
      return maxStoreFilesCount;
    }
  }

  @Test
  public void testFlush() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HSTORE_BLOCKING_STORE_FILES_KEY, "1");

    HRegionServerMock server = new HRegionServerMock(conf);
    MemStoreFlusher flusher = new MemStoreFlusher(conf, server);

    HRegionMock region = new HRegionMock(new StringBytes("testFlush"), 1);

    // Make a flush request
    flusher.request(region, false);

    flusher.waitAllRequestDone();
    Assert.assertEquals("Number of flushing", 1, region.flushedCount.get());
  }

  /**
   * In MemStoreFlusher, if there are too many store files, a delay should be
   * performed waiting for some files to be merged. This case assure this delay
   * is performed.
   */
  @Test
  public void testDelay() throws Exception {
    final long blockingWaitTime = 2000;

    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HSTORE_BLOCKING_STORE_FILES_KEY, "1");
    conf.set(HConstants.HSTORE_BLOCKING_WAIT_TIME_KEY, "" + blockingWaitTime);

    HRegionServerMock server = new HRegionServerMock(conf);
    MemStoreFlusher flusher = new MemStoreFlusher(conf, server);

    // 3 is larger than 1
    HRegionMock region = new HRegionMock(new StringBytes("testDelay"), 3);

    // Make a flush request
    flusher.request(region, false);

    flusher.waitAllRequestDone();

    Assert.assertEquals("Number of flushing", 1, region.flushedCount.get());
    Assert.assertTrue("Number of checking should <= " + 100 + ", but got "
        + region.checkingCount.get(), region.checkingCount.get() <= 100);
  }
}
