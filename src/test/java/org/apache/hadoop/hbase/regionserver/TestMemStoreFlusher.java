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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;
import org.apache.hadoop.hbase.util.StringBytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Testcases for MemStoreFlusher
 */
@Category(SmallTests.class)
public class TestMemStoreFlusher {
  @Test(timeout = 30000)
  public void testFlush() throws Exception {
    final StringBytes TABLE_NAME = new StringBytes("testFlush");

    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HSTORE_BLOCKING_STORE_FILES_KEY, "1");

    // Create mocked server instance.
    HRegionServerIf server = Mockito.mock(HRegionServerIf.class);

    RegionServerMetrics metrics = new RegionServerMetrics(conf);
    Mockito.when(server.getMetrics()).thenReturn(metrics);

    // Create mocked region instance.
    HRegionIf region = Mockito.mock(HRegionIf.class);

    HRegionInfo info = new HRegionInfo(new HTableDescriptor(
        TABLE_NAME.getBytes()), null, null);
    Mockito.when(region.getRegionInfo()).thenReturn(info);

    // Create flusher
    MemStoreFlusher flusher = new MemStoreFlusher(conf, server);

    // Make a flush request
    flusher.request(region, false);

    flusher.waitAllRequestDone();

    Mockito.verify(region, Mockito.times(1)).flushMemstoreShapshot(false);
  }

  /**
   * In MemStoreFlusher, if there are too many store files, a delay should be
   * performed waiting for some files to be merged. This case assures this delay
   * is performed.
   */
  @Test(timeout = 30000)
  public void testDelay() throws Exception {
    final StringBytes TABLE_NAME = new StringBytes("testDelay");
    final long blockingWaitTime = 2000;

    final Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.HSTORE_BLOCKING_STORE_FILES_KEY, "1");
    conf.set(HConstants.HSTORE_BLOCKING_WAIT_TIME_KEY, "" + blockingWaitTime);

    HRegionServerIf server = Mockito.mock(HRegionServerIf.class);

    RegionServerMetrics metrics = new RegionServerMetrics(conf);
    Mockito.when(server.getMetrics()).thenReturn(metrics);

    // Create mocked region instance.
    HRegionIf region = Mockito.mock(HRegionIf.class);

    HRegionInfo info = new HRegionInfo(new HTableDescriptor(
        TABLE_NAME.getBytes()), null, null);
    Mockito.when(region.getRegionInfo()).thenReturn(info);
    // 3 is larger than 1 (HSTORE_BLOCKING_STORE_FILES)
    Mockito.when(region.maxStoreFilesCount()).thenReturn(3);

    MemStoreFlusher flusher = new MemStoreFlusher(conf, server);

    // Make a flush request
    flusher.request(region, false);

    flusher.waitAllRequestDone();

    Mockito.verify(region, Mockito.times(1)).flushMemstoreShapshot(false);
    Mockito.verify(region, Mockito.atMost(100)).maxStoreFilesCount();
  }
}
