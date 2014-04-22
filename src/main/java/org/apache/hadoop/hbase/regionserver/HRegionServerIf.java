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

import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerMetrics;

/**
 * The interface representing a region server.
 * A regions server serves a set of HRegions available to clients.
 *
 * The only online server is using HRegionServer as the implementation.
 */
public interface HRegionServerIf extends HRegionInterface {
  /**
   * @return what the regionserver thread name should be
   */
  String getRSThreadName();

  /**
   * Checks to see if the file system is still accessible.
   * If not, sets abortRequested and stopRequested
   *
   * @return false if file system is not available
   */
  void checkFileSystem();

  /**
   * Requests the region server to make a split on a specific region-store.
   */
  boolean requestSplit(HRegionIf r);

  /**
   * Requests the region server to make a compaction on a specific region-store.
   *
   * @param r the region-store.
   * @param why Why compaction requested -- used in debug messages
   */
  void requestCompaction(HRegionIf r, String why);

  /**
   * @return Region server metrics instance.
   */
  RegionServerMetrics getMetrics();

  /**
   * @return the size of global mem-store in bytes as an AtomicLong.
   */
  AtomicLong getGlobalMemstoreSize();

  /**
   * @return A new SortedMap of online regions sorted by region size with the
   *         first entry being the biggest.
   */
  SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize();

  /**
   * Protected utility method for safely obtaining an HRegion handle.
   * @param regionName Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  HRegion getRegion(final byte[] regionName) throws NotServingRegionException;
}
