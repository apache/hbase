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
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Pair;

/**
 * The interface of a storage instance of a whole region.
 *
 * The only online server is using HRegion as the implementation.
 */
public interface HRegionIf {
  /**
   * @return the HRegionInfo of this region
   */
  public HRegionInfo getRegionInfo();

  /**
   * Flushes the cache.
   *
   * When this method is called the cache will be flushed unless:
   * <ol>
   * <li>the cache is empty</li>
   * <li>the region is closed.</li>
   * <li>a flush is already in progress</li>
   * <li>writes are disabled</li>
   * </ol>
   *
   * <p>
   * This method may block for some time, so it should not be called from a
   * time-sensitive thread.
   *
   * @param selectiveFlushRequest If true, selectively flush column families
   *          which dominate the memstore size, provided it
   *          is enabled in the configuration.
   *
   * @return true if cache was flushed
   */
  public boolean flushMemstoreShapshot(boolean selectiveFlushRequest)
      throws IOException;

  /**
   * @return how info about the last flushes <time, size>
   */
  public List<Pair<Long, Long>> getRecentFlushInfo();

  /**
   * @return True if this region has references.
   */
  public boolean hasReferences();

  /**
   * @return the maximum number of files among all stores.
   */
  public int maxStoreFilesCount();
}
