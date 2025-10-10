/*
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

@org.apache.yetus.audience.InterfaceAudience.Private
public class RowCacheImpl implements RowCache {
  RowCacheImpl(long maxSizeBytes) {
    // TODO
  }

  @Override
  public void cacheRow(RowCacheKey key, RowCells value) {
    // TODO
  }

  @Override
  public void evictRow(RowCacheKey key) {
    // TODO
  }

  @Override
  public void evictRowsByRegion(HRegion region) {
    // TODO
  }

  @Override
  public long getCount() {
    // TODO
    return 0;
  }

  @Override
  public long getEvictedRowCount() {
    // TODO
    return 0;
  }

  @Override
  public long getHitCount() {
    // TODO
    return 0;
  }

  @Override
  public long getMaxSize() {
    // TODO
    return 0;
  }

  @Override
  public long getMissCount() {
    // TODO
    return 0;
  }

  @Override
  public RowCells getRow(RowCacheKey key, boolean caching) {
    // TODO
    return null;
  }

  @Override
  public long getSize() {
    // TODO
    return 0;
  }
}
