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

import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;

@org.apache.yetus.audience.InterfaceAudience.Private
public class RowCacheKey implements HeapSize {
  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(RowCacheKey.class, false);

  private final String encodedRegionName;
  private final byte[] rowKey;

  // When a region is reopened or bulk-loaded, its rowCacheSeqNum is used to generate new keys that
  // bypass the existing cache. This mechanism is effective when ROW_CACHE_EVICT_ON_CLOSE is set to
  // false.
  private final long rowCacheSeqNum;

  public RowCacheKey(HRegion region, byte[] rowKey) {
    this.encodedRegionName = region.getRegionInfo().getEncodedName();
    this.rowKey = Objects.requireNonNull(rowKey, "rowKey cannot be null");
    this.rowCacheSeqNum = region.getRowCacheSeqNum();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    RowCacheKey that = (RowCacheKey) o;
    return rowCacheSeqNum == that.rowCacheSeqNum
      && Objects.equals(encodedRegionName, that.encodedRegionName)
      && Objects.deepEquals(rowKey, that.rowKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(encodedRegionName, Arrays.hashCode(rowKey), rowCacheSeqNum);
  }

  @Override
  public String toString() {
    return encodedRegionName + '_' + Bytes.toStringBinary(rowKey) + '_' + rowCacheSeqNum;
  }

  @Override
  public long heapSize() {
    return FIXED_OVERHEAD + ClassSize.align(rowKey.length);
  }

  boolean isSameRegion(HRegion region) {
    return this.encodedRegionName.equals(region.getRegionInfo().getEncodedName());
  }
}
