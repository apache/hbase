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
package org.apache.hadoop.hbase.io.hfile;

import java.util.Arrays;
import java.util.Objects;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Cache Key for use with implementations of {@link BlockCache}
 */
@InterfaceAudience.Private
public class RowCacheKey extends BlockCacheKey {
  private static final long serialVersionUID = -686874540957524887L;
  public static final long FIXED_OVERHEAD = ClassSize.estimateBase(RowCacheKey.class, false);

  private final byte[] rowKey;
  // Row cache keys should not be evicted on close, since the cache may contain many entries and
  // eviction would be slow. Instead, the region’s rowCacheSeqNum is used to generate new keys that
  // ignore the existing cache when the region is reopened or bulk-loaded.
  private final long rowCacheSeqNum;

  public RowCacheKey(HRegion region, byte[] rowKey) {
    super(region.getRegionInfo().getEncodedName(), 0, region.getRegionInfo().getReplicaId() == 0,
      BlockType.ROW_CELLS);

    this.rowKey = Objects.requireNonNull(rowKey, "rowKey cannot be null");
    this.rowCacheSeqNum = region.getRowCacheSeqNum();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    RowCacheKey that = (RowCacheKey) o;
    return rowCacheSeqNum == that.rowCacheSeqNum && Arrays.equals(rowKey, that.rowKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), Arrays.hashCode(rowKey), Long.hashCode(rowCacheSeqNum));
  }

  @Override
  public String toString() {
    return super.toString() + '_' + Bytes.toStringBinary(this.rowKey) + '_' + rowCacheSeqNum;
  }

  @Override
  public long heapSize() {
    return FIXED_OVERHEAD + ClassSize.align(rowKey.length);
  }
}
