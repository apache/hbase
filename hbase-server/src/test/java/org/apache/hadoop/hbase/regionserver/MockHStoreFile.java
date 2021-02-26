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
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

/** A mock used so our tests don't deal with actual StoreFiles */
@InterfaceAudience.Private
public class MockHStoreFile extends HStoreFile {
  long length = 0;
  boolean isRef = false;
  long ageInDisk;
  long sequenceid;
  private Map<byte[], byte[]> metadata = new TreeMap<>(Bytes.BYTES_COMPARATOR);
  byte[] splitPoint = null;
  TimeRangeTracker timeRangeTracker;
  long entryCount;
  boolean isMajor;
  HDFSBlocksDistribution hdfsBlocksDistribution;
  long modificationTime;
  boolean compactedAway;

  MockHStoreFile(HBaseTestingUtility testUtil, Path testPath,
      long length, long ageInDisk, boolean isRef, long sequenceid) throws IOException {
    super(testUtil.getTestFileSystem(), testPath, testUtil.getConfiguration(),
        new CacheConfig(testUtil.getConfiguration()), BloomType.NONE, true);
    this.length = length;
    this.isRef = isRef;
    this.ageInDisk = ageInDisk;
    this.sequenceid = sequenceid;
    this.isMajor = false;
    hdfsBlocksDistribution = new HDFSBlocksDistribution();
    hdfsBlocksDistribution.addHostsAndBlockWeight(new String[]
      { DNS.getHostname(testUtil.getConfiguration(), DNS.ServerType.REGIONSERVER) }, 1);
    modificationTime = EnvironmentEdgeManager.currentTime();
  }

  void setLength(long newLen) {
    this.length = newLen;
  }

  @Override
  public long getMaxSequenceId() {
    return sequenceid;
  }

  @Override
  public boolean isMajorCompactionResult() {
    return isMajor;
  }

  public void setIsMajor(boolean isMajor) {
    this.isMajor = isMajor;
  }

  @Override
  public boolean isReference() {
    return this.isRef;
  }

  @Override
  public boolean isBulkLoadResult() {
    return false;
  }

  @Override
  public byte[] getMetadataValue(byte[] key) {
    return this.metadata.get(key);
  }

  public void setMetadataValue(byte[] key, byte[] value) {
    this.metadata.put(key, value);
  }

  void setTimeRangeTracker(TimeRangeTracker timeRangeTracker) {
    this.timeRangeTracker = timeRangeTracker;
  }

  void setEntries(long entryCount) {
    this.entryCount = entryCount;
  }

  @Override
  public OptionalLong getMinimumTimestamp() {
    return timeRangeTracker == null ? OptionalLong.empty()
        : OptionalLong.of(timeRangeTracker.getMin());
  }

  @Override
  public OptionalLong getMaximumTimestamp() {
    return timeRangeTracker == null ? OptionalLong.empty()
        : OptionalLong.of(timeRangeTracker.getMax());
  }

  @Override
  public void markCompactedAway() {
    this.compactedAway = true;
  }

  @Override
  public boolean isCompactedAway() {
    return compactedAway;
  }

  @Override
  public long getModificationTimeStamp() {
    return getModificationTimestamp();
  }

  @Override
  public long getModificationTimestamp() {
    return modificationTime;
  }

  @Override
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return hdfsBlocksDistribution;
  }

  @Override
  public void initReader() throws IOException {
  }

  @Override
  public StoreFileScanner getPreadScanner(boolean cacheBlocks, long readPt, long scannerOrder,
      boolean canOptimizeForNonNullColumn) {
    return getReader().getStoreFileScanner(cacheBlocks, true, false, readPt, scannerOrder,
      canOptimizeForNonNullColumn);
  }

  @Override
  public StoreFileScanner getStreamScanner(boolean canUseDropBehind, boolean cacheBlocks,
      boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn)
      throws IOException {
    return getReader().getStoreFileScanner(cacheBlocks, false, isCompaction, readPt, scannerOrder,
      canOptimizeForNonNullColumn);
  }

  @Override
  public StoreFileReader getReader() {
    final long len = this.length;
    final TimeRangeTracker timeRangeTracker = this.timeRangeTracker;
    final long entries = this.entryCount;
    return new StoreFileReader() {
      @Override
      public long length() {
        return len;
      }

      @Override
      public long getMaxTimestamp() {
        return timeRange == null? Long.MAX_VALUE: timeRangeTracker.getMax();
      }

      @Override
      public long getEntries() {
        return entries;
      }

      @Override
      public void close(boolean evictOnClose) throws IOException {
        // no-op
      }

      @Override
      public Optional<Cell> getLastKey() {
        if (splitPoint != null) {
          return Optional.of(CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
              .setType(Cell.Type.Put)
              .setRow(Arrays.copyOf(splitPoint, splitPoint.length + 1)).build());
        } else {
          return Optional.empty();
        }
      }

      @Override
      public Optional<Cell> midKey() throws IOException {
        if (splitPoint != null) {
          return Optional.of(CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
              .setType(Cell.Type.Put).setRow(splitPoint).build());
        } else {
          return Optional.empty();
        }
      }

      @Override
      public Optional<Cell> getFirstKey() {
        if (splitPoint != null) {
          return Optional.of(CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
              .setType(Cell.Type.Put).setRow(splitPoint, 0, splitPoint.length - 1)
              .build());
        } else {
          return Optional.empty();
        }
      }
    };
  }

  @Override
  public OptionalLong getBulkLoadTimestamp() {
    // we always return false for isBulkLoadResult so we do not have a bulk load timestamp
    return OptionalLong.empty();
  }
}
