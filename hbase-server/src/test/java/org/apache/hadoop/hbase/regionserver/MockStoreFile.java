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
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/** A mock used so our tests don't deal with actual StoreFiles */
public class MockStoreFile extends StoreFile {
  long length = 0;
  boolean isRef = false;
  long ageInDisk;
  long sequenceid;
  private Map<byte[], byte[]> metadata = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
  byte[] splitPoint = null;
  TimeRangeTracker timeRangeTracker;
  long entryCount;
  boolean isMajor;
  HDFSBlocksDistribution hdfsBlocksDistribution;
  long modificationTime;
  
  MockStoreFile(HBaseTestingUtility testUtil, Path testPath,
      long length, long ageInDisk, boolean isRef, long sequenceid) throws IOException {
    super(testUtil.getTestFileSystem(), testPath, testUtil.getConfiguration(),
      new CacheConfig(testUtil.getConfiguration()), BloomType.NONE);
    this.length = length;
    this.isRef = isRef;
    this.ageInDisk = ageInDisk;
    this.sequenceid = sequenceid;
    this.isMajor = false;
    hdfsBlocksDistribution = new HDFSBlocksDistribution();
    hdfsBlocksDistribution.addHostsAndBlockWeight(
      new String[] { HRegionServer.getHostname(testUtil.getConfiguration()) }, 1);
    modificationTime = EnvironmentEdgeManager.currentTimeMillis();
  }

  void setLength(long newLen) {
    this.length = newLen;
  }

  @Override
  byte[] getFileSplitPoint(KVComparator comparator) throws IOException {
    return this.splitPoint;
  }

  @Override
  public long getMaxSequenceId() {
    return sequenceid;
  }

  @Override
  public boolean isMajorCompaction() {
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

  public Long getMinimumTimestamp() {
    return (timeRangeTracker == null) ?
      null : timeRangeTracker.getMin();
  }
	  
  public Long getMaximumTimestamp() {
    return (timeRangeTracker == null) ?
      null : timeRangeTracker.getMax();
  }

  @Override
  public long getModificationTimeStamp() {
    return modificationTime;
  }

  @Override
  public HDFSBlocksDistribution getHDFSBlockDistribution() {
    return hdfsBlocksDistribution;
  }

  @Override
  public StoreFile.Reader getReader() {
    final long len = this.length;
    final TimeRangeTracker timeRangeTracker = this.timeRangeTracker;
    final long entries = this.entryCount;
    return new StoreFile.Reader() {
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
    };
  }
}
