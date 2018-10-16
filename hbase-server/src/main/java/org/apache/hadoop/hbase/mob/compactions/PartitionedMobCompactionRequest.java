/**
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
package org.apache.hadoop.hbase.mob.compactions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An implementation of {@link MobCompactionRequest} that is used in
 * {@link PartitionedMobCompactor}.
 * The mob files that have the same start key and date in their names belong to
 * the same partition.
 */
@InterfaceAudience.Private
public class PartitionedMobCompactionRequest extends MobCompactionRequest {

  protected List<CompactionDelPartition> delPartitions;
  protected Collection<CompactionPartition> compactionPartitions;

  public PartitionedMobCompactionRequest(Collection<CompactionPartition> compactionPartitions,
    List<CompactionDelPartition> delPartitions) {
    this.selectionTime = EnvironmentEdgeManager.currentTime();
    this.compactionPartitions = compactionPartitions;
    this.delPartitions = delPartitions;
  }

  /**
   * Gets the compaction partitions.
   * @return The compaction partitions.
   */
  public Collection<CompactionPartition> getCompactionPartitions() {
    return this.compactionPartitions;
  }

  /**
   * Gets the del files.
   * @return The del files.
   */
  public List<CompactionDelPartition> getDelPartitions() {
    return this.delPartitions;
  }

  /**
   * The partition in the mob compaction.
   * The mob files that have the same start key and date in their names belong to
   * the same partition.
   */
  protected static class CompactionPartition {
    private List<FileStatus> files = new ArrayList<>();
    private CompactionPartitionId partitionId;

    // The startKey and endKey of this partition, both are inclusive.
    private byte[] startKey;
    private byte[] endKey;

    public CompactionPartition(CompactionPartitionId partitionId) {
      this.partitionId = partitionId;
    }

    public CompactionPartitionId getPartitionId() {
      return this.partitionId;
    }

    public void addFile(FileStatus file) {
      files.add(file);
    }

    public List<FileStatus> listFiles() {
      return Collections.unmodifiableList(files);
    }

    public int getFileCount () {
      return files.size();
    }

    public byte[] getStartKey() {
      return startKey;
    }

    /**
     * Set start key of this partition, only if the input startKey is less than
     * the current start key.
     */
    public void setStartKey(final byte[] startKey) {
      if ((this.startKey == null) || (Bytes.compareTo(startKey, this.startKey) < 0)) {
        this.startKey = startKey;
      }
    }

    public byte[] getEndKey() {
      return endKey;
    }

    /**
     * Set end key of this partition, only if the input endKey is greater than
     * the current end key.
     */
    public void setEndKey(final byte[] endKey) {
      if ((this.endKey == null) || (Bytes.compareTo(endKey, this.endKey) > 0)) {
        this.endKey = endKey;
      }
    }
  }

  /**
   * The partition id that consists of start key and date of the mob file name.
   */
  public static class CompactionPartitionId {
    private String startKey;
    private String date;
    private String latestDate;
    private long threshold;

    public CompactionPartitionId() {
      // initialize these fields to empty string
      this.startKey = "";
      this.date = "";
      this.latestDate = "";
      this.threshold = 0;
    }

    public CompactionPartitionId(String startKey, String date) {
      if (startKey == null || date == null) {
        throw new IllegalArgumentException("Neither of start key and date could be null");
      }
      this.startKey = startKey;
      this.date = date;
      this.latestDate = "";
      this.threshold = 0;
    }

    public void setThreshold (final long threshold) {
      this.threshold = threshold;
    }

    public long getThreshold () {
      return this.threshold;
    }

    public String getStartKey() {
      return this.startKey;
    }

    public void setStartKey(final String startKey) {
      this.startKey = startKey;
    }

    public String getDate() {
      return this.date;
    }

    public void setDate(final String date) {
      this.date = date;
    }

    public String getLatestDate () { return this.latestDate; }

    public void updateLatestDate(final String latestDate) {
      if (this.latestDate.compareTo(latestDate) < 0) {
        this.latestDate = latestDate;
      }
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + startKey.hashCode();
      result = 31 * result + date.hashCode();
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CompactionPartitionId)) {
        return false;
      }
      CompactionPartitionId another = (CompactionPartitionId) obj;
      if (!this.startKey.equals(another.startKey)) {
        return false;
      }
      if (!this.date.equals(another.date)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return new StringBuilder(startKey).append(date).toString();
    }
  }

  /**
   * The delete file partition in the mob compaction.
   * The delete partition is defined as [startKey, endKey] pair.
   * The mob delete files that have the same start key and end key belong to
   * the same partition.
   */
  protected static class CompactionDelPartition {
    private List<Path> delFiles = new ArrayList<Path>();
    private List<HStoreFile> storeFiles = new ArrayList<>();
    private CompactionDelPartitionId id;

    public CompactionDelPartition(CompactionDelPartitionId id) {
      this.id = id;
    }

    public CompactionDelPartitionId getId() {
      return this.id;
    }

    void addDelFile(FileStatus file) {
      delFiles.add(file.getPath());
    }
    public void addStoreFile(HStoreFile file) {
      storeFiles.add(file);
    }

    public List<HStoreFile> getStoreFiles() {
      return storeFiles;
    }

    List<Path> listDelFiles() {
      return Collections.unmodifiableList(delFiles);
    }

    void addDelFileList(final Collection<Path> list) {
      delFiles.addAll(list);
    }

    int getDelFileCount () {
      return delFiles.size();
    }

    void cleanDelFiles() {
      delFiles.clear();
    }
  }

  /**
   * The delete partition id that consists of start key and end key
   */
  public static class CompactionDelPartitionId implements Comparable<CompactionDelPartitionId> {
    private byte[] startKey;
    private byte[] endKey;

    public CompactionDelPartitionId() {
    }

    public CompactionDelPartitionId(final byte[] startKey, final byte[] endKey) {
      this.startKey = startKey;
      this.endKey = endKey;
    }

    public byte[] getStartKey() {
      return this.startKey;
    }
    public void setStartKey(final byte[] startKey) {
      this.startKey = startKey;
    }

    public byte[] getEndKey() {
      return this.endKey;
    }
    public void setEndKey(final byte[] endKey) {
      this.endKey = endKey;
    }

    @Override
    public int compareTo(CompactionDelPartitionId o) {
      /*
       * 1). Compare the start key, if the k1 < k2, then k1 is less
       * 2). If start Key is same, check endKey, k1 < k2, k1 is less
       *     If both are same, then they are equal.
       */
      int result = Bytes.compareTo(this.startKey, o.getStartKey());
      if (result != 0) {
        return result;
      }

      return Bytes.compareTo(this.endKey, o.getEndKey());
    }

    @Override
    public int hashCode() {
      int result = 17;
      result = 31 * result + java.util.Arrays.hashCode(startKey);
      result = 31 * result + java.util.Arrays.hashCode(endKey);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CompactionDelPartitionId)) {
        return false;
      }
      CompactionDelPartitionId another = (CompactionDelPartitionId) obj;

      return (this.compareTo(another) == 0);
    }
  }
}
