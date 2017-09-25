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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;

/**
 * This class holds all logical details necessary to run a compaction.
 */
@InterfaceAudience.Private
public class CompactionRequest {

  // was this compaction promoted to an off-peak
  private boolean isOffPeak = false;
  private enum DisplayCompactionType { MINOR, ALL_FILES, MAJOR }
  private DisplayCompactionType isMajor = DisplayCompactionType.MINOR;
  private int priority = Store.NO_PRIORITY;
  private Collection<HStoreFile> filesToCompact;

  // CompactRequest object creation time.
  private long selectionTime;
  // System time used to compare objects in FIFO order. TODO: maybe use selectionTime?
  private long timeInNanos;
  private String regionName = "";
  private String storeName = "";
  private long totalSize = -1L;
  private CompactionLifeCycleTracker tracker = CompactionLifeCycleTracker.DUMMY;

  public CompactionRequest(Collection<HStoreFile> files) {
    this.selectionTime = EnvironmentEdgeManager.currentTime();
    this.timeInNanos = System.nanoTime();
    this.filesToCompact = Preconditions.checkNotNull(files, "files for compaction can not null");
    recalculateSize();
  }

  public void updateFiles(Collection<HStoreFile> files) {
    this.filesToCompact = Preconditions.checkNotNull(files, "files for compaction can not null");
    recalculateSize();
  }

  public Collection<HStoreFile> getFiles() {
    return this.filesToCompact;
  }

  /**
   * Sets the region/store name, for logging.
   */
  public void setDescription(String regionName, String storeName) {
    this.regionName = regionName;
    this.storeName = storeName;
  }

  /** Gets the total size of all StoreFiles in compaction */
  public long getSize() {
    return totalSize;
  }

  public boolean isAllFiles() {
    return this.isMajor == DisplayCompactionType.MAJOR
        || this.isMajor == DisplayCompactionType.ALL_FILES;
  }

  public boolean isMajor() {
    return this.isMajor == DisplayCompactionType.MAJOR;
  }

  /** Gets the priority for the request */
  public int getPriority() {
    return priority;
  }

  /** Sets the priority for the request */
  public void setPriority(int p) {
    this.priority = p;
  }

  public boolean isOffPeak() {
    return this.isOffPeak;
  }

  public void setOffPeak(boolean value) {
    this.isOffPeak = value;
  }

  public long getSelectionTime() {
    return this.selectionTime;
  }

  public long getSelectionNanoTime() {
    return this.timeInNanos;
  }

  /**
   * Specify if this compaction should be a major compaction based on the state of the store
   * @param isMajor <tt>true</tt> if the system determines that this compaction should be a major
   *          compaction
   */
  public void setIsMajor(boolean isMajor, boolean isAllFiles) {
    assert isAllFiles || !isMajor;
    this.isMajor = !isAllFiles ? DisplayCompactionType.MINOR
        : (isMajor ? DisplayCompactionType.MAJOR : DisplayCompactionType.ALL_FILES);
  }

  public void setTracker(CompactionLifeCycleTracker tracker) {
    this.tracker = tracker;
  }

  public CompactionLifeCycleTracker getTracker() {
    return tracker;
  }

  @Override
  public String toString() {
    String fsList = filesToCompact.stream().filter(f -> f.getReader() != null)
        .map(f -> TraditionalBinaryPrefix.long2String(f.getReader().length(), "", 1))
        .collect(Collectors.joining(", "));

    return "regionName=" + regionName + ", storeName=" + storeName + ", fileCount=" +
        this.getFiles().size() + ", fileSize=" +
        TraditionalBinaryPrefix.long2String(totalSize, "", 1) +
        ((fsList.isEmpty()) ? "" : " (" + fsList + ")") + ", priority=" + priority + ", time=" +
        timeInNanos;
  }

  /**
   * Recalculate the size of the compaction based on current files.
   * @param files files that should be included in the compaction
   */
  private void recalculateSize() {
    this.totalSize = filesToCompact.stream().map(HStoreFile::getReader)
        .mapToLong(r -> r != null ? r.length() : 0L).sum();
  }
}
