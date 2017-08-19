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

import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;

import org.apache.hadoop.hbase.shaded.com.google.common.base.Preconditions;

/**
 * This class holds all logical details necessary to run a compaction.
 */
@InterfaceAudience.LimitedPrivate({ "coprocessor" })
@InterfaceStability.Evolving
public class CompactionRequest {

  // was this compaction promoted to an off-peak
  private boolean isOffPeak = false;
  private enum DisplayCompactionType { MINOR, ALL_FILES, MAJOR }
  private DisplayCompactionType isMajor = DisplayCompactionType.MINOR;
  private int priority = Store.NO_PRIORITY;
  private Collection<StoreFile> filesToCompact;

  // CompactRequest object creation time.
  private long selectionTime;
  // System time used to compare objects in FIFO order. TODO: maybe use selectionTime?
  private long timeInNanos;
  private String regionName = "";
  private String storeName = "";
  private long totalSize = -1L;

  /**
   * This ctor should be used by coprocessors that want to subclass CompactionRequest.
   */
  public CompactionRequest() {
    this.selectionTime = EnvironmentEdgeManager.currentTime();
    this.timeInNanos = System.nanoTime();
  }

  public CompactionRequest(Collection<StoreFile> files) {
    this();
    Preconditions.checkNotNull(files);
    this.filesToCompact = files;
    recalculateSize();
  }

  public void updateFiles(Collection<StoreFile> files) {
    this.filesToCompact = files;
    recalculateSize();
  }

  /**
   * Called before compaction is executed by CompactSplitThread; for use by coproc subclasses.
   */
  public void beforeExecute() {}

  /**
   * Called after compaction is executed by CompactSplitThread; for use by coproc subclasses.
   */
  public void afterExecute() {}

  /**
   * Combines the request with other request. Coprocessors subclassing CR may override
   * this if they want to do clever things based on CompactionPolicy selection that
   * is passed to this method via "other". The default implementation just does a copy.
   * @param other Request to combine with.
   * @return The result (may be "this" or "other").
   */
  public CompactionRequest combineWith(CompactionRequest other) {
    this.filesToCompact = new ArrayList<>(other.getFiles());
    this.isOffPeak = other.isOffPeak;
    this.isMajor = other.isMajor;
    this.priority = other.priority;
    this.selectionTime = other.selectionTime;
    this.timeInNanos = other.timeInNanos;
    this.regionName = other.regionName;
    this.storeName = other.storeName;
    this.totalSize = other.totalSize;
    recalculateSize();
    return this;
  }

  public Collection<StoreFile> getFiles() {
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
    long sz = 0;
    for (StoreFile sf : this.filesToCompact) {
      StoreFileReader r = sf.getReader();
      sz += r == null ? 0 : r.length();
    }
    this.totalSize = sz;
  }
}

