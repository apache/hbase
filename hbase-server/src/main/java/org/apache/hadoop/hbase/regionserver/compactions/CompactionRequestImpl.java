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

import static org.apache.hadoop.hbase.regionserver.Store.NO_PRIORITY;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * This class holds all logical details necessary to run a compaction.
 */
@InterfaceAudience.Private
public class CompactionRequestImpl implements CompactionRequest {

  // was this compaction promoted to an off-peak
  private boolean isOffPeak = false;
  private enum DisplayCompactionType { MINOR, ALL_FILES, MAJOR }
  private DisplayCompactionType isMajor = DisplayCompactionType.MINOR;
  private int priority = NO_PRIORITY;
  private Collection<HStoreFile> filesToCompact;
  private boolean isAfterSplit = false;

  // CompactRequest object creation time.
  private long selectionTime;
  private String regionName = "";
  private String storeName = "";
  private long totalSize = -1L;
  private CompactionLifeCycleTracker tracker = CompactionLifeCycleTracker.DUMMY;

  public CompactionRequestImpl(Collection<HStoreFile> files) {
    this.selectionTime = EnvironmentEdgeManager.currentTime();
    this.filesToCompact = Preconditions.checkNotNull(files, "files for compaction can not null");
    recalculateSize();
  }

  public void updateFiles(Collection<HStoreFile> files) {
    this.filesToCompact = Preconditions.checkNotNull(files, "files for compaction can not null");
    recalculateSize();
  }

  @Override
  public Collection<HStoreFile> getFiles() {
    return Collections.unmodifiableCollection(this.filesToCompact);
  }

  /**
   * Sets the region/store name, for logging.
   */
  public void setDescription(String regionName, String storeName) {
    this.regionName = regionName;
    this.storeName = storeName;
  }

  /** Gets the total size of all StoreFiles in compaction */
  @Override
  public long getSize() {
    return totalSize;
  }

  @Override
  public boolean isAllFiles() {
    return this.isMajor == DisplayCompactionType.MAJOR
        || this.isMajor == DisplayCompactionType.ALL_FILES;
  }

  @Override
  public boolean isMajor() {
    return this.isMajor == DisplayCompactionType.MAJOR;
  }

  /** Gets the priority for the request */
  @Override
  public int getPriority() {
    return priority;
  }

  /** Sets the priority for the request */
  public void setPriority(int p) {
    this.priority = p;
  }

  @Override
  public boolean isOffPeak() {
    return this.isOffPeak;
  }

  public void setOffPeak(boolean value) {
    this.isOffPeak = value;
  }

  @Override
  public long getSelectionTime() {
    return this.selectionTime;
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

  public boolean isAfterSplit() {
    return isAfterSplit;
  }

  public void setAfterSplit(boolean afterSplit) {
    isAfterSplit = afterSplit;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((filesToCompact == null) ? 0 : filesToCompact.hashCode());
    result = prime * result + ((isMajor == null) ? 0 : isMajor.hashCode());
    result = prime * result + (isOffPeak ? 1231 : 1237);
    result = prime * result + priority;
    result = prime * result + ((regionName == null) ? 0 : regionName.hashCode());
    result = prime * result + (int) (selectionTime ^ (selectionTime >>> 32));
    result = prime * result + ((storeName == null) ? 0 : storeName.hashCode());
    result = prime * result + (int) (totalSize ^ (totalSize >>> 32));
    result = prime * result + ((tracker == null) ? 0 : tracker.hashCode());
    result = prime * result + (isAfterSplit ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CompactionRequestImpl other = (CompactionRequestImpl) obj;
    if (filesToCompact == null) {
      if (other.filesToCompact != null) {
        return false;
      }
    } else if (!filesToCompact.equals(other.filesToCompact)) {
      return false;
    }
    if (isMajor != other.isMajor) {
      return false;
    }
    if (isOffPeak != other.isOffPeak) {
      return false;
    }
    if (priority != other.priority) {
      return false;
    }
    if (regionName == null) {
      if (other.regionName != null) {
        return false;
      }
    } else if (!regionName.equals(other.regionName)) {
      return false;
    }
    if (selectionTime != other.selectionTime) {
      return false;
    }
    if (storeName == null) {
      if (other.storeName != null) {
        return false;
      }
    } else if (!storeName.equals(other.storeName)) {
      return false;
    }
    if (totalSize != other.totalSize) {
      return false;
    }
    if (isAfterSplit != other.isAfterSplit) {
      return false;
    }
    if (tracker == null) {
      if (other.tracker != null) {
        return false;
      }
    } else if (!tracker.equals(other.tracker)) {
      return false;
    }
    return true;
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
        selectionTime;
  }

  /**
   * Recalculate the size of the compaction based on current files.
   */
  private void recalculateSize() {
    this.totalSize = filesToCompact.stream().map(HStoreFile::getReader)
        .mapToLong(r -> r != null ? r.length() : 0L).sum();
  }
}
