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
package org.apache.hadoop.hbase.compactionserver;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * since we do not maintain StoreFileManager in compaction server(can't refresh when flush). we use
 * external storage(this class) to record compacting files, and initialize a new HStore in
 * {@link CompactionThreadManager#selectCompaction} every time when request compaction
 */
@InterfaceAudience.Private
class CompactionServerStorage {
  private static Logger LOG = LoggerFactory.getLogger(CompactionServerStorage.class);
  private final ConcurrentMap<String, ConcurrentMap<String, Set<String>>> selectedFiles =
      new ConcurrentHashMap<>();
  private final ConcurrentMap<String, ConcurrentMap<String, Set<String>>> compactedFiles =
      new ConcurrentHashMap<>();
  /**
   * Mark files as completed, called after CS finished compaction and RS accepted the results of
   * this compaction, these compacted files will be deleted by RS if no reader referenced to them.
   */
  boolean addCompactedFiles(RegionInfo regionInfo, ColumnFamilyDescriptor cfd,
      List<String> compactedFiles) {
    Set<String> compactedFileSet = getCompactedStoreFiles(regionInfo, cfd);
    synchronized (compactedFileSet) {
      compactedFileSet.addAll(compactedFiles);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Mark files as compacted, region: {}, cf, files: {}", regionInfo,
          cfd.getNameAsString(), compactedFileSet);
      }
    }
    return true;
  }

  /**
   * Mark files as selected, called after the files are selected and before the compaction is
   * started. Avoid a file is selected twice in two compaction.
   * @return True if these files don't be selected, false if these files are already selected.
   */
  boolean addSelectedFiles(RegionInfo regionInfo, ColumnFamilyDescriptor cfd,
      List<String> selectedFiles) {
    Set<String> selectedFileSet = getSelectedStoreFiles(regionInfo, cfd);
    synchronized (selectedFileSet) {
      for (String selectedFile : selectedFiles) {
        if (selectedFileSet.contains(selectedFile)) {
          return false;
        }
      }
      selectedFileSet.addAll(selectedFiles);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Mark files are selected, region: {}, cf: {}, files: {}",
          regionInfo.getEncodedName(), cfd.getNameAsString(), selectedFiles);
      }
    }
    return true;
  }

  /**
   * Get files which are compacted, called before select files to do compaction.
   * @return The files which are compacted
   */
  Set<String> getCompactedStoreFiles(RegionInfo regionInfo, ColumnFamilyDescriptor cfd) {
    return getStoreFiles(this.compactedFiles, regionInfo, cfd);
  }

  /**
   * Get files which are compacting, called before select files to do compaction.
   * @return The files which are compacting
   */
  Set<String> getSelectedStoreFiles(RegionInfo regionInfo, ColumnFamilyDescriptor cfd) {
    return getStoreFiles(this.selectedFiles, regionInfo, cfd);
  }

  private Set<String> getStoreFiles(
      ConcurrentMap<String, ConcurrentMap<String, Set<String>>> fileMap, RegionInfo regionInfo,
      ColumnFamilyDescriptor cfd) {
    String encodedName = regionInfo.getEncodedName();
    String family = cfd.getNameAsString();
    Map<String, Set<String>> familyFilesMap =
        fileMap.computeIfAbsent(encodedName, v -> new ConcurrentHashMap<>());
    return familyFilesMap.computeIfAbsent(family, v -> new HashSet<>());
  }

  /**
   * Remove files from selected, called:
   * 1. after the compaction is failed;
   * 2. after the compaction is finished and report to RS failed;
   * 3. after the compaction is finished and report to RS succeeded (and will mark these files
   *    as compacted).
   */
  void removeSelectedFiles(RegionInfo regionInfo, ColumnFamilyDescriptor cfd,
      List<String> selectedFiles) {
    Set<String> selectedFileSet = getSelectedStoreFiles(regionInfo, cfd);
    synchronized (selectedFileSet) {
      selectedFileSet.removeAll(selectedFiles);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Remove files from selected, region: {}, cf: {}, files: {}",
          regionInfo.getEncodedName(), cfd.getNameAsString(), selectedFiles);
      }
    }
  }

  /**
   * Remove compacted files which are already deleted by RS
   */
  void cleanupCompactedFiles(RegionInfo regionInfo, ColumnFamilyDescriptor cfd,
      Set<String> storeFileNames) {
    Set<String> compactedFileSet = getCompactedStoreFiles(regionInfo, cfd);
    synchronized (compactedFileSet) {
      compactedFileSet.retainAll(storeFileNames);
    }
  }
}
