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

import static org.apache.hadoop.hbase.regionserver.StoreFileWriter.shouldEnableHistoricalCompactionFiles;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Default implementation of StoreFileManager. Not thread-safe.
 */
@InterfaceAudience.Private
class DefaultStoreFileManager implements StoreFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStoreFileManager.class);

  private final CellComparator cellComparator;
  private final CompactionConfiguration comConf;
  private final int blockingFileCount;
  private final Comparator<HStoreFile> storeFileComparator;

  static class StoreFileList {
    /**
     * List of store files inside this store. This is an immutable list that is atomically replaced
     * when its contents change.
     */
    final ImmutableList<HStoreFile> all;
    /**
     * List of store files that include the latest cells inside this store. This is an immutable
     * list that is atomically replaced when its contents change.
     */
    @Nullable
    final ImmutableList<HStoreFile> live;

    StoreFileList(ImmutableList<HStoreFile> storeFiles, ImmutableList<HStoreFile> liveStoreFiles) {
      this.all = storeFiles;
      this.live = liveStoreFiles;
    }
  }

  private volatile StoreFileList storeFiles;

  /**
   * List of compacted files inside this store that needs to be excluded in reads because further
   * new reads will be using only the newly created files out of compaction. These compacted files
   * will be deleted/cleared once all the existing readers on these compacted files are done.
   */
  private volatile ImmutableList<HStoreFile> compactedfiles = ImmutableList.of();
  private final boolean enableLiveFileTracking;

  public DefaultStoreFileManager(CellComparator cellComparator,
    Comparator<HStoreFile> storeFileComparator, Configuration conf,
    CompactionConfiguration comConf) {
    this.cellComparator = cellComparator;
    this.storeFileComparator = storeFileComparator;
    this.comConf = comConf;
    blockingFileCount =
      conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT);
    enableLiveFileTracking = shouldEnableHistoricalCompactionFiles(conf);
    storeFiles =
      new StoreFileList(ImmutableList.of(), enableLiveFileTracking ? ImmutableList.of() : null);
  }

  private List<HStoreFile> getLiveFiles(Collection<HStoreFile> storeFiles) throws IOException {
    List<HStoreFile> liveFiles = new ArrayList<>(storeFiles.size());
    for (HStoreFile file : storeFiles) {
      file.initReader();
      if (!file.isHistorical()) {
        liveFiles.add(file);
      }
    }
    return liveFiles;
  }

  @Override
  public void loadFiles(List<HStoreFile> storeFiles) throws IOException {
    this.storeFiles = new StoreFileList(ImmutableList.sortedCopyOf(storeFileComparator, storeFiles),
      enableLiveFileTracking
        ? ImmutableList.sortedCopyOf(storeFileComparator, getLiveFiles(storeFiles))
        : null);
  }

  @Override
  public final Collection<HStoreFile> getStoreFiles() {
    return storeFiles.all;
  }

  @Override
  public Collection<HStoreFile> getCompactedfiles() {
    return compactedfiles;
  }

  @Override
  public void insertNewFiles(Collection<HStoreFile> sfs) throws IOException {
    storeFiles = new StoreFileList(
      ImmutableList.sortedCopyOf(storeFileComparator, Iterables.concat(storeFiles.all, sfs)),
      enableLiveFileTracking
        ? ImmutableList.sortedCopyOf(storeFileComparator,
          Iterables.concat(storeFiles.live, getLiveFiles(sfs)))
        : null);
  }

  @Override
  public ImmutableCollection<HStoreFile> clearFiles() {
    ImmutableList<HStoreFile> result = storeFiles.all;
    storeFiles =
      new StoreFileList(ImmutableList.of(), enableLiveFileTracking ? ImmutableList.of() : null);
    return result;
  }

  @Override
  public Collection<HStoreFile> clearCompactedFiles() {
    List<HStoreFile> result = compactedfiles;
    compactedfiles = ImmutableList.of();
    return result;
  }

  @Override
  public final int getStorefileCount() {
    return storeFiles.all.size();
  }

  @Override
  public final int getCompactedFilesCount() {
    return compactedfiles.size();
  }

  @Override
  public void addCompactionResults(Collection<HStoreFile> newCompactedfiles,
    Collection<HStoreFile> results) throws IOException {
    ImmutableList<HStoreFile> liveStoreFiles = null;
    if (enableLiveFileTracking) {
      liveStoreFiles = ImmutableList.sortedCopyOf(storeFileComparator,
        Iterables.concat(Iterables.filter(storeFiles.live, sf -> !newCompactedfiles.contains(sf)),
          getLiveFiles(results)));
    }
    storeFiles =
      new StoreFileList(
        ImmutableList
          .sortedCopyOf(storeFileComparator,
            Iterables.concat(
              Iterables.filter(storeFiles.all, sf -> !newCompactedfiles.contains(sf)), results)),
        liveStoreFiles);
    // Mark the files as compactedAway once the storefiles and compactedfiles list is finalized
    // Let a background thread close the actual reader on these compacted files and also
    // ensure to evict the blocks from block cache so that they are no longer in
    // cache
    List<HStoreFile> filesToClose = new ArrayList<>(newCompactedfiles);
    try {
      HStoreFile.increaseStoreFilesRefeCount(newCompactedfiles);
      newCompactedfiles.forEach(hStoreFile -> {
        StoreFileReader reader = hStoreFile.getReader();
        try {
          if (reader == null) {
            hStoreFile.initReader();
          } else {
            filesToClose.remove(hStoreFile);
          }
        } catch (IOException e) {
          LOG.warn("Couldn't initialize reader for " + hStoreFile, e);
          throw new RuntimeException(e);
        } finally {
          hStoreFile.markCompactedAway();
        }
      });
      compactedfiles = ImmutableList.sortedCopyOf(storeFileComparator,
        Iterables.concat(compactedfiles, newCompactedfiles));
    } finally {
      HStoreFile.decreaseStoreFilesRefeCount(newCompactedfiles);
      filesToClose.forEach(HStoreFile::closeStoreFile);
    }
  }

  @Override
  public void removeCompactedFiles(Collection<HStoreFile> removedCompactedfiles) {
    compactedfiles = compactedfiles.stream().filter(sf -> !removedCompactedfiles.contains(sf))
      .sorted(storeFileComparator).collect(ImmutableList.toImmutableList());
  }

  @Override
  public final Iterator<HStoreFile> getCandidateFilesForRowKeyBefore(KeyValue targetKey) {
    return storeFiles.all.reverse().iterator();
  }

  @Override
  public Iterator<HStoreFile> updateCandidateFilesForRowKeyBefore(
    Iterator<HStoreFile> candidateFiles, KeyValue targetKey, Cell candidate) {
    // Default store has nothing useful to do here.
    // TODO: move this comment when implementing Level:
    // Level store can trim the list by range, removing all the files which cannot have
    // any useful candidates less than "candidate".
    return candidateFiles;
  }

  @Override
  public final Optional<byte[]> getSplitPoint() throws IOException {
    return StoreUtils.getSplitPoint(storeFiles.all, cellComparator);
  }

  @Override
  public Collection<HStoreFile> getFilesForScan(byte[] startRow, boolean includeStartRow,
    byte[] stopRow, boolean includeStopRow, boolean onlyLatestVersion) {
    if (onlyLatestVersion && enableLiveFileTracking) {
      return storeFiles.live;
    }
    // We cannot provide any useful input and already have the files sorted by seqNum.
    return getStoreFiles();
  }

  @Override
  public int getStoreCompactionPriority() {
    int priority = blockingFileCount - storeFiles.all.size();
    return (priority == HStore.PRIORITY_USER) ? priority + 1 : priority;
  }

  @Override
  public Collection<HStoreFile> getUnneededFiles(long maxTs, List<HStoreFile> filesCompacting) {
    ImmutableList<HStoreFile> files = storeFiles.all;
    // 1) We can never get rid of the last file which has the maximum seqid.
    // 2) Files that are not the latest can't become one due to (1), so the rest are fair game.
    return files.stream().limit(Math.max(0, files.size() - 1)).filter(sf -> {
      long fileTs = sf.getReader().getMaxTimestamp();
      if (fileTs < maxTs && !filesCompacting.contains(sf)) {
        LOG.info("Found an expired store file {} whose maxTimestamp is {}, which is below {}",
          sf.getPath(), fileTs, maxTs);
        return true;
      } else {
        return false;
      }
    }).collect(Collectors.toList());
  }

  @Override
  public double getCompactionPressure() {
    int storefileCount = getStorefileCount();
    int minFilesToCompact = comConf.getMinFilesToCompact();
    if (storefileCount <= minFilesToCompact) {
      return 0.0;
    }
    return (double) (storefileCount - minFilesToCompact) / (blockingFileCount - minFilesToCompact);
  }

  @Override
  public Comparator<HStoreFile> getStoreFileComparator() {
    return storeFileComparator;
  }
}
