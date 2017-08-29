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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;

import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableCollection;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.shaded.com.google.common.collect.Lists;

/**
 * Default implementation of StoreFileManager. Not thread-safe.
 */
@InterfaceAudience.Private
class DefaultStoreFileManager implements StoreFileManager {
  private static final Log LOG = LogFactory.getLog(DefaultStoreFileManager.class);

  private final CellComparator kvComparator;
  private final CompactionConfiguration comConf;
  private final int blockingFileCount;
  private final Comparator<StoreFile> storeFileComparator;
  /**
   * List of store files inside this store. This is an immutable list that
   * is atomically replaced when its contents change.
   */
  private volatile ImmutableList<StoreFile> storefiles = null;
  /**
   * List of compacted files inside this store that needs to be excluded in reads
   * because further new reads will be using only the newly created files out of compaction.
   * These compacted files will be deleted/cleared once all the existing readers on these
   * compacted files are done.
   */
  private volatile List<StoreFile> compactedfiles = null;

  public DefaultStoreFileManager(CellComparator kvComparator,
      Comparator<StoreFile> storeFileComparator, Configuration conf,
      CompactionConfiguration comConf) {
    this.kvComparator = kvComparator;
    this.storeFileComparator = storeFileComparator;
    this.comConf = comConf;
    this.blockingFileCount =
        conf.getInt(HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT);
  }

  @Override
  public void loadFiles(List<StoreFile> storeFiles) {
    sortAndSetStoreFiles(storeFiles);
  }

  @Override
  public final Collection<StoreFile> getStorefiles() {
    // TODO: I can return a null list of StoreFiles? That'll mess up clients. St.Ack 20151111
    return storefiles;
  }

  @Override
  public Collection<StoreFile> getCompactedfiles() {
    return compactedfiles;
  }

  @Override
  public void insertNewFiles(Collection<StoreFile> sfs) throws IOException {
    ArrayList<StoreFile> newFiles = new ArrayList<>(storefiles);
    newFiles.addAll(sfs);
    sortAndSetStoreFiles(newFiles);
  }

  @Override
  public ImmutableCollection<StoreFile> clearFiles() {
    ImmutableList<StoreFile> result = storefiles;
    storefiles = ImmutableList.of();
    return result;
  }

  @Override
  public Collection<StoreFile> clearCompactedFiles() {
    List<StoreFile> result = compactedfiles;
    compactedfiles = new ArrayList<>();
    return result;
  }

  @Override
  public final int getStorefileCount() {
    return storefiles.size();
  }

  @Override
  public final int getCompactedFilesCount() {
    if (compactedfiles == null) {
      return 0;
    }
    return compactedfiles.size();
  }

  @Override
  public void addCompactionResults(
    Collection<StoreFile> newCompactedfiles, Collection<StoreFile> results) {
    ArrayList<StoreFile> newStoreFiles = Lists.newArrayList(storefiles);
    newStoreFiles.removeAll(newCompactedfiles);
    if (!results.isEmpty()) {
      newStoreFiles.addAll(results);
    }
    sortAndSetStoreFiles(newStoreFiles);
    ArrayList<StoreFile> updatedCompactedfiles = null;
    if (this.compactedfiles != null) {
      updatedCompactedfiles = new ArrayList<>(this.compactedfiles);
      updatedCompactedfiles.addAll(newCompactedfiles);
    } else {
      updatedCompactedfiles = new ArrayList<>(newCompactedfiles);
    }
    markCompactedAway(newCompactedfiles);
    this.compactedfiles = sortCompactedfiles(updatedCompactedfiles);
  }

  // Mark the files as compactedAway once the storefiles and compactedfiles list is finalized
  // Let a background thread close the actual reader on these compacted files and also
  // ensure to evict the blocks from block cache so that they are no longer in
  // cache
  private void markCompactedAway(Collection<StoreFile> compactedFiles) {
    for (StoreFile file : compactedFiles) {
      file.markCompactedAway();
    }
  }

  @Override
  public void removeCompactedFiles(Collection<StoreFile> removedCompactedfiles) throws IOException {
    ArrayList<StoreFile> updatedCompactedfiles = null;
    if (this.compactedfiles != null) {
      updatedCompactedfiles = new ArrayList<>(this.compactedfiles);
      updatedCompactedfiles.removeAll(removedCompactedfiles);
      this.compactedfiles = sortCompactedfiles(updatedCompactedfiles);
    }
  }

  @Override
  public final Iterator<StoreFile> getCandidateFilesForRowKeyBefore(final KeyValue targetKey) {
    return new ArrayList<>(Lists.reverse(this.storefiles)).iterator();
  }

  @Override
  public Iterator<StoreFile> updateCandidateFilesForRowKeyBefore(
      Iterator<StoreFile> candidateFiles, final KeyValue targetKey, final Cell candidate) {
    // Default store has nothing useful to do here.
    // TODO: move this comment when implementing Level:
    // Level store can trim the list by range, removing all the files which cannot have
    // any useful candidates less than "candidate".
    return candidateFiles;
  }

  @Override
  public final byte[] getSplitPoint() throws IOException {
    List<StoreFile> storefiles = this.storefiles;
    if (storefiles.isEmpty()) {
      return null;
    }
    Optional<StoreFile> largestFile = StoreUtils.getLargestFile(storefiles);
    return largestFile.isPresent()
        ? StoreUtils.getFileSplitPoint(largestFile.get(), kvComparator).orElse(null) : null;
  }

  @Override
  public final Collection<StoreFile> getFilesForScan(byte[] startRow, boolean includeStartRow,
      byte[] stopRow, boolean includeStopRow) {
    // We cannot provide any useful input and already have the files sorted by seqNum.
    return getStorefiles();
  }

  @Override
  public int getStoreCompactionPriority() {
    int priority = blockingFileCount - storefiles.size();
    return (priority == HStore.PRIORITY_USER) ? priority + 1 : priority;
  }

  @Override
  public Collection<StoreFile> getUnneededFiles(long maxTs, List<StoreFile> filesCompacting) {
    Collection<StoreFile> expiredStoreFiles = null;
    ImmutableList<StoreFile> files = storefiles;
    // 1) We can never get rid of the last file which has the maximum seqid.
    // 2) Files that are not the latest can't become one due to (1), so the rest are fair game.
    for (int i = 0; i < files.size() - 1; ++i) {
      StoreFile sf = files.get(i);
      long fileTs = sf.getReader().getMaxTimestamp();
      if (fileTs < maxTs && !filesCompacting.contains(sf)) {
        LOG.info("Found an expired store file: " + sf.getPath()
            + " whose maxTimeStamp is " + fileTs + ", which is below " + maxTs);
        if (expiredStoreFiles == null) {
          expiredStoreFiles = new ArrayList<>();
        }
        expiredStoreFiles.add(sf);
      }
    }
    return expiredStoreFiles;
  }

  private void sortAndSetStoreFiles(List<StoreFile> storeFiles) {
    Collections.sort(storeFiles, storeFileComparator);
    storefiles = ImmutableList.copyOf(storeFiles);
  }

  private List<StoreFile> sortCompactedfiles(List<StoreFile> storefiles) {
    // Sorting may not be really needed here for the compacted files?
    Collections.sort(storefiles, storeFileComparator);
    return new ArrayList<>(storefiles);
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
  public Comparator<StoreFile> getStoreFileComparator() {
    return storeFileComparator;
  }
}

