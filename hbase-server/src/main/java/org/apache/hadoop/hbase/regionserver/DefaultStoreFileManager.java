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
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Default implementation of StoreFileManager. Not thread-safe.
 */
@InterfaceAudience.Private
class DefaultStoreFileManager implements StoreFileManager {
  private static final Log LOG = LogFactory.getLog(DefaultStoreFileManager.class);

  private final KVComparator kvComparator;
  private final CompactionConfiguration comConf;
  private final int blockingFileCount;

  /**
   * List of store files inside this store. This is an immutable list that
   * is atomically replaced when its contents change.
   */
  private volatile ImmutableList<StoreFile> storefiles = null;

  public DefaultStoreFileManager(KVComparator kvComparator, Configuration conf,
      CompactionConfiguration comConf) {
    this.kvComparator = kvComparator;
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
  public void insertNewFiles(Collection<StoreFile> sfs) throws IOException {
    ArrayList<StoreFile> newFiles = new ArrayList<StoreFile>(storefiles);
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
  public final int getStorefileCount() {
    return storefiles.size();
  }

  @Override
  public void addCompactionResults(
    Collection<StoreFile> compactedFiles, Collection<StoreFile> results) {
    ArrayList<StoreFile> newStoreFiles = Lists.newArrayList(storefiles);
    newStoreFiles.removeAll(compactedFiles);
    if (!results.isEmpty()) {
      newStoreFiles.addAll(results);
    }
    sortAndSetStoreFiles(newStoreFiles);
  }

  @Override
  public final Iterator<StoreFile> getCandidateFilesForRowKeyBefore(final KeyValue targetKey) {
    return new ArrayList<StoreFile>(Lists.reverse(this.storefiles)).iterator();
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
    if (this.storefiles.isEmpty()) {
      return null;
    }
    return StoreUtils.getLargestFile(this.storefiles).getFileSplitPoint(this.kvComparator);
  }

  @Override
  public final Collection<StoreFile> getFilesForScanOrGet(boolean isGet,
      byte[] startRow, byte[] stopRow) {
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
          expiredStoreFiles = new ArrayList<StoreFile>();
        }
        expiredStoreFiles.add(sf);
      }
    }
    return expiredStoreFiles;
  }

  private void sortAndSetStoreFiles(List<StoreFile> storeFiles) {
    Collections.sort(storeFiles, StoreFile.Comparators.SEQ_ID);
    storefiles = ImmutableList.copyOf(storeFiles);
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
}

