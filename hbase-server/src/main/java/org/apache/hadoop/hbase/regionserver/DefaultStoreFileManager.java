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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Default implementation of StoreFileManager. Not thread-safe.
 */
@InterfaceAudience.Private
class DefaultStoreFileManager implements StoreFileManager {
  static final Log LOG = LogFactory.getLog(DefaultStoreFileManager.class);

  private final KVComparator kvComparator;
  private final Configuration conf;

  /**
   * List of store files inside this store. This is an immutable list that
   * is atomically replaced when its contents change.
   */
  private volatile ImmutableList<StoreFile> storefiles = null;

  public DefaultStoreFileManager(KVComparator kvComparator, Configuration conf) {
    this.kvComparator = kvComparator;
    this.conf = conf;
  }

  @Override
  public void loadFiles(List<StoreFile> storeFiles) {
    sortAndSetStoreFiles(storeFiles);
  }

  @Override
  public final Collection<StoreFile> getStorefiles() {
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
      Iterator<StoreFile> candidateFiles, final KeyValue targetKey, final KeyValue candidate) {
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
    int blockingFileCount = conf.getInt(
        HStore.BLOCKING_STOREFILES_KEY, HStore.DEFAULT_BLOCKING_STOREFILE_COUNT);
    int priority = blockingFileCount - storefiles.size();
    return (priority == HStore.PRIORITY_USER) ? priority + 1 : priority;
  }

  private void sortAndSetStoreFiles(List<StoreFile> storeFiles) {
    Collections.sort(storeFiles, StoreFile.Comparators.SEQ_ID);
    storefiles = ImmutableList.copyOf(storeFiles);
  }

}

