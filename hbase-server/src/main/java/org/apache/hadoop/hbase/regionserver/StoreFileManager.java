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

import com.google.common.collect.ImmutableCollection;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Manages the store files and basic metadata about that that determines the logical structure
 * (e.g. what files to return for scan, how to determine split point, and such).
 * Does NOT affect the physical structure of files in HDFS.
 * Example alternative structures - the default list of files by seqNum; levelDB one sorted
 * by level and seqNum.
 *
 * Implementations are assumed to be not thread safe.
 */
@InterfaceAudience.Private
public interface StoreFileManager {
  /**
   * Loads the initial store files into empty StoreFileManager.
   * @param storeFiles The files to load.
   */
  void loadFiles(List<StoreFile> storeFiles);

  /**
   * Adds new files, either for from MemStore flush or bulk insert, into the structure.
   * @param sfs New store files.
   */
  void insertNewFiles(Collection<StoreFile> sfs) throws IOException;

  /**
   * Adds compaction results into the structure.
   * @param compactedFiles The input files for the compaction.
   * @param results The resulting files for the compaction.
   */
  void addCompactionResults(
      Collection<StoreFile> compactedFiles, Collection<StoreFile> results) throws IOException;

  /**
   * Clears all the files currently in use and returns them.
   * @return The files previously in use.
   */
  ImmutableCollection<StoreFile> clearFiles();

  /**
   * Gets the snapshot of the store files currently in use. Can be used for things like metrics
   * and checks; should not assume anything about relations between store files in the list.
   * @return The list of StoreFiles.
   */
  Collection<StoreFile> getStorefiles();

  /**
   * Returns the number of files currently in use.
   * @return The number of files.
   */
  int getStorefileCount();

  /**
   * Gets the store files to scan for a Scan or Get request.
   * @param isGet Whether it's a get.
   * @param startRow Start row of the request.
   * @param stopRow Stop row of the request.
   * @return The list of files that are to be read for this request.
   */
  Collection<StoreFile> getFilesForScanOrGet(
    boolean isGet, byte[] startRow, byte[] stopRow
  );

  /**
   * Gets initial, full list of candidate store files to check for row-key-before.
   * @param targetKey The key that is the basis of the search.
   * @return The files that may have the key less than or equal to targetKey, in reverse
   *         order of new-ness, and preference for target key.
   */
  Iterator<StoreFile> getCandidateFilesForRowKeyBefore(
    KeyValue targetKey
  );

  /**
   * Updates the candidate list for finding row key before. Based on the list of candidates
   * remaining to check from getCandidateFilesForRowKeyBefore, targetKey and current candidate,
   * may trim and reorder the list to remove the files where a better candidate cannot be found.
   * @param candidateFiles The candidate files not yet checked for better candidates - return
   *                       value from {@link #getCandidateFilesForRowKeyBefore(KeyValue)},
   *                       with some files already removed.
   * @param targetKey The key to search for.
   * @param candidate The current best candidate found.
   * @return The list to replace candidateFiles.
   */
  Iterator<StoreFile> updateCandidateFilesForRowKeyBefore(
    Iterator<StoreFile> candidateFiles, KeyValue targetKey, KeyValue candidate
  );


  /**
   * Gets the split point for the split of this set of store files (approx. middle).
   * @return The mid-point, or null if no split is possible.
   * @throws IOException
   */
  byte[] getSplitPoint() throws IOException;

  /**
   * @return The store compaction priority.
   */
  int getStoreCompactionPriority();

  /**
   * @param maxTs Maximum expired timestamp.
   * @param filesCompacting Files that are currently compacting.
   * @return The files which don't have any necessary data according to TTL and other criteria.
   */
  Collection<StoreFile> getUnneededFiles(long maxTs, List<StoreFile> filesCompacting);

  /**
   * @return the compaction pressure used for compaction throughput tuning.
   * @see Store#getCompactionPressure()
   */
  double getCompactionPressure();

  /**
   * @return the comparator used to sort storefiles. Usually, the
   *         {@link StoreFile#getMaxSequenceId()} is the first priority.
   */
  Comparator<StoreFile> getStoreFileComparator();
}
