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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * A compaction policy determines how to select files for compaction,
 * how to compact them, and how to generate the compacted files.
 */
@InterfaceAudience.Private
public abstract class CompactionPolicy {
  protected CompactionConfiguration comConf;
  protected StoreConfigInformation storeConfigInfo;

  public CompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    this.storeConfigInfo = storeConfigInfo;
    this.comConf = new CompactionConfiguration(conf, this.storeConfigInfo);
  }

  /**
   * This is called before coprocessor preCompactSelection and should filter the candidates
   * for coprocessor; i.e. exclude the files that definitely cannot be compacted at this time.
   * @param candidateFiles candidate files, ordered from oldest to newest
   * @param filesCompacting files currently compacting
   * @return the list of files that can theoretically be compacted.
   */
  public abstract List<StoreFile> preSelectCompaction(
      List<StoreFile> candidateFiles, final List<StoreFile> filesCompacting);

  /**
   * @param candidateFiles candidate files, ordered from oldest to newest
   * @return subset copy of candidate list that meets compaction criteria
   * @throws java.io.IOException
   */
  public abstract CompactSelection selectCompaction(
    final List<StoreFile> candidateFiles, final boolean isUserCompaction,
    final boolean mayUseOffPeak, final boolean forceMajor) throws IOException;

  /**
   * @param storeFiles Store files in the store.
   * @return The system compaction priority of the store, based on storeFiles.
   *         The priority range is as such - the smaller values are higher priority;
   *         1 is user priority; only very important, blocking compactions should use
   *         values lower than that. With default settings, depending on the number of
   *         store files, the non-blocking priority will be in 2-6 range.
   */
  public abstract int getSystemCompactionPriority(final Collection<StoreFile> storeFiles);

  /**
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  public abstract boolean isMajorCompaction(
    final Collection<StoreFile> filesToCompact) throws IOException;

  /**
   * @param compactionSize Total size of some compaction
   * @return whether this should be a large or small compaction
   */
  public abstract boolean throttleCompaction(long compactionSize);

  /**
   * @param storeFiles Current store files.
   * @param filesCompacting files currently compacting.
   * @return whether a compactionSelection is possible
   */
  public abstract boolean needsCompaction(final Collection<StoreFile> storeFiles,
      final List<StoreFile> filesCompacting);

  /**
   * Inform the policy that some configuration has been change,
   * so cached value should be updated it any.
   */
  public void setConf(Configuration conf) {
    this.comConf = new CompactionConfiguration(conf, this.storeConfigInfo);
  }
}
