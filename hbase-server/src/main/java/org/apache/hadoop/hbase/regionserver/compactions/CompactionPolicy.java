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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A compaction policy determines how to select files for compaction,
 * how to compact them, and how to generate the compacted files.
 */
@InterfaceAudience.Private
public abstract class CompactionPolicy extends Configured {

  /**
   * The name of the configuration parameter that specifies
   * the class of a compaction policy that is used to compact
   * HBase store files.
   */
  public static final String COMPACTION_POLICY_KEY =
    "hbase.hstore.compaction.policy";

  private static final Class<? extends CompactionPolicy>
    DEFAULT_COMPACTION_POLICY_CLASS = DefaultCompactionPolicy.class;

  CompactionConfiguration comConf;
  Compactor compactor;
  HStore store;

  /**
   * @param candidateFiles candidate files, ordered from oldest to newest
   * @return subset copy of candidate list that meets compaction criteria
   * @throws java.io.IOException
   */
  public abstract CompactSelection selectCompaction(
    final List<StoreFile> candidateFiles, final boolean isUserCompaction,
    final boolean forceMajor) throws IOException;

  /**
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  public abstract boolean isMajorCompaction(
    final List<StoreFile> filesToCompact) throws IOException;

  /**
   * @param compactionSize Total size of some compaction
   * @return whether this should be a large or small compaction
   */
  public abstract boolean throttleCompaction(long compactionSize);

  /**
   * @param numCandidates Number of candidate store files
   * @return whether a compactionSelection is possible
   */
  public abstract boolean needsCompaction(int numCandidates);

  /**
   * Inform the policy that some configuration has been change,
   * so cached value should be updated it any.
   */
  public void updateConfiguration() {
    if (getConf() != null && store != null) {
      comConf = new CompactionConfiguration(getConf(), store);
    }
  }

  /**
   * Get the compactor for this policy
   * @return the compactor for this policy
   */
  public Compactor getCompactor() {
    return compactor;
  }

  /**
   * Set the new configuration
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    updateConfiguration();
  }

  /**
   * Upon construction, this method will be called with the HStore
   * to be governed. It will be called once and only once.
   */
  protected void configureForStore(HStore store) {
    this.store = store;
    updateConfiguration();
  }

  /**
   * Create the CompactionPolicy configured for the given HStore.
   * @param store
   * @param conf
   * @return a CompactionPolicy
   * @throws IOException
   */
  public static CompactionPolicy create(HStore store,
      Configuration conf) throws IOException {
    Class<? extends CompactionPolicy> clazz =
      getCompactionPolicyClass(store.getFamily(), conf);
    CompactionPolicy policy = ReflectionUtils.newInstance(clazz, conf);
    policy.configureForStore(store);
    return policy;
  }

  static Class<? extends CompactionPolicy> getCompactionPolicyClass(
      HColumnDescriptor family, Configuration conf) throws IOException {
    String className = conf.get(COMPACTION_POLICY_KEY,
      DEFAULT_COMPACTION_POLICY_CLASS.getName());

    try {
      Class<? extends CompactionPolicy> clazz =
        Class.forName(className).asSubclass(CompactionPolicy.class);
      return clazz;
    } catch (Exception  e) {
      throw new IOException(
        "Unable to load configured region compaction policy '"
        + className + "' for column '" + family.getNameAsString()
        + "'", e);
    }
  }
}
