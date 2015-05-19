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
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * StoreEngine is a factory that can create the objects necessary for HStore to operate.
 * Since not all compaction policies, compactors and store file managers are compatible,
 * they are tied together and replaced together via StoreEngine-s.
 */
@InterfaceAudience.Private
public abstract class StoreEngine<SF extends StoreFlusher,
    CP extends CompactionPolicy, C extends Compactor, SFM extends StoreFileManager> {
  protected SF storeFlusher;
  protected CP compactionPolicy;
  protected C compactor;
  protected SFM storeFileManager;

  /**
   * The name of the configuration parameter that specifies the class of
   * a store engine that is used to manage and compact HBase store files.
   */
  public static final String STORE_ENGINE_CLASS_KEY = "hbase.hstore.engine.class";

  private static final Class<? extends StoreEngine<?, ?, ?, ?>>
    DEFAULT_STORE_ENGINE_CLASS = DefaultStoreEngine.class;

  /**
   * @return Compaction policy to use.
   */
  public CompactionPolicy getCompactionPolicy() {
    return this.compactionPolicy;
  }

  /**
   * @return Compactor to use.
   */
  public Compactor getCompactor() {
    return this.compactor;
  }

  /**
   * @return Store file manager to use.
   */
  public StoreFileManager getStoreFileManager() {
    return this.storeFileManager;
  }

  /**
   * @return Store flusher to use.
   */
  public StoreFlusher getStoreFlusher() {
    return this.storeFlusher;
  }

  /**
   * @param filesCompacting Files currently compacting
   * @return whether a compaction selection is possible
   */
  public abstract boolean needsCompaction(List<StoreFile> filesCompacting);

  /**
   * Creates an instance of a compaction context specific to this engine.
   * Doesn't actually select or start a compaction. See CompactionContext class comment.
   * @return New CompactionContext object.
   */
  public abstract CompactionContext createCompaction() throws IOException;

  /**
   * Create the StoreEngine's components.
   */
  protected abstract void createComponents(
      Configuration conf, Store store, CellComparator kvComparator) throws IOException;

  private void createComponentsOnce(
      Configuration conf, Store store, CellComparator kvComparator) throws IOException {
    assert compactor == null && compactionPolicy == null
        && storeFileManager == null && storeFlusher == null;
    createComponents(conf, store, kvComparator);
    assert compactor != null && compactionPolicy != null
        && storeFileManager != null && storeFlusher != null;
  }

  /**
   * Create the StoreEngine configured for the given Store.
   * @param store The store. An unfortunate dependency needed due to it
   *              being passed to coprocessors via the compactor.
   * @param conf Store configuration.
   * @param kvComparator KVComparator for storeFileManager.
   * @return StoreEngine to use.
   */
  public static StoreEngine<?, ?, ?, ?> create(
      Store store, Configuration conf, CellComparator kvComparator) throws IOException {
    String className = conf.get(STORE_ENGINE_CLASS_KEY, DEFAULT_STORE_ENGINE_CLASS.getName());
    try {
      StoreEngine<?,?,?,?> se = ReflectionUtils.instantiateWithCustomCtor(
          className, new Class[] { }, new Object[] { });
      se.createComponentsOnce(conf, store, kvComparator);
      return se;
    } catch (Exception e) {
      throw new IOException("Unable to load configured store engine '" + className + "'", e);
    }
  }
}
