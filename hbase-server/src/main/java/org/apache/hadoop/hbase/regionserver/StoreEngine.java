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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * StoreEngine is a factory that can create the objects necessary for HStore to operate. Since not
 * all compaction policies, compactors and store file managers are compatible, they are tied
 * together and replaced together via StoreEngine-s.
 * <p/>
 * It is a bit confusing that we have a StoreFileManager(SFM) and then a StoreFileTracker(SFT). As
 * its name says, SFT is used to track the store files list. The reason why we have a SFT beside SFM
 * is that, when introducing stripe compaction, we introduced the StoreEngine and also the SFM, but
 * actually, the SFM here is not a general 'Manager', it is only designed to manage the in memory
 * 'stripes', so we can select different store files when scanning or compacting. The 'tracking' of
 * store files is actually done in {@link org.apache.hadoop.hbase.regionserver.HRegionFileSystem}
 * and {@link HStore} before we have SFT. And since SFM is designed to only holds in memory states,
 * we will hold write lock when updating it, the lock is also used to protect the normal read/write
 * requests. This means we'd better not add IO operations to SFM. And also, no matter what the in
 * memory state is, stripe or not, it does not effect how we track the store files. So consider all
 * these facts, here we introduce a separated SFT to track the store files.
 */
@InterfaceAudience.Private
public abstract class StoreEngine<SF extends StoreFlusher,
    CP extends CompactionPolicy, C extends Compactor, SFM extends StoreFileManager,
    SFT extends StoreFileTracker> {
  protected SF storeFlusher;
  protected CP compactionPolicy;
  protected C compactor;
  protected SFM storeFileManager;
  protected SFT storeFileTracker;

  /**
   * The name of the configuration parameter that specifies the class of
   * a store engine that is used to manage and compact HBase store files.
   */
  public static final String STORE_ENGINE_CLASS_KEY = "hbase.hstore.engine.class";

  private static final Class<? extends StoreEngine<?, ?, ?, ?, ?>>
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
   * Returns the store file tracker to use
   */
  public StoreFileTracker getStoreFileTracker() {
    return storeFileTracker;
  }

  protected final StoreFileTracker createStoreFileTracker(HStore store) {
    return StoreFileTrackerFactory.create(store.conf, store.getRegionInfo().getTable(),
      store.isPrimaryReplicaStore(), store.getStoreContext());
  }

  /**
   * @param filesCompacting Files currently compacting
   * @return whether a compaction selection is possible
   */
  public abstract boolean needsCompaction(List<HStoreFile> filesCompacting);

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
      Configuration conf, HStore store, CellComparator cellComparator) throws IOException;

  private void createComponentsOnce(Configuration conf, HStore store, CellComparator cellComparator)
    throws IOException {
    assert compactor == null && compactionPolicy == null && storeFileManager == null &&
      storeFlusher == null && storeFileTracker == null;
    createComponents(conf, store, cellComparator);
    assert compactor != null && compactionPolicy != null && storeFileManager != null &&
      storeFlusher != null && storeFileTracker != null;
  }

  /**
   * Create the StoreEngine configured for the given Store.
   * @param store The store. An unfortunate dependency needed due to it
   *              being passed to coprocessors via the compactor.
   * @param conf Store configuration.
   * @param cellComparator CellComparator for storeFileManager.
   * @return StoreEngine to use.
   */
  public static StoreEngine<?, ?, ?, ?, ?> create(HStore store, Configuration conf,
    CellComparator cellComparator) throws IOException {
    String className = conf.get(STORE_ENGINE_CLASS_KEY, DEFAULT_STORE_ENGINE_CLASS.getName());
    try {
      StoreEngine<?, ?, ?, ?, ?> se =
        ReflectionUtils.instantiateWithCustomCtor(className, new Class[] {}, new Object[] {});
      se.createComponentsOnce(conf, store, cellComparator);
      return se;
    } catch (Exception e) {
      throw new IOException("Unable to load configured store engine '" + className + "'", e);
    }
  }
}
