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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * StoreEngine is a factory that can create the objects necessary for HStore to operate.
 * Since not all compaction policies, compactors and store file managers are compatible,
 * they are tied together and replaced together via StoreEngine-s.
 */
@InterfaceAudience.Private
public abstract class StoreEngine {
  protected final Store store;
  protected final Configuration conf;
  protected final KVComparator comparator;

  private final PP<CompactionPolicy> compactionPolicy = new PP<CompactionPolicy>();
  private final PP<Compactor> compactor = new PP<Compactor>();
  private final PP<StoreFileManager> storeFileManager = new PP<StoreFileManager>();
  private boolean isInitialized = false;

  /**
   * The name of the configuration parameter that specifies the class of
   * a store engine that is used to manage and compact HBase store files.
   */
  public static final String STORE_ENGINE_CLASS_KEY = "hbase.hstore.engine.class";

  private static final Class<? extends StoreEngine>
    DEFAULT_STORE_ENGINE_CLASS = DefaultStoreEngine.class;

  /**
   * @return Compaction policy to use.
   */
  public CompactionPolicy getCompactionPolicy() {
    createComponentsOnce();
    return this.compactionPolicy.get();
  }

  /**
   * @return Compactor to use.
   */
  public Compactor getCompactor() {
    createComponentsOnce();
    return this.compactor.get();
  }

  /**
   * @return Store file manager to use.
   */
  public StoreFileManager getStoreFileManager() {
    createComponentsOnce();
    return this.storeFileManager.get();
  }

  protected StoreEngine(Configuration conf, Store store, KVComparator comparator) {
    this.store = store;
    this.conf = conf;
    this.comparator = comparator;
  }

  /**
   * Create the StoreEngine's components.
   * @param storeFileManager out parameter for StoreFileManager.
   * @param compactionPolicy out parameter for CompactionPolicy.
   * @param compactor out parameter for Compactor.
   */
  protected abstract void createComponents(PP<StoreFileManager> storeFileManager,
      PP<CompactionPolicy> compactionPolicy, PP<Compactor> compactor);

  private void createComponentsOnce() {
    if (isInitialized) return;
    createComponents(storeFileManager, compactionPolicy, compactor);
    isInitialized = true;
  }

  /**
   * Create the StoreEngine configured for the given Store.
   * @param store The store. An unfortunate dependency needed due to it
   *              being passed to coprocessors via the compactor.
   * @param conf Store configuration.
   * @param kvComparator KVComparator for storeFileManager.
   * @return StoreEngine to use.
   */
  public static StoreEngine create(Store store, Configuration conf, KVComparator kvComparator)
      throws IOException {
    String className = conf.get(STORE_ENGINE_CLASS_KEY, DEFAULT_STORE_ENGINE_CLASS.getName());
    try {
      return ReflectionUtils.instantiateWithCustomCtor(className,
          new Class[] { Configuration.class, Store.class, KVComparator.class },
          new Object[] { conf, store, kvComparator });
    } catch (Exception e) {
      throw new IOException("Unable to load configured store engine '" + className + "'", e);
    }
  }

  /**
   * To allow StoreEngine-s to have custom dependencies between 3 components, we want to create
   * them in one place. To return multiple, simulate C++ pointer to pointers/C# out params.
   */
  protected static class PP<T> {
    private T t = null;
    public void set(T t) {
      this.t = t;
    }
    public T get() {
      return this.t;
    }
  }
}
