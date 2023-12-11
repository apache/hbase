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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFileManager;
import org.apache.hadoop.hbase.regionserver.DefaultStoreFlusher;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StoreFileComparators;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTION_ENABLE_DUAL_FILE_WRITER_KEY;
import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS;
import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY;

/**
 * MobStoreEngine creates the mob specific compactor, and store flusher.
 */
@InterfaceAudience.Private
public class MobStoreEngine extends StoreEngine<DefaultStoreFlusher, RatioBasedCompactionPolicy,
  DefaultMobStoreCompactor, DefaultStoreFileManager> {
  public final static String MOB_COMPACTOR_CLASS_KEY = "hbase.hstore.mobengine.compactor.class";

  @Override
  public boolean needsCompaction(List<HStoreFile> filesCompacting) {
    return compactionPolicy.needsCompaction(this.storeFileManager.getStorefiles(), filesCompacting);
  }

  protected void createStoreFlusher(Configuration conf, HStore store) throws IOException {
    // When using MOB, we use DefaultMobStoreFlusher always
    // Just use the compactor and compaction policy as that in DefaultStoreEngine. We can have MOB
    // specific compactor and policy when that is implemented.
    storeFlusher = new DefaultMobStoreFlusher(conf, store);
  }

  /**
   * Creates the DefaultMobCompactor.
   */

  protected void createCompactor(Configuration conf, HStore store) throws IOException {
    createCompactor(conf, store, MOB_COMPACTOR_CLASS_KEY, DefaultMobStoreCompactor.class.getName());
  }
  @Override
  protected void createComponents(Configuration conf, HStore store, CellComparator kvComparator)
    throws IOException {
    createCompactor(conf, store);
    createCompactionPolicy(conf, store);
    createStoreFlusher(conf, store);
    boolean enableDualFileWriter = conf.getBoolean(DEFAULT_COMPACTION_ENABLE_DUAL_FILE_WRITER_KEY,
      false);
    storeFileManager = new DefaultStoreFileManager(kvComparator, StoreFileComparators.SEQ_ID, conf,
      compactionPolicy.getConf());
  }

  protected void createCompactionPolicy(Configuration conf, HStore store) throws IOException {
    createCompactionPolicy(conf, store, DEFAULT_COMPACTION_POLICY_CLASS_KEY,
      DEFAULT_COMPACTION_POLICY_CLASS.getName());
  }


  @Override
  public CompactionContext createCompaction() {
    return new MobStoreEngine.DefaultCompactionContext();
  }

  private class DefaultCompactionContext extends CompactionContext {
    @Override
    public boolean select(List<HStoreFile> filesCompacting, boolean isUserCompaction,
      boolean mayUseOffPeak, boolean forceMajor) throws IOException {
      request = compactionPolicy.selectCompaction(storeFileManager.getStorefiles(), filesCompacting,
        isUserCompaction, mayUseOffPeak, forceMajor);
      return request != null;
    }

    @Override
    public List<Path> compact(ThroughputController throughputController, User user)
      throws IOException {
      return compactor.compact(request, throughputController, user);
    }

    @Override
    public List<HStoreFile> preSelect(List<HStoreFile> filesCompacting) {
      return compactionPolicy.preSelectCompactionForCoprocessor(storeFileManager.getStorefiles(),
        filesCompacting);
    }
  }

}
