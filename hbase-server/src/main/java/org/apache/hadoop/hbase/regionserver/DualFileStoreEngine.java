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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.DualFileCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.ExploringCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * HBASE-25972 This store engine allows us to store data in two files,
 * one for the latest cells and the other for the rest of the cells (i.e.,
 * older put cells and delete markers).
 */
@InterfaceAudience.Private
public class DualFileStoreEngine extends StoreEngine<DefaultStoreFlusher,
  RatioBasedCompactionPolicy, DualFileCompactor, DefaultStoreFileManager> {
  public static final String DUAL_FILE_STORE_FLUSHER_CLASS_KEY =
    "hbase.hstore.dualfileengine.storeflusher.class";
  public static final String DUAL_FILE_COMPACTOR_CLASS_KEY =
    "hbase.hstore.dualfileengine.compactor.class";
  public static final String DUAL_FILE_COMPACTION_POLICY_CLASS_KEY =
    "hbase.hstore.dualfileengine.compactionpolicy.class";

  public static final Class<? extends DefaultStoreFlusher> DUAL_FILE_STORE_FLUSHER_CLASS =
    DefaultStoreFlusher.class;
  public static final Class<? extends DualFileCompactor> DUAL_FILE_COMPACTOR_CLASS =
    DualFileCompactor.class;
  public static final Class<? extends RatioBasedCompactionPolicy>
    DUAL_FILE_COMPACTION_POLICY_CLASS = ExploringCompactionPolicy.class;
  @Override
  public boolean needsCompaction(List<HStoreFile> filesCompacting) {
    return compactionPolicy.needsCompaction(storeFileManager.getStorefiles(), filesCompacting);
  }

  @Override
  public CompactionContext createCompaction() throws IOException {
    return new DualFileCompactionContext();
  }

  @Override
  protected void createComponents(Configuration conf, HStore store, CellComparator kvComparator)
    throws IOException {
    createCompactor(conf, store, DUAL_FILE_COMPACTOR_CLASS_KEY,
      DUAL_FILE_COMPACTOR_CLASS.getName());
    createCompactionPolicy(conf, store, DUAL_FILE_COMPACTION_POLICY_CLASS_KEY,
      DUAL_FILE_COMPACTION_POLICY_CLASS.getName());
    createStoreFlusher(conf, store, DUAL_FILE_STORE_FLUSHER_CLASS_KEY,
      DUAL_FILE_STORE_FLUSHER_CLASS.getName());
    this.storeFileManager = new DualFileStoreFileManager(kvComparator,
      StoreFileComparators.SEQ_ID_MAX_TIMESTAMP, conf, compactionPolicy.getConf());
  }

  private class DualFileCompactionContext extends CompactionContext {
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
