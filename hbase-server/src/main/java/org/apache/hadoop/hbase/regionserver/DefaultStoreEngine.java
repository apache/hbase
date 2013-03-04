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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;

/**
 * Default StoreEngine creates the default compactor, policy, and store file manager, or
 * their derivatives.
 */
@InterfaceAudience.Private
public class DefaultStoreEngine extends StoreEngine<
  DefaultCompactionPolicy, DefaultCompactor, DefaultStoreFileManager> {

  public DefaultStoreEngine(Configuration conf, Store store, KVComparator comparator) {
    super(conf, store, comparator);
  }

  @Override
  protected void createComponents() {
    storeFileManager = new DefaultStoreFileManager(this.comparator, this.conf);

    // TODO: compactor and policy may be separately pluggable, but must derive from default ones.
    compactor = new DefaultCompactor(this.conf, this.store);
    compactionPolicy = new DefaultCompactionPolicy(this.conf, this.store/*as StoreConfigInfo*/);
  }

  @Override
  protected CompactionContext createCompactionContext() {
    return new DefaultCompactionContext();
  }

  private class DefaultCompactionContext extends CompactionContext {
    @Override
    public boolean select(List<StoreFile> filesCompacting, boolean isUserCompaction,
        boolean mayUseOffPeak, boolean forceMajor) throws IOException {
      request = compactionPolicy.selectCompaction(storeFileManager.getStorefiles(),
          filesCompacting, isUserCompaction, mayUseOffPeak, forceMajor);
      return request != null;
    }

    @Override
    public List<Path> compact() throws IOException {
      return compactor.compact(request);
    }

    @Override
    public List<StoreFile> preSelect(List<StoreFile> filesCompacting) {
      return compactionPolicy.preSelectCompactionForCoprocessor(
          storeFileManager.getStorefiles(), filesCompacting);
    }
  }
}
