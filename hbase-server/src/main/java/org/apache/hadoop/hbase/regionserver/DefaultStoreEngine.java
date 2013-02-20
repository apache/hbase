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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;

/**
 * Default StoreEngine creates the default compactor, policy, and store file manager.
 */
@InterfaceAudience.Private
public class DefaultStoreEngine extends StoreEngine {
  public DefaultStoreEngine(Configuration conf, Store store, KVComparator comparator) {
    super(conf, store, comparator);
  }

  @Override
  protected void createComponents(PP<StoreFileManager> storeFileManager,
      PP<CompactionPolicy> compactionPolicy, PP<Compactor> compactor) {
    storeFileManager.set(new DefaultStoreFileManager(this.comparator));
    compactionPolicy.set(new DefaultCompactionPolicy(this.conf, this.store));
    compactor.set(new DefaultCompactor(this.conf, this.store));
  }
}
