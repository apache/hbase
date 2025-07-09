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

import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CustomDateTieredCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.CustomTieredCompactor;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Extension of {@link DateTieredStoreEngine} that uses a pluggable value provider for extracting
 * the value to be used for comparison in this tiered compaction. Differently from the existing Date
 * Tiered Compaction, this doesn't yield multiple tiers or files, but rather provides two tiers
 * based on a configurable “cut-off” age. All rows with the cell tiering value older than this
 * “cut-off” age would be placed together in an “old” tier, whilst younger rows would go to a
 * separate, “young” tier file.
 */
@InterfaceAudience.Private
public class CustomTieredStoreEngine extends DateTieredStoreEngine {

  @Override
  protected void createComponents(Configuration conf, HStore store, CellComparator kvComparator)
    throws IOException {
    CompoundConfiguration config = new CompoundConfiguration();
    config.add(conf);
    config.add(store.conf);
    config.set(DEFAULT_COMPACTION_POLICY_CLASS_KEY,
      CustomDateTieredCompactionPolicy.class.getName());
    createCompactionPolicy(config, store);
    this.storeFileManager = new DefaultStoreFileManager(kvComparator,
      StoreFileComparators.SEQ_ID_MAX_TIMESTAMP, config, compactionPolicy.getConf());
    this.storeFlusher = new DefaultStoreFlusher(config, store);
    this.compactor = new CustomTieredCompactor(config, store);
  }

}
