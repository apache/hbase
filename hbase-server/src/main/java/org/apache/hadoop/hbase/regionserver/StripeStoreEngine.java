/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactor;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;

import com.google.common.base.Preconditions;

/**
 * The storage engine that implements the stripe-based store/compaction scheme.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class StripeStoreEngine extends StoreEngine<StripeStoreFlusher,
  StripeCompactionPolicy, StripeCompactor, StripeStoreFileManager> {
  private static final Log LOG = LogFactory.getLog(StripeStoreEngine.class);
  private StripeStoreConfig config;

  @Override
  public boolean needsCompaction(List<StoreFile> filesCompacting) {
    return this.compactionPolicy.needsCompactions(this.storeFileManager, filesCompacting);
  }

  @Override
  public CompactionContext createCompaction() {
    return new StripeCompaction();
  }

  @Override
  protected void createComponents(
      Configuration conf, Store store, KVComparator comparator) throws IOException {
    this.config = new StripeStoreConfig(conf, store);
    this.compactionPolicy = new StripeCompactionPolicy(conf, store, config);
    this.storeFileManager = new StripeStoreFileManager(comparator, conf, this.config);
    this.storeFlusher = new StripeStoreFlusher(
      conf, store, this.compactionPolicy, this.storeFileManager);
    this.compactor = new StripeCompactor(conf, store);
  }

  /**
   * Represents one instance of stripe compaction, with the necessary context and flow.
   */
  private class StripeCompaction extends CompactionContext {
    private StripeCompactionPolicy.StripeCompactionRequest stripeRequest = null;

    @Override
    public List<StoreFile> preSelect(List<StoreFile> filesCompacting) {
      return compactionPolicy.preSelectFilesForCoprocessor(storeFileManager, filesCompacting);
    }

    @Override
    public boolean select(List<StoreFile> filesCompacting, boolean isUserCompaction,
        boolean mayUseOffPeak, boolean forceMajor) throws IOException {
      this.stripeRequest = compactionPolicy.selectCompaction(
          storeFileManager, filesCompacting, mayUseOffPeak);
      this.request = (this.stripeRequest == null)
          ? new CompactionRequest(new ArrayList<StoreFile>()) : this.stripeRequest.getRequest();
      return this.stripeRequest != null;
    }

    @Override
    public void forceSelect(CompactionRequest request) {
      super.forceSelect(request);
      if (this.stripeRequest != null) {
        this.stripeRequest.setRequest(this.request);
      } else {
        LOG.warn("Stripe store is forced to take an arbitrary file list and compact it.");
        this.stripeRequest = compactionPolicy.createEmptyRequest(storeFileManager, this.request);
      }
    }

    @Override
    public List<Path> compact(CompactionThroughputController throughputController)
        throws IOException {
      Preconditions.checkArgument(this.stripeRequest != null, "Cannot compact without selection");
      return this.stripeRequest.execute(compactor, throughputController);
    }
  }
}
