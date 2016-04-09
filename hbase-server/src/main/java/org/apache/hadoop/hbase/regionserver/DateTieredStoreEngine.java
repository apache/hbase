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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactor;
import org.apache.hadoop.hbase.security.User;

/**
 * HBASE-15400 This store engine allows us to store data in date tiered layout with exponential sizing
 *   so that the more recent data has more granularity. Time-range scan will perform the best with most recent data.
 *   When data reach maxAge, they are compacted in fixed-size time windows for TTL and archiving.
 *   Please refer to design spec for more details. 
 *   https://docs.google.com/document/d/1_AmlNb2N8Us1xICsTeGDLKIqL6T-oHoRLZ323MG_uy8/edit#heading=h.uk6y5pd3oqgx
 */
@InterfaceAudience.Private
public class DateTieredStoreEngine extends
    StoreEngine<DefaultStoreFlusher, DateTieredCompactionPolicy, DateTieredCompactor, DefaultStoreFileManager> {
  
  @Override
  public boolean needsCompaction(List<StoreFile> filesCompacting) {
    return compactionPolicy.needsCompaction(storeFileManager.getStorefiles(),
      filesCompacting);
  }

  @Override
  public CompactionContext createCompaction() throws IOException {
    return new DateTieredCompactionContext();
  }

  @Override
  protected void createComponents(Configuration conf, Store store, KVComparator kvComparator)
      throws IOException {
    this.compactionPolicy = new DateTieredCompactionPolicy(conf, store);
    this.storeFileManager = new DefaultStoreFileManager(kvComparator, conf,
        compactionPolicy.getConf());
    this.storeFlusher = new DefaultStoreFlusher(conf, store);
    this.compactor = new DateTieredCompactor(conf, store);
  }

  private final class DateTieredCompactionContext extends CompactionContext {

    @Override
    public List<StoreFile> preSelect(List<StoreFile> filesCompacting) {
      return compactionPolicy.preSelectCompactionForCoprocessor(storeFileManager.getStorefiles(),
        filesCompacting);
    }

    @Override
    public boolean select(List<StoreFile> filesCompacting, boolean isUserCompaction,
        boolean mayUseOffPeak, boolean forceMajor) throws IOException {
      request = compactionPolicy.selectCompaction(storeFileManager.getStorefiles(), filesCompacting,
        isUserCompaction, mayUseOffPeak, forceMajor);
      return request != null;
    }

    @Override
    public void forceSelect(CompactionRequest request) {
      if (!(request instanceof DateTieredCompactionRequest)) {
        throw new IllegalArgumentException("DateTieredCompactionRequest is expected. Actual: "
            + request.getClass().getCanonicalName());
      } 
      super.forceSelect(request);
    }

    @Override
    public List<Path> compact(CompactionThroughputController throughputController) throws IOException {
      return compact(throughputController, null);
    }

    public List<Path> compact(CompactionThroughputController throughputController, User user)
        throws IOException {
      if (request instanceof DateTieredCompactionRequest) {
        return compactor.compact(request, ((DateTieredCompactionRequest) request).getBoundaries(), 
          throughputController, user);
      } else {
        throw new IllegalArgumentException("DateTieredCompactionRequest is expected. Actual: "
          + request.getClass().getCanonicalName());
      }
    }
  }
}
