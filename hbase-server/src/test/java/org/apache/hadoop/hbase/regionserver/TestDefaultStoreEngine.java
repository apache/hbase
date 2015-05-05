/*
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({RegionServerTests.class, SmallTests.class})
public class TestDefaultStoreEngine {
  public static class DummyStoreFlusher extends DefaultStoreFlusher {
    public DummyStoreFlusher(Configuration conf, Store store) {
      super(conf, store);
    }
  }

  public static class DummyCompactor extends DefaultCompactor {
    public DummyCompactor(Configuration conf, Store store) {
      super(conf, store);
    }
  }

  public static class DummyCompactionPolicy extends RatioBasedCompactionPolicy {
    public DummyCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
      super(conf, storeConfigInfo);
    }
  }

  @Test
  public void testCustomParts() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(DefaultStoreEngine.DEFAULT_COMPACTOR_CLASS_KEY, DummyCompactor.class.getName());
    conf.set(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
        DummyCompactionPolicy.class.getName());
    conf.set(DefaultStoreEngine.DEFAULT_STORE_FLUSHER_CLASS_KEY,
        DummyStoreFlusher.class.getName());
    Store mockStore = Mockito.mock(Store.class);
    StoreEngine<?, ?, ?, ?> se = StoreEngine.create(mockStore, conf, CellComparator.COMPARATOR);
    Assert.assertTrue(se instanceof DefaultStoreEngine);
    Assert.assertTrue(se.getCompactionPolicy() instanceof DummyCompactionPolicy);
    Assert.assertTrue(se.getStoreFlusher() instanceof DummyStoreFlusher);
    Assert.assertTrue(se.getCompactor() instanceof DummyCompactor);
  }
}
