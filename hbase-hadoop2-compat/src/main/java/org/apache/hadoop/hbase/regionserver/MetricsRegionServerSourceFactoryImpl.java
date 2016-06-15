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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Factory to create MetricsRegionServerSource when given a  MetricsRegionServerWrapper
 */
@InterfaceAudience.Private
public class MetricsRegionServerSourceFactoryImpl implements MetricsRegionServerSourceFactory {
  public static enum FactoryStorage {
    INSTANCE;
    private Object aggLock = new Object();
    private MetricsRegionAggregateSourceImpl aggImpl;
    private MetricsTableAggregateSourceImpl tblAggImpl;
    private MetricsHeapMemoryManagerSourceImpl heapMemMngImpl;
  }

  private synchronized MetricsRegionAggregateSourceImpl getAggregate() {
    synchronized (FactoryStorage.INSTANCE.aggLock) {
      if (FactoryStorage.INSTANCE.aggImpl == null) {
        FactoryStorage.INSTANCE.aggImpl = new MetricsRegionAggregateSourceImpl();
      }
      return FactoryStorage.INSTANCE.aggImpl;
    }
  }

  @Override
  public synchronized MetricsTableAggregateSourceImpl getTableAggregate() {
    synchronized (FactoryStorage.INSTANCE.aggLock) {
      if (FactoryStorage.INSTANCE.tblAggImpl == null) {
        FactoryStorage.INSTANCE.tblAggImpl = new MetricsTableAggregateSourceImpl();
      }
      return FactoryStorage.INSTANCE.tblAggImpl;
    }
  }

  @Override
  public synchronized MetricsHeapMemoryManagerSource getHeapMemoryManager(
      long globalMemStoreSize, long blockCacheSize) {
    synchronized (FactoryStorage.INSTANCE.aggLock) {
      if (FactoryStorage.INSTANCE.heapMemMngImpl == null) {
        FactoryStorage.INSTANCE.heapMemMngImpl =
            new MetricsHeapMemoryManagerSourceImpl(globalMemStoreSize, blockCacheSize);
      }
      return FactoryStorage.INSTANCE.heapMemMngImpl;
    }
  }

  @Override
  public synchronized MetricsRegionServerSource createServer(MetricsRegionServerWrapper regionServerWrapper) {
    return new MetricsRegionServerSourceImpl(regionServerWrapper);
  }

  @Override
  public MetricsRegionSource createRegion(MetricsRegionWrapper wrapper) {
    return new MetricsRegionSourceImpl(wrapper, getAggregate());
  }

  @Override
  public MetricsTableSource createTable(String table, MetricsTableWrapperAggregate wrapper) {
    return new MetricsTableSourceImpl(table, getTableAggregate(), wrapper);
  }
}
