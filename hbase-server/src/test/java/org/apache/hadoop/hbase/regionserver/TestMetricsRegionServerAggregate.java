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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ SmallTests.class, RegionServerTests.class })
public class TestMetricsRegionServerAggregate {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetricsRegionServerAggregate.class);

  @Test
  public void test() {
    AtomicInteger retVal = new AtomicInteger(0);
    Answer defaultAnswer = invocation -> {
      Class<?> returnType = invocation.getMethod().getReturnType();

      if (returnType.equals(Integer.TYPE) || returnType.equals(Integer.class)) {
        return retVal.get();
      } else if (returnType.equals(Long.TYPE) || returnType.equals(Long.class)) {
        return (long) retVal.get();
      }
      return Mockito.RETURNS_DEFAULTS.answer(invocation);
    };

    ServerName serverName = mock(ServerName.class);
    when(serverName.getHostname()).thenReturn("foo");
    WALFactory walFactory = mock(WALFactory.class);
    RpcServerInterface rpcServer = mock(RpcServerInterface.class);
    AtomicInteger storeFileCount = new AtomicInteger(1);
    HRegion regionOne = getMockedRegion(defaultAnswer, "a", "foo", true, storeFileCount);
    HRegion regionTwo = getMockedRegion(defaultAnswer, "b", "bar", true, storeFileCount);
    HRegion regionThree = getMockedRegion(defaultAnswer, "c", "foo", false, storeFileCount);
    HRegion regionFour = getMockedRegion(defaultAnswer, "d", "bar", false, storeFileCount);
    List<HRegion> regions = Lists.newArrayList(regionOne, regionTwo, regionThree, regionFour);

    int numStoresPerRegion = 2;
    for (HRegion region : regions) {
      // if adding more stores, update numStoresPerRegion so that tests below continue working
      assertEquals(numStoresPerRegion, region.getStores().size());
    }

    HRegionServer regionServer = mock(HRegionServer.class, defaultAnswer);
    when(regionServer.getWalFactory()).thenReturn(walFactory);
    when(regionServer.getOnlineRegionsLocalContext()).thenReturn(regions);
    when(regionServer.getServerName()).thenReturn(serverName);
    Configuration conf = HBaseConfiguration.create();
    int metricsPeriodSec = 600;
    // set a very long period so that it doesn't actually run during our very quick test
    conf.setLong(HConstants.REGIONSERVER_METRICS_PERIOD, metricsPeriodSec * 1000);
    when(regionServer.getConfiguration()).thenReturn(conf);
    when(regionServer.getRpcServer()).thenReturn(rpcServer);

    MetricsRegionServerWrapperImpl wrapper = new MetricsRegionServerWrapperImpl(regionServer);

    // we need to control the edge because rate calculations expect a
    // stable interval relative to the configured period
    ManualEnvironmentEdge edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);

    try {
      for (int i = 1; i <= 10; i++) {
        edge.incValue(wrapper.getPeriod());
        retVal.incrementAndGet();
        wrapper.forceRecompute();

        int numRegions = regions.size();
        int totalStores = numRegions * numStoresPerRegion;

        // there are N regions, and each has M stores. everything gets aggregated, so
        // multiply expected values accordingly
        int expectedForRegions = retVal.get() * numRegions;
        int expectedForStores = retVal.get() * totalStores;

        assertEquals(totalStores, wrapper.getNumStores());
        assertEquals(expectedForStores, wrapper.getFlushedCellsCount());
        assertEquals(expectedForStores, wrapper.getCompactedCellsCount());
        assertEquals(expectedForStores, wrapper.getMajorCompactedCellsCount());
        assertEquals(expectedForStores, wrapper.getFlushedCellsSize());
        assertEquals(expectedForStores, wrapper.getCompactedCellsSize());
        assertEquals(expectedForStores, wrapper.getMajorCompactedCellsSize());
        assertEquals(expectedForRegions, wrapper.getCellsCountCompactedFromMob());
        assertEquals(expectedForRegions, wrapper.getCellsCountCompactedToMob());
        assertEquals(expectedForRegions, wrapper.getCellsSizeCompactedFromMob());
        assertEquals(expectedForRegions, wrapper.getCellsSizeCompactedToMob());
        assertEquals(expectedForRegions, wrapper.getMobFlushCount());
        assertEquals(expectedForRegions, wrapper.getMobFlushedCellsCount());
        assertEquals(expectedForRegions, wrapper.getMobFlushedCellsSize());
        assertEquals(expectedForRegions, wrapper.getMobScanCellsCount());
        assertEquals(expectedForRegions, wrapper.getMobScanCellsSize());
        assertEquals(expectedForRegions, wrapper.getCheckAndMutateChecksFailed());
        assertEquals(expectedForRegions, wrapper.getCheckAndMutateChecksPassed());
        assertEquals(expectedForStores, wrapper.getStoreFileIndexSize());
        assertEquals(expectedForStores, wrapper.getTotalStaticIndexSize());
        assertEquals(expectedForStores, wrapper.getTotalStaticBloomSize());
        assertEquals(expectedForStores, wrapper.getBloomFilterRequestsCount());
        assertEquals(expectedForStores, wrapper.getBloomFilterNegativeResultsCount());
        assertEquals(expectedForStores, wrapper.getBloomFilterEligibleRequestsCount());
        assertEquals(expectedForRegions, wrapper.getNumMutationsWithoutWAL());
        assertEquals(expectedForRegions, wrapper.getDataInMemoryWithoutWAL());
        assertEquals(expectedForRegions, wrapper.getAverageRegionSize());
        assertEquals(expectedForRegions, wrapper.getBlockedRequestsCount());
        assertEquals(expectedForStores, wrapper.getNumReferenceFiles());
        assertEquals(expectedForStores, wrapper.getMemStoreSize());
        assertEquals(expectedForStores, wrapper.getOnHeapMemStoreSize());
        assertEquals(expectedForStores, wrapper.getOffHeapMemStoreSize());
        assertEquals(expectedForStores, wrapper.getStoreFileSize());
        assertEquals(expectedForRegions, wrapper.getReadRequestsCount());
        assertEquals(expectedForRegions, wrapper.getCpRequestsCount());
        assertEquals(expectedForRegions, wrapper.getFilteredReadRequestsCount());
        assertEquals(expectedForRegions, wrapper.getWriteRequestsCount());
        assertEquals(expectedForRegions * 2, wrapper.getTotalRowActionRequestCount());

        // If we have N regions, each with M stores. That's N*M stores in total. In creating those
        // stores, we increment the number and age of storefiles for each one. So the first
        // store has 1 file of 1 age, then 2 files of 2 age, etc.
        // formula for 1+2+3..+n
        assertEquals((totalStores * (totalStores + 1)) / 2, wrapper.getNumStoreFiles());
        assertEquals(totalStores, wrapper.getMaxStoreFiles());
        assertEquals(totalStores, wrapper.getMaxStoreFileAge());
        assertEquals(1, wrapper.getMinStoreFileAge());
        assertEquals(totalStores / 2, wrapper.getAvgStoreFileAge());

        // there are four regions, two are primary and the other two secondary
        // for each type, one region has 100% locality, the other has 0%.
        // this just proves we correctly aggregate for each
        assertEquals(50.0, wrapper.getPercentFileLocal(), 0.0001);
        assertEquals(50.0, wrapper.getPercentFileLocalSecondaryRegions(), 0.0001);

        // readRequestCount and writeRequestCount are tracking the value of i, which increases by 1
        // each interval. There are N regions, so the delta each interval is N*i=N. So the rate is
        // simply N / period.
        assertEquals((double) numRegions / metricsPeriodSec, wrapper.getReadRequestsRatePerSecond(),
          0.0001);
        assertEquals((double) numRegions / metricsPeriodSec,
          wrapper.getWriteRequestsRatePerSecond(), 0.0001);
        // total of above, so multiply by 2
        assertEquals((double) numRegions / metricsPeriodSec * 2, wrapper.getRequestsPerSecond(),
          0.0001);
        // Similar logic to above, except there are M totalStores and each one is of
        // size tracking i. So the rate is just M / period.
        assertEquals((double) totalStores / metricsPeriodSec, wrapper.getStoreFileSizeGrowthRate(),
          0.0001);
      }
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  @Test
  public void testWalMetricsForRegionServer() throws InterruptedException {
    long numLogFiles = 10;
    long logFileSize = 10240;
    String hostname = "foo";
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(HConstants.REGIONSERVER_METRICS_PERIOD, 1000);

    HRegionServer regionServer = mock(HRegionServer.class);
    when(regionServer.getConfiguration()).thenReturn(conf);
    RpcServerInterface rpcServer = mock(RpcServerInterface.class);
    when(regionServer.getRpcServer()).thenReturn(rpcServer);
    WALFactory walFactory = mock(WALFactory.class);
    WALProvider walProvider = mock(WALProvider.class);
    when(walProvider.getNumLogFiles()).thenReturn(numLogFiles);
    when(walProvider.getLogFileSize()).thenReturn(logFileSize);
    List<WALProvider> providers = new ArrayList<>();
    providers.add(walProvider);
    when(walFactory.getAllWALProviders()).thenReturn(providers);
    when(regionServer.getWalFactory()).thenReturn(walFactory);
    ServerName serverName = mock(ServerName.class);
    when(serverName.getHostname()).thenReturn(hostname);
    when(regionServer.getServerName()).thenReturn(serverName);

    MetricsRegionServerWrapperImpl wrapper = new MetricsRegionServerWrapperImpl(regionServer);
    MetricsRegionServerWrapperImpl.RegionServerMetricsWrapperRunnable runnable =
      wrapper.new RegionServerMetricsWrapperRunnable();
    runnable.run();
    assertEquals(numLogFiles, wrapper.getNumWALFiles());
    assertEquals(logFileSize, wrapper.getWALFileSize());

    runnable.run();
    assertEquals(numLogFiles, wrapper.getNumWALFiles());
    assertEquals(logFileSize, wrapper.getWALFileSize());
  }

  private HRegion getMockedRegion(Answer defaultAnswer, String name, String localOnHost,
    boolean isPrimary, AtomicInteger storeFileCount) {
    RegionInfo regionInfo = mock(RegionInfo.class);
    when(regionInfo.getEncodedName()).thenReturn(name);
    if (!isPrimary) {
      when(regionInfo.getReplicaId()).thenReturn(RegionInfo.DEFAULT_REPLICA_ID + 1);
    }
    HDFSBlocksDistribution distribution = new HDFSBlocksDistribution();
    distribution.addHostsAndBlockWeight(new String[] { localOnHost }, 100);

    HStore store = getMockedStore(HStore.class, defaultAnswer, storeFileCount);
    HMobStore mobStore = getMockedStore(HMobStore.class, defaultAnswer, storeFileCount);

    HRegion region = mock(HRegion.class, defaultAnswer);
    when(region.getRegionInfo()).thenReturn(regionInfo);
    when(region.getHDFSBlocksDistribution()).thenReturn(distribution);
    when(region.getStores()).thenReturn(Lists.newArrayList(store, mobStore));
    return region;
  }

  private <T extends HStore> T getMockedStore(Class<T> clazz, Answer defaultAnswer,
    AtomicInteger storeFileCount) {
    T store = mock(clazz, defaultAnswer);
    int storeFileCountVal = storeFileCount.getAndIncrement();
    when(store.getStorefilesCount()).thenReturn(storeFileCountVal);
    when(store.getAvgStoreFileAge()).thenReturn(OptionalDouble.of(storeFileCountVal));
    when(store.getMaxStoreFileAge()).thenReturn(OptionalLong.of(storeFileCountVal));
    when(store.getMinStoreFileAge()).thenReturn(OptionalLong.of(storeFileCountVal));
    MemStoreSize memStore = mock(MemStoreSize.class, defaultAnswer);
    when(store.getMemStoreSize()).thenReturn(memStore);
    return store;
  }

}
