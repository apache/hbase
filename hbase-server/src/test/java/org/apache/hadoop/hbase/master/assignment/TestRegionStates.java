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
package org.apache.hadoop.hbase.master.assignment;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestRegionStates {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionStates.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionStates.class);

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static ThreadPoolExecutor threadPool;
  private static ExecutorCompletionService<Object> executorService;

  @BeforeClass
  public static void setUp() throws Exception {
    threadPool = Threads.getBoundedCachedThreadPool(32, 60L, TimeUnit.SECONDS,
      new ThreadFactoryBuilder().setNameFormat("ProcedureDispatcher-pool-%d").setDaemon(true)
        .setUncaughtExceptionHandler((t, e) -> LOG.warn("Failed thread " + t.getName(), e))
        .build());
    executorService = new ExecutorCompletionService<>(threadPool);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    threadPool.shutdown();
  }

  @Before
  public void testSetup() {
  }

  @After
  public void testTearDown() throws Exception {
    while (true) {
      Future<Object> f = executorService.poll();
      if (f == null) break;
      f.get();
    }
  }

  private static void waitExecutorService(final int count) throws Exception {
    for (int i = 0; i < count; ++i) {
      executorService.take().get();
    }
  }

  // ==========================================================================
  //  Regions related
  // ==========================================================================

  @Test
  public void testRegionDoubleCreation() throws Exception {
    // NOTE: RegionInfo sort by table first, so we are relying on that
    final TableName TABLE_NAME_A = TableName.valueOf("testOrderedByTableA");
    final TableName TABLE_NAME_B = TableName.valueOf("testOrderedByTableB");
    final TableName TABLE_NAME_C = TableName.valueOf("testOrderedByTableC");
    final RegionStates stateMap = new RegionStates();
    final int NRUNS = 1000;
    final int NSMALL_RUNS = 3;

    // add some regions for table B
    for (int i = 0; i < NRUNS; ++i) {
      addRegionNode(stateMap, TABLE_NAME_B, i);
    }
    // re-add the regions for table B
    for (int i = 0; i < NRUNS; ++i) {
      addRegionNode(stateMap, TABLE_NAME_B, i);
    }
    waitExecutorService(NRUNS * 2);

    // add two other tables A and C that will be placed before and after table B (sort order)
    for (int i = 0; i < NSMALL_RUNS; ++i) {
      addRegionNode(stateMap, TABLE_NAME_A, i);
      addRegionNode(stateMap, TABLE_NAME_C, i);
    }
    waitExecutorService(NSMALL_RUNS * 2);
    // check for the list of regions of the 3 tables
    checkTableRegions(stateMap, TABLE_NAME_A, NSMALL_RUNS);
    checkTableRegions(stateMap, TABLE_NAME_B, NRUNS);
    checkTableRegions(stateMap, TABLE_NAME_C, NSMALL_RUNS);
  }

  private void checkTableRegions(final RegionStates stateMap, final TableName tableName,
    final int nregions) {
    List<RegionStateNode> rns = stateMap.getTableRegionStateNodes(tableName);
    assertEquals(nregions, rns.size());
    for (int i = 1; i < rns.size(); ++i) {
      long a = Bytes.toLong(rns.get(i - 1).getRegionInfo().getStartKey());
      long b = Bytes.toLong(rns.get(i + 0).getRegionInfo().getStartKey());
      assertEquals(b, a + 1);
    }
  }

  private void addRegionNode(final RegionStates stateMap,
      final TableName tableName, final long regionId) {
    executorService.submit(new Callable<Object>() {
      @Override
      public Object call() {
        return stateMap.getOrCreateRegionStateNode(RegionInfoBuilder.newBuilder(tableName)
            .setStartKey(Bytes.toBytes(regionId))
            .setEndKey(Bytes.toBytes(regionId + 1))
            .setSplit(false)
            .setRegionId(0)
            .build());
      }
    });
  }

  private RegionInfo createRegionInfo(final TableName tableName, final long regionId) {
    return RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes(regionId))
        .setEndKey(Bytes.toBytes(regionId + 1))
        .setSplit(false)
        .setRegionId(0)
        .build();
  }

  @Test
  public void testPerf() throws Exception {
    final TableName TABLE_NAME = TableName.valueOf("testPerf");
    final int NRUNS = 1000000; // 1M
    final RegionStates stateMap = new RegionStates();

    long st = System.currentTimeMillis();
    for (int i = 0; i < NRUNS; ++i) {
      final int regionId = i;
      executorService.submit(new Callable<Object>() {
        @Override
        public Object call() {
          RegionInfo hri = createRegionInfo(TABLE_NAME, regionId);
          return stateMap.getOrCreateRegionStateNode(hri);
        }
      });
    }
    waitExecutorService(NRUNS);
    long et = System.currentTimeMillis();
    LOG.info(String.format("PERF STATEMAP INSERT: %s %s/sec",
      StringUtils.humanTimeDiff(et - st),
      StringUtils.humanSize(NRUNS / ((et - st) / 1000.0f))));

    st = System.currentTimeMillis();
    for (int i = 0; i < NRUNS; ++i) {
      final int regionId = i;
      executorService.submit(new Callable<Object>() {
        @Override
        public Object call() {
          RegionInfo hri = createRegionInfo(TABLE_NAME, regionId);
          return stateMap.getRegionState(hri);
        }
      });
    }

    waitExecutorService(NRUNS);
    et = System.currentTimeMillis();
    LOG.info(String.format("PERF STATEMAP GET: %s %s/sec",
      StringUtils.humanTimeDiff(et - st),
      StringUtils.humanSize(NRUNS / ((et - st) / 1000.0f))));
  }

  @Test
  public void testPerfSingleThread() {
    final TableName TABLE_NAME = TableName.valueOf("testPerf");
    final int NRUNS = 1 * 1000000; // 1M

    final RegionStates stateMap = new RegionStates();
    long st = System.currentTimeMillis();
    for (int i = 0; i < NRUNS; ++i) {
      stateMap.createRegionStateNode(createRegionInfo(TABLE_NAME, i));
    }
    long et = System.currentTimeMillis();
    LOG.info(String.format("PERF SingleThread: %s %s/sec",
        StringUtils.humanTimeDiff(et - st),
      StringUtils.humanSize(NRUNS / ((et - st) / 1000.0f))));
  }
}
