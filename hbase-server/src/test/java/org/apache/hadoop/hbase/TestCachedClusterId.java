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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestContext;
import org.apache.hadoop.hbase.MultithreadedTestUtil.TestThread;
import org.apache.hadoop.hbase.master.CachedClusterId;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestCachedClusterId {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCachedClusterId.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static String clusterId;
  private static HMaster activeMaster;
  private static HMaster standByMaster;

  private static class GetClusterIdThread extends TestThread {
    CachedClusterId cachedClusterId;
    public GetClusterIdThread(TestContext ctx, CachedClusterId clusterId) {
      super(ctx);
      cachedClusterId = clusterId;
    }

    @Override
    public void doWork() throws Exception {
      assertEquals(clusterId, cachedClusterId.getFromCacheOrFetch());
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    activeMaster = TEST_UTIL.getHBaseCluster().getMaster();
    clusterId = activeMaster.getClusterId();
    standByMaster = TEST_UTIL.getHBaseCluster().startMaster().getMaster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testClusterIdMatch() {
    assertEquals(clusterId, standByMaster.getClusterId());
  }

  @Test
  public void testMultiThreadedGetClusterId() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    CachedClusterId cachedClusterId = new CachedClusterId(TEST_UTIL.getHBaseCluster().getMaster(),
      conf);
    TestContext context = new TestContext(conf);
    int numThreads = 16;
    for (int i = 0; i < numThreads; i++) {
      context.addThread(new GetClusterIdThread(context, cachedClusterId));
    }
    context.startThreads();
    context.stop();
    int cacheMisses = cachedClusterId.getCacheStats();
    assertEquals(cacheMisses, 1);
  }
}
