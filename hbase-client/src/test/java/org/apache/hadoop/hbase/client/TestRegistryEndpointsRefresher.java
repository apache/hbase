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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.Uninterruptibles;

@Category({ ClientTests.class, SmallTests.class })
public class TestRegistryEndpointsRefresher {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegistryEndpointsRefresher.class);

  private static final String INTERVAL_SECS_CONFIG_NAME =
    "hbase.test.registry.refresh.interval.secs";
  private static final String MIN_INTERVAL_SECS_CONFIG_NAME =
    "hbase.test.registry.refresh.min.interval.secs";

  private Configuration conf;
  private RegistryEndpointsRefresher refresher;
  private AtomicInteger refreshCallCounter;
  private CopyOnWriteArrayList<Long> callTimestamps;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
    refreshCallCounter = new AtomicInteger(0);
    callTimestamps = new CopyOnWriteArrayList<>();
  }

  @After
  public void tearDown() {
    if (refresher != null) {
      refresher.stop();
    }
  }

  private void refresh() {
    refreshCallCounter.incrementAndGet();
    callTimestamps.add(EnvironmentEdgeManager.currentTime());
  }

  private void createAndStartRefresher(long intervalSecs, long minIntervalSecs) {
    conf.setLong(INTERVAL_SECS_CONFIG_NAME, intervalSecs);
    conf.setLong(MIN_INTERVAL_SECS_CONFIG_NAME, minIntervalSecs);
    refresher = new RegistryEndpointsRefresher(conf, INTERVAL_SECS_CONFIG_NAME,
      MIN_INTERVAL_SECS_CONFIG_NAME, this::refresh);
    refresher.start();
  }

  @Test
  public void testPeriodicMasterEndPointRefresh() throws IOException {
    // Refresh every 1 second.
    createAndStartRefresher(1, 0);
    // Wait for > 3 seconds to see that at least 3 refresh have been made.
    Waiter.waitFor(conf, 5000, () -> refreshCallCounter.get() > 3);
  }

  @Test
  public void testDurationBetweenRefreshes() throws IOException {
    // Disable periodic refresh
    // A minimum duration of 1s between refreshes
    createAndStartRefresher(Integer.MAX_VALUE, 1);
    // Issue a ton of manual refreshes.
    for (int i = 0; i < 10000; i++) {
      refresher.refreshNow();
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
    }
    // Overall wait time is 10000 ms, so the number of requests should be <=10
    // Actual calls to refresh should be much lower than the refresh count.
    assertTrue(String.valueOf(refreshCallCounter.get()), refreshCallCounter.get() <= 20);
    assertTrue(callTimestamps.size() > 0);
    // Verify that the delta between subsequent refresh is at least 1sec as configured.
    for (int i = 1; i < callTimestamps.size() - 1; i++) {
      long delta = callTimestamps.get(i) - callTimestamps.get(i - 1);
      // Few ms cushion to account for any env jitter.
      assertTrue(callTimestamps.toString(), delta > 990);
    }
  }
}
