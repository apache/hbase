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

import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ClientTests.class, SmallTests.class})
public class TestMasterAddressRefresher {

  static class DummyConnection implements Connection {
    private final Configuration conf;

    DummyConnection(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
      return null;
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool) throws IOException {
      return null;
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
      return null;
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
      return null;
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName) throws IOException {
      return null;
    }

    @Override
    public Admin getAdmin() throws IOException {
      return null;
    }

    @Override
    public String getClusterId() throws IOException {
      return null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean isClosed() {
      return false;
    }

    @Override
    public void abort(String why, Throwable e) {

    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }

  private static class DummyMasterRegistry extends MasterRegistry {

    private final AtomicInteger getMastersCallCounter = new AtomicInteger(0);
    private final List<Long> callTimeStamps = new ArrayList<>();

    @Override
    public void init(Connection connection) throws IOException {
      super.init(connection);
    }

    @Override
    List<ServerName> getMasters() {
      getMastersCallCounter.incrementAndGet();
      callTimeStamps.add(EnvironmentEdgeManager.currentTime());
      return new ArrayList<>();
    }

    public int getMastersCount() {
      return getMastersCallCounter.get();
    }

    public List<Long> getCallTimeStamps() {
      return callTimeStamps;
    }
  }

  @Test
  public void testPeriodicMasterEndPointRefresh() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // Refresh every 1 second.
    conf.setLong(MasterAddressRefresher.PERIODIC_REFRESH_INTERVAL_SECS, 1);
    conf.setLong(MasterAddressRefresher.MIN_SECS_BETWEEN_REFRESHES, 0);
    final DummyMasterRegistry registry = new DummyMasterRegistry();
    registry.init(new DummyConnection(conf));
    // Wait for > 3 seconds to see that at least 3 getMasters() RPCs have been made.
    Waiter.waitFor(
        conf, 5000, new Waiter.Predicate<Exception>() {
          @Override
          public boolean evaluate() throws Exception {
            return registry.getMastersCount() > 3;
          }
        });
  }

  @Test
  public void testDurationBetweenRefreshes() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // Disable periodic refresh
    conf.setLong(MasterAddressRefresher.PERIODIC_REFRESH_INTERVAL_SECS, Integer.MAX_VALUE);
    // A minimum duration of 1s between refreshes
    conf.setLong(MasterAddressRefresher.MIN_SECS_BETWEEN_REFRESHES, 1);
    DummyMasterRegistry registry = new DummyMasterRegistry();
    registry.init(new DummyConnection(conf));
    // Issue a ton of manual refreshes.
    for (int i = 0; i < 10000; i++) {
      registry.masterAddressRefresher.refreshNow();
      Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
    }
    // Overall wait time is 10000 ms, so the number of requests should be <=10
    List<Long> callTimeStamps = registry.getCallTimeStamps();
    // Actual calls to getMasters() should be much lower than the refresh count.
    Assert.assertTrue(
        String.valueOf(registry.getMastersCount()), registry.getMastersCount() <= 20);
    Assert.assertTrue(callTimeStamps.size() > 0);
    // Verify that the delta between subsequent RPCs is at least 1sec as configured.
    for (int i = 1; i < callTimeStamps.size() - 1; i++) {
      long delta = callTimeStamps.get(i) - callTimeStamps.get(i - 1);
      // Few ms cushion to account for any env jitter.
      Assert.assertTrue(callTimeStamps.toString(), delta > 990);
    }
  }
}
