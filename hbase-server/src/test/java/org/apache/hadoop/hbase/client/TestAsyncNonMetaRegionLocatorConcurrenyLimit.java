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
package org.apache.hadoop.hbase.client;

import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.client.AsyncNonMetaRegionLocator.MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStartRow;
import static org.apache.hadoop.hbase.client.ConnectionUtils.isEmptyStopRow;
import static org.apache.hadoop.hbase.coprocessor.CoprocessorHost.REGION_COPROCESSOR_CONF_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncNonMetaRegionLocatorConcurrenyLimit {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncNonMetaRegionLocatorConcurrenyLimit.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static AsyncConnectionImpl CONN;

  private static AsyncNonMetaRegionLocator LOCATOR;

  private static byte[][] SPLIT_KEYS;

  private static int MAX_ALLOWED = 2;

  private static AtomicInteger CONCURRENCY = new AtomicInteger(0);

  private static AtomicInteger MAX_CONCURRENCY = new AtomicInteger(0);

  public static final class CountingRegionObserver implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
        InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
      if (c.getEnvironment().getRegionInfo().isMetaRegion()) {
        int concurrency = CONCURRENCY.incrementAndGet();
        for (;;) {
          int max = MAX_CONCURRENCY.get();
          if (concurrency <= max) {
            break;
          }
          if (MAX_CONCURRENCY.compareAndSet(max, concurrency)) {
            break;
          }
        }
        Threads.sleepWithoutInterrupt(10);
      }
      return hasNext;
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
        InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
      if (c.getEnvironment().getRegionInfo().isMetaRegion()) {
        CONCURRENCY.decrementAndGet();
      }
      return hasNext;
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set(REGION_COPROCESSOR_CONF_KEY, CountingRegionObserver.class.getName());
    conf.setInt(MAX_CONCURRENT_LOCATE_REQUEST_PER_TABLE, MAX_ALLOWED);
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    ConnectionRegistry registry =
        ConnectionRegistryFactory.getRegistry(TEST_UTIL.getConfiguration());
    CONN = new AsyncConnectionImpl(TEST_UTIL.getConfiguration(), registry,
      registry.getClusterId().get(), User.getCurrent());
    LOCATOR = new AsyncNonMetaRegionLocator(CONN);
    SPLIT_KEYS = IntStream.range(1, 256).mapToObj(i -> Bytes.toBytes(String.format("%02x", i)))
      .toArray(byte[][]::new);
    TEST_UTIL.createTable(TABLE_NAME, FAMILY, SPLIT_KEYS);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  private void assertLocs(List<CompletableFuture<RegionLocations>> futures)
      throws InterruptedException, ExecutionException {
    assertEquals(256, futures.size());
    for (int i = 0; i < futures.size(); i++) {
      HRegionLocation loc = futures.get(i).get().getDefaultRegionLocation();
      if (i == 0) {
        assertTrue(isEmptyStartRow(loc.getRegion().getStartKey()));
      } else {
        assertEquals(String.format("%02x", i), Bytes.toString(loc.getRegion().getStartKey()));
      }
      if (i == futures.size() - 1) {
        assertTrue(isEmptyStopRow(loc.getRegion().getEndKey()));
      } else {
        assertEquals(String.format("%02x", i + 1), Bytes.toString(loc.getRegion().getEndKey()));
      }
    }
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    List<CompletableFuture<RegionLocations>> futures =
      IntStream.range(0, 256).mapToObj(i -> Bytes.toBytes(String.format("%02x", i)))
        .map(r -> LOCATOR.getRegionLocations(TABLE_NAME, r, RegionReplicaUtil.DEFAULT_REPLICA_ID,
          RegionLocateType.CURRENT, false))
        .collect(toList());
    assertLocs(futures);
    assertTrue("max allowed is " + MAX_ALLOWED + " but actual is " + MAX_CONCURRENCY.get(),
      MAX_CONCURRENCY.get() <= MAX_ALLOWED);
  }
}
