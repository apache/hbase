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

import static org.apache.hadoop.hbase.HConstants.META_REPLICAS_NUM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestZKAsyncRegistry {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ZKAsyncRegistry REGISTRY;

  // waits for all replicas to have region location
  static void waitUntilAllReplicasHavingRegionLocation(TableName tbl) throws IOException {
    TEST_UTIL.waitFor(TEST_UTIL.getConfiguration()
        .getLong("hbase.client.sync.wait.timeout.msec", 60000),
        200, true, new ExplainingPredicate<IOException>() {
      @Override
      public String explainFailure() throws IOException {
        return TEST_UTIL.explainTableAvailability(tbl);
      }

      @Override
      public boolean evaluate() throws IOException {
        AtomicBoolean ready = new AtomicBoolean(true);
        try {
          RegionLocations locs = REGISTRY.getMetaRegionLocation().get();
          assertEquals(3, locs.getRegionLocations().length);
          IntStream.range(0, 3).forEach(i -> {
            HRegionLocation loc = locs.getRegionLocation(i);
            if (loc == null) {
              ready.set(false);
            }
          });
        } catch (Exception e) {
          ready.set(false);
        }
        return ready.get();
      }
    });
  }
  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(META_REPLICAS_NUM, 3);
    TEST_UTIL.startMiniCluster(3);
    REGISTRY = new ZKAsyncRegistry(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(REGISTRY);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException, IOException {
    assertEquals(TEST_UTIL.getHBaseCluster().getClusterStatus().getClusterId(),
      REGISTRY.getClusterId().get());
    assertEquals(TEST_UTIL.getHBaseCluster().getClusterStatus().getServersSize(),
      REGISTRY.getCurrentNrHRS().get().intValue());
    assertEquals(TEST_UTIL.getHBaseCluster().getMaster().getServerName(),
      REGISTRY.getMasterAddress().get());
    assertEquals(-1, REGISTRY.getMasterInfoPort().get().intValue());
    waitUntilAllReplicasHavingRegionLocation(TableName.META_TABLE_NAME);
    RegionLocations locs = REGISTRY.getMetaRegionLocation().get();
    assertEquals(3, locs.getRegionLocations().length);
    IntStream.range(0, 3).forEach(i -> {
      HRegionLocation loc = locs.getRegionLocation(i);
      assertNotNull("Replica " + i + " doesn't have location", loc);
      assertTrue(loc.getRegionInfo().getTable().equals(TableName.META_TABLE_NAME));
      assertEquals(i, loc.getRegionInfo().getReplicaId());
    });
  }
}
