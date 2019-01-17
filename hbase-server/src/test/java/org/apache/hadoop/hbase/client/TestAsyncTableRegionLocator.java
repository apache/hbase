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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ MediumTests.class, ClientTests.class })
public class TestAsyncTableRegionLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableRegionLocator.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("async");

  private static AsyncConnection CONN;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.createMultiRegionTable(TABLE_NAME, Bytes.toBytes("cf"));
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
    TEST_UTIL.getAdmin().balancerSwitch(false, true);
    CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    TEST_UTIL.shutdownMiniCluster();
  }

  private void assertLocEquals(Map<RegionInfo, ServerName> region2Loc)
      throws InterruptedException, ExecutionException {
    for (HRegionLocation loc : CONN.getRegionLocator(TABLE_NAME).getAllRegionLocations().get()) {
      ServerName expected = region2Loc.remove(loc.getRegion());
      assertNotNull(expected);
      assertEquals(expected, loc.getServerName());
    }
  }

  @Test
  public void testGetAll() throws InterruptedException, ExecutionException {
    Map<RegionInfo, ServerName> region2Loc = TEST_UTIL.getMiniHBaseCluster()
      .getRegionServerThreads().stream().map(t -> t.getRegionServer())
      .flatMap(rs -> rs.getRegions(TABLE_NAME).stream()
        .map(r -> Pair.create(r.getRegionInfo(), rs.getServerName())))
      .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
    MutableInt maxDepth = new MutableInt(0);
    MutableInt depth = new MutableInt(0) {

      private static final long serialVersionUID = 5887112211305087650L;

      @Override
      public int incrementAndGet() {
        int val = super.incrementAndGet();
        if (val > maxDepth.intValue()) {
          maxDepth.setValue(val);
        }
        return val;
      }
    };
    // first time, read from meta
    AsyncTableRegionLocatorImpl.STACK_DEPTH.set(depth);
    assertLocEquals(new HashMap<>(region2Loc));
    assertTrue(maxDepth.intValue() > 0);
    assertTrue(maxDepth.intValue() <= AsyncTableRegionLocatorImpl.MAX_STACK_DEPTH);

    // second time, read from cache
    maxDepth.setValue(0);
    depth.setValue(0);
    AsyncTableRegionLocatorImpl.STACK_DEPTH.set(depth);
    assertLocEquals(new HashMap<>(region2Loc));
    assertTrue(maxDepth.intValue() > 0);
    assertTrue(maxDepth.intValue() <= AsyncTableRegionLocatorImpl.MAX_STACK_DEPTH);
  }
}
