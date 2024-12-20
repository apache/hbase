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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMetaTableIsolationConditional {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaTableIsolationConditional.class);

  private static final ServerName SERVER_1 = ServerName.valueOf("server1", 12345, 1);
  private static final ServerName SERVER_2 = ServerName.valueOf("server2", 12345, 1);
  private static final ServerName SERVER_3 = ServerName.valueOf("server3", 12345, 1);

  private static final RegionInfo META_REGION =
    RegionInfoBuilder.newBuilder(TableName.META_TABLE_NAME).build();
  private static final RegionInfo USER_REGION =
    RegionInfoBuilder.newBuilder(TableName.valueOf("userTable")).build();

  /**
   * Test that no violation is detected when `hbase:meta` is placed on an empty server.
   */
  @Test
  public void testNoViolationWhenMetaOnEmptyServer() {
    Set<ServerName> serversHostingMeta = Collections.emptySet();
    Set<ServerName> emptyServers = new HashSet<>(Set.of(SERVER_1));

    RegionPlan regionPlan = new RegionPlan(META_REGION, null, SERVER_1);

    assertFalse("No violation when `hbase:meta` is placed on an empty server",
      MetaTableIsolationConditional.checkViolation(regionPlan, serversHostingMeta, emptyServers));
  }

  /**
   * Test that no violation is detected when `hbase:meta` is moved to a server already hosting
   * `hbase:meta`.
   */
  @Test
  public void testNoViolationWhenMetaOnMetaServer() {
    Set<ServerName> serversHostingMeta = new HashSet<>(Set.of(SERVER_1));
    Set<ServerName> emptyServers = Collections.emptySet();

    RegionPlan regionPlan = new RegionPlan(META_REGION, null, SERVER_1);

    assertFalse("No violation when `hbase:meta` is placed on a server already hosting `hbase:meta`",
      MetaTableIsolationConditional.checkViolation(regionPlan, serversHostingMeta, emptyServers));
  }

  /**
   * Test that a violation is detected when `hbase:meta` is placed on a server hosting other
   * regions.
   */
  @Test
  public void testViolationWhenMetaOnNonEmptyServer() {
    Set<ServerName> serversHostingMeta = Collections.emptySet();
    Set<ServerName> emptyServers = new HashSet<>(Set.of(SERVER_2));

    RegionPlan regionPlan = new RegionPlan(META_REGION, null, SERVER_3);

    assertTrue("Violation detected when `hbase:meta` is placed on a server hosting other regions",
      MetaTableIsolationConditional.checkViolation(regionPlan, serversHostingMeta, emptyServers));
  }

  /**
   * Test that a violation is detected when a non-meta region is placed on a server hosting
   * `hbase:meta`.
   */
  @Test
  public void testViolationWhenNonMetaOnMetaServer() {
    Set<ServerName> serversHostingMeta = new HashSet<>(Set.of(SERVER_1));
    Set<ServerName> emptyServers = Collections.emptySet();

    RegionPlan regionPlan = new RegionPlan(USER_REGION, null, SERVER_1);

    assertTrue(
      "Violation detected when a non-meta region is placed on a server hosting `hbase:meta`",
      MetaTableIsolationConditional.checkViolation(regionPlan, serversHostingMeta, emptyServers));
  }

  /**
   * Test that no violation is detected when a non-meta region is placed on a server not hosting
   * `hbase:meta`.
   */
  @Test
  public void testNoViolationWhenNonMetaOnNonMetaServer() {
    Set<ServerName> serversHostingMeta = new HashSet<>(Set.of(SERVER_1));
    Set<ServerName> emptyServers = Collections.emptySet();

    RegionPlan regionPlan = new RegionPlan(USER_REGION, null, SERVER_2);

    assertFalse(
      "No violation when a non-meta region is placed on a server not hosting `hbase:meta`",
      MetaTableIsolationConditional.checkViolation(regionPlan, serversHostingMeta, emptyServers));
  }

  /**
   * Test that no violation is detected when `hbase:meta` is placed on the only available empty
   * server.
   */
  @Test
  public void testNoViolationWhenMetaOnLastEmptyServer() {
    Set<ServerName> serversHostingMeta = Collections.emptySet();
    Set<ServerName> emptyServers = new HashSet<>(Set.of(SERVER_2));

    RegionPlan regionPlan = new RegionPlan(META_REGION, null, SERVER_2);

    assertFalse("No violation when `hbase:meta` is placed on the only available empty server",
      MetaTableIsolationConditional.checkViolation(regionPlan, serversHostingMeta, emptyServers));
  }

  /**
   * Test that a violation is detected when `hbase:meta` is moved to a non-empty server.
   */
  @Test
  public void testViolationWhenMetaMovedToNonEmptyServer() {
    Set<ServerName> serversHostingMeta = new HashSet<>(Set.of(SERVER_1));
    Set<ServerName> emptyServers = new HashSet<>(Set.of(SERVER_2));

    RegionPlan regionPlan = new RegionPlan(META_REGION, SERVER_1, SERVER_3);

    assertTrue("Violation detected when `hbase:meta` is moved to a non-empty server",
      MetaTableIsolationConditional.checkViolation(regionPlan, serversHostingMeta, emptyServers));
  }
}
