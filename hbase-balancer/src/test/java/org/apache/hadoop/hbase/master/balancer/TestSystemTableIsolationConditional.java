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
public class TestSystemTableIsolationConditional {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSystemTableIsolationConditional.class);

  private static final ServerName SERVER_1 = ServerName.valueOf("server1", 123, 1);
  private static final ServerName SERVER_2 = ServerName.valueOf("server2", 123, 1);

  @Test
  public void testSystemTableMovedToSystemTableServer() {
    Set<ServerName> serversHostingSystemTables = new HashSet<>();
    serversHostingSystemTables.add(SERVER_1);

    RegionInfo systemTableRegion =
      RegionInfoBuilder.newBuilder(TableName.valueOf("hbase:meta")).build();
    RegionPlan regionPlan = new RegionPlan(systemTableRegion, null, SERVER_1);

    assertFalse("No violation when system table is moved to a server hosting only system tables",
      SystemTableIsolationConditional.checkViolation(regionPlan, serversHostingSystemTables,
        Collections.emptySet()));
  }

  @Test
  public void testSystemTableMovedToNonSystemTableServer() {
    Set<ServerName> serversHostingSystemTables = new HashSet<>();
    serversHostingSystemTables.add(SERVER_1);

    RegionInfo systemTableRegion =
      RegionInfoBuilder.newBuilder(TableName.valueOf("hbase:meta")).build();
    RegionPlan regionPlan = new RegionPlan(systemTableRegion, null, SERVER_2);

    assertTrue(
      "Violation detected when system table is moved to a server not hosting system tables",
      SystemTableIsolationConditional.checkViolation(regionPlan, serversHostingSystemTables,
        Collections.emptySet()));
  }

  @Test
  public void testNonSystemTableMovedToNonSystemTableServer() {
    Set<ServerName> serversHostingSystemTables = new HashSet<>();
    serversHostingSystemTables.add(SERVER_1);

    RegionInfo nonSystemTableRegion =
      RegionInfoBuilder.newBuilder(TableName.valueOf("testTable")).build();
    RegionPlan regionPlan = new RegionPlan(nonSystemTableRegion, null, SERVER_2);

    assertFalse("No violation when non-system table is moved to a server not hosting system tables",
      SystemTableIsolationConditional.checkViolation(regionPlan, serversHostingSystemTables,
        Collections.emptySet()));
  }

  @Test
  public void testNonSystemTableMovedToSystemTableServer() {
    Set<ServerName> serversHostingSystemTables = new HashSet<>();
    serversHostingSystemTables.add(SERVER_1);

    RegionInfo nonSystemTableRegion =
      RegionInfoBuilder.newBuilder(TableName.valueOf("testTable")).build();
    RegionPlan regionPlan = new RegionPlan(nonSystemTableRegion, null, SERVER_1);

    assertTrue(
      "Violation detected when non-system table is moved to a server hosting system tables",
      SystemTableIsolationConditional.checkViolation(regionPlan, serversHostingSystemTables,
        Collections.emptySet()));
  }
}
