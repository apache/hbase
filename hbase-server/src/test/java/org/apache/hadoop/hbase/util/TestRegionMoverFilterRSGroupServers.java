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
package org.apache.hadoop.hbase.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.RegionMover.RegionMoverBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests for {@link RegionMover#filterRSGroupServers}. Spins up a 2-node mini cluster so we can
 * build a real RegionMover instance; the method under test is pure in-memory logic. Branch-2's
 * hbase-server module cannot reach {@code Admin.getRSGroup}/{@code moveServersToRSGroup} (those
 * live in hbase-rsgroup, which depends on hbase-server, not the reverse), so {@link RSGroupInfo}
 * instances here are constructed directly rather than read back from the master as in the
 * master-branch equivalent of this test (see HBASE-30331 / #8552) -- the scenarios and intent
 * match.
 */
@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
public class TestRegionMoverFilterRSGroupServers {

  private static final Logger LOG =
    LoggerFactory.getLogger(TestRegionMoverFilterRSGroupServers.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private RegionMover buildMover() throws Exception {
    ServerName any = TEST_UTIL.getAdmin().getRegionServers().iterator().next();
    return new RegionMoverBuilder(any.getHostname() + ":" + any.getPort(),
      TEST_UTIL.getConfiguration()).build();
  }

  private static Address addressOf(ServerName sn) {
    return Address.fromParts(sn.getHostname(), sn.getPort());
  }

  /**
   * Reproduces HBASE-30331/HBASE-22740: a group named "default" whose real membership does not
   * include every online server (e.g. because a server was moved out of it into a custom group)
   * must still be filtered down to its actual members. filterRSGroupServers must not short-circuit
   * on the group's name and return every online server regardless of membership.
   */
  @Test
  public void testDefaultGroupFiltersToActualMembers() throws Exception {
    try (RegionMover rm = buildMover()) {
      List<ServerName> allServers = new ArrayList<>(TEST_UTIL.getAdmin().getRegionServers());
      assertEquals(2, allServers.size(), "Mini cluster should have started with 2 region servers");

      ServerName movedOut = allServers.get(0);
      ServerName inDefault = allServers.get(1);

      // Simulates the master-computed "default" group after movedOut was moved to another
      // group: only inDefault remains, even though the group is still named "default".
      RSGroupInfo defaultGroup = new RSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
      defaultGroup.addServer(addressOf(inDefault));

      Collection<ServerName> result = rm.filterRSGroupServers(defaultGroup, allServers);

      assertEquals(1, result.size(),
        "filterRSGroupServers should return only the default group's actual members");
      assertTrue(result.contains(inDefault),
        "Server that is an actual member of the default group must be returned as a destination");
      assertFalse(result.contains(movedOut),
        "Server not in the default group's real membership must not be returned as a "
          + "destination just because the group being filtered is named 'default'");
    }
  }

  /** A non-default group with one member must return only that member. */
  @Test
  public void testNonDefaultGroupFiltersToMembers() throws Exception {
    try (RegionMover rm = buildMover()) {
      List<ServerName> allServers = new ArrayList<>(TEST_UTIL.getAdmin().getRegionServers());
      assertEquals(2, allServers.size(), "Mini cluster should have started with 2 region servers");

      ServerName member = allServers.get(0);
      ServerName other = allServers.get(1);
      RSGroupInfo group = new RSGroupInfo("testgroup");
      group.addServer(addressOf(member));

      Collection<ServerName> result = rm.filterRSGroupServers(group, allServers);
      assertEquals(1, result.size(),
        "filterRSGroupServers should return only the non-default group's actual members");
      assertTrue(result.contains(member),
        "Server that is an actual member of the group must be returned as a destination");
      assertFalse(result.contains(other),
        "Server that is not a member of the group must not be returned as a destination");
    }
  }

  /**
   * A group's real member must not be returned as a destination when it is absent from the
   * {@code onlineServers} snapshot handed to the filter (e.g. the server is currently offline or
   * was already excluded upstream).
   */
  @Test
  public void testGroupMemberAbsentFromOnlineServersReturnsEmpty() throws Exception {
    try (RegionMover rm = buildMover()) {
      List<ServerName> allServers = new ArrayList<>(TEST_UTIL.getAdmin().getRegionServers());
      assertEquals(2, allServers.size(), "Mini cluster should have started with 2 region servers");

      ServerName member = allServers.get(0);
      ServerName other = allServers.get(1);
      RSGroupInfo group = new RSGroupInfo("testgroup");
      group.addServer(addressOf(member));

      // The group's only member is not part of the online-servers snapshot passed in.
      Collection<ServerName> result =
        rm.filterRSGroupServers(group, Collections.singletonList(other));
      assertTrue(result.isEmpty(),
        "Group member absent from the online-servers snapshot must not be returned as a "
          + "destination");
    }
  }
}
