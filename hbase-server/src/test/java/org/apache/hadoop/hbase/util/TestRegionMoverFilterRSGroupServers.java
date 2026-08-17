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
import java.util.HashSet;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.RegionMover.RegionMoverBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RegionMover#filterRSGroupServers}. RSGroups is enabled on the mini cluster
 * and servers are moved between groups via the real {@code moveServersToRSGroup} admin call -- no
 * test constructs an {@link RSGroupInfo} by hand. Every {@code RSGroupInfo} used here is read back
 * from the master via {@code admin.getRSGroup(...)}, which computes "default" membership as "online
 * servers minus servers claimed by other groups" (see RSGroupInfoManagerImpl#getDefaultServers), so
 * its membership shrinks on its own once a move happens instead of being asserted into existence by
 * the test.
 */
@Tag(MiscTests.TAG)
@Tag(MediumTests.TAG)
public class TestRegionMoverFilterRSGroupServers {

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    RSGroupUtil.enableRSGroup(TEST_UTIL.getConfiguration());
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
   * Reproduces HBASE-30331: once a server is moved out of "default" into a custom group, the
   * master-computed "default" RSGroupInfo only lists the server that remains. filterRSGroupServers
   * must honor that real membership instead of short-circuiting on the group name and returning
   * every online server.
   */
  @Test
  public void testDefaultGroupFiltersToActualMembers() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    List<ServerName> allServers = new ArrayList<>(admin.getRegionServers());
    assertEquals(2, allServers.size());

    ServerName movedOut = allServers.get(0);
    ServerName inDefault = allServers.get(1);
    String groupName = "test_default_filter";

    admin.addRSGroup(groupName);
    admin.moveServersToRSGroup(new HashSet<>(List.of(addressOf(movedOut))), groupName);
    try {
      // Master-computed membership, not something we constructed ourselves.
      RSGroupInfo defaultGroup = admin.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
      assertEquals(1, defaultGroup.getServers().size());
      assertTrue(defaultGroup.containsServer(addressOf(inDefault)));

      try (RegionMover rm = buildMover()) {
        Collection<ServerName> result = rm.filterRSGroupServers(defaultGroup, allServers);

        assertEquals(1, result.size());
        assertTrue(result.contains(inDefault));
        assertFalse(result.contains(movedOut),
          "Server moved out of default must not be returned as a destination just because the "
            + "group being filtered is named 'default'");
      }
    } finally {
      admin.moveServersToRSGroup(new HashSet<>(List.of(addressOf(movedOut))),
        RSGroupInfo.DEFAULT_GROUP);
      admin.removeRSGroup(groupName);
    }
  }

  /**
   * A non-default group with one member must return only that member. Same real-cluster approach as
   * above: the group and its membership come from actual {@code moveServersToRSGroup} calls, not a
   * hand-built {@link RSGroupInfo}.
   */
  @Test
  public void testNonDefaultGroupFiltersToMembers() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    List<ServerName> allServers = new ArrayList<>(admin.getRegionServers());
    assertEquals(2, allServers.size());

    ServerName member = allServers.get(0);
    ServerName other = allServers.get(1);
    String groupName = "test_nondefault_filter";

    admin.addRSGroup(groupName);
    admin.moveServersToRSGroup(new HashSet<>(List.of(addressOf(member))), groupName);
    try {
      RSGroupInfo group = admin.getRSGroup(groupName);
      assertEquals(1, group.getServers().size());
      assertTrue(group.containsServer(addressOf(member)));

      try (RegionMover rm = buildMover()) {
        Collection<ServerName> result = rm.filterRSGroupServers(group, allServers);
        assertEquals(1, result.size());
        assertTrue(result.contains(member));
        assertFalse(result.contains(other));
      }
    } finally {
      admin.moveServersToRSGroup(new HashSet<>(List.of(addressOf(member))),
        RSGroupInfo.DEFAULT_GROUP);
      admin.removeRSGroup(groupName);
    }
  }

  /**
   * A group's real member must not be returned as a destination when it is absent from the
   * {@code onlineServers} snapshot handed to the filter (e.g. the server is currently offline or
   * was already excluded upstream). Uses a real group/member from {@code moveServersToRSGroup}, not
   * a fabricated, never-existed host address.
   */
  @Test
  public void testGroupMemberAbsentFromOnlineServersReturnsEmpty() throws Exception {
    Admin admin = TEST_UTIL.getAdmin();
    List<ServerName> allServers = new ArrayList<>(admin.getRegionServers());
    assertEquals(2, allServers.size());

    ServerName member = allServers.get(0);
    ServerName other = allServers.get(1);
    String groupName = "test_absent_filter";

    admin.addRSGroup(groupName);
    admin.moveServersToRSGroup(new HashSet<>(List.of(addressOf(member))), groupName);
    try {
      RSGroupInfo group = admin.getRSGroup(groupName);
      assertEquals(1, group.getServers().size());
      assertTrue(group.containsServer(addressOf(member)));

      try (RegionMover rm = buildMover()) {
        // The group's only member is not part of the online-servers snapshot passed in.
        Collection<ServerName> result =
          rm.filterRSGroupServers(group, Collections.singletonList(other));
        assertTrue(result.isEmpty());
      }
    } finally {
      admin.moveServersToRSGroup(new HashSet<>(List.of(addressOf(member))),
        RSGroupInfo.DEFAULT_GROUP);
      admin.removeRSGroup(groupName);
    }
  }
}
