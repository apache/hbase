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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.RSGroupTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

/**
 * Black-box tests for regex-based automatic RSGroup membership
 * (hbase.rsgroup.regex.&lt;groupname&gt;=&lt;regex&gt;), driven entirely through the public
 * {@link RSGroupAdmin} API, live RegionServer add/remove against the minicluster, and captured log
 * output -- no reflection or visibility changes against {@link RSGroupInfoManagerImpl}.
 */
@Tag(MediumTests.TAG)
public class TestRegexBasedRSGroupMembership extends TestRSGroupsBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestRegexBasedRSGroupMembership.class);
  private static final String REGEX_PREFIX = "hbase.rsgroup.regex.";

  private final List<JVMClusterUtil.RegionServerThread> fakeRegionServers = new ArrayList<>();
  private final Set<String> regexConfigKeysSet = new HashSet<>();

  @BeforeAll
  public static void setUp() throws Exception {
    setUpTestBeforeClass();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    tearDownAfterClass();
  }

  @BeforeEach
  public void beforeMethod(TestInfo testInfo) throws Exception {
    setUpBeforeMethod(testInfo);
  }

  @AfterEach
  public void afterMethod() throws Exception {
    // Stop any fake-hostname RS this test started before handing back to the shared teardown,
    // which only ever starts servers to top back up to NUM_SLAVES_BASE, never stops extras.
    for (JVMClusterUtil.RegionServerThread rst : new ArrayList<>(fakeRegionServers)) {
      stopFakeRegionServer(rst);
    }
    // The master Configuration object is shared across every test method in this class; undo
    // any regex config this test added so it can't leak into the next method.
    for (String key : regexConfigKeysSet) {
      master.getConfiguration().unset(key);
      TEST_UTIL.getConfiguration().unset(key);
    }
    regexConfigKeysSet.clear();
    tearDownAfterMethod();
    RSGroupBasedLoadBalancer balancer = (RSGroupBasedLoadBalancer) master.getLoadBalancer();
    balancer.resetAssignmentCallFlagsForTest();
    balancer.setFallbackEnabledForTest(false);
  }

  // ============================== helpers ==============================

  /**
   * Starts a genuinely new RegionServer process (thread) in the shared minicluster that reports
   * itself under a caller-chosen hostname, so regex membership rules can target it individually.
   * The base cluster's real RS all share one real, resolvable hostname (this machine's address);
   * {@link RSRpcServices}'s constructor unconditionally resolves whatever hostname is configured
   * (independent of any RPC bind-address override), so the caller-chosen hostname here must itself
   * be a real, resolvable, bindable literal -- "127.0.0.1" and "localhost" both work and are
   * guaranteed distinct from the base cluster's real hostname, which is exactly what every test in
   * this class relies on to get a controllable, non-shared identity.
   */
  private JVMClusterUtil.RegionServerThread startFakeHostnameRS(String hostname) throws Exception {
    Configuration rsConf = new Configuration(TEST_UTIL.getConfiguration());
    rsConf.set(DNS.UNSAFE_RS_HOSTNAME_KEY, hostname);
    rsConf.set("hbase.regionserver.ipc.address", "localhost");
    // The shared TEST_UTIL configuration's regionserver port is never rewritten to an ephemeral
    // value by minicluster startup (the base RS get their distinct ports through a separate,
    // per-instance mechanism), so without this override every fake RS started here would try to
    // bind the literal default port (16020) and collide with any other fake RS still alive.
    rsConf.set(HConstants.REGIONSERVER_PORT, "0");
    // Same issue for the embedded web UI (default port 16030); these fake RS don't need one.
    rsConf.set(HConstants.REGIONSERVER_INFO_PORT, "-1");
    LocalHBaseCluster localCluster = ((MiniHBaseCluster) cluster).hbaseCluster;
    JVMClusterUtil.RegionServerThread rst =
      localCluster.addRegionServer(rsConf, localCluster.getRegionServers().size());
    rst.start();
    rst.waitForServerOnline();
    fakeRegionServers.add(rst);
    final ServerName sn = rst.getRegionServer().getServerName();
    TEST_UTIL.waitFor(WAIT_TIMEOUT,
      () -> master.getServerManager().getOnlineServersList().contains(sn));
    LOG.info("Started fake-hostname RS {}", sn);
    return rst;
  }

  private void stopFakeRegionServer(JVMClusterUtil.RegionServerThread rst) throws Exception {
    fakeRegionServers.remove(rst);
    final ServerName sn = rst.getRegionServer().getServerName();
    rst.getRegionServer().stop("test cleanup");
    TEST_UTIL.waitFor(WAIT_TIMEOUT,
      () -> !master.getServerManager().getOnlineServersList().contains(sn));
    LOG.info("Stopped fake-hostname RS {}", sn);
  }

  private void killFakeRegionServer(JVMClusterUtil.RegionServerThread rst) throws Exception {
    fakeRegionServers.remove(rst);
    final ServerName sn = rst.getRegionServer().getServerName();
    ((MiniHBaseCluster) cluster).killRegionServer(sn);
    TEST_UTIL.waitFor(WAIT_TIMEOUT,
      () -> !master.getServerManager().getOnlineServersList().contains(sn));
    LOG.info("Killed fake-hostname RS {}", sn);
  }

  /**
   * The listener thread that drives automatic regex-based reassignment only wakes up on an actual
   * server add/remove event -- a bare config mutation is not enough. Forces a wake-up cycle
   * (add+remove a bystander RS) so a config change just made takes effect without requiring an
   * explicit RSGroupAdmin RPC. Only used by tests that *want* the automatic reconciliation to run
   * and correct things; the trigger RS's own transient membership doesn't affect assertions
   * elsewhere since those only check for specific other servers' addresses.
   */
  private void triggerListenerCycle() throws Exception {
    JVMClusterUtil.RegionServerThread trigger = startFakeHostnameRS("127.0.0.1");
    stopFakeRegionServer(trigger);
  }

  private void setRegex(String groupName, String regex) {
    String key = REGEX_PREFIX + groupName;
    master.getConfiguration().set(key, regex);
    // The master's Configuration is a distinct object from TEST_UTIL's (the minicluster copies
    // configuration per daemon), so mirror the change there too --
    // VerifyingRSGroupAdminClient#verify
    // reads it to know which servers' membership is regex-driven.
    TEST_UTIL.getConfiguration().set(key, regex);
    regexConfigKeysSet.add(key);
  }

  /**
   * Tears down a regex-governed group: the regex is first dropped on the master only -- TEST_UTIL's
   * copy still tells VerifyingRSGroupAdminClient which servers' membership is regex-driven -- and
   * then the group is emptied and removed.
   */
  private void removeRegexGroup(String groupName) throws Exception {
    // Move the tables out while the regex is still active: the flush this triggers must keep not
    // persisting the group's servers.
    rsGroupAdmin.moveTables(rsGroupAdmin.getRSGroupInfo(groupName).getTables(),
      RSGroupInfo.DEFAULT_GROUP);
    String key = REGEX_PREFIX + groupName;
    master.getConfiguration().unset(key);
    try {
      removeGroup(groupName);
    } finally {
      clearRegex(groupName);
    }
  }

  /** Reads a group's servers the way RegionMover and the master web UI do. */
  private Set<Address> rsGroupTableAccessorServers(String groupName) throws IOException {
    RSGroupInfo info =
      RSGroupTableAccessor.getRSGroupInfo(TEST_UTIL.getConnection(), Bytes.toBytes(groupName));
    return info == null ? new HashSet<>() : new HashSet<>(info.getServers());
  }

  /**
   * Fails over to a freshly started master (the minicluster's masters are threads): a backup is
   * started, the active master is stopped, and the shared handles are re-pointed.
   */
  private void restartMaster() throws Exception {
    MiniHBaseCluster miniCluster = TEST_UTIL.getMiniHBaseCluster();
    int oldIndex = -1;
    List<JVMClusterUtil.MasterThread> masters = miniCluster.getMasterThreads();
    for (int i = 0; i < masters.size(); i++) {
      if (masters.get(i).getMaster() == master) {
        oldIndex = i;
      }
    }
    miniCluster.startMaster();
    miniCluster.stopMaster(oldIndex);
    miniCluster.waitOnMaster(oldIndex);
    assertTrue(miniCluster.waitForActiveAndReadyMaster(WAIT_TIMEOUT));
    // Clients bootstrap from the configured master address, which is now the dead master's.
    TEST_UTIL.getConfiguration().set(HConstants.MASTER_ADDRS_KEY,
      miniCluster.getMaster().getServerName().getAddress().toString());
    TEST_UTIL.closeConnection();
    initialize();
  }

  private void clearRegex(String groupName) {
    String key = REGEX_PREFIX + groupName;
    master.getConfiguration().unset(key);
    TEST_UTIL.getConfiguration().unset(key);
    regexConfigKeysSet.remove(key);
  }

  private Address addressOf(JVMClusterUtil.RegionServerThread rst) {
    return rst.getRegionServer().getServerName().getAddress();
  }

  private static class LogCapturer {
    private final StringWriter sw = new StringWriter();
    private final org.apache.logging.log4j.core.appender.WriterAppender appender;
    private final org.apache.logging.log4j.core.Logger logger;

    LogCapturer(org.apache.logging.log4j.core.Logger logger) {
      this.logger = logger;
      this.appender = org.apache.logging.log4j.core.appender.WriterAppender.newBuilder()
        .setName("test-regex-membership").setTarget(sw).build();
      this.appender.start();
      this.logger.addAppender(this.appender);
    }

    String getOutput() {
      return sw.toString();
    }

    void stopCapturing() {
      this.logger.removeAppender(this.appender);
      this.appender.stop();
    }
  }

  private static LogCapturer captureRSGroupInfoManagerLog() {
    return new LogCapturer(
      (org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
        .getLogger(RSGroupInfoManagerImpl.class));
  }

  // ============================== tests ==============================

  @Test
  public void testNoRegexConfigBaseline() throws Exception {
    // No hbase.rsgroup.regex.* configured at all: every server must remain in default, exactly
    // as it would without this feature. This is also implicitly exercised by every other test in
    // this module, but pin it down explicitly for this feature.
    RSGroupInfo defaultGroup = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(NUM_SLAVES_BASE, defaultGroup.getServers().size());
  }

  @Test
  public void testSingleRegexAutoJoinOnLiveAdd() throws Exception {
    String groupName = getGroupName("auto");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.1");

    JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
    Address addr = addressOf(rst);
    try {
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
      // No explicit moveServers call was made -- the server auto-joined purely from the
      // ServerEventsListenerThread reacting to its own arrival.
      assertFalse(
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
    } finally {
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testRegexWithQuantifiersMatches() throws Exception {
    String groupName = getGroupName("quant");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.\\d{1,3}");

    JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
    Address addr = addressOf(rst);
    try {
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
      // No explicit moveServers call was made -- the server auto-joined purely from the
      // ServerEventsListenerThread reacting to its own arrival.
      assertFalse(
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
    } finally {
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testFullStringMatchSemantics() throws Exception {
    // The regex is a strict prefix of the hostname; full-string Pattern#matches semantics must
    // NOT treat this as a match.
    String groupName = getGroupName("prefix");
    rsGroupAdmin.addRSGroup(groupName);
    // Strict prefix of "127.0.0.1" -- must NOT match under full-string Pattern#matches semantics.
    setRegex(groupName, "127\\.0\\.0");

    JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
    Address addr = addressOf(rst);
    try {
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
    } finally {
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testRegexTargetingDefaultGroupIsNoOpAndWarns() throws Exception {
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(RSGroupInfo.DEFAULT_GROUP, "127\\.0\\.0\\..*");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> capturer.getOutput()
        .contains("cannot target the reserved '" + RSGroupInfo.DEFAULT_GROUP + "' group"));
    } finally {
      capturer.stopCapturing();
      clearRegex(RSGroupInfo.DEFAULT_GROUP);
    }
  }

  @Test
  public void testInvalidRegexSyntaxIgnoredAndWarns() throws Exception {
    String groupName = getGroupName("badregex");
    rsGroupAdmin.addRSGroup(groupName);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(groupName, "[");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> capturer.getOutput().contains("Invalid regex '[' for RSGroup '" + groupName + "'"));
    } finally {
      capturer.stopCapturing();
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testInvalidGroupNameKeyIgnoredAndWarns() throws Exception {
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex("not a valid name", "127\\.0\\.0\\..*");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> capturer.getOutput().contains("is not a valid RSGroup name"));
    } finally {
      capturer.stopCapturing();
      clearRegex("not a valid name");
    }
  }

  @Test
  public void testListenerDoesNotStickToStaleServer() throws Exception {
    // A server leaves, then a *different* server with a matching hostname joins later; the
    // listener thread must not get stuck comparing against a stale previous-assignments snapshot.
    String groupName = getGroupName("stale");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\..*");
    try {
      JVMClusterUtil.RegionServerThread first = startFakeHostnameRS("127.0.0.1");
      Address firstAddr = addressOf(first);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(firstAddr));

      stopFakeRegionServer(first);

      // Different startcode (fresh ServerName) under the same matching hostname.
      JVMClusterUtil.RegionServerThread second = startFakeHostnameRS("127.0.0.1");
      Address secondAddr = addressOf(second);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(secondAddr));
    } finally {
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testDanglingGroupFallsBackThenResolvesOnceGroupCreated() throws Exception {
    String groupName = getGroupName("dangling");
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      // Group does not exist yet when the regex is configured and the matching RS joins.
      setRegex(groupName, "127\\.0\\.0\\..*");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> capturer.getOutput()
        .contains("RSGroup '" + groupName + "' does not exist -- create it with addRSGroup"));

      // Now create the group; on the next cycle the server should migrate in, no restart needed.
      rsGroupAdmin.addRSGroup(groupName);
      triggerListenerCycle();
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
    } finally {
      capturer.stopCapturing();
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testAmbiguousMatchFallsBackThenResolvesOnceOverlapFixed() throws Exception {
    String groupOne = getGroupName("ambig1");
    String groupTwo = getGroupName("ambig2");
    rsGroupAdmin.addRSGroup(groupOne);
    rsGroupAdmin.addRSGroup(groupTwo);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(groupOne, "127\\.0\\.0\\..*");
      setRegex(groupTwo, "127\\.0\\.0\\.1");

      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> capturer.getOutput().contains("matches regexes for multiple RSGroups"));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupOne).getServers().contains(addr));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupTwo).getServers().contains(addr));

      // Fix the overlap
      setRegex(groupOne, "10\\.0\\.0\\..*");
      triggerListenerCycle();
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupTwo).getServers().contains(addr));
    } finally {
      capturer.stopCapturing();
      removeRegexGroup(groupOne);
      removeRegexGroup(groupTwo);
    }
  }

  @Test
  public void testRegexCoveringEveryServerEmptiesDefaultAndWarns() throws Exception {
    // Regex placement is always applied: a catch-all regex matches every online server (all base
    // RS share one real hostname), so they all move into the regex group and default is left
    // empty. That is a warning about an unusual configuration, not an error.
    // (Explicit moveServers can never empty default -- an unrelated, longstanding
    // RSGroupInfoManagerImpl#moveServers guard keeps >=1 server there -- so the empty-default
    // state is driven purely through the automatic regex-reconciliation path.)
    String regexGroup = getGroupName("emptydefault");
    rsGroupAdmin.addRSGroup(regexGroup);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(regexGroup, ".*");
      triggerListenerCycle();

      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> capturer.getOutput().contains("leaves RSGroup 'default' with no online servers"));
      assertTrue(capturer.getOutput().contains(RSGroupBasedLoadBalancer.FALLBACK_GROUP_ENABLE_KEY));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(regexGroup).getServers().size() == NUM_SLAVES_BASE);
      assertTrue(rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().isEmpty());

      // Narrow the regex so it only targets a new server, not the base cluster's shared
      // hostname -- the base servers are unmatched again and return to default.
      setRegex(regexGroup, "127\\.0\\.0\\..*");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(regexGroup).getServers().contains(addr)
          && rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size()
              == NUM_SLAVES_BASE);
    } finally {
      capturer.stopCapturing();
      removeRegexGroup(regexGroup);
    }
  }

  @Test
  public void testCatchAllRegexMovesEveryServerAndGroupTablesStayAssigned() throws Exception {
    // A regex group with a live member and a bound table is broadened to a catch-all. Every
    // server is placed in its regex group (default is left empty), and the group's table stays
    // fully assigned on its original member.
    String groupName = getGroupName("catchall");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.1");
    RSGroupBasedLoadBalancer balancer = (RSGroupBasedLoadBalancer) master.getLoadBalancer();
    balancer.setFallbackEnabledForTest(true);
    try {
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
      ServerName sn = getServerName(addr);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 5);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), groupName);
      assertEquals(5, getTableServerRegionMap().get(tableName).get(sn).size());

      setRegex(groupName, ".*");
      triggerListenerCycle();
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> {
        Set<Address> servers = rsGroupAdmin.getRSGroupInfo(groupName).getServers();
        return servers.contains(addr) && servers.size() == NUM_SLAVES_BASE + 1
          && rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().isEmpty();
      });
      assertEquals(5, getTableServerRegionMap().get(tableName).get(sn).size());
    } finally {
      balancer.setFallbackEnabledForTest(false);
      // moveTables requires the target group to have a server, and default was emptied above, so
      // narrow the regex back first to let the base servers return to default.
      setRegex(groupName, "127\\.0\\.0\\.1");
      triggerListenerCycle();
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size()
            == NUM_SLAVES_BASE);
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testStoredServersOfRegexGovernedGroupAreIgnoredOnRefresh() throws Exception {
    String groupName = getGroupName("stale");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.1");
    try {
      JVMClusterUtil.RegionServerThread ipRs = startFakeHostnameRS("127.0.0.1");
      JVMClusterUtil.RegionServerThread nameRs = startFakeHostnameRS("localhost");
      Address ipAddr = addressOf(ipRs);
      Address nameAddr = addressOf(nameRs);
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> rsGroupTableAccessorServers(groupName).contains(ipAddr)
        && rsGroupTableAccessorServers(RSGroupInfo.DEFAULT_GROUP).contains(nameAddr));
      assertFalse(rsGroupTableAccessorServers(groupName).contains(nameAddr));

      // Point the regex at the other server and fail over. A new master copies the minicluster
      // configuration, so set it there too. The table still stores the old membership, which
      // refresh must ignore.
      setRegex(groupName, "localhost");
      TEST_UTIL.getMiniHBaseCluster().getConfiguration().set(REGEX_PREFIX + groupName, "localhost");
      restartMaster();

      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(nameAddr));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(ipAddr));
      // The startup flush rewrites the stored copy from the recomputed membership.
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupTableAccessorServers(groupName).contains(nameAddr)
          && !rsGroupTableAccessorServers(groupName).contains(ipAddr)
          && rsGroupTableAccessorServers(RSGroupInfo.DEFAULT_GROUP).contains(ipAddr));
    } finally {
      TEST_UTIL.getMiniHBaseCluster().getConfiguration().unset(REGEX_PREFIX + groupName);
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testRegexGovernedGroupMembershipIsPersistedForTableReaders() throws Exception {
    // Regex-governed membership is computed on the fly, so it is visible even while hbase:rsgroup
    // is unavailable. Once the table is back, a flush stores it so that direct readers of the
    // table (RSGroupTableAccessor) see it too.
    String groupName = getGroupName("persist");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.1");
    boolean tableDisabled = false;
    try {
      TEST_UTIL.getAdmin().disableTable(RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME);
      tableDisabled = true;

      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));

      TEST_UTIL.getAdmin().enableTable(RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME);
      tableDisabled = false;
      // Any group mutation flushes every group.
      String flushGroup = getGroupName("persistflush");
      rsGroupAdmin.addRSGroup(flushGroup);
      removeGroup(flushGroup);

      assertTrue(rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
      assertTrue(rsGroupTableAccessorServers(groupName).contains(addr));
    } finally {
      if (tableDisabled) {
        TEST_UTIL.getAdmin().enableTable(RSGroupInfoManagerImpl.RSGROUP_TABLE_NAME);
      }
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testMoveContradictingRegexIsRejected() throws Exception {
    String groupName = getGroupName("reject");
    String otherGroup = getGroupName("rejectother");
    rsGroupAdmin.addRSGroup(groupName);
    rsGroupAdmin.addRSGroup(otherGroup);
    setRegex(groupName, "127\\.0\\.0\\..*");
    try {
      // A server whose hostname does not match the regex cannot be moved into the regex group ...
      JVMClusterUtil.RegionServerThread nonMatching = startFakeHostnameRS("localhost");
      Address nonMatchingAddr = addressOf(nonMatching);
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP)
        .getServers().contains(nonMatchingAddr));
      assertThrows(IOException.class,
        () -> rsGroupAdmin.moveServers(Sets.newHashSet(nonMatchingAddr), groupName));
      assertTrue(rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers()
        .contains(nonMatchingAddr));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(nonMatchingAddr));

      // ... but can still be placed in any other group.
      rsGroupAdmin.moveServers(Sets.newHashSet(nonMatchingAddr), otherGroup);
      assertTrue(rsGroupAdmin.getRSGroupInfo(otherGroup).getServers().contains(nonMatchingAddr));
      rsGroupAdmin.moveServers(Sets.newHashSet(nonMatchingAddr), RSGroupInfo.DEFAULT_GROUP);

      // A server the regex claims cannot be moved out of, or to any group other than, its group.
      JVMClusterUtil.RegionServerThread matching = startFakeHostnameRS("127.0.0.1");
      Address matchingAddr = addressOf(matching);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(matchingAddr));
      assertThrows(IOException.class,
        () -> rsGroupAdmin.moveServers(Sets.newHashSet(matchingAddr), RSGroupInfo.DEFAULT_GROUP));
      assertThrows(IOException.class,
        () -> rsGroupAdmin.moveServers(Sets.newHashSet(matchingAddr), otherGroup));
      assertTrue(rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(matchingAddr));
    } finally {
      removeGroup(otherGroup);
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testUnrelatedAdminOpsSucceedUnderActiveRegex() throws Exception {
    String regexGroup = getGroupName("activeregex");
    String otherGroup = getGroupName("other");
    rsGroupAdmin.addRSGroup(regexGroup);
    setRegex(regexGroup, "127\\.0\\.0\\..*");
    try {
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(regexGroup).getServers().contains(addr));

      // Unrelated admin operations must not be spuriously blocked by the invariant check.
      // moveTables requires the target group to already have at least one server, so use a
      // base (non-regex-matched, admin-manageable) server rather than an empty new group.
      RSGroupInfo otherGroupInfo = addGroup(otherGroup, 1);
      Set<Address> otherGroupServers = otherGroupInfo.getServers();
      TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), otherGroup);
      assertTrue(rsGroupAdmin.getRSGroupInfo(otherGroup).getTables().contains(tableName));

      String renamedGroup = otherGroup + "_renamed";
      rsGroupAdmin.renameRSGroup(otherGroup, renamedGroup);
      RSGroupInfo renamedGroupInfo = rsGroupAdmin.getRSGroupInfo(renamedGroup);
      assertEquals(otherGroupServers, renamedGroupInfo.getServers());
      assertTrue(renamedGroupInfo.getTables().contains(tableName));
      assertFalse(
        rsGroupAdmin.listRSGroups().stream().anyMatch(g -> g.getName().equals(otherGroup)));

      rsGroupAdmin.moveTables(rsGroupAdmin.getRSGroupInfo(renamedGroup).getTables(),
        RSGroupInfo.DEFAULT_GROUP);
      assertTrue(
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getTables().contains(tableName));
      assertTrue(rsGroupAdmin.getRSGroupInfo(renamedGroup).getTables().isEmpty());

      rsGroupAdmin.moveServers(rsGroupAdmin.getRSGroupInfo(renamedGroup).getServers(),
        RSGroupInfo.DEFAULT_GROUP);
      Set<Address> defaultServersAfterMoveBack =
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers();
      for (Address server : otherGroupServers) {
        assertTrue(defaultServersAfterMoveBack.contains(server));
      }
      assertTrue(rsGroupAdmin.getRSGroupInfo(renamedGroup).getServers().isEmpty());

      rsGroupAdmin.removeRSGroup(renamedGroup);
      assertFalse(
        rsGroupAdmin.listRSGroups().stream().anyMatch(g -> g.getName().equals(renamedGroup)));

      // Confirm the unrelated operations above never disturbed the active regex-governed group.
      assertTrue(rsGroupAdmin.getRSGroupInfo(regexGroup).getServers().contains(addr));
    } finally {
      TEST_UTIL.deleteTable(tableName);
      removeRegexGroup(regexGroup);
    }
  }

  @Test
  public void testAutomaticMoveDoesNotFireCoprocessorHooks() throws Exception {
    String groupName = getGroupName("hooks");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\..*");
    try {
      assertFalse(observer.preMoveServersCalled);
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
      // Purely automatic, listener-thread-driven reassignment must not go through the
      // RSGroupAdminServer RPC surface, so the moveServers coprocessor hooks must not fire.
      assertFalse(observer.preMoveServersCalled);
      assertFalse(observer.postMoveServersCalled);

      // An explicit admin-driven move, by contrast, does fire the hooks. Use one of the
      // base cluster's own (non-regex-matched) RS for this, rather than disturbing the
      // regex-matched fake RS started above, which keeps running under its active regex.
      String otherGroup = getGroupName("hooksother");
      rsGroupAdmin.addRSGroup(otherGroup);
      Address baseServerAddr =
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().iterator().next();
      rsGroupAdmin.moveServers(Sets.newHashSet(baseServerAddr), otherGroup);
      assertTrue(observer.preMoveServersCalled);
      assertTrue(observer.postMoveServersCalled);
      removeGroup(otherGroup);

      // The unrelated explicit move above must not have disturbed the regex-governed group.
      assertTrue(rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
    } finally {
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testRefreshRemovesRegexMatchedServerFromStoredAdminGroup() throws Exception {
    // A server stored in an admin-managed group before a regex claimed it must not end up in two
    // groups after a manager restart. refresh cannot reject (it would wedge startup), so the regex
    // wins in memory and a WARN is logged.
    String adminGroup = getGroupName("refreshadmin");
    String regexGroup = getGroupName("refreshregex");
    rsGroupAdmin.addRSGroup(adminGroup);
    rsGroupAdmin.addRSGroup(regexGroup);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      rsGroupAdmin.moveServers(Sets.newHashSet(addr), adminGroup);

      // The regex arrives while the server is already stored in the admin group. A new master
      // copies the minicluster configuration, so set it there too before the failover.
      setRegex(regexGroup, "127\\.0\\.0\\..*");
      TEST_UTIL.getMiniHBaseCluster().getConfiguration().set(REGEX_PREFIX + regexGroup,
        "127\\.0\\.0\\..*");
      restartMaster();
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(regexGroup).getServers().contains(addr));
      assertFalse(rsGroupAdmin.getRSGroupInfo(adminGroup).getServers().contains(addr));
      assertFalse(
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      assertTrue(capturer.getOutput().contains("are stored in RSGroup '" + adminGroup + "'"));

      // The startup flush replaces the stale stored copy: the admin group no longer holds the
      // server and the regex-governed group stores it.
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> !rsGroupTableAccessorServers(adminGroup).contains(addr)
        && rsGroupTableAccessorServers(regexGroup).contains(addr));
    } finally {
      capturer.stopCapturing();
      TEST_UTIL.getMiniHBaseCluster().getConfiguration().unset(REGEX_PREFIX + regexGroup);
      removeRegexGroup(regexGroup);
      removeGroup(adminGroup);
    }
  }

  @Test
  public void testRegexConfigDriftDoesNotBlockUnrelatedAdminOps() throws Exception {
    // A regex that contradicts where a server was explicitly placed must not block any unrelated
    // admin operation.
    String groupName = getGroupName("drift");
    String bystanderGroup = getGroupName("bystander");
    rsGroupAdmin.addRSGroup(groupName);
    rsGroupAdmin.addRSGroup(bystanderGroup);
    String renamedBystander = bystanderGroup + "_renamed";
    try {
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));

      // Legal at the time: no regex governs groupName yet.
      rsGroupAdmin.moveServers(Sets.newHashSet(addr), groupName);

      // Now introduce a regex for groupName that does NOT match the server already placed there.
      // No listener cycle runs, so the contradiction stays un-reconciled.
      setRegex(groupName, "10\\.0\\.0\\..*");

      rsGroupAdmin.renameRSGroup(bystanderGroup, renamedBystander);
      assertTrue(
        rsGroupAdmin.listRSGroups().stream().anyMatch(g -> g.getName().equals(renamedBystander)));
    } finally {
      removeRegexGroup(groupName);
      removeGroup(
        rsGroupAdmin.getRSGroupInfo(bystanderGroup) != null ? bystanderGroup : renamedBystander);
    }
  }

  @Test
  public void testNewlyJoinedRegexMemberReceivesRegionsOnBalance() throws Exception {
    // A new RS auto-joins an already-regex-governed group while the group's sole existing
    // member holds every region of a table bound to that group; running the balancer must then
    // hand some of those regions to the newly-joined RS too. The first-member baseline is
    // established by moveTables itself: RSGroupAdminServer#moveTables synchronously relocates
    // every region of a table with no RSGroup-scoped descriptor yet onto the target group's
    // servers (see moveTableRegionsToGroup, which blocks on the move futures before returning),
    // so with sn1 as the sole group member at that point all regions are already there once
    // moveTables returns -- no separate balance step is needed for that part. Only the
    // second-member handoff genuinely needs an explicit balance: adding a group member does not
    // by itself retrigger any region movement.
    String groupName = getGroupName("balancenewmember");
    rsGroupAdmin.addRSGroup(groupName);
    // "127.0.0.1" and "localhost" are the only two distinct, real, independently-reachable
    // identities startFakeHostnameRS supports (its RPC bind address is always "localhost"
    // regardless of advertised hostname).
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    try {
      JVMClusterUtil.RegionServerThread rst1 = startFakeHostnameRS("127.0.0.1");
      Address addr1 = addressOf(rst1);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr1));
      ServerName sn1 = getServerName(addr1);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), groupName);
      Map<ServerName, List<String>> perServerAfterMove = getTableServerRegionMap().get(tableName);
      assertEquals(10, perServerAfterMove.get(sn1).size());

      admin.balancerSwitch(true, true);
      try {
        // A second RS matching the same regex joins while the group is already active; it must
        // auto-join with no explicit RPC.
        JVMClusterUtil.RegionServerThread rst2 = startFakeHostnameRS("localhost");
        Address addr2 = addressOf(rst2);
        TEST_UTIL.waitFor(WAIT_TIMEOUT,
          () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr2));
        ServerName sn2 = getServerName(addr2);

        rsGroupAdmin.balanceRSGroup(groupName);
        TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> {
          Map<ServerName, List<String>> perServer = getTableServerRegionMap().get(tableName);
          if (perServer == null) {
            return false;
          }
          List<String> onNewMember = perServer.get(sn2);
          if (onNewMember == null || onNewMember.isEmpty()) {
            return false;
          }
          int total = perServer.values().stream().mapToInt(List::size).sum();
          return total == 10;
        });
      } finally {
        admin.balancerSwitch(false, true);
      }
    } finally {
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testRestartTriggeredMembershipConfinesTableToGroupServers() throws Exception {
    // RS join normally (no regex active yet), landing in default like any other RS. Only
    // afterward is a regex configured targeting their hostnames; a bare config change does not
    // reconcile membership by itself, so an actual lifecycle event -- restarting one of the two
    // RS -- is used to force the listener thread to notice and migrate both into the new group
    // (the recompute triggered by one server's add/remove event considers every online server,
    // not just the one that triggered it). A table is then bound to that group, and its regions
    // must land only on the group's members, with those members hosting only that table's
    // regions -- a bidirectional isolation check.
    String groupName = getGroupName("restartisolation");
    // "127.0.0.1" and "localhost" are the only two distinct, real, independently-reachable
    // identities startFakeHostnameRS supports (its RPC bind address is always "localhost"
    // regardless of advertised hostname -- an arbitrary loopback alias like "127.0.0.2" would
    // advertise an address nothing is actually listening on, stalling any real region-open RPC).
    JVMClusterUtil.RegionServerThread rst1 = startFakeHostnameRS("127.0.0.1");
    JVMClusterUtil.RegionServerThread rst2 = startFakeHostnameRS("localhost");
    Address addr2 = addressOf(rst2);
    TEST_UTIL.waitFor(WAIT_TIMEOUT,
      () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr2));

    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    try {
      stopFakeRegionServer(rst1);
      JVMClusterUtil.RegionServerThread rst1Restarted = startFakeHostnameRS("127.0.0.1");
      Address addr1Restarted = addressOf(rst1Restarted);

      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr1Restarted));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr2));

      ServerName sn1 = getServerName(addr1Restarted);
      ServerName sn2 = getServerName(addr2);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      // moveTables synchronously relocates every region of a table with no RSGroup-scoped
      // descriptor yet onto one of the target group's servers (moveTableRegionsToGroup blocks on
      // the move futures before returning), so confinement to {sn1, sn2} is already guaranteed
      // once this call returns -- no separate balance step is needed.
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), groupName);
      Map<ServerName, List<String>> perServerAfterMove = getTableServerRegionMap().get(tableName);
      int totalAfterMove = 0;
      for (Map.Entry<ServerName, List<String>> entry : perServerAfterMove.entrySet()) {
        assertTrue(entry.getKey().equals(sn1) || entry.getKey().equals(sn2));
        totalAfterMove += entry.getValue().size();
      }
      assertEquals(6, totalAfterMove);

      // Bidirectional isolation: the group's own members must host nothing but this table.
      for (RegionInfo region : admin.getRegions(sn1)) {
        assertEquals(tableName, region.getTable());
      }
      for (RegionInfo region : admin.getRegions(sn2)) {
        assertEquals(tableName, region.getTable());
      }
    } finally {
      removeRegexGroup(groupName);
    }
  }

  @Test
  public void testUngracefulCrashOfRegexMemberReassignsRegionsOnlyToOtherGroupMembers()
    throws Exception {
    // ServerCrashProcedure recovers a dead server's regions by default with
    // hbase.master.scp.retain.assignment=false, i.e. forceNewPlan=true for every ASSIGN TRSP it
    // creates -- which routes through roundRobinAssignment rather than retainAssignment (see
    // ServerCrashProcedure#assignRegions). This test's job is to confirm
    // RSGroupBasedLoadBalancer#roundRobinAssignment still confines the
    // crashed regex member's regions to the *other* online members of its own group -- never to
    // default or an unrelated group -- rather than trusting the RSGroup-partitioning logic to
    // "just work". It also asserts, via the test-only call-tracking flags, exactly which
    // assignment methods actually ran for each step: randomAssignment + retainAssignment during
    // moveTables, and roundRobinAssignment (not retainAssignment) during crash recovery.
    String groupName = getGroupName("crashroundrobin");
    rsGroupAdmin.addRSGroup(groupName);
    // "127.0.0.1" and "localhost" are the only two distinct, real, independently-reachable
    // identities startFakeHostnameRS supports (its RPC bind address is always "localhost"
    // regardless of advertised hostname).
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    // A second, unrelated RSGroup holding one server borrowed from default -- proves crash
    // recovery's per-group candidate filtering excludes every other group, not just default.
    String otherGroupName = getGroupName("crashroundrobinother");
    RSGroupInfo otherGroupInfo = addGroup(otherGroupName, 1);
    ServerName otherGroupServer = getServerName(otherGroupInfo.getServers().iterator().next());
    RSGroupBasedLoadBalancer balancer = (RSGroupBasedLoadBalancer) master.getLoadBalancer();
    try {
      JVMClusterUtil.RegionServerThread rst1 = startFakeHostnameRS("127.0.0.1");
      Address addr1 = addressOf(rst1);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr1));
      ServerName sn1 = getServerName(addr1);

      JVMClusterUtil.RegionServerThread rst2 = startFakeHostnameRS("localhost");
      Address addr2 = addressOf(rst2);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr2));
      ServerName sn2 = getServerName(addr2);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      // moveTables synchronously relocates every region of a table with no RSGroup-scoped
      // descriptor yet onto the target group's servers (moveTableRegionsToGroup blocks on the
      // move futures before returning), so confinement to {sn1, sn2} is already guaranteed once
      // this call returns.
      balancer.resetAssignmentCallFlagsForTest();
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), groupName);
      // moveTableRegionsToGroup picks each region's destination via randomAssignment, then moves
      // it there via a TRSP with a non-null target (forceNewPlan=false), which the
      // AssignmentManager
      // queue routes through retainAssignment to commit.
      assertTrue(balancer.isRandomAssignmentInvoked());
      assertTrue(balancer.isRetainAssignmentInvoked());
      assertFalse(balancer.isRoundRobinAssignmentInvoked());
      Map<ServerName, List<String>> perServerBeforeCrash = getTableServerRegionMap().get(tableName);
      int totalBeforeCrash = 0;
      for (Map.Entry<ServerName, List<String>> entry : perServerBeforeCrash.entrySet()) {
        assertTrue(entry.getKey().equals(sn1) || entry.getKey().equals(sn2));
        totalBeforeCrash += entry.getValue().size();
      }
      assertEquals(6, totalBeforeCrash);

      // retainAssignment's random fallback (BaseLoadBalancer#randomAssignment) places each of
      // these regions independently, so it is possible -- roughly 3% of runs -- for all 6 to land
      // on one of the two servers alone. Crash whichever server actually has regions, so
      // ServerCrashProcedure always has at least one region to reassign and roundRobinAssignment
      // is guaranteed to fire; default to the usual sn1-crashes/sn2-survives case otherwise.
      boolean sn1HasNoRegions =
        !perServerBeforeCrash.containsKey(sn1) || perServerBeforeCrash.get(sn1).isEmpty();
      JVMClusterUtil.RegionServerThread rstToCrash = sn1HasNoRegions ? rst2 : rst1;
      ServerName survivor = sn1HasNoRegions ? sn1 : sn2;

      // Simulate an ungraceful crash
      balancer.resetAssignmentCallFlagsForTest();
      killFakeRegionServer(rstToCrash);

      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> {
        Map<ServerName, List<String>> perServer = getTableServerRegionMap().get(tableName);
        if (perServer == null) {
          return false;
        }
        int total = 0;
        for (Map.Entry<ServerName, List<String>> entry : perServer.entrySet()) {
          // Every region must have landed on the group's sole remaining member -- never on
          // default or any other group, and never back on the now-dead server.
          if (!entry.getKey().equals(survivor)) {
            return false;
          }
          total += entry.getValue().size();
        }
        return total == 6;
      });
      // Confirms the crash-recovery ASSIGN TRSPs went through roundRobinAssignment, not
      // retainAssignment -- expected since hbase.master.scp.retain.assignment defaults to false.
      assertTrue(balancer.isRoundRobinAssignmentInvoked());
      assertFalse(balancer.isRetainAssignmentInvoked());

      // The surviving group member must host nothing but this table's regions.
      for (RegionInfo region : admin.getRegions(survivor)) {
        assertEquals(tableName, region.getTable());
      }
      // The unrelated group's own member must not have received any of the crashed member's
      // regions either -- recovery must stay confined to the crashed server's own group.
      for (RegionInfo region : admin.getRegions(otherGroupServer)) {
        assertFalse(region.getTable().equals(tableName));
      }
    } finally {
      removeRegexGroup(groupName);
      removeGroup(otherGroupName);
    }
  }

  @Test
  public void testFallbackWhenAllRegexMembersCrash() throws Exception {
    // RSGroupBasedLoadBalancer#generateGroupAssignments only ever looks at a group's currently
    // online configured servers, regardless of how they became members -- regex-driven or
    // admin-driven membership is indistinguishable to it. This confirms the
    // hbase.rsgroup.fallback.enable=true fallback-to-default path works the same way when the
    // group that loses every one of its servers is regex-governed.
    String groupName = getGroupName("fallbackregex");
    rsGroupAdmin.addRSGroup(groupName);
    // "127.0.0.1" and "localhost" are the only two distinct, real, independently-reachable
    // identities startFakeHostnameRS supports (its RPC bind address is always "localhost"
    // regardless of advertised hostname).
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    RSGroupBasedLoadBalancer balancer = (RSGroupBasedLoadBalancer) master.getLoadBalancer();
    balancer.setFallbackEnabledForTest(true);
    try {
      JVMClusterUtil.RegionServerThread rst1 = startFakeHostnameRS("127.0.0.1");
      Address addr1 = addressOf(rst1);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr1));
      ServerName sn1 = getServerName(addr1);

      JVMClusterUtil.RegionServerThread rst2 = startFakeHostnameRS("localhost");
      Address addr2 = addressOf(rst2);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr2));
      ServerName sn2 = getServerName(addr2);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      // moveTables synchronously relocates every region of a table with no RSGroup-scoped
      // descriptor yet onto the target group's servers (moveTableRegionsToGroup blocks on the
      // move futures before returning), so confinement to {sn1, sn2} is already guaranteed once
      // this call returns.
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), groupName);

      // Both regex members must be hosting regions of the table before either is killed.
      Map<ServerName, List<String>> perServerBeforeCrash = getTableServerRegionMap().get(tableName);
      int totalBeforeCrash = 0;
      for (ServerName member : new ServerName[] { sn1, sn2 }) {
        List<String> regionsOnMember = perServerBeforeCrash.get(member);
        assertFalse(regionsOnMember == null || regionsOnMember.isEmpty());
        totalBeforeCrash += regionsOnMember.size();
      }
      assertEquals(6, totalBeforeCrash);

      // Kill both regex members -- the group is left with zero online servers. Unlike the base
      // cluster's servers (which remain in default throughout this test), these two fake RS were
      // never part of default, so default keeps its original online servers and the fallback
      // path lands there directly, with no need for a second-level fallback.
      killFakeRegionServer(rst1);
      killFakeRegionServer(rst2);

      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> {
        Map<ServerName, List<String>> perServer = getTableServerRegionMap().get(tableName);
        if (perServer == null) {
          return false;
        }
        RSGroupInfo defaultGroup;
        try {
          defaultGroup = rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP);
        } catch (IOException e) {
          return false;
        }
        int total = 0;
        for (Map.Entry<ServerName, List<String>> entry : perServer.entrySet()) {
          // Every region must have landed on a server that is currently in default -- never left
          // stranded on the now-dead sn1/sn2, and never on any unrelated group.
          if (!defaultGroup.getServers().contains(entry.getKey().getAddress())) {
            return false;
          }
          total += entry.getValue().size();
        }
        return total == 6;
      });
    } finally {
      balancer.setFallbackEnabledForTest(false);
      removeRegexGroup(groupName);
    }
  }
}
