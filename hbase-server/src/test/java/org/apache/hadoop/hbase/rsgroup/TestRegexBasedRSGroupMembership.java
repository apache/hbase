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
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RSGroupTests;
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
 * {@link org.apache.hadoop.hbase.client.Admin} RSGroup API, live RegionServer add/remove against
 * the minicluster, and captured log output -- no reflection or visibility changes against
 * {@link RSGroupInfoManagerImpl}.
 */
@Tag(RSGroupTests.TAG)
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
    // The MASTER/ADMIN Configuration objects are shared across every test method in this class;
    // undo any regex config this test added to both so it can't leak into the next method.
    boolean hadLeftoverRegex = !regexConfigKeysSet.isEmpty();
    for (String key : regexConfigKeysSet) {
      MASTER.getConfiguration().unset(key);
      ADMIN.getConfiguration().unset(key);
    }
    regexConfigKeysSet.clear();
    if (hadLeftoverRegex) {
      // See #clearRegex -- a server this now-cleared regex had auto-claimed stays put in the
      // live view until a serverAdded/serverRemoved event fires. Force one so
      // tearDownAfterMethod's group cleanup (-> VerifyingRSGroupAdmin#verify) doesn't see a
      // stale claim.
      triggerListenerCycle();
    }
    tearDownAfterMethod();
    RSGroupBasedLoadBalancer.resetAssignmentCallFlagsForTest();
    ((RSGroupBasedLoadBalancer) MASTER.getLoadBalancer()).setFallbackEnabledForTest(false);
  }

  // ============================== helpers ==============================

  /**
   * Starts a genuinely new RegionServer process (thread) in the shared minicluster that reports
   * itself under a caller-chosen hostname, so regex membership rules can target it individually.
   * The base cluster's real RS all share one real, resolvable hostname (this machine's address);
   * the RPC services constructor unconditionally resolves whatever hostname is configured
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
    LocalHBaseCluster localCluster = ((SingleProcessHBaseCluster) CLUSTER).hbaseCluster;
    JVMClusterUtil.RegionServerThread rst =
      localCluster.addRegionServer(rsConf, localCluster.getRegionServers().size());
    rst.start();
    rst.waitForServerOnline();
    fakeRegionServers.add(rst);
    final ServerName sn = rst.getRegionServer().getServerName();
    TEST_UTIL.waitFor(WAIT_TIMEOUT,
      () -> MASTER.getServerManager().getOnlineServersList().contains(sn));
    LOG.info("Started fake-hostname RS {}", sn);
    return rst;
  }

  private void stopFakeRegionServer(JVMClusterUtil.RegionServerThread rst) throws Exception {
    fakeRegionServers.remove(rst);
    final ServerName sn = rst.getRegionServer().getServerName();
    rst.getRegionServer().stop("test cleanup");
    TEST_UTIL.waitFor(WAIT_TIMEOUT,
      () -> !MASTER.getServerManager().getOnlineServersList().contains(sn));
    LOG.info("Stopped fake-hostname RS {}", sn);
  }

  private void killFakeRegionServer(JVMClusterUtil.RegionServerThread rst) throws Exception {
    fakeRegionServers.remove(rst);
    final ServerName sn = rst.getRegionServer().getServerName();
    ((SingleProcessHBaseCluster) CLUSTER).killRegionServer(sn);
    TEST_UTIL.waitFor(WAIT_TIMEOUT,
      () -> !MASTER.getServerManager().getOnlineServersList().contains(sn));
    LOG.info("Killed fake-hostname RS {}", sn);
  }

  /**
   * Server-membership recompute happens on the background {@code ServerEventsListenerThread}, woken
   * via {@code notify()} from the {@code serverAdded}/{@code serverRemoved} callback -- it is only
   * ever triggered by an actual server add/remove event, a bare config mutation is not enough.
   * Forces such an event (add+remove a bystander RS) so a config change just made takes effect
   * without requiring an explicit RSGroup admin RPC. Only used by tests that *want* the automatic
   * reconciliation to run and correct things; the trigger RS's own transient membership doesn't
   * affect assertions elsewhere since those only check for specific other servers' addresses.
   * Because the recompute runs asynchronously on that thread, callers still need to poll (e.g. via
   * {@code TEST_UTIL.waitFor}) for its actual effect rather than assuming it has landed the instant
   * this method returns.
   */
  private void triggerListenerCycle() throws Exception {
    JVMClusterUtil.RegionServerThread trigger = startFakeHostnameRS("127.0.0.1");
    stopFakeRegionServer(trigger);
  }

  private void setRegex(String groupName, String regex) {
    String key = REGEX_PREFIX + groupName;
    MASTER.getConfiguration().set(key, regex);
    // ADMIN's own Configuration is a distinct object from MASTER's (the minicluster deep-copies
    // configuration per daemon), so mirror the change there too -- VerifyingRSGroupAdmin#verify
    // reads it to independently resolve regex-based membership the same way MASTER does.
    ADMIN.getConfiguration().set(key, regex);
    regexConfigKeysSet.add(key);
  }

  private void clearRegex(String groupName) throws Exception {
    String key = REGEX_PREFIX + groupName;
    MASTER.getConfiguration().unset(key);
    ADMIN.getConfiguration().unset(key);
    regexConfigKeysSet.remove(key);
    // Same story as #triggerListenerCycle's javadoc: recompute only runs on a serverAdded/
    // serverRemoved event, so a server this regex had auto-claimed stays put in the live view
    // until one fires. Force one now so cleanup (removeGroup -> VerifyingRSGroupAdmin#verify)
    // doesn't see a stale claim this now-cleared regex can no longer justify -- regex-governed
    // membership is persisted exactly like any other group's, so verify's plain persisted-vs-live
    // comparison would otherwise report the leftover member as a real discrepancy.
    triggerListenerCycle();
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
    RSGroupInfo defaultGroup = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    assertEquals(NUM_SLAVES_BASE, defaultGroup.getServers().size());
  }

  @Test
  public void testSingleRegexAutoJoinOnLiveAdd() throws Exception {
    String groupName = getGroupName("auto");
    ADMIN.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.1");

    JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
    Address addr = addressOf(rst);
    try {
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr));
      // No explicit moveServersToRSGroup call was made -- the server auto-joined purely from the
      // ServerListener reacting to its own arrival.
      assertFalse(ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
    }
  }

  @Test
  public void testRegexWithQuantifiersMatches() throws Exception {
    String groupName = getGroupName("quant");
    ADMIN.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.\\d{1,3}");

    JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
    Address addr = addressOf(rst);
    try {
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr));
      // No explicit moveServersToRSGroup call was made -- the server auto-joined purely from the
      // ServerListener reacting to its own arrival.
      assertFalse(ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
    }
  }

  @Test
  public void testFullStringMatchSemantics() throws Exception {
    // The regex is a strict prefix of the hostname; full-string Pattern#matches semantics must
    // NOT treat this as a match.
    String groupName = getGroupName("prefix");
    ADMIN.addRSGroup(groupName);
    // Strict prefix of "127.0.0.1" -- must NOT match under full-string Pattern#matches semantics.
    setRegex(groupName, "127\\.0\\.0");

    JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
    Address addr = addressOf(rst);
    try {
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      assertFalse(ADMIN.getRSGroup(groupName).getServers().contains(addr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
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
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
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
    ADMIN.addRSGroup(groupName);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(groupName, "[");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> capturer.getOutput().contains("Invalid regex '[' for RSGroup '" + groupName + "'"));
    } finally {
      capturer.stopCapturing();
      clearRegex(groupName);
      removeGroup(groupName);
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
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
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
    // recompute must not get stuck comparing against a stale previous-assignments snapshot.
    String groupName = getGroupName("stale");
    ADMIN.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\..*");
    try {
      JVMClusterUtil.RegionServerThread first = startFakeHostnameRS("127.0.0.1");
      Address firstAddr = addressOf(first);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(firstAddr));

      stopFakeRegionServer(first);

      // Different startcode (fresh ServerName) under the same matching hostname.
      JVMClusterUtil.RegionServerThread second = startFakeHostnameRS("127.0.0.1");
      Address secondAddr = addressOf(second);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(secondAddr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
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
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> capturer.getOutput()
        .contains("RSGroup '" + groupName + "' does not exist -- create it with addRSGroup"));

      // Now create the group; on the next cycle the server should migrate in, no restart needed.
      ADMIN.addRSGroup(groupName);
      triggerListenerCycle();
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr));
    } finally {
      capturer.stopCapturing();
      clearRegex(groupName);
      removeGroup(groupName);
    }
  }

  @Test
  public void testAmbiguousMatchFallsBackThenResolvesOnceOverlapFixed() throws Exception {
    String groupOne = getGroupName("ambig1");
    String groupTwo = getGroupName("ambig2");
    ADMIN.addRSGroup(groupOne);
    ADMIN.addRSGroup(groupTwo);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(groupOne, "127\\.0\\.0\\..*");
      setRegex(groupTwo, "127\\.0\\.0\\.1");

      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> capturer.getOutput().contains("matches regexes for multiple RSGroups"));
      assertFalse(ADMIN.getRSGroup(groupOne).getServers().contains(addr));
      assertFalse(ADMIN.getRSGroup(groupTwo).getServers().contains(addr));

      // Fix the overlap
      setRegex(groupOne, "10\\.0\\.0\\..*");
      triggerListenerCycle();
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> ADMIN.getRSGroup(groupTwo).getServers().contains(addr));
    } finally {
      capturer.stopCapturing();
      clearRegex(groupOne);
      clearRegex(groupTwo);
      removeGroup(groupOne);
      removeGroup(groupTwo);
    }
  }

  @Test
  public void testEmptyDefaultGroupGuardTripsAndClears() throws Exception {
    // Explicit moveServersToRSGroup can never empty default in one call (a longstanding,
    // unrelated RSGroupAdminServer guard always keeps >=1 server there), so parking every base RS
    // in an admin-managed group first (as an earlier version of this test tried) is a non-starter.
    // The automatic regex-reconciliation path bypasses that admin-level guard entirely, so instead
    // we drive the empty-default scenario purely through it: a catch-all regex matches every
    // online server at once (all base RS share one real hostname), with no explicit move at all.
    String regexGroup = getGroupName("guardregex");
    ADMIN.addRSGroup(regexGroup);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(regexGroup, ".*");
      triggerListenerCycle();

      // Every online server (all base RS) now matches the catch-all -- the guard must trip
      // rather than emptying default: base servers stay exactly where they are.
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> capturer.getOutput()
        .contains("would leave RSGroup 'default' with no online servers"));
      assertEquals(NUM_SLAVES_BASE,
        ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size());
      assertTrue(ADMIN.getRSGroup(regexGroup).getServers().isEmpty());

      // Narrow the regex so it only targets a new server, not the base cluster's shared
      // hostname -- the guard's precondition no longer covers every online server (the base
      // servers are unmatched again), so it clears and the new server is free to join.
      setRegex(regexGroup, "127\\.0\\.0\\..*");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(regexGroup).getServers().contains(addr));
      assertEquals(NUM_SLAVES_BASE,
        ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    } finally {
      capturer.stopCapturing();
      clearRegex(regexGroup);
      removeGroup(regexGroup);
    }
  }

  @Test
  public void testExplicitMoveViolatingInvariantRejected() throws Exception {
    String groupName = getGroupName("reject");
    ADMIN.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\..*");
    try {
      // A server whose hostname does NOT match the regex may not be explicitly moved into a
      // regex-governed group.
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("localhost");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));

      assertThrows(DoNotRetryIOException.class,
        () -> ADMIN.moveServersToRSGroup(Sets.newHashSet(addr), groupName));

      // No partial mutation: server is still exactly where it started.
      assertTrue(ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));
      assertFalse(ADMIN.getRSGroup(groupName).getServers().contains(addr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
    }
  }

  @Test
  public void testUnrelatedAdminOpsSucceedUnderActiveRegex() throws Exception {
    String regexGroup = getGroupName("activeregex");
    String otherGroup = getGroupName("other");
    ADMIN.addRSGroup(regexGroup);
    setRegex(regexGroup, "127\\.0\\.0\\..*");
    try {
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(regexGroup).getServers().contains(addr));

      // Unrelated admin operations must not be spuriously blocked by the invariant check.
      // setRSGroup requires the target group to already have at least one server, so use a
      // base (non-regex-matched, admin-manageable) server rather than an empty new group.
      RSGroupInfo otherGroupInfo = addGroup(otherGroup, 1);
      Set<Address> otherGroupServers = otherGroupInfo.getServers();
      TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));
      ADMIN.setRSGroup(Sets.newHashSet(tableName), otherGroup);
      // Table-to-group membership lives on the TableDescriptor (RegionServerGroup), not in
      // RSGroupInfo#getTables() -- the latter is only ever populated by flushConfig/refresh from
      // persisted storage and setRSGroup never touches it. ADMIN.getRSGroup(TableName) is the
      // live, authoritative direction to check this (see RSGroupUtil#getRSGroupInfo).
      assertEquals(otherGroup, ADMIN.getRSGroup(tableName).getName());

      String renamedGroup = otherGroup + "_renamed";
      ADMIN.renameRSGroup(otherGroup, renamedGroup);
      RSGroupInfo renamedGroupInfo = ADMIN.getRSGroup(renamedGroup);
      assertEquals(otherGroupServers, renamedGroupInfo.getServers());
      assertEquals(renamedGroup, ADMIN.getRSGroup(tableName).getName());
      assertFalse(ADMIN.listRSGroups().stream().anyMatch(g -> g.getName().equals(otherGroup)));

      ADMIN.setRSGroup(Sets.newHashSet(tableName), RSGroupInfo.DEFAULT_GROUP);
      assertEquals(RSGroupInfo.DEFAULT_GROUP, ADMIN.getRSGroup(tableName).getName());

      ADMIN.moveServersToRSGroup(ADMIN.getRSGroup(renamedGroup).getServers(),
        RSGroupInfo.DEFAULT_GROUP);
      Set<Address> defaultServersAfterMoveBack =
        ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers();
      for (Address server : otherGroupServers) {
        assertTrue(defaultServersAfterMoveBack.contains(server));
      }
      assertTrue(ADMIN.getRSGroup(renamedGroup).getServers().isEmpty());

      ADMIN.removeRSGroup(renamedGroup);
      assertFalse(ADMIN.listRSGroups().stream().anyMatch(g -> g.getName().equals(renamedGroup)));

      // Confirm the unrelated operations above never disturbed the active regex-governed group.
      assertTrue(ADMIN.getRSGroup(regexGroup).getServers().contains(addr));
    } finally {
      TEST_UTIL.deleteTable(tableName);
      clearRegex(regexGroup);
      removeGroup(regexGroup);
    }
  }

  @Test
  public void testAutomaticMoveDoesNotFireCoprocessorHooks() throws Exception {
    String groupName = getGroupName("hooks");
    ADMIN.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\..*");
    try {
      assertFalse(OBSERVER.preMoveServersCalled);
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr));
      // Purely automatic, event-driven reassignment must not go through the RSGroupAdminServer
      // RPC surface, so the moveServers coprocessor hooks must not fire.
      assertFalse(OBSERVER.preMoveServersCalled);
      assertFalse(OBSERVER.postMoveServersCalled);

      // An explicit admin-driven move, by contrast, does fire the hooks. Use one of the
      // base cluster's own (non-regex-matched) RS for this, rather than disturbing the
      // regex-matched fake RS started above, which keeps running under its active regex.
      String otherGroup = getGroupName("hooksother");
      ADMIN.addRSGroup(otherGroup);
      Address baseServerAddr =
        ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().iterator().next();
      ADMIN.moveServersToRSGroup(Sets.newHashSet(baseServerAddr), otherGroup);
      assertTrue(OBSERVER.preMoveServersCalled);
      assertTrue(OBSERVER.postMoveServersCalled);
      removeGroup(otherGroup);

      // The unrelated explicit move above must not have disturbed the regex-governed group.
      assertTrue(ADMIN.getRSGroup(groupName).getServers().contains(addr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
    }
  }

  @Test
  public void testDriftBlocksUnrelatedSubsequentFlushConfig() throws Exception {
    // Once persisted state is made to violate the invariant, any subsequent unrelated
    // flushConfig-triggering admin call also fails until the drift is fixed.
    String groupName = getGroupName("drift");
    String bystanderGroup = getGroupName("bystander");
    ADMIN.addRSGroup(groupName);
    ADMIN.addRSGroup(bystanderGroup);
    try {
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));

      // Manually place the server into groupName via an explicit move while no regex governs
      // groupName yet -- this is legal at the time it happens.
      ADMIN.moveServersToRSGroup(Sets.newHashSet(addr), groupName);

      // Now introduce a regex for groupName that does NOT match the server already placed
      // there -- this creates drift between persisted state and the (now live) invariant.
      // Deliberately do NOT trigger a listener cycle here: any server add/remove event would
      // make the automatic reconciliation recompute a fresh, self-consistent assignment from the
      // current regex (moving the server back out of groupName), healing the drift before we get
      // a chance to observe it. The drift must persist purely from the un-reconciled config
      // change until an unrelated admin RPC's flushConfig call trips over it.
      setRegex(groupName, "10\\.0\\.0\\..*");

      // Any subsequent, otherwise-unrelated admin call that triggers flushConfig must now fail.
      assertThrows(IOException.class,
        () -> ADMIN.renameRSGroup(bystanderGroup, bystanderGroup + "_renamed"));

      // The rejected rename must not have partially applied: the old name still exists, and
      // no group under the new name was created.
      assertTrue(ADMIN.listRSGroups().stream().anyMatch(g -> g.getName().equals(bystanderGroup)));
      assertFalse(ADMIN.listRSGroups().stream()
        .anyMatch(g -> g.getName().equals(bystanderGroup + "_renamed")));

      // Fixing the regex so it matches the server already placed in groupName resolves the
      // drift; flushConfig re-reads the config live, so the very same, previously-rejected
      // admin call now succeeds without needing a listener cycle or a restart.
      setRegex(groupName, "127\\.0\\.0\\..*");
      ADMIN.renameRSGroup(bystanderGroup, bystanderGroup + "_renamed");
      assertTrue(ADMIN.listRSGroups().stream()
        .anyMatch(g -> g.getName().equals(bystanderGroup + "_renamed")));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
      RSGroupInfo bystander = ADMIN.getRSGroup(bystanderGroup);
      if (bystander == null) {
        bystander = ADMIN.getRSGroup(bystanderGroup + "_renamed");
        if (bystander != null) {
          removeGroup(bystander.getName());
        }
      } else {
        removeGroup(bystanderGroup);
      }
    }
  }

  @Test
  public void testNewlyJoinedRegexMemberReceivesRegionsOnBalance() throws Exception {
    // A new RS auto-joins an already-regex-governed group while the group's sole existing
    // member holds every region of a table bound to that group; running the balancer must then
    // hand some of those regions to the newly-joined RS too. The first-member baseline is
    // established by setRSGroup itself: RSGroupAdminServer#setRSGroup synchronously relocates
    // every region of a table with no RSGroup-scoped descriptor yet onto the target group's
    // servers (see moveTableRegionsToGroup, which blocks on the move futures before returning),
    // so with sn1 as the sole group member at that point all regions are already there once
    // setRSGroup returns -- no separate balance step is needed for that part. Only the
    // second-member handoff genuinely needs an explicit balance: adding a group member does not
    // by itself retrigger any region movement.
    String groupName = getGroupName("balancenewmember");
    ADMIN.addRSGroup(groupName);
    // "127.0.0.1" and "localhost" are the only two distinct, real, independently-reachable
    // identities startFakeHostnameRS supports (its RPC bind address is always "localhost"
    // regardless of advertised hostname).
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    try {
      JVMClusterUtil.RegionServerThread rst1 = startFakeHostnameRS("127.0.0.1");
      Address addr1 = addressOf(rst1);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr1));
      ServerName sn1 = getServerName(addr1);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 10);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      ADMIN.setRSGroup(Sets.newHashSet(tableName), groupName);
      Map<ServerName, List<String>> perServerAfterMove = getTableServerRegionMap().get(tableName);
      assertEquals(10, perServerAfterMove.get(sn1).size());

      ADMIN.balancerSwitch(true, true);
      try {
        // A second RS matching the same regex joins while the group is already active; it must
        // auto-join with no explicit RPC.
        JVMClusterUtil.RegionServerThread rst2 = startFakeHostnameRS("localhost");
        Address addr2 = addressOf(rst2);
        TEST_UTIL.waitFor(WAIT_TIMEOUT,
          () -> ADMIN.getRSGroup(groupName).getServers().contains(addr2));
        ServerName sn2 = getServerName(addr2);

        ADMIN.balanceRSGroup(groupName);
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
        ADMIN.balancerSwitch(false, true);
      }
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
    }
  }

  @Test
  public void testRestartTriggeredMembershipConfinesTableToGroupServers() throws Exception {
    // RS join normally (no regex active yet), landing in default like any other RS. Only
    // afterward is a regex configured targeting their hostnames; a bare config change does not
    // reconcile membership by itself, so an actual lifecycle event -- restarting one of the two
    // RS -- is used to force the recompute to notice and migrate both into the new group
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
      () -> ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr2));

    ADMIN.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    try {
      stopFakeRegionServer(rst1);
      JVMClusterUtil.RegionServerThread rst1Restarted = startFakeHostnameRS("127.0.0.1");
      Address addr1Restarted = addressOf(rst1Restarted);

      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr1Restarted));
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr2));

      ServerName sn1 = getServerName(addr1Restarted);
      ServerName sn2 = getServerName(addr2);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      // setRSGroup synchronously relocates every region of a table with no RSGroup-scoped
      // descriptor yet onto one of the target group's servers (moveTableRegionsToGroup blocks on
      // the move futures before returning), so confinement to {sn1, sn2} is already guaranteed
      // once this call returns -- no separate balance step is needed.
      ADMIN.setRSGroup(Sets.newHashSet(tableName), groupName);
      Map<ServerName, List<String>> perServerAfterMove = getTableServerRegionMap().get(tableName);
      int totalAfterMove = 0;
      for (Map.Entry<ServerName, List<String>> entry : perServerAfterMove.entrySet()) {
        assertTrue(entry.getKey().equals(sn1) || entry.getKey().equals(sn2));
        totalAfterMove += entry.getValue().size();
      }
      assertEquals(6, totalAfterMove);

      // Bidirectional isolation: the group's own members must host nothing but this table.
      for (RegionInfo region : ADMIN.getRegions(sn1)) {
        assertEquals(tableName, region.getTable());
      }
      for (RegionInfo region : ADMIN.getRegions(sn2)) {
        assertEquals(tableName, region.getTable());
      }
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
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
    // setRSGroup, and roundRobinAssignment (not retainAssignment) during crash recovery.
    String groupName = getGroupName("crashroundrobin");
    ADMIN.addRSGroup(groupName);
    // "127.0.0.1" and "localhost" are the only two distinct, real, independently-reachable
    // identities startFakeHostnameRS supports (its RPC bind address is always "localhost"
    // regardless of advertised hostname).
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    // A second, unrelated RSGroup holding one server borrowed from default -- proves crash
    // recovery's per-group candidate filtering excludes every other group, not just default.
    String otherGroupName = getGroupName("crashroundrobinother");
    RSGroupInfo otherGroupInfo = addGroup(otherGroupName, 1);
    ServerName otherGroupServer = getServerName(otherGroupInfo.getServers().iterator().next());
    try {
      JVMClusterUtil.RegionServerThread rst1 = startFakeHostnameRS("127.0.0.1");
      Address addr1 = addressOf(rst1);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr1));
      ServerName sn1 = getServerName(addr1);

      JVMClusterUtil.RegionServerThread rst2 = startFakeHostnameRS("localhost");
      Address addr2 = addressOf(rst2);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr2));
      ServerName sn2 = getServerName(addr2);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      // setRSGroup synchronously relocates every region of a table with no RSGroup-scoped
      // descriptor yet onto the target group's servers (moveTablesAndWait blocks on the
      // ModifyTableProcedure, whose child ReopenTableRegionsProcedure blocks on the reopened
      // regions), so confinement to {sn1, sn2} is already guaranteed once this call returns.
      RSGroupBasedLoadBalancer.resetAssignmentCallFlagsForTest();
      ADMIN.setRSGroup(Sets.newHashSet(tableName), groupName);
      // ReopenTableRegionsProcedure closes and reassigns every region via a single batched
      // retainAssignment call; since none of these regions' previous hosts are group members,
      // the wrapped internal balancer's own retain logic (not this class's randomAssignment)
      // places them on random candidate hosts within the group -- see BaseLoadBalancer's
      // "assigned to random hosts" log path invoked from within retainAssignment.
      assertFalse(RSGroupBasedLoadBalancer.isRandomAssignmentInvoked);
      assertTrue(RSGroupBasedLoadBalancer.isRetainAssignmentInvoked);
      assertFalse(RSGroupBasedLoadBalancer.isRoundRobinAssignmentInvoked);
      Map<ServerName, List<String>> perServerBeforeCrash = getTableServerRegionMap().get(tableName);
      int totalBeforeCrash = 0;
      for (Map.Entry<ServerName, List<String>> entry : perServerBeforeCrash.entrySet()) {
        assertTrue(entry.getKey().equals(sn1) || entry.getKey().equals(sn2));
        totalBeforeCrash += entry.getValue().size();
      }
      assertEquals(6, totalBeforeCrash);

      // Simulate an ungraceful crash of sn1
      RSGroupBasedLoadBalancer.resetAssignmentCallFlagsForTest();
      killFakeRegionServer(rst1);

      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> {
        Map<ServerName, List<String>> perServer = getTableServerRegionMap().get(tableName);
        if (perServer == null) {
          return false;
        }
        int total = 0;
        for (Map.Entry<ServerName, List<String>> entry : perServer.entrySet()) {
          // Every region must have landed on the group's sole remaining member -- never on
          // default or any other group, and never back on the now-dead sn1.
          if (!entry.getKey().equals(sn2)) {
            return false;
          }
          total += entry.getValue().size();
        }
        return total == 6;
      });
      // Confirms the crash-recovery ASSIGN TRSPs went through roundRobinAssignment, not
      // retainAssignment -- expected since hbase.master.scp.retain.assignment defaults to false.
      assertTrue(RSGroupBasedLoadBalancer.isRoundRobinAssignmentInvoked);
      assertFalse(RSGroupBasedLoadBalancer.isRetainAssignmentInvoked);

      // The surviving group member must host nothing but this table's regions.
      for (RegionInfo region : ADMIN.getRegions(sn2)) {
        assertEquals(tableName, region.getTable());
      }
      // The unrelated group's own member must not have received any of the crashed member's
      // regions either -- recovery must stay confined to sn1's own group.
      for (RegionInfo region : ADMIN.getRegions(otherGroupServer)) {
        assertFalse(region.getTable().equals(tableName));
      }
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
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
    ADMIN.addRSGroup(groupName);
    // "127.0.0.1" and "localhost" are the only two distinct, real, independently-reachable
    // identities startFakeHostnameRS supports (its RPC bind address is always "localhost"
    // regardless of advertised hostname).
    setRegex(groupName, "127\\.0\\.0\\.1|localhost");
    RSGroupBasedLoadBalancer balancer = (RSGroupBasedLoadBalancer) MASTER.getLoadBalancer();
    balancer.setFallbackEnabledForTest(true);
    try {
      JVMClusterUtil.RegionServerThread rst1 = startFakeHostnameRS("127.0.0.1");
      Address addr1 = addressOf(rst1);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr1));
      ServerName sn1 = getServerName(addr1);

      JVMClusterUtil.RegionServerThread rst2 = startFakeHostnameRS("localhost");
      Address addr2 = addressOf(rst2);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> ADMIN.getRSGroup(groupName).getServers().contains(addr2));
      ServerName sn2 = getServerName(addr2);

      TEST_UTIL.createMultiRegionTable(tableName, Bytes.toBytes("f"), 6);
      TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
      // setRSGroup synchronously relocates every region of a table with no RSGroup-scoped
      // descriptor yet onto the target group's servers (moveTableRegionsToGroup blocks on the
      // move futures before returning), so confinement to {sn1, sn2} is already guaranteed once
      // this call returns.
      ADMIN.setRSGroup(Sets.newHashSet(tableName), groupName);

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
          defaultGroup = ADMIN.getRSGroup(RSGroupInfo.DEFAULT_GROUP);
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
      clearRegex(groupName);
      removeGroup(groupName);
    }
  }
}
