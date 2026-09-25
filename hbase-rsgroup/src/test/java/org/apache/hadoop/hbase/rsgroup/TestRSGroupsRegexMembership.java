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
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
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
 * {@link RSGroupAdmin} API, live RegionServer add/remove against the minicluster, and captured
 * log output -- no reflection or visibility changes against {@link RSGroupInfoManagerImpl}.
 */
@Tag(MediumTests.TAG)
public class TestRSGroupsRegexMembership extends TestRSGroupsBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestRSGroupsRegexMembership.class);
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
    }
    regexConfigKeysSet.clear();
    tearDownAfterMethod();
  }

  // ============================== helpers ==============================

  /**
   * Starts a genuinely new RegionServer process (thread) in the shared minicluster that reports
   * itself under a caller-chosen hostname, so regex membership rules can target it individually.
   * The base cluster's real RS all share one real, resolvable hostname (this machine's address);
   * {@link RSRpcServices}'s constructor unconditionally resolves whatever hostname is configured
   * (independent of any RPC bind-address override), so the caller-chosen hostname here must
   * itself be a real, resolvable, bindable literal -- "127.0.0.1" and "localhost" both work and
   * are guaranteed distinct from the base cluster's real hostname, which is exactly what every
   * test in this class relies on to get a controllable, non-shared identity.
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

  /**
   * The listener thread that drives automatic regex-based reassignment only wakes up on an
   * actual server add/remove event -- a bare config mutation is not enough. Forces a wake-up
   * cycle (add+remove a bystander RS) so a config change just made takes effect without
   * requiring an explicit RSGroupAdmin RPC. Only used by tests that *want* the automatic
   * reconciliation to run and correct things; the trigger RS's own transient membership doesn't
   * affect assertions elsewhere since those only check for specific other servers' addresses.
   */
  private void triggerListenerCycle() throws Exception {
    JVMClusterUtil.RegionServerThread trigger = startFakeHostnameRS("127.0.0.1");
    stopFakeRegionServer(trigger);
  }

  private void setRegex(String groupName, String regex) {
    String key = REGEX_PREFIX + groupName;
    master.getConfiguration().set(key, regex);
    regexConfigKeysSet.add(key);
  }

  private void clearRegex(String groupName) {
    String key = REGEX_PREFIX + groupName;
    master.getConfiguration().unset(key);
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
    }
  }

  private static LogCapturer captureRSGroupInfoManagerLog() {
    return new LogCapturer((org.apache.logging.log4j.core.Logger) org.apache.logging.log4j.LogManager
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
      assertFalse(rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers()
        .contains(addr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
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

      // Different port (fresh Address) under the same matching hostname.
      JVMClusterUtil.RegionServerThread second = startFakeHostnameRS("127.0.0.1");
      Address secondAddr = addressOf(second);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(secondAddr));
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
      clearRegex(groupName);
      removeGroup(groupName);
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
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> capturer.getOutput()
        .contains("matches regexes for multiple RSGroups"));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupOne).getServers().contains(addr));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupTwo).getServers().contains(addr));

      // Fix the overlap live: narrow groupOne's regex so only groupTwo still matches.
      setRegex(groupOne, "10\\.0\\.0\\..*");
      triggerListenerCycle();
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(groupTwo).getServers().contains(addr));
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
    // Explicit moveServers can never empty default in one call (a longstanding, unrelated
    // RSGroupAdminServer guard always keeps >=1 server there), so parking every base RS in an
    // admin-managed group first (as an earlier version of this test tried) is a non-starter. The
    // automatic regex-reconciliation path bypasses that admin-level guard entirely, so instead we
    // drive the empty-default scenario purely through it: a catch-all regex matches every online
    // server at once (all base RS share one real hostname), with no explicit move at all.
    String regexGroup = getGroupName("guardregex");
    rsGroupAdmin.addRSGroup(regexGroup);
    LogCapturer capturer = captureRSGroupInfoManagerLog();
    try {
      setRegex(regexGroup, ".*");
      triggerListenerCycle();

      // Every online server (all base RS) now matches the catch-all -- the guard must trip
      // rather than emptying default: base servers stay exactly where they are.
      TEST_UTIL.waitFor(WAIT_TIMEOUT, () -> capturer.getOutput()
        .contains("would leave RSGroup 'default' with no online servers"));
      // Locks in the documented WARN level (the guard's Javadoc says ERROR; actual code says
      // WARN) as observable behavior.
      assertFalse(capturer.getOutput().contains("ERROR"));
      assertEquals(NUM_SLAVES_BASE,
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size());
      assertTrue(rsGroupAdmin.getRSGroupInfo(regexGroup).getServers().isEmpty());

      // Narrow the regex so it only targets a new server, not the base cluster's shared
      // hostname -- the guard's precondition no longer covers every online server (the base
      // servers are unmatched again), so it clears and the new server is free to join.
      setRegex(regexGroup, "127\\.0\\.0\\..*");
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(regexGroup).getServers().contains(addr));
      assertEquals(NUM_SLAVES_BASE,
        rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().size());
    } finally {
      capturer.stopCapturing();
      clearRegex(regexGroup);
      removeGroup(regexGroup);
    }
  }

  @Test
  public void testExplicitMoveViolatingInvariantRejected() throws Exception {
    String groupName = getGroupName("reject");
    rsGroupAdmin.addRSGroup(groupName);
    setRegex(groupName, "127\\.0\\.0\\..*");
    try {
      // A server whose hostname does NOT match the regex may not be explicitly moved into a
      // regex-governed group.
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("localhost");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));

      assertThrows(DoNotRetryIOException.class,
        () -> rsGroupAdmin.moveServers(Sets.newHashSet(addr), groupName));

      // No partial mutation: server is still exactly where it started.
      assertTrue(rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers()
        .contains(addr));
      assertFalse(rsGroupAdmin.getRSGroupInfo(groupName).getServers().contains(addr));
    } finally {
      clearRegex(groupName);
      removeGroup(groupName);
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
      addGroup(otherGroup, 1);
      TEST_UTIL.createTable(tableName, Bytes.toBytes("f"));
      rsGroupAdmin.moveTables(Sets.newHashSet(tableName), otherGroup);
      String renamedGroup = otherGroup + "_renamed";
      rsGroupAdmin.renameRSGroup(otherGroup, renamedGroup);
      rsGroupAdmin.moveTables(rsGroupAdmin.getRSGroupInfo(renamedGroup).getTables(),
        RSGroupInfo.DEFAULT_GROUP);
      rsGroupAdmin.moveServers(rsGroupAdmin.getRSGroupInfo(renamedGroup).getServers(),
        RSGroupInfo.DEFAULT_GROUP);
      rsGroupAdmin.removeRSGroup(renamedGroup);
    } finally {
      clearRegex(regexGroup);
      removeGroup(regexGroup);
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

      // An explicit admin-driven move, by contrast, does fire the hooks.
      String otherGroup = getGroupName("hooksother");
      rsGroupAdmin.addRSGroup(otherGroup);
      clearRegex(groupName);
      stopFakeRegionServer(rst);
      JVMClusterUtil.RegionServerThread plain = startFakeHostnameRS("127.0.0.1");
      Address plainAddr = addressOf(plain);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers()
          .contains(plainAddr));
      rsGroupAdmin.moveServers(Sets.newHashSet(plainAddr), otherGroup);
      assertTrue(observer.preMoveServersCalled);
      assertTrue(observer.postMoveServersCalled);
      removeGroup(otherGroup);
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
    rsGroupAdmin.addRSGroup(groupName);
    rsGroupAdmin.addRSGroup(bystanderGroup);
    try {
      JVMClusterUtil.RegionServerThread rst = startFakeHostnameRS("127.0.0.1");
      Address addr = addressOf(rst);
      TEST_UTIL.waitFor(WAIT_TIMEOUT,
        () -> rsGroupAdmin.getRSGroupInfo(RSGroupInfo.DEFAULT_GROUP).getServers().contains(addr));

      // Manually place the server into groupName via an explicit move while no regex governs
      // groupName yet -- this is legal at the time it happens.
      rsGroupAdmin.moveServers(Sets.newHashSet(addr), groupName);

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
        () -> rsGroupAdmin.renameRSGroup(bystanderGroup, bystanderGroup + "_renamed"));
    } finally {
      clearRegex(groupName);
      // Fix the drift before teardown's own admin calls run, or they'll fail the same way.
      RSGroupInfo group = rsGroupAdmin.getRSGroupInfo(groupName);
      if (group != null && !group.getServers().isEmpty()) {
        rsGroupAdmin.moveServers(group.getServers(), RSGroupInfo.DEFAULT_GROUP);
      }
      removeGroup(groupName);
      RSGroupInfo bystander = rsGroupAdmin.getRSGroupInfo(bystanderGroup);
      if (bystander == null) {
        bystander = rsGroupAdmin.getRSGroupInfo(bystanderGroup + "_renamed");
        if (bystander != null) {
          removeGroup(bystander.getName());
        }
      } else {
        removeGroup(bystanderGroup);
      }
    }
  }
}
