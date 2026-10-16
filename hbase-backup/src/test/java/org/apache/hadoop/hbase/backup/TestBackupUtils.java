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
package org.apache.hadoop.hbase.backup;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

@Tag(SmallTests.TAG)
public class TestBackupUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupUtils.class);

  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Configuration conf = TEST_UTIL.getConfiguration();

  @Test
  public void testGetBulkOutputDir() {
    // Create a user who is not the current user
    String fooUserName = "foo1234";
    String fooGroupName = "group1";
    UserGroupInformation ugi =
      UserGroupInformation.createUserForTesting(fooUserName, new String[] { fooGroupName });
    // Get user's home directory
    Path fooHomeDirectory = ugi.doAs(new PrivilegedAction<Path>() {
      @Override
      public Path run() {
        try (FileSystem fs = FileSystem.get(conf)) {
          return fs.getHomeDirectory();
        } catch (IOException ioe) {
          LOG.error("Failed to get foo's home directory", ioe);
        }
        return null;
      }
    });

    Path bulkOutputDir = ugi.doAs(new PrivilegedAction<Path>() {
      @Override
      public Path run() {
        try {
          return BackupUtils.getBulkOutputDir("test", conf, false);
        } catch (IOException ioe) {
          LOG.error("Failed to get bulk output dir path", ioe);
        }
        return null;
      }
    });
    // Make sure the directory is in foo1234's home directory
    assertTrue(bulkOutputDir.toString().startsWith(fooHomeDirectory.toString()));
  }

  @Test
  public void testFilesystemWalHostNameParsing() throws IOException {
    String[] hosts =
      new String[] { "10.20.30.40", "127.0.0.1", "localhost", "a-region-server.domain.com" };

    Path walRootDir = CommonFSUtils.getWALRootDir(conf);
    Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);

    int port = 60030;
    for (String host : hosts) {
      ServerName serverName = ServerName.valueOf(host, port, 1234);

      Path testOldWalPath = new Path(oldLogDir,
        serverName + BackupUtils.LOGNAME_SEPARATOR + EnvironmentEdgeManager.currentTime());
      assertEquals(host + Addressing.HOSTNAME_PORT_SEPARATOR + port,
        BackupUtils.parseHostFromOldLog(testOldWalPath));

      Path testMasterWalPath =
        new Path(oldLogDir, testOldWalPath.getName() + MasterRegionFactory.ARCHIVED_WAL_SUFFIX);
      assertNull(BackupUtils.parseHostFromOldLog(testMasterWalPath));

      // org.apache.hadoop.hbase.wal.BoundedGroupingStrategy does this
      Path testOldWalWithRegionGroupingPath = new Path(oldLogDir,
        serverName + BackupUtils.LOGNAME_SEPARATOR + serverName + BackupUtils.LOGNAME_SEPARATOR
          + "regiongroup-0" + BackupUtils.LOGNAME_SEPARATOR + EnvironmentEdgeManager.currentTime());
      assertEquals(host + Addressing.HOSTNAME_PORT_SEPARATOR + port,
        BackupUtils.parseHostFromOldLog(testOldWalWithRegionGroupingPath));

      Path testOldWalInServerDirPath = new Path(new Path(oldLogDir, serverName.toString()),
        "wal" + BackupUtils.LOGNAME_SEPARATOR + EnvironmentEdgeManager.currentTime());
      assertEquals(host + Addressing.HOSTNAME_PORT_SEPARATOR + port,
        BackupUtils.parseHostFromOldLog(testOldWalInServerDirPath));
    }

  }

  @Test
  public void testGetRolledHostsKeepsOnlyHostsWhoseRollResultChanged() {
    Map<String, Long> previousLogRolls =
      ImmutableMap.of("rolled:16020", 100L, "offline:16020", 200L, "removed:16020", 300L);
    Map<String, Long> latestLogRolls =
      ImmutableMap.of("rolled:16020", 150L, "offline:16020", 200L, "new:16020", 400L);

    assertEquals(ImmutableMap.of("rolled:16020", 150L, "new:16020", 400L),
      BackupUtils.getRolledHosts(previousLogRolls, latestLogRolls));
  }

  @Test
  public void testComputeLogBoundariesUsesRollResultForRolledHosts() throws IOException {
    Map<String, Long> rolledHosts = ImmutableMap.of("rolled:16020", 100L);
    List<String> coveredLogs = ImmutableList.of("/hbase/oldWALs/rolled%2C16020%2C1.500");
    List<String> pendingLogs = ImmutableList.of("/hbase/WALs/rolled,16020,1/rolled%2C16020%2C1.50");

    assertEquals(ImmutableMap.of("rolled:16020", 100L),
      BackupUtils.computeLogBoundaries(rolledHosts, coveredLogs, pendingLogs));
  }

  @Test
  public void testComputeLogBoundariesUsesNewestCoveredLogForOtherHosts() throws IOException {
    List<String> coveredLogs = ImmutableList.of("/hbase/oldWALs/offline%2C16020%2C1.200",
      "/hbase/oldWALs/offline%2C16020%2C1.300",
      "/hbase/WALs/offline,16020,1/offline%2C16020%2C1.250");

    assertEquals(ImmutableMap.of("offline:16020", 300L),
      BackupUtils.computeLogBoundaries(ImmutableMap.of(), coveredLogs, ImmutableList.of()));
  }

  @Test
  public void testComputeLogBoundariesCapsBelowOldestPendingLog() throws IOException {
    List<String> coveredLogs = ImmutableList.of("/hbase/oldWALs/splitting%2C16020%2C1.400");
    List<String> pendingLogs =
      ImmutableList.of("/hbase/WALs/splitting,16020,1-splitting/splitting%2C16020%2C1.350",
        "/hbase/WALs/splitting,16020,1-splitting/splitting%2C16020%2C1.370");

    assertEquals(ImmutableMap.of("splitting:16020", 349L),
      BackupUtils.computeLogBoundaries(ImmutableMap.of(), coveredLogs, pendingLogs));
  }

  @Test
  public void testComputeLogBoundariesKeepsHostWithOnlyPendingLogs() throws IOException {
    List<String> pendingLogs =
      ImmutableList.of("/hbase/WALs/joined,16020,1/joined%2C16020%2C1.500");

    assertEquals(ImmutableMap.of("joined:16020", 499L),
      BackupUtils.computeLogBoundaries(ImmutableMap.of(), ImmutableList.of(), pendingLogs));
  }

  @Test
  public void testComputeLogBoundariesSkipsUnparseableLogs() throws IOException {
    List<String> coveredLogs = ImmutableList.of("/hbase/oldWALs/not-a-wal");

    assertEquals(ImmutableMap.of("rolled:16020", 100L), BackupUtils.computeLogBoundaries(
      ImmutableMap.of("rolled:16020", 100L), coveredLogs, ImmutableList.of()));
  }

  @Test
  public void testComputeLogBoundariesKeepsPreviousBoundaryWhileHostHasLogs() throws IOException {
    Map<String, Long> previousBoundaries =
      ImmutableMap.of("stuck:16020", 1200L, "gone:16020", 800L);

    assertEquals(ImmutableMap.of("stuck:16020", 1200L),
      BackupUtils.computeLogBoundaries(ImmutableMap.of(), previousBoundaries,
        ImmutableSet.of("stuck:16020"), ImmutableList.of(), ImmutableList.of()));
  }

  @Test
  public void testComputeLogBoundariesPrefersNewBoundaryOverPreviousOne() throws IOException {
    Map<String, Long> previousBoundaries =
      ImmutableMap.of("rolled:16020", 100L, "offline:16020", 200L, "pending:16020", 300L);
    List<String> coveredLogs = ImmutableList.of("/hbase/oldWALs/offline%2C16020%2C1.250");
    List<String> pendingLogs =
      ImmutableList.of("/hbase/WALs/pending,16020,1/pending%2C16020%2C1.400");

    assertEquals(
      ImmutableMap.of("rolled:16020", 150L, "offline:16020", 250L, "pending:16020", 399L),
      BackupUtils.computeLogBoundaries(ImmutableMap.of("rolled:16020", 150L), previousBoundaries,
        ImmutableSet.of("rolled:16020", "offline:16020", "pending:16020"), coveredLogs,
        pendingLogs));
  }
}
