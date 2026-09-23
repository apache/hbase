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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.backup.util.BackupUtils;
import org.apache.hadoop.hbase.master.region.MasterRegionFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(SmallTests.TAG)
public class TestBackupUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestBackupUtils.class);

  protected static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  protected static Configuration conf = TEST_UTIL.getConfiguration();

  private static FileSystem dummyFs;
  private static Path backupRootDir;

  @BeforeAll
  public static void setUp() throws IOException {
    dummyFs = TEST_UTIL.getTestFileSystem();
    backupRootDir = TEST_UTIL.getDataTestDirOnTestFS("backupUT");
  }

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

  // Ensure getValidWalDirs() uses UTC timestamps regardless of what time zone the test is run in.
  @Test
  public void testGetValidWalDirForAllTimeZonesSingleDay() throws IOException {
    // This UTC test time is a time when it is still "yesterday" in other time zones (such as PST)
    List<String> walDateDirs = List.of("2026-01-23");
    Path walDir = new Path(backupRootDir, "WALs");

    // 10-minute window in UTC between start and end time
    long startTime = Instant.parse("2026-01-23T01:00:00Z").toEpochMilli();
    long endTime = startTime + (10 * 60 * 1000);

    testGetValidWalDirs(startTime, endTime, walDir, walDateDirs, 1, walDateDirs);
  }

  // Ensure getValidWalDirs() works as expected for time ranges across multiple days for all time
  // zones
  @Test
  public void testGetValidWalDirsForAllTimeZonesMultiDay() throws IOException {
    List<String> walDateDirs = List.of("2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02");
    List<String> expectedValidWalDirs = List.of("2025-12-31", "2026-01-01");
    Path walDir = new Path(backupRootDir, "WALs");

    // 10-minute window in UTC between start and end time that spans over two days
    long startTime = Instant.parse("2025-12-31T23:55:00Z").toEpochMilli();
    long endTime = Instant.parse("2026-01-01T00:05:00Z").toEpochMilli();

    testGetValidWalDirs(startTime, endTime, walDir, walDateDirs, 2, expectedValidWalDirs);
  }

  @Test
  public void testGetValidWalDirExactlyMidnightUTC() throws IOException {
    List<String> walDateDirs = List.of("2026-01-23");
    Path walDir = new Path(backupRootDir, "WALs");
    // This instant is UTC
    long startAndEndTime = Instant.parse("2026-01-23T00:00:00.000Z").toEpochMilli();

    testGetValidWalDirs(startAndEndTime, startAndEndTime, walDir, walDateDirs, 1, walDateDirs);
  }

  @Test
  public void testGetValidWalDirOneMsBeforeMidnightUTC() throws IOException {
    List<String> walDateDirs = List.of("2026-01-23");
    Path walDir = new Path(backupRootDir, "WALs");
    // This instant is UTC
    long startAndEndTime = Instant.parse("2026-01-23T23:59:59.999Z").toEpochMilli();

    testGetValidWalDirs(startAndEndTime, startAndEndTime, walDir, walDateDirs, 1, walDateDirs);
  }

  @Test
  public void testGetRolledHostsKeepsOnlyHostsWhoseRollResultChanged() {
    Map<String, Long> previousLogRolls =
      Map.of("rolled:16020", 100L, "offline:16020", 200L, "removed:16020", 300L);
    Map<String, Long> latestLogRolls =
      Map.of("rolled:16020", 150L, "offline:16020", 200L, "new:16020", 400L);

    assertEquals(Map.of("rolled:16020", 150L, "new:16020", 400L),
      BackupUtils.getRolledHosts(previousLogRolls, latestLogRolls));
  }

  @Test
  public void testComputeLogBoundariesUsesRollResultForRolledHosts() throws IOException {
    Map<String, Long> rolledHosts = Map.of("rolled:16020", 100L);
    List<String> coveredLogs = List.of("/hbase/oldWALs/rolled%2C16020%2C1.500");
    List<String> pendingLogs = List.of("/hbase/WALs/rolled,16020,1/rolled%2C16020%2C1.50");

    assertEquals(Map.of("rolled:16020", 100L),
      BackupUtils.computeLogBoundaries(rolledHosts, coveredLogs, pendingLogs));
  }

  @Test
  public void testComputeLogBoundariesUsesNewestCoveredLogForOtherHosts() throws IOException {
    List<String> coveredLogs =
      List.of("/hbase/oldWALs/offline%2C16020%2C1.200", "/hbase/oldWALs/offline%2C16020%2C1.300",
        "/hbase/WALs/offline,16020,1/offline%2C16020%2C1.250");

    assertEquals(Map.of("offline:16020", 300L),
      BackupUtils.computeLogBoundaries(Map.of(), coveredLogs, List.of()));
  }

  @Test
  public void testComputeLogBoundariesCapsBelowOldestPendingLog() throws IOException {
    List<String> coveredLogs = List.of("/hbase/oldWALs/splitting%2C16020%2C1.400");
    List<String> pendingLogs =
      List.of("/hbase/WALs/splitting,16020,1-splitting/splitting%2C16020%2C1.350",
        "/hbase/WALs/splitting,16020,1-splitting/splitting%2C16020%2C1.370");

    assertEquals(Map.of("splitting:16020", 349L),
      BackupUtils.computeLogBoundaries(Map.of(), coveredLogs, pendingLogs));
  }

  @Test
  public void testComputeLogBoundariesKeepsHostWithOnlyPendingLogs() throws IOException {
    List<String> pendingLogs = List.of("/hbase/WALs/joined,16020,1/joined%2C16020%2C1.500");

    assertEquals(Map.of("joined:16020", 499L),
      BackupUtils.computeLogBoundaries(Map.of(), List.of(), pendingLogs));
  }

  @Test
  public void testComputeLogBoundariesSkipsUnparseableLogs() throws IOException {
    List<String> coveredLogs = List.of("/hbase/oldWALs/not-a-wal");

    assertEquals(Map.of("rolled:16020", 100L),
      BackupUtils.computeLogBoundaries(Map.of("rolled:16020", 100L), coveredLogs, List.of()));
  }

  @Test
  public void testComputeLogBoundariesKeepsPreviousBoundaryWhileHostHasLogs() throws IOException {
    Map<String, Long> previousBoundaries = Map.of("stuck:16020", 1200L, "gone:16020", 800L);

    assertEquals(Map.of("stuck:16020", 1200L), BackupUtils.computeLogBoundaries(Map.of(),
      previousBoundaries, Set.of("stuck:16020"), List.of(), List.of()));
  }

  @Test
  public void testComputeLogBoundariesPrefersNewBoundaryOverPreviousOne() throws IOException {
    Map<String, Long> previousBoundaries =
      Map.of("rolled:16020", 100L, "offline:16020", 200L, "pending:16020", 300L);
    List<String> coveredLogs = List.of("/hbase/oldWALs/offline%2C16020%2C1.250");
    List<String> pendingLogs = List.of("/hbase/WALs/pending,16020,1/pending%2C16020%2C1.400");

    assertEquals(Map.of("rolled:16020", 150L, "offline:16020", 250L, "pending:16020", 399L),
      BackupUtils.computeLogBoundaries(Map.of("rolled:16020", 150L), previousBoundaries,
        Set.of("rolled:16020", "offline:16020", "pending:16020"), coveredLogs, pendingLogs));
  }

  protected void testGetValidWalDirs(long startTime, long endTime, Path walDir,
    List<String> availableWalDateDirs, int numExpectedValidWalDirs,
    List<String> expectedValidWalDirs) throws IOException {
    TimeZone defaultTimeZone = TimeZone.getDefault();
    try {
      // This UTC test time is a time when it is still "yesterday" in other time zones (such as PST)
      for (String dirName : availableWalDateDirs) {
        dummyFs.mkdirs(new Path(walDir, dirName));
      }

      // Ensure we can get valid WAL dirs regardless of the test environment's time zone
      for (String timeZone : ZoneId.getAvailableZoneIds()) {
        // Force test environment to use specified time zone
        TimeZone.setDefault(TimeZone.getTimeZone(timeZone));

        List<String> validWalDirs = BackupUtils.getValidWalDirs(TEST_UTIL.getConfiguration(),
          backupRootDir, startTime, endTime);

        // Verify the correct number of valid WAL dirs was found
        assertEquals(numExpectedValidWalDirs, validWalDirs.size(),
          "The number of valid WAL dirs should be " + numExpectedValidWalDirs + " for time zone "
            + timeZone);

        // Verify the list of valid WAL dirs is as expected
        for (String dirName : expectedValidWalDirs) {
          assertTrue(validWalDirs.stream().anyMatch(path -> path.endsWith("/" + dirName)),
            "Expected " + dirName + " to be a valid WAL dir");
        }

        // Verify the list of valid WAL dirs does not contain anything expected to be invalid
        List<String> expectedInvalidWalDirs = new ArrayList<>(availableWalDateDirs);
        expectedInvalidWalDirs.removeAll(expectedValidWalDirs);
        for (String dirName : expectedInvalidWalDirs) {
          assertFalse(validWalDirs.contains(dirName),
            "Expected " + dirName + " to NOT be a valid WAL dir");
        }
      }
    } finally {
      TimeZone.setDefault(defaultTimeZone);
      dummyFs.delete(walDir, true);
    }
  }
}
