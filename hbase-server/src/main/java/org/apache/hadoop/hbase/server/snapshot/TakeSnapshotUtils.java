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
package org.apache.hadoop.hbase.server.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.server.errorhandling.ExceptionListener;
import org.apache.hadoop.hbase.server.errorhandling.OperationAttemptTimer;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.exception.CorruptedSnapshotException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Utilities for useful when taking a snapshot
 */
public class TakeSnapshotUtils {

  private static final Log LOG = LogFactory.getLog(TakeSnapshotUtils.class);

  private TakeSnapshotUtils() {
    // private constructor for util class
  }

  /**
   * Get the per-region snapshot description location.
   * <p>
   * Under the per-snapshot directory, specific files per-region are kept in a similar layout as per
   * the current directory layout.
   * @param desc description of the snapshot
   * @param rootDir root directory for the hbase installation
   * @param regionName encoded name of the region (see {@link HRegionInfo#encodeRegionName(byte[])})
   * @return path to the per-region directory for the snapshot
   */
  public static Path getRegionSnapshotDirectory(SnapshotDescription desc, Path rootDir,
      String regionName) {
    Path snapshotDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(desc, rootDir);
    return HRegion.getRegionDir(snapshotDir, regionName);
  }

  /**
   * Get the home directory for store-level snapshot files.
   * <p>
   * Specific files per store are kept in a similar layout as per the current directory layout.
   * @param regionDir snapshot directory for the parent region, <b>not</b> the standard region
   *          directory. See {@link #getRegionSnapshotDirectory(SnapshotDescription, Path, String)}
   * @param family name of the store to snapshot
   * @return path to the snapshot home directory for the store/family
   */
  public static Path getStoreSnapshotDirectory(Path regionDir, String family) {
    return HStore.getStoreHomedir(regionDir, Bytes.toBytes(family));
  }

  /**
   * Get the snapshot directory for each family to be added to the the snapshot
   * @param snapshot description of the snapshot being take
   * @param snapshotRegionDir directory in the snapshot where the region directory information
   *          should be stored
   * @param families families to be added (can be null)
   * @return paths to the snapshot directory for each family, in the same order as the families
   *         passed in
   */
  public static List<Path> getFamilySnapshotDirectories(SnapshotDescription snapshot,
      Path snapshotRegionDir, FileStatus[] families) {
    if (families == null || families.length == 0) return Collections.emptyList();

    List<Path> familyDirs = new ArrayList<Path>(families.length);
    for (FileStatus family : families) {
      // build the reference directory name
      familyDirs.add(getStoreSnapshotDirectory(snapshotRegionDir, family.getPath().getName()));
    }
    return familyDirs;
  }

  /**
   * Create a snapshot timer for the master which notifies the monitor when an error occurs
   * @param snapshot snapshot to monitor
   * @param conf configuration to use when getting the max snapshot life
   * @param monitor monitor to notify when the snapshot life expires
   * @return the timer to use update to signal the start and end of the snapshot
   */
  @SuppressWarnings("rawtypes")
  public static OperationAttemptTimer getMasterTimerAndBindToMonitor(SnapshotDescription snapshot,
      Configuration conf, ExceptionListener monitor) {
    long maxTime = SnapshotDescriptionUtils.getMaxMasterTimeout(conf, snapshot.getType(),
      SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);
    return new OperationAttemptTimer(monitor, maxTime, snapshot);
  }

  /**
   * Verify that all the expected logs got referenced
   * @param fs filesystem where the logs live
   * @param logsDir original logs directory
   * @param serverNames names of the servers that involved in the snapshot
   * @param snapshot description of the snapshot being taken
   * @param snapshotLogDir directory for logs in the snapshot
   * @throws IOException
   */
  public static void verifyAllLogsGotReferenced(FileSystem fs, Path logsDir,
      Set<String> serverNames, SnapshotDescription snapshot, Path snapshotLogDir)
      throws IOException {
    assertTrue(snapshot, "Logs directory doesn't exist in snapshot", fs.exists(logsDir));
    // for each of the server log dirs, make sure it matches the main directory
    Multimap<String, String> snapshotLogs = getMapOfServersAndLogs(fs, snapshotLogDir, serverNames);
    Multimap<String, String> realLogs = getMapOfServersAndLogs(fs, logsDir, serverNames);
    if (realLogs != null) {
      assertNotNull(snapshot, "No server logs added to snapshot", snapshotLogs);
    } else if (realLogs == null) {
      assertNull(snapshot, "Snapshotted server logs that don't exist", snapshotLogs);
    }

    // check the number of servers
    Set<Entry<String, Collection<String>>> serverEntries = realLogs.asMap().entrySet();
    Set<Entry<String, Collection<String>>> snapshotEntries = snapshotLogs.asMap().entrySet();
    assertEquals(snapshot, "Not the same number of snapshot and original server logs directories",
      serverEntries.size(), snapshotEntries.size());

    // verify we snapshotted each of the log files
    for (Entry<String, Collection<String>> serverLogs : serverEntries) {
      // if the server is not the snapshot, skip checking its logs
      if (!serverNames.contains(serverLogs.getKey())) continue;
      Collection<String> snapshotServerLogs = snapshotLogs.get(serverLogs.getKey());
      assertNotNull(snapshot, "Snapshots missing logs for server:" + serverLogs.getKey(),
        snapshotServerLogs);

      // check each of the log files
      assertEquals(snapshot,
        "Didn't reference all the log files for server:" + serverLogs.getKey(), serverLogs
            .getValue().size(), snapshotServerLogs.size());
      for (String log : serverLogs.getValue()) {
        assertTrue(snapshot, "Snapshot logs didn't include " + log,
          snapshotServerLogs.contains(log));
      }
    }
  }

  /**
   * Verify one of a snapshot's region's recovered.edits, has been at the surface (file names,
   * length), match the original directory.
   * @param fs filesystem on which the snapshot had been taken
   * @param rootDir full path to the root hbase directory
   * @param regionInfo info for the region
   * @param snapshot description of the snapshot that was taken
   * @throws IOException if there is an unexpected error talking to the filesystem
   */
  public static void verifyRecoveredEdits(FileSystem fs, Path rootDir, HRegionInfo regionInfo,
      SnapshotDescription snapshot) throws IOException {
    Path regionDir = HRegion.getRegionDir(rootDir, regionInfo);
    Path editsDir = HLogUtil.getRegionDirRecoveredEditsDir(regionDir);
    Path snapshotRegionDir = TakeSnapshotUtils.getRegionSnapshotDirectory(snapshot, rootDir,
      regionInfo.getEncodedName());
    Path snapshotEditsDir = HLogUtil.getRegionDirRecoveredEditsDir(snapshotRegionDir);

    FileStatus[] edits = FSUtils.listStatus(fs, editsDir);
    FileStatus[] snapshotEdits = FSUtils.listStatus(fs, snapshotEditsDir);
    if (edits == null) {
      assertNull(snapshot, "Snapshot has edits but table doesn't", snapshotEdits);
      return;
    }

    assertNotNull(snapshot, "Table has edits, but snapshot doesn't", snapshotEdits);

    // check each of the files
    assertEquals(snapshot, "Not same number of edits in snapshot as table", edits.length,
      snapshotEdits.length);

    // make sure we have a file with the same name as the original
    // it would be really expensive to verify the content matches the original
    for (FileStatus edit : edits) {
      for (FileStatus sEdit : snapshotEdits) {
        if (sEdit.getPath().equals(edit.getPath())) {
          assertEquals(snapshot, "Snapshot file" + sEdit.getPath()
              + " length not equal to the original: " + edit.getPath(), edit.getLen(),
            sEdit.getLen());
          break;
        }
      }
      assertTrue(snapshot, "No edit in snapshot with name:" + edit.getPath(), false);
    }
  }

  private static void assertNull(SnapshotDescription snapshot, String msg, Object isNull)
      throws CorruptedSnapshotException {
    if (isNull != null) {
      throw new CorruptedSnapshotException(msg + ", Expected " + isNull + " to be null.", snapshot);
    }
  }

  private static void assertNotNull(SnapshotDescription snapshot, String msg, Object notNull)
      throws CorruptedSnapshotException {
    if (notNull == null) {
      throw new CorruptedSnapshotException(msg + ", Expected object to not be null, but was null.",
          snapshot);
    }
  }

  private static void assertTrue(SnapshotDescription snapshot, String msg, boolean isTrue)
      throws CorruptedSnapshotException {
    if (!isTrue) {
      throw new CorruptedSnapshotException(msg + ", Expected true, but was false", snapshot);
    }
  }

  /**
   * Assert that the expect matches the gotten amount
   * @param msg message to add the to exception
   * @param expected
   * @param gotten
   * @throws CorruptedSnapshotException thrown if the two elements don't match
   */
  private static void assertEquals(SnapshotDescription snapshot, String msg, int expected,
      int gotten) throws CorruptedSnapshotException {
    if (expected != gotten) {
      throw new CorruptedSnapshotException(msg + ". Expected:" + expected + ", got:" + gotten,
          snapshot);
    }
  }

  /**
   * Assert that the expect matches the gotten amount
   * @param msg message to add the to exception
   * @param expected
   * @param gotten
   * @throws CorruptedSnapshotException thrown if the two elements don't match
   */
  private static void assertEquals(SnapshotDescription snapshot, String msg, long expected,
      long gotten) throws CorruptedSnapshotException {
    if (expected != gotten) {
      throw new CorruptedSnapshotException(msg + ". Expected:" + expected + ", got:" + gotten,
          snapshot);
    }
  }

  /**
   * @param logdir
   * @param toInclude list of servers to include. If empty or null, returns all servers
   * @return maps of servers to all their log files. If there is no log directory, returns
   *         <tt>null</tt>
   */
  private static Multimap<String, String> getMapOfServersAndLogs(FileSystem fs, Path logdir,
      Collection<String> toInclude) throws IOException {
    // create a path filter based on the passed directories to include
    PathFilter filter = toInclude == null || toInclude.size() == 0 ? null
        : new MatchesDirectoryNames(toInclude);

    // get all the expected directories
    FileStatus[] serverLogDirs = FSUtils.listStatus(fs, logdir, filter);
    if (serverLogDirs == null) return null;

    // map those into a multimap of servername -> [log files]
    Multimap<String, String> map = HashMultimap.create();
    for (FileStatus server : serverLogDirs) {
      FileStatus[] serverLogs = FSUtils.listStatus(fs, server.getPath(), null);
      if (serverLogs == null) continue;
      for (FileStatus log : serverLogs) {
        map.put(server.getPath().getName(), log.getPath().getName());
      }
    }
    return map;
  }

  /**
   * Path filter that only accepts paths where that have a {@link Path#getName()} that is contained
   * in the specified collection.
   */
  private static class MatchesDirectoryNames implements PathFilter {

    Collection<String> paths;

    public MatchesDirectoryNames(Collection<String> dirNames) {
      this.paths = dirNames;
    }

    @Override
    public boolean accept(Path path) {
      return paths.contains(path.getName());
    }
  }

  /**
   * Get the log directory for a specific snapshot
   * @param snapshotDir directory where the specific snapshot will be store
   * @param serverName name of the parent regionserver for the log files
   * @return path to the log home directory for the archive files.
   */
  public static Path getSnapshotHLogsDir(Path snapshotDir, String serverName) {
    return new Path(snapshotDir, HLogUtil.getHLogDirectoryName(serverName));
  }
}