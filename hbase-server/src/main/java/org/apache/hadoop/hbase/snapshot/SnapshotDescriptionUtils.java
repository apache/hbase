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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.ShadedAccessControlUtil;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;

/**
 * Utility class to help manage {@link SnapshotDescription SnapshotDesriptions}.
 * <p>
 * Snapshots are laid out on disk like this:
 *
 * <pre>
 * /hbase/.snapshots
 *          /.tmp                &lt;---- working directory
 *          /[snapshot name]     &lt;----- completed snapshot
 * </pre>
 *
 * A completed snapshot named 'completed' then looks like (multiple regions, servers, files, etc.
 * signified by '...' on the same directory depth).
 *
 * <pre>
 * /hbase/.snapshots/completed
 *                   .snapshotinfo          &lt;--- Description of the snapshot
 *                   .tableinfo             &lt;--- Copy of the tableinfo
 *                    /.logs
 *                        /[server_name]
 *                            /... [log files]
 *                         ...
 *                   /[region name]           &lt;---- All the region's information
 *                   .regioninfo              &lt;---- Copy of the HRegionInfo
 *                      /[column family name]
 *                          /[hfile name]     &lt;--- name of the hfile in the real region
 *                          ...
 *                      ...
 *                    ...
 * </pre>
 *
 * Utility methods in this class are useful for getting the correct locations for different parts of
 * the snapshot, as well as moving completed snapshots into place (see
 * {@link #completeSnapshot}, and writing the
 * {@link SnapshotDescription} to the working snapshot directory.
 */
@InterfaceAudience.Private
public final class SnapshotDescriptionUtils {

  /**
   * Filter that only accepts completed snapshot directories
   */
  public static class CompletedSnaphotDirectoriesFilter extends FSUtils.BlackListDirFilter {

    /**
     * @param fs
     */
    public CompletedSnaphotDirectoriesFilter(FileSystem fs) {
      super(fs, Collections.singletonList(SNAPSHOT_TMP_DIR_NAME));
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotDescriptionUtils.class);
  /**
   * Version of the fs layout for a snapshot. Future snapshots may have different file layouts,
   * which we may need to read in differently.
   */
  public static final int SNAPSHOT_LAYOUT_VERSION = SnapshotManifestV2.DESCRIPTOR_VERSION;

  // snapshot directory constants
  /**
   * The file contains the snapshot basic information and it is under the directory of a snapshot.
   */
  public static final String SNAPSHOTINFO_FILE = ".snapshotinfo";

  /** Temporary directory under the snapshot directory to store in-progress snapshots */
  public static final String SNAPSHOT_TMP_DIR_NAME = ".tmp";

  /**
   * The configuration property that determines the filepath of the snapshot
   * base working directory
   */
  public static final String SNAPSHOT_WORKING_DIR = "hbase.snapshot.working.dir";

  // snapshot operation values
  /** Default value if no start time is specified */
  public static final long NO_SNAPSHOT_START_TIME_SPECIFIED = 0;

  // Default value if no ttl is specified for Snapshot
  private static final long NO_SNAPSHOT_TTL_SPECIFIED = 0;

  public static final String MASTER_SNAPSHOT_TIMEOUT_MILLIS = "hbase.snapshot.master.timeout.millis";

  /** By default, wait 300 seconds for a snapshot to complete */
  public static final long DEFAULT_MAX_WAIT_TIME = 60000 * 5 ;


  /**
   * By default, check to see if the snapshot is complete (ms)
   * @deprecated Use {@link #DEFAULT_MAX_WAIT_TIME} instead.
   * */
  @Deprecated
  public static final int SNAPSHOT_TIMEOUT_MILLIS_DEFAULT = 60000 * 5;

  /**
   * Conf key for # of ms elapsed before injecting a snapshot timeout error when waiting for
   * completion.
   * @deprecated Use {@link #MASTER_SNAPSHOT_TIMEOUT_MILLIS} instead.
   */
  @Deprecated
  public static final String SNAPSHOT_TIMEOUT_MILLIS_KEY = "hbase.snapshot.master.timeoutMillis";

  private SnapshotDescriptionUtils() {
    // private constructor for utility class
  }

  /**
   * @param conf {@link Configuration} from which to check for the timeout
   * @param type type of snapshot being taken
   * @param defaultMaxWaitTime Default amount of time to wait, if none is in the configuration
   * @return the max amount of time the master should wait for a snapshot to complete
   */
  public static long getMaxMasterTimeout(Configuration conf, SnapshotDescription.Type type,
      long defaultMaxWaitTime) {
    String confKey;
    switch (type) {
    case DISABLED:
    default:
      confKey = MASTER_SNAPSHOT_TIMEOUT_MILLIS;
    }
    return Math.max(conf.getLong(confKey, defaultMaxWaitTime),
        conf.getLong(SNAPSHOT_TIMEOUT_MILLIS_KEY, defaultMaxWaitTime));
  }

  /**
   * Get the snapshot root directory. All the snapshots are kept under this directory, i.e.
   * ${hbase.rootdir}/.snapshot
   * @param rootDir hbase root directory
   * @return the base directory in which all snapshots are kept
   */
  public static Path getSnapshotRootDir(final Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
  }

  /**
   * Get the directory for a specified snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param snapshot snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final SnapshotDescription snapshot, final Path rootDir) {
    return getCompletedSnapshotDir(snapshot.getName(), rootDir);
  }

  /**
   * Get the directory for a completed snapshot. This directory is a sub-directory of snapshot root
   * directory and all the data files for a snapshot are kept under this directory.
   * @param snapshotName name of the snapshot being taken
   * @param rootDir hbase root directory
   * @return the final directory for the completed snapshot
   */
  public static Path getCompletedSnapshotDir(final String snapshotName, final Path rootDir) {
    return getSpecifiedSnapshotDir(getSnapshotsDir(rootDir), snapshotName);
  }

  /**
   * Get the general working directory for snapshots - where they are built, where they are
   * temporarily copied on export, etc.
   * @param rootDir root directory of the HBase installation
   * @param conf Configuration of the HBase instance
   * @return Path to the snapshot tmp directory, relative to the passed root directory
   */
  public static Path getWorkingSnapshotDir(final Path rootDir, final Configuration conf) {
    return new Path(conf.get(SNAPSHOT_WORKING_DIR,
        getDefaultWorkingSnapshotDir(rootDir).toString()));
  }

  /**
   * Get the directory to build a snapshot, before it is finalized
   * @param snapshot snapshot that will be built
   * @param rootDir root directory of the hbase installation
   * @param conf Configuration of the HBase instance
   * @return {@link Path} where one can build a snapshot
   */
  public static Path getWorkingSnapshotDir(SnapshotDescription snapshot, final Path rootDir,
      Configuration conf) {
    return getWorkingSnapshotDir(snapshot.getName(), rootDir, conf);
  }

  /**
   * Get the directory to build a snapshot, before it is finalized
   * @param snapshotName name of the snapshot
   * @param rootDir root directory of the hbase installation
   * @param conf Configuration of the HBase instance
   * @return {@link Path} where one can build a snapshot
   */
  public static Path getWorkingSnapshotDir(String snapshotName, final Path rootDir,
      Configuration conf) {
    return getSpecifiedSnapshotDir(getWorkingSnapshotDir(rootDir, conf), snapshotName);
  }

  /**
   * Get the directory within the given filepath to store the snapshot instance
   * @param snapshotsDir directory to store snapshot directory within
   * @param snapshotName name of the snapshot to take
   * @return the final directory for the snapshot in the given filepath
   */
  private static final Path getSpecifiedSnapshotDir(final Path snapshotsDir, String snapshotName) {
    return new Path(snapshotsDir, snapshotName);
  }

  /**
   * @param rootDir hbase root directory
   * @return the directory for all completed snapshots;
   */
  public static final Path getSnapshotsDir(Path rootDir) {
    return new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
  }

  /**
   * Determines if the given workingDir is a subdirectory of the given "root directory"
   * @param workingDir a directory to check
   * @param rootDir root directory of the HBase installation
   * @return true if the given workingDir is a subdirectory of the given root directory,
   *   false otherwise
   */
  public static boolean isSubDirectoryOf(final Path workingDir, final Path rootDir) {
    return workingDir.toString().startsWith(rootDir.toString() + Path.SEPARATOR);
  }

  /**
   * Determines if the given workingDir is a subdirectory of the default working snapshot directory
   * @param workingDir a directory to check
   * @param conf configuration for the HBase cluster
   * @return true if the given workingDir is a subdirectory of the default working directory for
   *   snapshots, false otherwise
   * @throws IOException if we can't get the root dir
   */
  public static boolean isWithinDefaultWorkingDir(final Path workingDir, Configuration conf)
    throws IOException {
    Path defaultWorkingDir = getDefaultWorkingSnapshotDir(CommonFSUtils.getRootDir(conf));
    return workingDir.equals(defaultWorkingDir) || isSubDirectoryOf(workingDir, defaultWorkingDir);
  }

  /**
   * Get the default working directory for snapshots - where they are built, where they are
   * temporarily copied on export, etc.
   * @param rootDir root directory of the HBase installation
   * @return Path to the default snapshot tmp directory, relative to the passed root directory
   */
  private static Path getDefaultWorkingSnapshotDir(final Path rootDir) {
    return new Path(getSnapshotsDir(rootDir), SNAPSHOT_TMP_DIR_NAME);
  }

  /**
   * Convert the passed snapshot description into a 'full' snapshot description based on default
   * parameters, if none have been supplied. This resolves any 'optional' parameters that aren't
   * supplied to their default values.
   * @param snapshot general snapshot descriptor
   * @param conf Configuration to read configured snapshot defaults if snapshot is not complete
   * @return a valid snapshot description
   * @throws IllegalArgumentException if the {@link SnapshotDescription} is not a complete
   *           {@link SnapshotDescription}.
   */
  public static SnapshotDescription validate(SnapshotDescription snapshot, Configuration conf)
      throws IllegalArgumentException, IOException {
    if (!snapshot.hasTable()) {
      throw new IllegalArgumentException(
          "Descriptor doesn't apply to a table, so we can't build it.");
    }

    // set the creation time, if one hasn't been set
    long time = snapshot.getCreationTime();
    if (time == SnapshotDescriptionUtils.NO_SNAPSHOT_START_TIME_SPECIFIED) {
      time = EnvironmentEdgeManager.currentTime();
      LOG.debug("Creation time not specified, setting to:" + time + " (current time:"
          + EnvironmentEdgeManager.currentTime() + ").");
      SnapshotDescription.Builder builder = snapshot.toBuilder();
      builder.setCreationTime(time);
      snapshot = builder.build();
    }

    long ttl = snapshot.getTtl();
    // set default ttl(sec) if it is not set already or the value is out of the range
    if (ttl == SnapshotDescriptionUtils.NO_SNAPSHOT_TTL_SPECIFIED ||
        ttl > TimeUnit.MILLISECONDS.toSeconds(Long.MAX_VALUE)) {
      final long defaultSnapshotTtl = conf.getLong(HConstants.DEFAULT_SNAPSHOT_TTL_CONFIG_KEY,
          HConstants.DEFAULT_SNAPSHOT_TTL);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Snapshot current TTL value: {} resetting it to default value: {}", ttl,
            defaultSnapshotTtl);
      }
      ttl = defaultSnapshotTtl;
    }
    SnapshotDescription.Builder builder = snapshot.toBuilder();
    builder.setTtl(ttl);
    snapshot = builder.build();

    // set the acl to snapshot if security feature is enabled.
    if (isSecurityAvailable(conf)) {
      snapshot = writeAclToSnapshotDescription(snapshot, conf);
    }
    return snapshot;
  }

  /**
   * Write the snapshot description into the working directory of a snapshot
   * @param snapshot description of the snapshot being taken
   * @param workingDir working directory of the snapshot
   * @param fs {@link FileSystem} on which the snapshot should be taken
   * @throws IOException if we can't reach the filesystem and the file cannot be cleaned up on
   *           failure
   */
  public static void writeSnapshotInfo(SnapshotDescription snapshot, Path workingDir, FileSystem fs)
      throws IOException {
    FsPermission perms = CommonFSUtils.getFilePermissions(fs, fs.getConf(),
      HConstants.DATA_FILE_UMASK_KEY);
    Path snapshotInfo = new Path(workingDir, SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
    try (FSDataOutputStream out = CommonFSUtils.create(fs, snapshotInfo, perms, true)){
      snapshot.writeTo(out);
    } catch (IOException e) {
      // if we get an exception, try to remove the snapshot info
      if (!fs.delete(snapshotInfo, false)) {
        String msg = "Couldn't delete snapshot info file: " + snapshotInfo;
        LOG.error(msg);
        throw new IOException(msg);
      }
    }
  }

  /**
   * Read in the {@link org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription} stored for the snapshot in the passed directory
   * @param fs filesystem where the snapshot was taken
   * @param snapshotDir directory where the snapshot was stored
   * @return the stored snapshot description
   * @throws CorruptedSnapshotException if the
   * snapshot cannot be read
   */
  public static SnapshotDescription readSnapshotInfo(FileSystem fs, Path snapshotDir)
      throws CorruptedSnapshotException {
    Path snapshotInfo = new Path(snapshotDir, SNAPSHOTINFO_FILE);
    try (FSDataInputStream in = fs.open(snapshotInfo)){
      return SnapshotDescription.parseFrom(in);
    } catch (IOException e) {
      throw new CorruptedSnapshotException("Couldn't read snapshot info from:" + snapshotInfo, e);
    }
  }

  /**
   * Commits the snapshot process by moving the working snapshot
   * to the finalized filepath
   *
   * @param snapshotDir The file path of the completed snapshots
   * @param workingDir  The file path of the in progress snapshots
   * @param fs The file system of the completed snapshots
   * @param workingDirFs The file system of the in progress snapshots
   * @param conf Configuration
   *
   * @throws SnapshotCreationException if the snapshot could not be moved
   * @throws IOException the filesystem could not be reached
   */
  public static void completeSnapshot(Path snapshotDir, Path workingDir, FileSystem fs,
    FileSystem workingDirFs, final Configuration conf)
    throws SnapshotCreationException, IOException {
    LOG.debug("Sentinel is done, just moving the snapshot from " + workingDir + " to "
      + snapshotDir);
    // If the working and completed snapshot directory are on the same file system, attempt
    // to rename the working snapshot directory to the completed location. If that fails,
    // or the file systems differ, attempt to copy the directory over, throwing an exception
    // if this fails
    URI workingURI = workingDirFs.getUri();
    URI rootURI = fs.getUri();
    if ((!workingURI.getScheme().equals(rootURI.getScheme()) ||
      workingURI.getAuthority() == null ||
      !workingURI.getAuthority().equals(rootURI.getAuthority()) ||
      workingURI.getUserInfo() == null ||
      !workingURI.getUserInfo().equals(rootURI.getUserInfo()) ||
      !fs.rename(workingDir, snapshotDir)) && !FileUtil.copy(workingDirFs, workingDir, fs,
      snapshotDir, true, true, conf)) {
      throw new SnapshotCreationException("Failed to copy working directory(" + workingDir
        + ") to completed directory(" + snapshotDir + ").");
    }
  }

  /**
   * Check if the user is this table snapshot's owner
   * @param snapshot the table snapshot description
   * @param user the user
   * @return true if the user is the owner of the snapshot,
   *         false otherwise or the snapshot owner field is not present.
   */
  public static boolean isSnapshotOwner(org.apache.hadoop.hbase.client.SnapshotDescription snapshot,
      User user) {
    if (user == null) return false;
    return user.getShortName().equals(snapshot.getOwner());
  }

  public static boolean isSecurityAvailable(Configuration conf) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf); Admin admin = conn.getAdmin()) {
      return admin.tableExists(PermissionStorage.ACL_TABLE_NAME);
    }
  }

  private static SnapshotDescription writeAclToSnapshotDescription(SnapshotDescription snapshot,
      Configuration conf) throws IOException {
    ListMultimap<String, UserPermission> perms =
        User.runAsLoginUser(new PrivilegedExceptionAction<ListMultimap<String, UserPermission>>() {
          @Override
          public ListMultimap<String, UserPermission> run() throws Exception {
            return PermissionStorage.getTablePermissions(conf,
              TableName.valueOf(snapshot.getTable()));
          }
        });
    return snapshot.toBuilder()
        .setUsersAndPermissions(ShadedAccessControlUtil.toUserTablePermissions(perms)).build();
  }
}
