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
package org.apache.hadoop.hbase.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ActiveClusterSuffix;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class abstracts a bunch of operations the HMaster needs to interact with the underlying file
 * system like creating the initial layout, checking file system status, etc.
 */
@InterfaceAudience.Private
public class MasterFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(MasterFileSystem.class);

  /** Parameter name for HBase instance root directory permission */
  public static final String HBASE_DIR_PERMS = "hbase.rootdir.perms";

  /** Parameter name for HBase WAL directory permission */
  public static final String HBASE_WAL_DIR_PERMS = "hbase.wal.dir.perms";

  // HBase configuration
  private final Configuration conf;
  // Persisted unique cluster ID
  private ClusterId clusterId;
  // Persisted unique Active Cluster Suffix
  private ActiveClusterSuffix activeClusterSuffix;
  // Keep around for convenience.
  private final FileSystem fs;
  // Keep around for convenience.
  private final FileSystem walFs;
  // root log directory on the FS
  private final Path rootdir;
  // hbase temp directory used for table construction and deletion
  private final Path tempdir;
  // root hbase directory on the FS
  private final Path walRootDir;

  /*
   * In a secure env, the protected sub-directories and files under the HBase rootDir would be
   * restricted. The sub-directory will have '700' except the bulk load staging dir, which will have
   * '711'. The default '700' can be overwritten by setting the property 'hbase.rootdir.perms'. The
   * protected files (version file, clusterId file) will have '600'. The rootDir itself will be
   * created with HDFS default permissions if it does not exist. We will check the rootDir
   * permissions to make sure it has 'x' for all to ensure access to the staging dir. If it does
   * not, we will add it.
   */
  // Permissions for the directories under rootDir that need protection
  private final FsPermission secureRootSubDirPerms;
  // Permissions for the files under rootDir that need protection
  private final FsPermission secureRootFilePerms = new FsPermission("600");
  // Permissions for bulk load staging directory under rootDir
  private final FsPermission HiddenDirPerms = FsPermission.valueOf("-rwx--x--x");

  private boolean isSecurityEnabled;

  public MasterFileSystem(Configuration conf) throws IOException {
    this.conf = conf;
    // Set filesystem to be that of this.rootdir else we get complaints about
    // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
    // default localfs. Presumption is that rootdir is fully-qualified before
    // we get to here with appropriate fs scheme.
    this.rootdir = CommonFSUtils.getRootDir(conf);
    this.tempdir = new Path(this.rootdir, HConstants.HBASE_TEMP_DIRECTORY);
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    this.fs = this.rootdir.getFileSystem(conf);
    this.walRootDir = CommonFSUtils.getWALRootDir(conf);
    this.walFs = CommonFSUtils.getWALFileSystem(conf);
    CommonFSUtils.setFsDefault(conf, new Path(this.walFs.getUri()));
    walFs.setConf(conf);
    CommonFSUtils.setFsDefault(conf, new Path(this.fs.getUri()));
    // make sure the fs has the same conf
    fs.setConf(conf);
    this.secureRootSubDirPerms = new FsPermission(conf.get("hbase.rootdir.perms", "700"));
    this.isSecurityEnabled = "kerberos".equalsIgnoreCase(conf.get("hbase.security.authentication"));
    // setup the filesystem variable
    createInitialFileSystemLayout();
    HFileSystem.addLocationsOrderInterceptor(conf);
  }

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the meta region exists and is readable, if not create it. Create hbase.version and
   * the hbase:meta directory if not one.</li>
   * </ol>
   * Idempotent.
   */
  private void createInitialFileSystemLayout() throws IOException {
    final String[] protectedSubDirs =
      new String[] { HConstants.BASE_NAMESPACE_DIR, HConstants.HFILE_ARCHIVE_DIRECTORY,
        HConstants.HBCK_SIDELINEDIR_NAME, MobConstants.MOB_DIR_NAME };

    // With the introduction of RegionProcedureStore,
    // there's no need to create MasterProcWAL dir here anymore. See HBASE-23715
    final String[] protectedSubLogDirs =
      new String[] { HConstants.HREGION_LOGDIR_NAME, HConstants.HREGION_OLDLOGDIR_NAME,
        HConstants.CORRUPT_DIR_NAME, ReplicationUtils.REMOTE_WAL_DIR_NAME };
    // check if the root directory exists
    checkRootDir(this.rootdir, conf, this.fs);

    // Check the directories under rootdir.
    checkTempDir(this.tempdir, conf, this.fs);
    for (String subDir : protectedSubDirs) {
      checkSubDir(new Path(this.rootdir, subDir), HBASE_DIR_PERMS);
    }

    final String perms;
    if (!this.walRootDir.equals(this.rootdir)) {
      perms = HBASE_WAL_DIR_PERMS;
    } else {
      perms = HBASE_DIR_PERMS;
    }
    for (String subDir : protectedSubLogDirs) {
      checkSubDir(new Path(this.walRootDir, subDir), perms);
    }

    checkStagingDir();

    // Handle the last few special files and set the final rootDir permissions
    // rootDir needs 'x' for all to support bulk load staging dir
    if (isSecurityEnabled) {
      fs.setPermission(new Path(rootdir, HConstants.VERSION_FILE_NAME), secureRootFilePerms);
      fs.setPermission(new Path(rootdir, HConstants.CLUSTER_ID_FILE_NAME), secureRootFilePerms);
      fs.setPermission(new Path(rootdir, HConstants.ACTIVE_CLUSTER_SUFFIX_FILE_NAME),
        secureRootFilePerms);
    }
    FsPermission currentRootPerms = fs.getFileStatus(this.rootdir).getPermission();
    if (
      !currentRootPerms.getUserAction().implies(FsAction.EXECUTE)
        || !currentRootPerms.getGroupAction().implies(FsAction.EXECUTE)
        || !currentRootPerms.getOtherAction().implies(FsAction.EXECUTE)
    ) {
      LOG.warn("rootdir permissions do not contain 'excute' for user, group or other. "
        + "Automatically adding 'excute' permission for all");
      fs.setPermission(this.rootdir,
        new FsPermission(currentRootPerms.getUserAction().or(FsAction.EXECUTE),
          currentRootPerms.getGroupAction().or(FsAction.EXECUTE),
          currentRootPerms.getOtherAction().or(FsAction.EXECUTE)));
    }
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public FileSystem getWALFileSystem() {
    return this.walFs;
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  /** Returns HBase root dir. */
  public Path getRootDir() {
    return this.rootdir;
  }

  /** Returns HBase root log dir. */
  public Path getWALRootDir() {
    return this.walRootDir;
  }

  /** Returns the directory for a give {@code region}. */
  public Path getRegionDir(RegionInfo region) {
    return FSUtils.getRegionDirFromRootDir(getRootDir(), region);
  }

  /** Returns HBase temp dir. */
  public Path getTempDir() {
    return this.tempdir;
  }

  /** Returns The unique identifier generated for this cluster */
  public ClusterId getClusterId() {
    return clusterId;
  }

  /**
   * Get the rootdir. Make sure its wholesome and exists before returning.
   * @return hbase.rootdir (after checks for existence and bootstrapping if needed populating the
   *         directory with necessary bootup files).
   */
  private void checkRootDir(final Path rd, final Configuration c, final FileSystem fs)
    throws IOException {
    int threadWakeFrequency = c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, threadWakeFrequency);

    // Filesystem is good. Go ahead and check for hbase.rootdir.
    FileStatus status;
    try {
      status = fs.getFileStatus(rd);
    } catch (FileNotFoundException e) {
      status = null;
    }
    int versionFileWriteAttempts = c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
      HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
    try {
      if (status == null) {
        if (!fs.mkdirs(rd)) {
          throw new IOException("Can not create configured '" + HConstants.HBASE_DIR + "' " + rd);
        }
        // DFS leaves safe mode with 0 DNs when there are 0 blocks.
        // We used to handle this by checking the current DN count and waiting until
        // it is nonzero. With security, the check for datanode count doesn't work --
        // it is a privileged op. So instead we adopt the strategy of the jobtracker
        // and simply retry file creation during bootstrap indefinitely. As soon as
        // there is one datanode it will succeed. Permission problems should have
        // already been caught by mkdirs above.
        FSUtils.setVersion(fs, rd, threadWakeFrequency, versionFileWriteAttempts);
      } else {
        if (!status.isDirectory()) {
          throw new IllegalArgumentException(
            "Configured '" + HConstants.HBASE_DIR + "' " + rd + " is not a directory.");
        }
        // as above
        FSUtils.checkVersion(fs, rd, true, threadWakeFrequency, versionFileWriteAttempts);
      }
    } catch (DeserializationException de) {
      LOG.error(HBaseMarkers.FATAL, "Please fix invalid configuration for '{}' {}",
        HConstants.HBASE_DIR, rd, de);
      throw new IOException(de);
    } catch (IllegalArgumentException iae) {
      LOG.error(HBaseMarkers.FATAL, "Please fix invalid configuration for '{}' {}",
        HConstants.HBASE_DIR, rd, iae);
      throw iae;
    }
    // Make sure cluster ID exists
    if (
      !FSUtils.checkFileExistsInHbaseRootDir(fs, rootdir, HConstants.CLUSTER_ID_FILE_NAME,
        threadWakeFrequency)
    ) {
      FSUtils.setClusterId(fs, rootdir, new ClusterId(), threadWakeFrequency);
    }
    clusterId = FSUtils.getClusterId(fs, rootdir);
    negotiateActiveClusterSuffixFile(threadWakeFrequency);
  }

  /**
   * Make sure the hbase temp directory exists and is empty. NOTE that this method is only executed
   * once just after the master becomes the active one.
   */
  void checkTempDir(final Path tmpdir, final Configuration c, final FileSystem fs)
    throws IOException {
    // If the temp directory exists, clear the content (left over, from the previous run)
    if (fs.exists(tmpdir)) {
      // Archive table in temp, maybe left over from failed deletion,
      // if not the cleaner will take care of them.
      for (Path tableDir : FSUtils.getTableDirs(fs, tmpdir)) {
        HFileArchiver.archiveRegions(c, fs, this.rootdir, tableDir,
          FSUtils.getRegionDirs(fs, tableDir));
        if (!FSUtils.getRegionDirs(fs, tableDir).isEmpty()) {
          LOG.warn("Found regions in tmp dir after archiving table regions, {}", tableDir);
        }
      }
      // if acl sync to hdfs is enabled, then skip delete tmp dir because ACLs are set
      if (!SnapshotScannerHDFSAclHelper.isAclSyncToHdfsEnabled(c) && !fs.delete(tmpdir, true)) {
        throw new IOException("Unable to clean the temp directory: " + tmpdir);
      }
    }

    // Create the temp directory
    if (!fs.exists(tmpdir)) {
      if (isSecurityEnabled) {
        if (!fs.mkdirs(tmpdir, secureRootSubDirPerms)) {
          throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
        }
      } else {
        if (!fs.mkdirs(tmpdir)) {
          throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
        }
      }
    }
  }

  /**
   * Make sure the directories under rootDir have good permissions. Create if necessary.
   */
  private void checkSubDir(final Path p, final String dirPermsConfName) throws IOException {
    FileSystem fs = p.getFileSystem(conf);
    FsPermission dirPerms = new FsPermission(conf.get(dirPermsConfName, "700"));
    if (!fs.exists(p)) {
      if (isSecurityEnabled) {
        if (!fs.mkdirs(p, secureRootSubDirPerms)) {
          throw new IOException("HBase directory '" + p + "' creation failure.");
        }
      } else {
        if (!fs.mkdirs(p)) {
          throw new IOException("HBase directory '" + p + "' creation failure.");
        }
      }
    }
    if (isSecurityEnabled && !dirPerms.equals(fs.getFileStatus(p).getPermission())) {
      // check whether the permission match
      LOG.warn("Found HBase directory permissions NOT matching expected permissions for "
        + p.toString() + " permissions=" + fs.getFileStatus(p).getPermission() + ", expecting "
        + dirPerms + ". Automatically setting the permissions. "
        + "You can change the permissions by setting \"" + dirPermsConfName
        + "\" in hbase-site.xml " + "and restarting the master");
      fs.setPermission(p, dirPerms);
    }
  }

  /**
   * Check permissions for bulk load staging directory. This directory has special hidden
   * permissions. Create it if necessary.
   */
  private void checkStagingDir() throws IOException {
    Path p = new Path(this.rootdir, HConstants.BULKLOAD_STAGING_DIR_NAME);
    try {
      if (!this.fs.exists(p)) {
        if (!this.fs.mkdirs(p, HiddenDirPerms)) {
          throw new IOException("Failed to create staging directory " + p.toString());
        }
      }
      this.fs.setPermission(p, HiddenDirPerms);

    } catch (IOException e) {
      LOG.error("Failed to create or set permission on staging directory " + p.toString());
      throw new IOException(
        "Failed to create or set permission on staging directory " + p.toString(), e);
    }
  }

  public void deleteFamilyFromFS(RegionInfo region, byte[] familyName) throws IOException {
    deleteFamilyFromFS(rootdir, region, familyName);
  }

  public void deleteFamilyFromFS(Path rootDir, RegionInfo region, byte[] familyName)
    throws IOException {
    // archive family store files
    Path tableDir = CommonFSUtils.getTableDir(rootDir, region.getTable());
    HFileArchiver.archiveFamily(fs, conf, region, tableDir, familyName);

    // delete the family folder
    Path familyDir =
      new Path(tableDir, new Path(region.getEncodedName(), Bytes.toString(familyName)));
    if (fs.delete(familyDir, true) == false) {
      if (fs.exists(familyDir)) {
        throw new IOException(
          "Could not delete family " + Bytes.toString(familyName) + " from FileSystem for region "
            + region.getRegionNameAsString() + "(" + region.getEncodedName() + ")");
      }
    }
  }

  public void stop() {
  }

  public void logFileSystemState(Logger log) throws IOException {
    CommonFSUtils.logFileSystemState(fs, rootdir, log);
  }

  private void negotiateActiveClusterSuffixFile(long wait) throws IOException {
    if (!isReadOnlyModeEnabled(conf)) {
      try {
        // verify the contents against the config set
        ActiveClusterSuffix acs = FSUtils.getActiveClusterSuffix(fs, rootdir);
        LOG.debug("File Suffix {} : Configured suffix {} :  Cluster ID : {}", acs,
          getSuffixFromConfig(), getClusterId());
        if (Objects.equals(acs.getActiveClusterSuffix(), getSuffixFromConfig())) {
          this.activeClusterSuffix = acs;
        } else {
          // throw error
          LOG.info("rootdir {} : Active Cluster File Suffix {} ", rootdir, acs);
          throw new IOException("Cannot start master, because another cluster is running in active "
            + "(read-write) mode on this storage location. Active Cluster Id: {} " + acs
            + " This cluster Id: " + getClusterId());
        }
        LOG.info(
          "This is the active cluster on this storage location, " + "File Suffix {} : Suffix {} : ",
          acs, getActiveClusterSuffix());
      } catch (FileNotFoundException fnfe) {
        // this is the active cluster, create active cluster suffix file if it does not exist
        FSUtils.setActiveClusterSuffix(fs, rootdir, getSuffixFileDataToWrite(), wait);
      }
    } else {
      // this is a replica cluster
      LOG.info("Replica cluster is being started in Read Only Mode");
    }
  }

  public ActiveClusterSuffix getActiveClusterSuffix() {
    return activeClusterSuffix;
  }

  private boolean isReadOnlyModeEnabled(Configuration conf) {
    return conf.getBoolean(HConstants.HBASE_GLOBAL_READONLY_ENABLED_KEY,
      HConstants.HBASE_GLOBAL_READONLY_ENABLED_DEFAULT);
  }

  private String getActiveClusterSuffixFromConfig(Configuration conf) {
    return conf.get(HConstants.HBASE_META_TABLE_SUFFIX,
      HConstants.HBASE_META_TABLE_SUFFIX_DEFAULT_VALUE);
  }

  public String getSuffixFromConfig() {
    return getClusterId().toString() + ":" + getActiveClusterSuffixFromConfig(conf);
  }

  // Used only for testing
  public byte[] getSuffixFileDataToCompare() {
    String str = this.activeClusterSuffix.toString();
    return str.getBytes(StandardCharsets.UTF_8);
  }

  //
  public byte[] getSuffixFileDataToWrite() {
    String str = getClusterId().toString() + ":" + getActiveClusterSuffixFromConfig(conf);
    this.activeClusterSuffix = new ActiveClusterSuffix(str);
    return str.getBytes(StandardCharsets.UTF_8);
  }
}
