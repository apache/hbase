/**
 *
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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.ipc.RemoteException;

/**
 * This class abstracts a bunch of operations the HMaster needs to interact with
 * the underlying file system like creating the initial layout, checking file
 * system status, etc.
 */
@InterfaceAudience.Private
public class MasterFileSystem {
  private static final Log LOG = LogFactory.getLog(MasterFileSystem.class);

  // HBase configuration
  private final Configuration conf;
  // Persisted unique cluster ID
  private ClusterId clusterId;
  // Keep around for convenience.
  private final FileSystem fs;
  // root hbase directory on the FS
  private final Path rootdir;
  // hbase temp directory used for table construction and deletion
  private final Path tempdir;

  private final MasterServices services;

  public MasterFileSystem(MasterServices services) throws IOException {
    this.conf = services.getConfiguration();
    this.services = services;
    // Set filesystem to be that of this.rootdir else we get complaints about
    // mismatched filesystems if hbase.rootdir is hdfs and fs.defaultFS is
    // default localfs.  Presumption is that rootdir is fully-qualified before
    // we get to here with appropriate fs scheme.
    this.rootdir = FSUtils.getRootDir(conf);
    this.tempdir = new Path(this.rootdir, HConstants.HBASE_TEMP_DIRECTORY);
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    this.fs = this.rootdir.getFileSystem(conf);
    FSUtils.setFsDefault(conf, new Path(this.fs.getUri()));
    // make sure the fs has the same conf
    fs.setConf(conf);
    // setup the filesystem variable
    createInitialFileSystemLayout();
    HFileSystem.addLocationsOrderInterceptor(conf);
  }

  /**
   * Create initial layout in filesystem.
   * <ol>
   * <li>Check if the meta region exists and is readable, if not create it.
   * Create hbase.version and the hbase:meta directory if not one.
   * </li>
   * </ol>
   * Idempotent.
   */
  private void createInitialFileSystemLayout() throws IOException {
    // check if the root directory exists
    checkRootDir(this.rootdir, conf, this.fs);

    // check if temp directory exists and clean it
    checkTempDir(this.tempdir, conf, this.fs);
  }

  public FileSystem getFileSystem() {
    return this.fs;
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * @return HBase root dir.
   */
  public Path getRootDir() {
    return this.rootdir;
  }

  /**
   * @return HBase temp dir.
   */
  public Path getTempDir() {
    return this.tempdir;
  }

  /**
   * @return The unique identifier generated for this cluster
   */
  public ClusterId getClusterId() {
    return clusterId;
  }

  /**
   * Get the rootdir.  Make sure its wholesome and exists before returning.
   * @param rd
   * @param c
   * @param fs
   * @return hbase.rootdir (after checks for existence and bootstrapping if
   * needed populating the directory with necessary bootup files).
   * @throws IOException
   */
  private Path checkRootDir(final Path rd, final Configuration c, final FileSystem fs)
      throws IOException {
    // If FS is in safe mode wait till out of it.
    FSUtils.waitOnSafeMode(c, c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));

    boolean isSecurityEnabled = "kerberos".equalsIgnoreCase(c.get("hbase.security.authentication"));
    FsPermission rootDirPerms = new FsPermission(c.get("hbase.rootdir.perms", "700"));

    // Filesystem is good. Go ahead and check for hbase.rootdir.
    try {
      if (!fs.exists(rd)) {
        if (isSecurityEnabled) {
          fs.mkdirs(rd, rootDirPerms);
        } else {
          fs.mkdirs(rd);
        }
        // DFS leaves safe mode with 0 DNs when there are 0 blocks.
        // We used to handle this by checking the current DN count and waiting until
        // it is nonzero. With security, the check for datanode count doesn't work --
        // it is a privileged op. So instead we adopt the strategy of the jobtracker
        // and simply retry file creation during bootstrap indefinitely. As soon as
        // there is one datanode it will succeed. Permission problems should have
        // already been caught by mkdirs above.
        FSUtils.setVersion(fs, rd, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
          10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      } else {
        if (!fs.isDirectory(rd)) {
          throw new IllegalArgumentException(rd.toString() + " is not a directory");
        }
        if (isSecurityEnabled && !rootDirPerms.equals(fs.getFileStatus(rd).getPermission())) {
          // check whether the permission match
          LOG.warn("Found rootdir permissions NOT matching expected \"hbase.rootdir.perms\" for "
              + "rootdir=" + rd.toString() + " permissions=" + fs.getFileStatus(rd).getPermission()
              + " and  \"hbase.rootdir.perms\" configured as "
              + c.get("hbase.rootdir.perms", "700") + ". Automatically setting the permissions. You"
              + " can change the permissions by setting \"hbase.rootdir.perms\" in hbase-site.xml "
              + "and restarting the master");
          fs.setPermission(rd, rootDirPerms);
        }
        // as above
        FSUtils.checkVersion(fs, rd, true, c.getInt(HConstants.THREAD_WAKE_FREQUENCY,
          10 * 1000), c.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
            HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS));
      }
    } catch (DeserializationException de) {
      LOG.fatal("Please fix invalid configuration for " + HConstants.HBASE_DIR, de);
      IOException ioe = new IOException();
      ioe.initCause(de);
      throw ioe;
    } catch (IllegalArgumentException iae) {
      LOG.fatal("Please fix invalid configuration for "
        + HConstants.HBASE_DIR + " " + rd.toString(), iae);
      throw iae;
    }
    // Make sure cluster ID exists
    if (!FSUtils.checkClusterIdExists(fs, rd, c.getInt(
        HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000))) {
      FSUtils.setClusterId(fs, rd, new ClusterId(), c.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000));
    }
    clusterId = FSUtils.getClusterId(fs, rd);

    // Make sure the meta region directory exists!
    if (!FSUtils.metaRegionExists(fs, rd)) {
      bootstrap(rd, c);
    }

    // Create tableinfo-s for hbase:meta if not already there.
    // assume, created table descriptor is for enabling table
    // meta table is a system table, so descriptors are predefined,
    // we should get them from registry.
    FSTableDescriptors fsd = new FSTableDescriptors(c, fs, rd);
    fsd.createTableDescriptor(
        new TableDescriptor(fsd.get(TableName.META_TABLE_NAME)));

    return rd;
  }

  /**
   * Make sure the hbase temp directory exists and is empty.
   * NOTE that this method is only executed once just after the master becomes the active one.
   */
  private void checkTempDir(final Path tmpdir, final Configuration c, final FileSystem fs)
      throws IOException {
    // If the temp directory exists, clear the content (left over, from the previous run)
    if (fs.exists(tmpdir)) {
      // Archive table in temp, maybe left over from failed deletion,
      // if not the cleaner will take care of them.
      for (Path tabledir: FSUtils.getTableDirs(fs, tmpdir)) {
        for (Path regiondir: FSUtils.getRegionDirs(fs, tabledir)) {
          HFileArchiver.archiveRegion(fs, this.rootdir, tabledir, regiondir);
        }
      }
      if (!fs.delete(tmpdir, true)) {
        throw new IOException("Unable to clean the temp directory: " + tmpdir);
      }
    }

    // Create the temp directory
    if (!fs.mkdirs(tmpdir)) {
      throw new IOException("HBase temp directory '" + tmpdir + "' creation failure.");
    }
  }

  private static void bootstrap(final Path rd, final Configuration c)
  throws IOException {
    LOG.info("BOOTSTRAP: creating hbase:meta region");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstrap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      HTableDescriptor metaDescriptor = new FSTableDescriptors(c).get(TableName.META_TABLE_NAME);
      setInfoFamilyCachingForMeta(metaDescriptor, false);
      HRegion meta = HRegion.createHRegion(metaHRI, rd, c, metaDescriptor, null);
      setInfoFamilyCachingForMeta(metaDescriptor, true);
      meta.close();
    } catch (IOException e) {
        e = e instanceof RemoteException ?
                ((RemoteException)e).unwrapRemoteException() : e;
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  /**
   * Enable in memory caching for hbase:meta
   */
  public static void setInfoFamilyCachingForMeta(HTableDescriptor metaDescriptor, final boolean b) {
    for (HColumnDescriptor hcd: metaDescriptor.getColumnFamilies()) {
      if (Bytes.equals(hcd.getName(), HConstants.CATALOG_FAMILY)) {
        hcd.setBlockCacheEnabled(b);
        hcd.setInMemory(b);
      }
    }
  }

  public void deleteFamilyFromFS(HRegionInfo region, byte[] familyName, boolean hasMob)
      throws IOException {
    // archive family store files
    Path tableDir = FSUtils.getTableDir(rootdir, region.getTable());
    HFileArchiver.archiveFamily(fs, conf, region, tableDir, familyName);

    // delete the family folder
    Path familyDir = new Path(tableDir,
      new Path(region.getEncodedName(), Bytes.toString(familyName)));
    if (fs.delete(familyDir, true) == false) {
      if (fs.exists(familyDir)) {
        throw new IOException("Could not delete family "
            + Bytes.toString(familyName) + " from FileSystem for region "
            + region.getRegionNameAsString() + "(" + region.getEncodedName()
            + ")");
      }
    }

    // archive and delete mob files
    if (hasMob) {
      Path mobTableDir =
          FSUtils.getTableDir(new Path(getRootDir(), MobConstants.MOB_DIR_NAME), region.getTable());
      HRegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(region.getTable());
      Path mobFamilyDir =
          new Path(mobTableDir,
              new Path(mobRegionInfo.getEncodedName(), Bytes.toString(familyName)));
      // archive mob family store files
      MobUtils.archiveMobStoreFiles(conf, fs, mobRegionInfo, mobFamilyDir, familyName);

      if (!fs.delete(mobFamilyDir, true)) {
        throw new IOException("Could not delete mob store files for family "
            + Bytes.toString(familyName) + " from FileSystem region "
            + mobRegionInfo.getRegionNameAsString() + "(" + mobRegionInfo.getEncodedName() + ")");
      }
    }
  }

  public void stop() {
  }

  public void logFileSystemState(Log log) throws IOException {
    FSUtils.logFileSystemState(fs, rootdir, log);
  }
}
