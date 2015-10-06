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

package org.apache.hadoop.hbase.fs.legacy;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.FsContext;
import org.apache.hadoop.hbase.fs.MasterFileSystem;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureConstants;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MetaUtils;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.backup.HFileArchiver;

@InterfaceAudience.Private
public class LegacyMasterFileSystem extends MasterFileSystem {
  private static final Log LOG = LogFactory.getLog(LegacyMasterFileSystem.class);

  private final Path sidelineDir;
  private final Path snapshotDir;
  private final Path archiveDataDir;
  private final Path archiveDir;
  private final Path tmpDataDir;
  private final Path dataDir;
  private final Path tmpDir;
  private final Path bulkDir;

  /*
   * In a secure env, the protected sub-directories and files under the HBase rootDir
   * would be restricted. The sub-directory will have '700' except the bulk load staging dir,
   * which will have '711'.  The default '700' can be overwritten by setting the property
   * 'hbase.rootdir.perms'. The protected files (version file, clusterId file) will have '600'.
   * The rootDir itself will be created with HDFS default permissions if it does not exist.
   * We will check the rootDir permissions to make sure it has 'x' for all to ensure access
   * to the staging dir. If it does not, we will add it.
   */
  // Permissions for the directories under rootDir that need protection
  private final FsPermission secureRootSubDirPerms;
  // Permissions for the files under rootDir that need protection
  private final FsPermission secureRootFilePerms = new FsPermission("600");
  // Permissions for bulk load staging directory under rootDir
  private final FsPermission HiddenDirPerms = FsPermission.valueOf("-rwx--x--x");

  private final boolean isSecurityEnabled;

  public LegacyMasterFileSystem(Configuration conf, FileSystem fs, Path rootDir) {
    super(conf, fs, rootDir);

    // base directories
    this.sidelineDir = LegacyLayout.getSidelineDir(rootDir);
    this.snapshotDir = LegacyLayout.getSnapshotDir(rootDir);
    this.archiveDir = LegacyLayout.getArchiveDir(rootDir);
    this.archiveDataDir = LegacyLayout.getDataDir(this.archiveDir);
    this.dataDir = LegacyLayout.getDataDir(rootDir);
    this.tmpDir = LegacyLayout.getTempDir(rootDir);
    this.tmpDataDir = LegacyLayout.getDataDir(this.tmpDir);
    this.bulkDir = LegacyLayout.getBulkDir(rootDir);

    this.secureRootSubDirPerms = new FsPermission(conf.get("hbase.rootdir.perms", "700"));
    this.isSecurityEnabled = "kerberos".equalsIgnoreCase(conf.get("hbase.security.authentication"));
  }

  // ==========================================================================
  //  PUBLIC Methods - Namespace related
  // ==========================================================================
  public void createNamespace(NamespaceDescriptor nsDescriptor) throws IOException {
    getFileSystem().mkdirs(getNamespaceDir(FsContext.DATA, nsDescriptor.getName()));
  }

  public void deleteNamespace(String namespaceName) throws IOException {
    FileSystem fs = getFileSystem();
    Path nsDir = getNamespaceDir(FsContext.DATA, namespaceName);

    try {
      for (FileStatus status : fs.listStatus(nsDir)) {
        if (!HConstants.HBASE_NON_TABLE_DIRS.contains(status.getPath().getName())) {
          throw new IOException("Namespace directory contains table dir: " + status.getPath());
        }
      }
      if (!fs.delete(nsDir, true)) {
        throw new IOException("Failed to remove namespace: " + namespaceName);
      }
    } catch (FileNotFoundException e) {
      // File already deleted, continue
      LOG.debug("deleteDirectory throws exception: " + e);
    }
  }

  public Collection<String> getNamespaces(FsContext ctx) throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(), getNamespaceDir(ctx));
    if (stats == null) return Collections.emptyList();

    ArrayList<String> namespaces = new ArrayList<String>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      namespaces.add(stats[i].getPath().getName());
    }
    return namespaces;
  }

  // should return or get a NamespaceDescriptor? how is that different from HTD?

  // ==========================================================================
  //  PUBLIC Methods - Table Descriptor related
  // ==========================================================================s
  @Override
  public boolean createTableDescriptor(FsContext ctx, HTableDescriptor tableDesc, boolean force)
      throws IOException {
    return LegacyTableDescriptor.createTableDescriptor(getFileSystem(),
      getTableDir(ctx, tableDesc.getTableName()), tableDesc, force);
  }

  @Override
  public void updateTableDescriptor(FsContext ctx, HTableDescriptor tableDesc) throws IOException {
    LegacyTableDescriptor.updateTableDescriptor(getFileSystem(),
        getTableDir(ctx, tableDesc.getTableName()), tableDesc);
  }

  @Override
  public HTableDescriptor getTableDescriptor(FsContext ctx, TableName tableName)
      throws IOException {
    return LegacyTableDescriptor.getTableDescriptorFromFs(
        getFileSystem(), getTableDir(ctx, tableName));
  }

  // ==========================================================================
  //  PUBLIC Methods - Table related
  // ==========================================================================
  @Override
  public void deleteTable(FsContext ctx, TableName tableName) throws IOException {
    Path tableDir = getTableDir(ctx, tableName);
    if (!FSUtils.deleteDirectory(getFileSystem(), tableDir)) {
      throw new IOException("Failed delete of " + tableName);
    }
  }

  @Override
  public Collection<TableName> getTables(FsContext ctx, String namespace)
      throws IOException {
    FileStatus[] stats = FSUtils.listStatus(getFileSystem(),
        getNamespaceDir(ctx, namespace), new FSUtils.UserTableDirFilter(getFileSystem()));
    if (stats == null) return Collections.emptyList();

    ArrayList<TableName> tables = new ArrayList<TableName>(stats.length);
    for (int i = 0; i < stats.length; ++i) {
      tables.add(TableName.valueOf(namespace, stats[i].getPath().getName()));
    }
    return tables;
  }

  // ==========================================================================
  //  PROTECTED Methods - Bootstrap
  // ==========================================================================
  @Override
  protected void bootstrapMeta() throws IOException {
    if (!FSUtils.metaRegionExists(getFileSystem(), getRootDir())) {
      bootstrapMeta(getRootDir(), getConfiguration());
    }

    // Create tableinfo-s for hbase:meta if not already there.
    // assume, created table descriptor is for enabling table
    // meta table is a system table, so descriptors are predefined,
    // we should get them from registry.
    createTableDescriptor(HTableDescriptor.metaTableDescriptor(getConfiguration()), false);
  }

  private static void bootstrapMeta(final Path rd, final Configuration c) throws IOException {
    LOG.info("BOOTSTRAP: creating hbase:meta region");
    try {
      // Bootstrapping, make sure blockcache is off.  Else, one will be
      // created here in bootstrap and it'll need to be cleaned up.  Better to
      // not make it in first place.  Turn off block caching for bootstrap.
      // Enable after.
      HRegionInfo metaHRI = new HRegionInfo(HRegionInfo.FIRST_META_REGIONINFO);
      HTableDescriptor metaDescriptor = HTableDescriptor.metaTableDescriptor(c);
      MetaUtils.setInfoFamilyCachingForMeta(metaDescriptor, false);
      HRegion meta = HRegion.createHRegion(metaHRI, rd, c, metaDescriptor, null);
      MetaUtils.setInfoFamilyCachingForMeta(metaDescriptor, true);
      meta.close();
    } catch (IOException e) {
        e = e instanceof RemoteException ?
                ((RemoteException)e).unwrapRemoteException() : e;
      LOG.error("bootstrap", e);
      throw e;
    }
  }

  @Override
  protected void startupCleanup() throws IOException {
    final FileSystem fs = getFileSystem();
    // Check the directories under rootdir.
    checkTempDir(getTempDir(), getConfiguration(), fs);
    final String[] protectedSubDirs = new String[] {
        HConstants.BASE_NAMESPACE_DIR,
        HConstants.HFILE_ARCHIVE_DIRECTORY,
        HConstants.HREGION_LOGDIR_NAME,
        HConstants.HREGION_OLDLOGDIR_NAME,
        MasterProcedureConstants.MASTER_PROCEDURE_LOGDIR,
        HConstants.CORRUPT_DIR_NAME,
        HConstants.HBCK_SIDELINEDIR_NAME,
        MobConstants.MOB_DIR_NAME
    };
    for (String subDir : protectedSubDirs) {
      checkSubDir(new Path(getRootDir(), subDir));
    }

    checkStagingDir();

    // Handle the last few special files and set the final rootDir permissions
    // rootDir needs 'x' for all to support bulk load staging dir
    if (isSecurityEnabled) {
      fs.setPermission(new Path(getRootDir(), HConstants.VERSION_FILE_NAME), secureRootFilePerms);
      fs.setPermission(new Path(getRootDir(), HConstants.CLUSTER_ID_FILE_NAME), secureRootFilePerms);
    }
    FsPermission currentRootPerms = fs.getFileStatus(getRootDir()).getPermission();
    if (!currentRootPerms.getUserAction().implies(FsAction.EXECUTE)
        || !currentRootPerms.getGroupAction().implies(FsAction.EXECUTE)
        || !currentRootPerms.getOtherAction().implies(FsAction.EXECUTE)) {
      LOG.warn("rootdir permissions do not contain 'excute' for user, group or other. "
        + "Automatically adding 'excute' permission for all");
      fs.setPermission(
        getRootDir(),
        new FsPermission(currentRootPerms.getUserAction().or(FsAction.EXECUTE), currentRootPerms
            .getGroupAction().or(FsAction.EXECUTE), currentRootPerms.getOtherAction().or(
          FsAction.EXECUTE)));
    }
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
          HFileArchiver.archiveRegion(fs, getRootDir(), tabledir, regiondir);
        }
      }
      if (!fs.delete(tmpdir, true)) {
        throw new IOException("Unable to clean the temp directory: " + tmpdir);
      }
    }

    // Create the temp directory
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

  /**
   * Make sure the directories under rootDir have good permissions. Create if necessary.
   * @param p
   * @throws IOException
   */
  private void checkSubDir(final Path p) throws IOException {
    final FileSystem fs = getFileSystem();
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
    else {
      if (isSecurityEnabled && !secureRootSubDirPerms.equals(fs.getFileStatus(p).getPermission())) {
        // check whether the permission match
        LOG.warn("Found HBase directory permissions NOT matching expected permissions for "
            + p.toString() + " permissions=" + fs.getFileStatus(p).getPermission()
            + ", expecting " + secureRootSubDirPerms + ". Automatically setting the permissions. "
            + "You can change the permissions by setting \"hbase.rootdir.perms\" in hbase-site.xml "
            + "and restarting the master");
        fs.setPermission(p, secureRootSubDirPerms);
      }
    }
  }

  /**
   * Check permissions for bulk load staging directory. This directory has special hidden
   * permissions. Create it if necessary.
   * @throws IOException
   */
  private void checkStagingDir() throws IOException {
    final FileSystem fs = getFileSystem();
    Path p = new Path(getRootDir(), HConstants.BULKLOAD_STAGING_DIR_NAME);
    try {
      if (!fs.exists(p)) {
        if (!fs.mkdirs(p, HiddenDirPerms)) {
          throw new IOException("Failed to create staging directory " + p.toString());
        }
      } else {
        fs.setPermission(p, HiddenDirPerms);
      }
    } catch (IOException e) {
      LOG.error("Failed to create or set permission on staging directory " + p.toString());
      throw new IOException("Failed to create or set permission on staging directory "
          + p.toString(), e);
    }
  }

  // ==========================================================================
  //  PROTECTED Methods - Path
  // ==========================================================================
  protected Path getNamespaceDir(FsContext ctx) {
    return getBaseDirFromContext(ctx);
  }

  protected Path getNamespaceDir(FsContext ctx, String namespace) {
    return LegacyLayout.getNamespaceDir(getBaseDirFromContext(ctx), namespace);
  }

  protected Path getTableDir(FsContext ctx, TableName table) {
    return LegacyLayout.getTableDir(getBaseDirFromContext(ctx), table);
  }

  protected Path getRegionDir(FsContext ctx, TableName table, HRegionInfo hri) {
    return LegacyLayout.getRegionDir(getBaseDirFromContext(ctx), table, hri);
  }

  public Path getTempDir() {
    return tmpDir;
  }

  protected Path getBaseDirFromContext(FsContext ctx) {
    switch (ctx) {
      case TEMP: return tmpDataDir;
      case DATA: return dataDir;
      case ARCHIVE: return archiveDataDir;
      case SNAPSHOT: return snapshotDir;
      case SIDELINE: return sidelineDir;
      default: throw new RuntimeException("Invalid context: " + ctx);
    }
  }
}
