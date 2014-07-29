/**
 * The Apache Software Foundation
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
package org.apache.hadoop.hbase.migration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Tool;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

/**
 * Upgrades old 0.94 filesystem layout to namespace layout
 * Does the following:
 *
 * - creates system namespace directory and move .META. table there
 * renaming .META. table to hbase:meta,
 * this in turn would require to re-encode the region directory name
 *
 * <p>The pre-0.96 paths and dir names are hardcoded in here.
 */
public class NamespaceUpgrade implements Tool {
  private static final Log LOG = LogFactory.getLog(NamespaceUpgrade.class);

  private Configuration conf;

  private FileSystem fs;

  private Path rootDir;
  private Path sysNsDir;
  private Path defNsDir;
  private Path baseDirs[];
  private Path backupDir;
  // First move everything to this tmp .data dir in case there is a table named 'data'
  private static final String TMP_DATA_DIR = ".data";
  // Old dir names to migrate.
  private static final String DOT_LOGS = ".logs";
  private static final String DOT_OLD_LOGS = ".oldlogs";
  private static final String DOT_CORRUPT = ".corrupt";
  private static final String DOT_SPLITLOG = "splitlog";
  private static final String DOT_ARCHIVE = ".archive";

  // The old default directory of hbase.dynamic.jars.dir(0.94.12 release).
  private static final String DOT_LIB_DIR = ".lib";

  private static final String OLD_ACL = "_acl_";
  /** Directories that are not HBase table directories */
  static final List<String> NON_USER_TABLE_DIRS = Arrays.asList(new String[] {
      DOT_LOGS,
      DOT_OLD_LOGS,
      DOT_CORRUPT,
      DOT_SPLITLOG,
      HConstants.HBCK_SIDELINEDIR_NAME,
      DOT_ARCHIVE,
      HConstants.SNAPSHOT_DIR_NAME,
      HConstants.HBASE_TEMP_DIRECTORY,
      TMP_DATA_DIR,
      OLD_ACL,
      DOT_LIB_DIR});

  public NamespaceUpgrade() throws IOException {
    super();
  }

  public void init() throws IOException {
    this.rootDir = FSUtils.getRootDir(conf);
    FSUtils.setFsDefault(getConf(), rootDir);
    this.fs = FileSystem.get(conf);
    Path tmpDataDir = new Path(rootDir, TMP_DATA_DIR);
    sysNsDir = new Path(tmpDataDir, NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    defNsDir = new Path(tmpDataDir, NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
    baseDirs = new Path[]{rootDir,
        new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY),
        new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY)};
    backupDir = new Path(rootDir, HConstants.MIGRATION_NAME);
  }


  public void upgradeTableDirs() throws IOException, DeserializationException {
    // if new version is written then upgrade is done
    if (verifyNSUpgrade(fs, rootDir)) {
      return;
    }

    makeNamespaceDirs();

    migrateTables();

    migrateSnapshots();

    migrateDotDirs();

    migrateMeta();

    migrateACL();

    deleteRoot();

    FSUtils.setVersion(fs, rootDir);
  }

  /**
   * Remove the -ROOT- dir. No longer of use.
   * @throws IOException
   */
  public void deleteRoot() throws IOException {
    Path rootDir = new Path(this.rootDir, "-ROOT-");
    if (this.fs.exists(rootDir)) {
      if (!this.fs.delete(rootDir, true)) LOG.info("Failed remove of " + rootDir);
      LOG.info("Deleted " + rootDir);
    }
  }

  /**
   * Rename all the dot dirs -- .data, .archive, etc. -- as data, archive, etc.; i.e. minus the dot.
   * @throws IOException
   */
  public void migrateDotDirs() throws IOException {
    // Dot dirs to rename.  Leave the tmp dir named '.tmp' and snapshots as .hbase-snapshot.
    final Path archiveDir = new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY);
    Path [][] dirs = new Path[][] {
      new Path [] {new Path(rootDir, DOT_CORRUPT), new Path(rootDir, HConstants.CORRUPT_DIR_NAME)},
      new Path [] {new Path(rootDir, DOT_LOGS), new Path(rootDir, HConstants.HREGION_LOGDIR_NAME)},
      new Path [] {new Path(rootDir, DOT_OLD_LOGS),
        new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME)},
      new Path [] {new Path(rootDir, TMP_DATA_DIR),
        new Path(rootDir, HConstants.BASE_NAMESPACE_DIR)},
      new Path[] { new Path(rootDir, DOT_LIB_DIR),
        new Path(rootDir, HConstants.LIB_DIR)}};
    for (Path [] dir: dirs) {
      Path src = dir[0];
      Path tgt = dir[1];
      if (!this.fs.exists(src)) {
        LOG.info("Does not exist: " + src);
        continue;
      }
      rename(src, tgt);
    }
    // Do the .archive dir.  Need to move its subdirs to the default ns dir under data dir... so
    // from '.archive/foo', to 'archive/data/default/foo'.
    Path oldArchiveDir = new Path(rootDir, DOT_ARCHIVE);
    if (this.fs.exists(oldArchiveDir)) {
      // This is a pain doing two nn calls but portable over h1 and h2.
      mkdirs(archiveDir);
      Path archiveDataDir = new Path(archiveDir, HConstants.BASE_NAMESPACE_DIR);
      mkdirs(archiveDataDir);
      rename(oldArchiveDir, new Path(archiveDataDir,
        NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR));
    }
    // Update the system and user namespace dirs removing the dot in front of .data.
    Path dataDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);
    sysNsDir = new Path(dataDir, NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    defNsDir = new Path(dataDir, NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
  }

  private void mkdirs(final Path p) throws IOException {
    if (!this.fs.mkdirs(p)) throw new IOException("Failed make of " + p);
  }

  private void rename(final Path src, final Path tgt) throws IOException {
    if (!fs.rename(src, tgt)) {
      throw new IOException("Failed move " + src + " to " + tgt);
    }
  }

  /**
   * Create the system and default namespaces dirs
   * @throws IOException
   */
  public void makeNamespaceDirs() throws IOException {
    if (!fs.exists(sysNsDir)) {
      if (!fs.mkdirs(sysNsDir)) {
        throw new IOException("Failed to create system namespace dir: " + sysNsDir);
      }
    }
    if (!fs.exists(defNsDir)) {
      if (!fs.mkdirs(defNsDir)) {
        throw new IOException("Failed to create default namespace dir: " + defNsDir);
      }
    }
  }

  /**
   * Migrate all tables into respective namespaces, either default or system.  We put them into
   * a temporary location, '.data', in case a user table is name 'data'.  In a later method we will
   * move stuff from .data to data.
   * @throws IOException
   */
  public void migrateTables() throws IOException {
    List<String> sysTables = Lists.newArrayList("-ROOT-",".META.", ".META");

    // Migrate tables including archive and tmp
    for (Path baseDir: baseDirs) {
      if (!fs.exists(baseDir)) continue;
      List<Path> oldTableDirs = FSUtils.getLocalTableDirs(fs, baseDir);
      for (Path oldTableDir: oldTableDirs) {
        if (NON_USER_TABLE_DIRS.contains(oldTableDir.getName())) continue;
        if (sysTables.contains(oldTableDir.getName())) continue;
        // Make the new directory under the ns to which we will move the table.
        Path nsDir = new Path(this.defNsDir,
          TableName.valueOf(oldTableDir.getName()).getQualifierAsString());
        LOG.info("Moving " + oldTableDir + " to " + nsDir);
        if (!fs.exists(nsDir.getParent())) {
          if (!fs.mkdirs(nsDir.getParent())) {
            throw new IOException("Failed to create namespace dir "+nsDir.getParent());
          }
        }
        if (sysTables.indexOf(oldTableDir.getName()) < 0) {
          LOG.info("Migrating table " + oldTableDir.getName() + " to " + nsDir);
          if (!fs.rename(oldTableDir, nsDir)) {
            throw new IOException("Failed to move "+oldTableDir+" to namespace dir "+nsDir);
          }
        }
      }
    }
  }

  public void migrateSnapshots() throws IOException {
    //migrate snapshot dir
    Path oldSnapshotDir = new Path(rootDir, HConstants.OLD_SNAPSHOT_DIR_NAME);
    Path newSnapshotDir = new Path(rootDir, HConstants.SNAPSHOT_DIR_NAME);
    if (fs.exists(oldSnapshotDir)) {
      boolean foundOldSnapshotDir = false;
      // Logic to verify old snapshot dir culled from SnapshotManager
      // ignore all the snapshots in progress
      FileStatus[] snapshots = fs.listStatus(oldSnapshotDir,
        new SnapshotDescriptionUtils.CompletedSnaphotDirectoriesFilter(fs));
      // loop through all the completed snapshots
      for (FileStatus snapshot : snapshots) {
        Path info = new Path(snapshot.getPath(), SnapshotDescriptionUtils.SNAPSHOTINFO_FILE);
        // if the snapshot is bad
        if (fs.exists(info)) {
          foundOldSnapshotDir = true;
          break;
        }
      }
      if(foundOldSnapshotDir) {
        LOG.info("Migrating snapshot dir");
        if (!fs.rename(oldSnapshotDir, newSnapshotDir)) {
          throw new IOException("Failed to move old snapshot dir "+
              oldSnapshotDir+" to new "+newSnapshotDir);
        }
      }
    }
  }

  public void migrateMeta() throws IOException {
    Path newMetaDir = new Path(this.sysNsDir, TableName.META_TABLE_NAME.getQualifierAsString());
    Path newMetaRegionDir =
      new Path(newMetaDir, HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
    Path oldMetaDir = new Path(rootDir, ".META.");
    if (fs.exists(oldMetaDir)) {
      LOG.info("Migrating meta table " + oldMetaDir.getName() + " to " + newMetaDir);
      if (!fs.rename(oldMetaDir, newMetaDir)) {
        throw new IOException("Failed to migrate meta table "
            + oldMetaDir.getName() + " to " + newMetaDir);
      }
    } else {
      // on windows NTFS, meta's name is .META (note the missing dot at the end)
      oldMetaDir = new Path(rootDir, ".META");
      if (fs.exists(oldMetaDir)) {
        LOG.info("Migrating meta table " + oldMetaDir.getName() + " to " + newMetaDir);
        if (!fs.rename(oldMetaDir, newMetaDir)) {
          throw new IOException("Failed to migrate meta table "
              + oldMetaDir.getName() + " to " + newMetaDir);
        }
      }
    }

    // Since meta table name has changed rename meta region dir from it's old encoding to new one
    Path oldMetaRegionDir = HRegion.getRegionDir(rootDir,
      new Path(newMetaDir, "1028785192").toString());
    if (fs.exists(oldMetaRegionDir)) {
      LOG.info("Migrating meta region " + oldMetaRegionDir + " to " + newMetaRegionDir);
      if (!fs.rename(oldMetaRegionDir, newMetaRegionDir)) {
        throw new IOException("Failed to migrate meta region "
            + oldMetaRegionDir + " to " + newMetaRegionDir);
      }
    }
    // Remove .tableinfo files as they refer to ".META.".
    // They will be recreated by master on startup.
    removeTableInfoInPre96Format(TableName.META_TABLE_NAME);

    Path oldRootDir = new Path(rootDir, "-ROOT-");
    if(!fs.rename(oldRootDir, backupDir)) {
      throw new IllegalStateException("Failed to old data: "+oldRootDir+" to "+backupDir);
    }
  }

  /**
   * Removes .tableinfo files that are laid in pre-96 format (i.e., the tableinfo files are under
   * table directory).
   * @param tableName
   * @throws IOException
   */
  private void removeTableInfoInPre96Format(TableName tableName) throws IOException {
    Path tableDir = FSUtils.getTableDir(rootDir, tableName);
    FileStatus[] status = FSUtils.listStatus(fs, tableDir, TABLEINFO_PATHFILTER);
    if (status == null) return;
    for (FileStatus fStatus : status) {
      FSUtils.delete(fs, fStatus.getPath(), false);
    }
  }

  public void migrateACL() throws IOException {

    TableName oldTableName = TableName.valueOf(OLD_ACL);
    Path oldTablePath = new Path(rootDir, oldTableName.getNameAsString());

    if(!fs.exists(oldTablePath)) {
      return;
    }

    LOG.info("Migrating ACL table");

    TableName newTableName = AccessControlLists.ACL_TABLE_NAME;
    Path newTablePath = FSUtils.getTableDir(rootDir, newTableName);
    HTableDescriptor oldDesc =
        readTableDescriptor(fs, getCurrentTableInfoStatus(fs, oldTablePath));

    if(FSTableDescriptors.getTableInfoPath(fs, newTablePath) == null) {
      LOG.info("Creating new tableDesc for ACL");
      HTableDescriptor newDesc = new HTableDescriptor(oldDesc);
      newDesc.setName(newTableName);
      new FSTableDescriptors(this.conf).createTableDescriptorForTableDirectory(
        newTablePath, newDesc, true);
    }


    ServerName fakeServer = ServerName.valueOf("nsupgrade", 96, 123);
    final WALFactory walFactory = new WALFactory(conf, null, fakeServer.toString());
    WAL metawal = walFactory.getMetaWAL(HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes());
    FSTableDescriptors fst = new FSTableDescriptors(conf);
    HRegion meta = HRegion.openHRegion(rootDir, HRegionInfo.FIRST_META_REGIONINFO,
        fst.get(TableName.META_TABLE_NAME), metawal, conf);
    HRegion region = null;
    try {
      for(Path regionDir : FSUtils.getRegionDirs(fs, oldTablePath)) {
        LOG.info("Migrating ACL region "+regionDir.getName());
        HRegionInfo oldRegionInfo = HRegionFileSystem.loadRegionInfoFileContent(fs, regionDir);
        HRegionInfo newRegionInfo =
            new HRegionInfo(newTableName,
                oldRegionInfo.getStartKey(),
                oldRegionInfo.getEndKey(),
                oldRegionInfo.isSplit(),
                oldRegionInfo.getRegionId());
        newRegionInfo.setOffline(oldRegionInfo.isOffline());
        region =
            new HRegion(
                HRegionFileSystem.openRegionFromFileSystem(conf, fs, oldTablePath,
                    oldRegionInfo, false),
                metawal,
                conf,
                oldDesc,
                null);
        region.initialize();
        updateAcls(region);
        // closing the region would flush it so we don't need an explicit flush to save
        // acl changes.
        region.close();

        //Create new region dir
        Path newRegionDir = new Path(newTablePath, newRegionInfo.getEncodedName());
        if(!fs.exists(newRegionDir)) {
          if(!fs.mkdirs(newRegionDir)) {
            throw new IllegalStateException("Failed to create new region dir: " + newRegionDir);
          }
        }

        //create new region info file, delete in case one exists
        HRegionFileSystem.openRegionFromFileSystem(conf, fs, newTablePath, newRegionInfo, false);

        //migrate region contents
        for(FileStatus file : fs.listStatus(regionDir, new FSUtils.UserTableDirFilter(fs)))  {
          if(file.getPath().getName().equals(HRegionFileSystem.REGION_INFO_FILE))
            continue;
          if(!fs.rename(file.getPath(), newRegionDir))  {
            throw new IllegalStateException("Failed to move file "+file.getPath()+" to " +
                newRegionDir);
          }
        }
        meta.put(MetaTableAccessor.makePutFromRegionInfo(newRegionInfo));
        meta.delete(MetaTableAccessor.makeDeleteFromRegionInfo(oldRegionInfo));
      }
    } finally {
      meta.flushcache();
      meta.waitForFlushesAndCompactions();
      meta.close();
      walFactory.close();
      if(region != null) {
        region.close();
      }
    }
    if(!fs.rename(oldTablePath, backupDir)) {
      throw new IllegalStateException("Failed to old data: "+oldTablePath+" to "+backupDir);
    }
  }

  /**
   * Deletes the old _acl_ entry, and inserts a new one using namespace.
   * @param region
   * @throws IOException
   */
  void updateAcls(HRegion region) throws IOException {
    byte[] rowKey = Bytes.toBytes(NamespaceUpgrade.OLD_ACL);
    // get the old _acl_ entry, if present.
    Get g = new Get(rowKey);
    Result r = region.get(g);
    if (r != null && r.size() > 0) {
      // create a put for new _acl_ entry with rowkey as hbase:acl
      Put p = new Put(AccessControlLists.ACL_GLOBAL_NAME);
      for (Cell c : r.rawCells()) {
        p.addImmutable(CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
      }
      region.put(p);
      // delete the old entry
      Delete del = new Delete(rowKey);
      region.delete(del);
    }

    // delete the old entry for '-ROOT-'
    rowKey = Bytes.toBytes(TableName.OLD_ROOT_STR);
    Delete del = new Delete(rowKey);
    region.delete(del);

    // rename .META. to hbase:meta
    rowKey = Bytes.toBytes(TableName.OLD_META_STR);
    g = new Get(rowKey);
    r = region.get(g);
    if (r != null && r.size() > 0) {
      // create a put for new .META. entry with rowkey as hbase:meta
      Put p = new Put(TableName.META_TABLE_NAME.getName());
      for (Cell c : r.rawCells()) {
        p.addImmutable(CellUtil.cloneFamily(c), CellUtil.cloneQualifier(c), CellUtil.cloneValue(c));
      }
      region.put(p);
      // delete the old entry
      del = new Delete(rowKey);
      region.delete(del);
    }
  }

  //Culled from FSTableDescriptors
  private static HTableDescriptor readTableDescriptor(FileSystem fs,
                                                      FileStatus status) throws IOException {
    int len = Ints.checkedCast(status.getLen());
    byte [] content = new byte[len];
    FSDataInputStream fsDataInputStream = fs.open(status.getPath());
    try {
      fsDataInputStream.readFully(content);
    } finally {
      fsDataInputStream.close();
    }
    HTableDescriptor htd = null;
    try {
      htd = HTableDescriptor.parseFrom(content);
    } catch (DeserializationException e) {
      throw new IOException("content=" + Bytes.toShort(content), e);
    }
    return htd;
  }

  private static final PathFilter TABLEINFO_PATHFILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      // Accept any file that starts with TABLEINFO_NAME
      return p.getName().startsWith(".tableinfo");
    }
  };

  static final Comparator<FileStatus> TABLEINFO_FILESTATUS_COMPARATOR =
  new Comparator<FileStatus>() {
    @Override
    public int compare(FileStatus left, FileStatus right) {
      return right.compareTo(left);
    }};

  // logic culled from FSTableDescriptors
  static FileStatus getCurrentTableInfoStatus(FileSystem fs, Path dir)
  throws IOException {
    FileStatus [] status = FSUtils.listStatus(fs, dir, TABLEINFO_PATHFILTER);
    if (status == null || status.length < 1) return null;
    FileStatus mostCurrent = null;
    for (FileStatus file : status) {
      if (mostCurrent == null || TABLEINFO_FILESTATUS_COMPARATOR.compare(file, mostCurrent) < 0) {
        mostCurrent = file;
      }
    }
    return mostCurrent;
  }

  public static boolean verifyNSUpgrade(FileSystem fs, Path rootDir)
      throws IOException {
    try {
      return FSUtils.getVersion(fs, rootDir).equals(HConstants.FILE_SYSTEM_VERSION);
    } catch (DeserializationException e) {
      throw new IOException("Failed to verify namespace upgrade", e);
    }
  }


  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1 || !args[0].equals("--upgrade")) {
      System.out.println("Usage: <CMD> --upgrade");
      return 0;
    }
    init();
    upgradeTableDirs();
    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
