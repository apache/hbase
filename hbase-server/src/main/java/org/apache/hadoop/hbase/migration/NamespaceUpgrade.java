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

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.List;

/**
 * Upgrades old 0.94 filesystem layout to namespace layout
 * Does the following:
 *
 * - creates system namespace directory and move .META. table there
 * renaming .META. table to hbase:meta,
 * this in turn would require to re-encode the region directory name
 */
public class NamespaceUpgrade implements Tool {
  private static final Log LOG = LogFactory.getLog(NamespaceUpgrade.class);

  private Configuration conf;

  private FileSystem fs;

  private Path rootDir;
  private Path sysNsDir;
  private Path defNsDir;
  private Path baseDirs[];

  public NamespaceUpgrade() throws IOException {
  }

  public void init() throws IOException {
    this.rootDir = FSUtils.getRootDir(conf);
    this.fs = FileSystem.get(conf);
    sysNsDir = FSUtils.getNamespaceDir(rootDir, NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    defNsDir = FSUtils.getNamespaceDir(rootDir, NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
    baseDirs = new Path[]{rootDir,
        new Path(rootDir, HConstants.HFILE_ARCHIVE_DIRECTORY),
        new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY)};
  }


  public void upgradeTableDirs()
      throws IOException, DeserializationException {


    //if new version is written then upgrade is done
    if (verifyNSUpgrade(fs, rootDir)) {
      return;
    }

    makeNamespaceDirs();

    migrateTables();

    migrateSnapshots();

    migrateMeta();

    FSUtils.setVersion(fs, rootDir);
  }

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

  public void migrateTables() throws IOException {
    List<String> sysTables = Lists.newArrayList("-ROOT-",".META.");

    //migrate tables including archive and tmp
    for(Path baseDir: baseDirs) {
      List<Path> oldTableDirs = FSUtils.getLocalTableDirs(fs, baseDir);
      for(Path oldTableDir: oldTableDirs) {
        if (!sysTables.contains(oldTableDir.getName())) {
          Path nsDir = FSUtils.getTableDir(baseDir,
              TableName.valueOf(oldTableDir.getName()));
          if(!fs.exists(nsDir.getParent())) {
            if(!fs.mkdirs(nsDir.getParent())) {
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
    Path newMetaRegionDir = HRegion.getRegionDir(rootDir, HRegionInfo.FIRST_META_REGIONINFO);
    Path newMetaDir = FSUtils.getTableDir(rootDir, TableName.META_TABLE_NAME);
    Path oldMetaDir = new Path(rootDir, ".META.");
    if (fs.exists(oldMetaDir)) {
      LOG.info("Migrating meta table " + oldMetaDir.getName() + " to " + newMetaDir);
      if (!fs.rename(oldMetaDir, newMetaDir)) {
        throw new IOException("Failed to migrate meta table "
            + oldMetaDir.getName() + " to " + newMetaDir);
      }
    }

    //since meta table name has changed
    //rename meta region dir from it's old encoding to new one
    Path oldMetaRegionDir = HRegion.getRegionDir(rootDir,
        new Path(newMetaDir, "1028785192").toString());
    if (fs.exists(oldMetaRegionDir)) {
      LOG.info("Migrating meta region " + oldMetaRegionDir + " to " + newMetaRegionDir);
      if (!fs.rename(oldMetaRegionDir, newMetaRegionDir)) {
        throw new IOException("Failed to migrate meta region "
            + oldMetaRegionDir + " to " + newMetaRegionDir);
      }
    }
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
    if(args.length < 1 || !args[0].equals("--upgrade")) {
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
