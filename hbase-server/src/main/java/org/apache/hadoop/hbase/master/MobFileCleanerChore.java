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

package org.apache.hadoop.hbase.master;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.ExpiredMobFileCleaner;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.procedure2.LockType;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HFileArchiveUtil;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Class ExpiredMobFileCleanerChore for running cleaner regularly to remove the expired
 * mob files.
 */
@InterfaceAudience.Private
public class MobFileCleanerChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(MobFileCleanerChore.class);
  private final HMaster master;
  private ExpiredMobFileCleaner cleaner;

  public MobFileCleanerChore(HMaster master) {
    super(master.getServerName() + "-ExpiredMobFileCleanerChore", master, master.getConfiguration()
      .getInt(MobConstants.MOB_CLEANER_PERIOD, MobConstants.DEFAULT_MOB_CLEANER_PERIOD), master
      .getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD), TimeUnit.SECONDS);
    this.master = master;
    cleaner = new ExpiredMobFileCleaner();
    cleaner.setConf(master.getConfiguration());
  }

  @VisibleForTesting
  public MobFileCleanerChore() {
    this.master = null;
  }
  
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="REC_CATCH_EXCEPTION",
    justification="Intentional")

  protected void chore() {
    try {

      TableDescriptors htds = master.getTableDescriptors();
      Map<String, TableDescriptor> map = htds.getAll();
      for (TableDescriptor htd : map.values()) {
        for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
          if (hcd.isMobEnabled() && hcd.getMinVersions() == 0) {
            // clean only for mob-enabled column.
            // obtain a read table lock before cleaning, synchronize with MobFileCompactionChore.
            final LockManager.MasterLock lock = master.getLockManager().createMasterLock(
                MobUtils.getTableLockName(htd.getTableName()), LockType.SHARED,
                this.getClass().getSimpleName() + ": Cleaning expired mob files");
            try {
              lock.acquire();
              cleaner.cleanExpiredMobFiles(htd.getTableName().getNameAsString(), hcd);
            } finally {
              lock.release();
            }
          }
        }
        // Now clean obsolete files for a table
        LOG.info("Cleaning obsolete MOB files ...");
        cleanupObsoleteMobFiles(master.getConfiguration(), htd.getTableName());
        LOG.info("Cleaning obsolete MOB files finished");
      }
    } catch (Exception e) {
      LOG.error("Fail to clean the expired mob files", e);
    } 
  }
  /**
   * Performs housekeeping file cleaning (called by MOB Cleaner chore)
   * @param conf configuration
   * @param table table name
   * @throws IOException
   */
  public void cleanupObsoleteMobFiles(Configuration conf, TableName table)
      throws IOException {

    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final Admin admin = conn.getAdmin();) {
      TableDescriptor htd = admin.getDescriptor(table);
      List<ColumnFamilyDescriptor> list = MobUtils.getMobColumnFamilies(htd);
      if (list.size() == 0) {
        LOG.info("Skipping non-MOB table [{}]",  table);
        return;
      }
      Path rootDir = FSUtils.getRootDir(conf);
      Path tableDir = FSUtils.getTableDir(rootDir, table);
      // How safe is this call?
      List<Path> regionDirs = FSUtils.getRegionDirs(FileSystem.get(conf), tableDir);

      Set<String> allActiveMobFileName = new HashSet<String>();
      FileSystem fs = FileSystem.get(conf);
      for (Path regionPath : regionDirs) {
        for (ColumnFamilyDescriptor hcd : list) {
          String family = hcd.getNameAsString();
          Path storePath = new Path(regionPath, family);
          boolean succeed = false;
          Set<String> regionMobs = new HashSet<String>();
          while (!succeed) {
            // TODO handle FNFE
            RemoteIterator<LocatedFileStatus> rit = fs.listLocatedStatus(storePath);
            List<Path> storeFiles = new ArrayList<Path>();
            // Load list of store files first
            while (rit.hasNext()) {
              Path p = rit.next().getPath();
              if (fs.isFile(p)) {
                storeFiles.add(p);
              }
            }
            try {
              for (Path pp : storeFiles) {
                HStoreFile sf =
                    new HStoreFile(fs, pp, conf, CacheConfig.DISABLED, BloomType.NONE, true);
                sf.initReader();
                byte[] mobRefData = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
                byte[] bulkloadMarkerData = sf.getMetadataValue(HStoreFile.BULKLOAD_TASK_KEY);
                if (mobRefData == null && bulkloadMarkerData == null) {
                  LOG.info("Found old store file with no MOB_FILE_REFS: {} - "
                    + "can not proceed until all old files will be MOB-compacted.", pp);
                  return;
                } else if (mobRefData == null && bulkloadMarkerData != null) {
                  LOG.info("Skipping file without MOB references (bulkloaded file):{}", pp);
                  continue;
                }
                if (mobRefData.length > 1) {
                  // if length = 1 means NULL, that there are no MOB references
                  // in this store file, but the file was created by new MOB code
                  String[] mobs = new String(mobRefData).split(",");
                  regionMobs.addAll(Arrays.asList(mobs));
                }
              }
            } catch (FileNotFoundException e) {
              // TODO
              LOG.warn(e.getMessage());
              continue;
            }
            succeed = true;
          }
          // Add MOB refs for current region/family
          allActiveMobFileName.addAll(regionMobs);
        } // END column families
      } // END regions
      // Check if number of MOB files too big (over 1M)
      if (allActiveMobFileName.size() > 1000000) {
        LOG.warn("Found too many active MOB files: {}, this may result in high memory pressure.", 
          allActiveMobFileName.size());
      }
      // Now scan MOB directories and find MOB files with no references to them
      long now = System.currentTimeMillis();
      long minAgeToArchive = conf.getLong(MobConstants.MOB_MINIMUM_FILE_AGE_TO_ARCHIVE_KEY,
        MobConstants.DEFAULT_MOB_MINIMUM_FILE_AGE_TO_ARCHIVE);
      for (ColumnFamilyDescriptor hcd : list) {
        List<Path> toArchive = new ArrayList<Path>();
        String family = hcd.getNameAsString();
        Path dir = MobUtils.getMobFamilyPath(conf, table, family);
        RemoteIterator<LocatedFileStatus> rit = fs.listLocatedStatus(dir);
        while (rit.hasNext()) {
          LocatedFileStatus lfs = rit.next();
          Path p = lfs.getPath();
          if (!allActiveMobFileName.contains(p.getName())) {
            // MOB is not in a list of active references, but it can be too
            // fresh, skip it in this case
            /* DEBUG */ LOG.debug(
              " Age=" + (now - fs.getFileStatus(p).getModificationTime()) + " MOB file=" + p);
            if (now - fs.getFileStatus(p).getModificationTime() > minAgeToArchive) {
              toArchive.add(p);
            } else {
              LOG.debug("Skipping fresh file: {}", p);
            }
          }
        }
        LOG.info(" MOB Cleaner found {} files for family={}", toArchive.size() , family);
        removeMobFiles(conf, table, family.getBytes(), toArchive);
        LOG.info(" MOB Cleaner archived {} files", toArchive.size());
      }
    }
  }
  
  
  /**
   * Archives the mob files.
   * @param conf The current configuration.
   * @param tableName The table name.
   * @param family The name of the column family.
   * @param storeFiles The files to be archived.
   * @throws IOException
   */
  public void removeMobFiles(Configuration conf, TableName tableName, byte[] family,
      List<Path> storeFiles) throws IOException {

    if (storeFiles.size() == 0) {
      // nothing to remove
      LOG.debug("Skipping archiving old MOB file: collection is empty");
      return;
    }
    Path mobTableDir = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    FileSystem fs = storeFiles.get(0).getFileSystem(conf);
    Path storeArchiveDir = HFileArchiveUtil.getStoreArchivePath(conf, MobUtils.getMobRegionInfo(tableName),
      mobTableDir, family);

    for (Path p : storeFiles) {
      Path archiveFilePath = new Path(storeArchiveDir, p.getName());
      if (fs.exists(archiveFilePath)) {
        LOG.warn("MOB Cleaner skip archiving: {} because it has been archived already", p);
        continue;
      }
      LOG.info("MOB Cleaner is archiving: {}", p);
      HFileArchiver.archiveStoreFile(conf, fs, MobUtils.getMobRegionInfo(tableName), mobTableDir, family, p);
    }
  }
}
