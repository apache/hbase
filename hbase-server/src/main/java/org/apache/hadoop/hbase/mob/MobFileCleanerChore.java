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

package org.apache.hadoop.hbase.mob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.SetMultimap;

/**
 * The class MobFileCleanerChore for running cleaner regularly to remove the expired
 * and obsolete (files which have no active references to) mob files.
 */
@InterfaceAudience.Private
public class MobFileCleanerChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(MobFileCleanerChore.class);
  private final HMaster master;
  private ExpiredMobFileCleaner cleaner;

  static {
    Configuration.addDeprecation(MobConstants.DEPRECATED_MOB_CLEANER_PERIOD,
      MobConstants.MOB_CLEANER_PERIOD);
  }

  public MobFileCleanerChore(HMaster master) {
    super(master.getServerName() + "-MobFileCleanerChore", master,
        master.getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
          MobConstants.DEFAULT_MOB_CLEANER_PERIOD),
        master.getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
          MobConstants.DEFAULT_MOB_CLEANER_PERIOD),
        TimeUnit.SECONDS);
    this.master = master;
    cleaner = new ExpiredMobFileCleaner();
    cleaner.setConf(master.getConfiguration());
    checkObsoleteConfigurations();
  }

  private void checkObsoleteConfigurations() {
    Configuration conf = master.getConfiguration();

    if (conf.get("hbase.mob.compaction.mergeable.threshold") != null) {
      LOG.warn("'hbase.mob.compaction.mergeable.threshold' is obsolete and not used anymore.");
    }
    if (conf.get("hbase.mob.delfile.max.count") != null) {
      LOG.warn("'hbase.mob.delfile.max.count' is obsolete and not used anymore.");
    }
    if (conf.get("hbase.mob.compaction.threads.max") != null) {
      LOG.warn("'hbase.mob.compaction.threads.max' is obsolete and not used anymore.");
    }
    if (conf.get("hbase.mob.compaction.batch.size") != null) {
      LOG.warn("'hbase.mob.compaction.batch.size' is obsolete and not used anymore.");
    }
  }

  @VisibleForTesting
  public MobFileCleanerChore() {
    this.master = null;
  }

  @Override
  protected void chore() {
    TableDescriptors htds = master.getTableDescriptors();

    Map<String, TableDescriptor> map = null;
    try {
      map = htds.getAll();
    } catch (IOException e) {
      LOG.error("MobFileCleanerChore failed", e);
      return;
    }
    for (TableDescriptor htd : map.values()) {
      for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
        if (hcd.isMobEnabled() && hcd.getMinVersions() == 0) {
          try {
            cleaner.cleanExpiredMobFiles(htd.getTableName().getNameAsString(), hcd);
          } catch (IOException e) {
            LOG.error("Failed to clean the expired mob files table={} family={}",
              htd.getTableName().getNameAsString(), hcd.getNameAsString(), e);
          }
        }
      }
      try {
        // Now clean obsolete files for a table
        LOG.info("Cleaning obsolete MOB files from table={}", htd.getTableName());
        cleanupObsoleteMobFiles(master.getConfiguration(), htd.getTableName());
        LOG.info("Cleaning obsolete MOB files finished for table={}", htd.getTableName());
      } catch (IOException e) {
        LOG.error("Failed to clean the obsolete mob files for table={}",htd.getTableName(), e);
      }
    }
  }

  /**
   * Performs housekeeping file cleaning (called by MOB Cleaner chore)
   * @param conf configuration
   * @param table table name
   * @throws IOException exception
   */
  public void cleanupObsoleteMobFiles(Configuration conf, TableName table) throws IOException {

    long minAgeToArchive =
        conf.getLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, MobConstants.DEFAULT_MIN_AGE_TO_ARCHIVE);
    // We check only those MOB files, which creation time is less
    // than maxCreationTimeToArchive. This is a current time - 1h. 1 hour gap
    // gives us full confidence that all corresponding store files will
    // exist at the time cleaning procedure begins and will be examined.
    // So, if MOB file creation time is greater than this maxTimeToArchive,
    // this will be skipped and won't be archived.
    long maxCreationTimeToArchive = EnvironmentEdgeManager.currentTime() - minAgeToArchive;
    try (final Connection conn = ConnectionFactory.createConnection(conf);
        final Admin admin = conn.getAdmin();) {
      TableDescriptor htd = admin.getDescriptor(table);
      List<ColumnFamilyDescriptor> list = MobUtils.getMobColumnFamilies(htd);
      if (list.size() == 0) {
        LOG.info("Skipping non-MOB table [{}]", table);
        return;
      } else {
        LOG.info("Only MOB files whose creation time older than {} will be archived, table={}",
          maxCreationTimeToArchive, table);
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
            if (!fs.exists(storePath)) {
              String errMsg =
                  String.format("Directory %s was deleted during MOB file cleaner chore"
                  + " execution, aborting MOB file cleaner chore.",
                storePath);
              throw new IOException(errMsg);
            }
            RemoteIterator<LocatedFileStatus> rit = fs.listLocatedStatus(storePath);
            List<Path> storeFiles = new ArrayList<Path>();
            // Load list of store files first
            while (rit.hasNext()) {
              Path p = rit.next().getPath();
              if (fs.isFile(p)) {
                storeFiles.add(p);
              }
            }
            LOG.info("Found {} store files in: {}", storeFiles.size(), storePath);
            Path currentPath = null;
            try {
              for (Path pp : storeFiles) {
                currentPath = pp;
                LOG.trace("Store file: {}", pp);
                HStoreFile sf =
                    new HStoreFile(fs, pp, conf, CacheConfig.DISABLED, BloomType.NONE, true);
                sf.initReader();
                byte[] mobRefData = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
                byte[] bulkloadMarkerData = sf.getMetadataValue(HStoreFile.BULKLOAD_TASK_KEY);
                // close store file to avoid memory leaks
                sf.closeStoreFile(true);
                if (mobRefData == null) {
                  if (bulkloadMarkerData == null) {
                    LOG.warn("Found old store file with no MOB_FILE_REFS: {} - "
                        + "can not proceed until all old files will be MOB-compacted.",
                      pp);
                    return;
                  } else {
                    LOG.debug("Skipping file without MOB references (bulkloaded file):{}", pp);
                    continue;
                  }
                }
                // file may or may not have MOB references, but was created by the distributed
                // mob compaction code.
                try {
                  SetMultimap<TableName, String> mobs = MobUtils.deserializeMobFileRefs(mobRefData)
                      .build();
                  LOG.debug("Found {} mob references for store={}", mobs.size(), sf);
                  LOG.trace("Specific mob references found for store={} : {}", sf, mobs);
                  regionMobs.addAll(mobs.values());
                } catch (RuntimeException exception) {
                  throw new IOException("failure getting mob references for hfile " + sf,
                      exception);
                }
              }
            } catch (FileNotFoundException e) {
              LOG.warn("Missing file:{} Starting MOB cleaning cycle from the beginning"+
                " due to error", currentPath, e);
              regionMobs.clear();
              continue;
            }
            succeed = true;
          }

          // Add MOB references for current region/family
          allActiveMobFileName.addAll(regionMobs);
        } // END column families
      } // END regions
      // Check if number of MOB files too big (over 1M)
      if (allActiveMobFileName.size() > 1000000) {
        LOG.warn("Found too many active MOB files: {}, table={}, "+
          "this may result in high memory pressure.",
          allActiveMobFileName.size(), table);
      }
      LOG.debug("Found: {} active mob refs for table={}",
        allActiveMobFileName.size(), table);
      allActiveMobFileName.stream().forEach(LOG::trace);

      // Now scan MOB directories and find MOB files with no references to them
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
            long creationTime = fs.getFileStatus(p).getModificationTime();
            if (creationTime < maxCreationTimeToArchive) {
              LOG.trace("Archiving MOB file {} creation time={}", p,
                (fs.getFileStatus(p).getModificationTime()));
              toArchive.add(p);
            } else {
              LOG.trace("Skipping fresh file: {}. Creation time={}", p,
                fs.getFileStatus(p).getModificationTime());
            }
          } else {
            LOG.trace("Keeping active MOB file: {}", p);
          }
        }
        LOG.info(" MOB Cleaner found {} files to archive for table={} family={}",
          toArchive.size(), table, family);
        archiveMobFiles(conf, table, family.getBytes(), toArchive);
        LOG.info(" MOB Cleaner archived {} files, table={} family={}",
          toArchive.size(), table, family);
      }
    }
  }

  /**
   * Archives the mob files.
   * @param conf The current configuration.
   * @param tableName The table name.
   * @param family The name of the column family.
   * @param storeFiles The files to be archived.
   * @throws IOException exception
   */
  public void archiveMobFiles(Configuration conf, TableName tableName, byte[] family,
      List<Path> storeFiles) throws IOException {

    if (storeFiles.size() == 0) {
      // nothing to remove
      LOG.debug("Skipping archiving old MOB files - no files found for table={} cf={}",
        tableName, Bytes.toString(family));
      return;
    }
    Path mobTableDir = FSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    FileSystem fs = storeFiles.get(0).getFileSystem(conf);

    for (Path p : storeFiles) {
      LOG.debug("MOB Cleaner is archiving: {}", p);
      HFileArchiver.archiveStoreFile(conf, fs, MobUtils.getMobRegionInfo(tableName),
        mobTableDir, family, p);
    }
  }
}
