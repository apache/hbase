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
package org.apache.hadoop.hbase.mob;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.SetMultimap;

@InterfaceAudience.Private
public final class MobFileCleanupUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MobFileCleanupUtil.class);

  private MobFileCleanupUtil() {
  }

  /**
   * Performs housekeeping file cleaning (called by MOB Cleaner chore)
   * @param conf  configuration
   * @param table table name
   * @throws IOException exception
   */
  public static void cleanupObsoleteMobFiles(Configuration conf, TableName table, Admin admin)
    throws IOException {
    long minAgeToArchive =
      conf.getLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY, MobConstants.DEFAULT_MIN_AGE_TO_ARCHIVE);
    // We check only those MOB files, which creation time is less
    // than maxCreationTimeToArchive. This is a current time - 1h. 1 hour gap
    // gives us full confidence that all corresponding store files will
    // exist at the time cleaning procedure begins and will be examined.
    // So, if MOB file creation time is greater than this maxTimeToArchive,
    // this will be skipped and won't be archived.
    long maxCreationTimeToArchive = EnvironmentEdgeManager.currentTime() - minAgeToArchive;
    TableDescriptor htd = admin.getDescriptor(table);
    List<ColumnFamilyDescriptor> list = MobUtils.getMobColumnFamilies(htd);
    if (list.size() == 0) {
      LOG.info("Skipping non-MOB table [{}]", table);
      return;
    } else {
      LOG.info("Only MOB files whose creation time older than {} will be archived, table={}",
        maxCreationTimeToArchive, table);
    }

    FileSystem fs = FileSystem.get(conf);
    Set<String> regionNames = new HashSet<>();
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path tableDir = CommonFSUtils.getTableDir(rootDir, table);
    List<Path> regionDirs = FSUtils.getRegionDirs(fs, tableDir);

    Set<String> allActiveMobFileName = new HashSet<String>();
    for (Path regionPath : regionDirs) {
      regionNames.add(regionPath.getName());
      HRegionFileSystem regionFS =
        HRegionFileSystem.create(conf, fs, tableDir, MobUtils.getMobRegionInfo(table));
      for (ColumnFamilyDescriptor hcd : list) {
        StoreFileTracker sft = StoreFileTrackerFactory.create(conf, htd, hcd, regionFS, false);
        String family = hcd.getNameAsString();
        Path storePath = new Path(regionPath, family);
        boolean succeed = false;
        Set<String> regionMobs = new HashSet<String>();

        while (!succeed) {
          if (!fs.exists(storePath)) {
            String errMsg = String.format("Directory %s was deleted during MOB file cleaner chore"
              + " execution, aborting MOB file cleaner chore.", storePath);
            throw new IOException(errMsg);
          }
          List<StoreFileInfo> storeFileInfos = sft.load();
          LOG.info("Found {} store files in: {}", storeFileInfos.size(), storePath);
          Path currentPath = null;
          try {
            for (StoreFileInfo storeFileInfo : storeFileInfos) {
              Path pp = storeFileInfo.getPath();
              currentPath = pp;
              LOG.trace("Store file: {}", pp);
              HStoreFile sf = null;
              byte[] mobRefData = null;
              byte[] bulkloadMarkerData = null;
              try {
                sf = new HStoreFile(storeFileInfo, BloomType.NONE, CacheConfig.DISABLED);
                sf.initReader();
                mobRefData = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
                bulkloadMarkerData = sf.getMetadataValue(HStoreFile.BULKLOAD_TASK_KEY);
                // close store file to avoid memory leaks
                sf.closeStoreFile(true);
              } catch (IOException ex) {
                // When FileBased SFT is active the store dir can contain corrupted or incomplete
                // files. So read errors are expected. We just skip these files.
                if (ex instanceof FileNotFoundException) {
                  throw ex;
                }
                LOG.debug("Failed to get mob data from file: {} due to error.", pp.toString(), ex);
                continue;
              }
              if (mobRefData == null) {
                if (bulkloadMarkerData == null) {
                  LOG.warn("Found old store file with no MOB_FILE_REFS: {} - "
                    + "can not proceed until all old files will be MOB-compacted.", pp);
                  return;
                } else {
                  LOG.debug("Skipping file without MOB references (bulkloaded file):{}", pp);
                  continue;
                }
              }
              // file may or may not have MOB references, but was created by the distributed
              // mob compaction code.
              try {
                SetMultimap<TableName, String> mobs =
                  MobUtils.deserializeMobFileRefs(mobRefData).build();
                LOG.debug("Found {} mob references for store={}", mobs.size(), sf);
                LOG.trace("Specific mob references found for store={} : {}", sf, mobs);
                regionMobs.addAll(mobs.values());
              } catch (RuntimeException exception) {
                throw new IOException("failure getting mob references for hfile " + sf, exception);
              }
            }
          } catch (FileNotFoundException e) {
            LOG.warn(
              "Missing file:{} Starting MOB cleaning cycle from the beginning" + " due to error",
              currentPath, e);
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
      LOG.warn("Found too many active MOB files: {}, table={}, "
        + "this may result in high memory pressure.", allActiveMobFileName.size(), table);
    }
    LOG.debug("Found: {} active mob refs for table={}", allActiveMobFileName.size(), table);
    allActiveMobFileName.stream().forEach(LOG::trace);

    // Now scan MOB directories and find MOB files with no references to them
    for (ColumnFamilyDescriptor hcd : list) {
      checkColumnFamilyDescriptor(conf, table, fs, admin, hcd, regionNames,
        maxCreationTimeToArchive);
    }
  }

  private static void checkColumnFamilyDescriptor(Configuration conf, TableName table,
    FileSystem fs, Admin admin, ColumnFamilyDescriptor hcd, Set<String> regionNames,
    long maxCreationTimeToArchive) throws IOException {
    List<Path> toArchive = new ArrayList<Path>();
    String family = hcd.getNameAsString();
    Path dir = MobUtils.getMobFamilyPath(conf, table, family);
    RemoteIterator<LocatedFileStatus> rit = fs.listLocatedStatus(dir);
    while (rit.hasNext()) {
      LocatedFileStatus lfs = rit.next();
      Path p = lfs.getPath();
      String[] mobParts = p.getName().split("_");
      String regionName = mobParts[mobParts.length - 1];

      if (!regionNames.contains(regionName)) {
        // MOB belonged to a region no longer hosted
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
        LOG.trace("Keeping MOB file with existing region: {}", p);
      }
    }
    LOG.info(" MOB Cleaner found {} files to archive for table={} family={}", toArchive.size(),
      table, family);
    archiveMobFiles(conf, table, admin, family.getBytes(), toArchive);
    LOG.info(" MOB Cleaner archived {} files, table={} family={}", toArchive.size(), table, family);
  }

  /**
   * Archives the mob files.
   * @param conf       The current configuration.
   * @param tableName  The table name.
   * @param family     The name of the column family.
   * @param storeFiles The files to be archived.
   * @throws IOException exception
   */
  private static void archiveMobFiles(Configuration conf, TableName tableName, Admin admin,
    byte[] family, List<Path> storeFiles) throws IOException {

    if (storeFiles.size() == 0) {
      // nothing to remove
      LOG.debug("Skipping archiving old MOB files - no files found for table={} cf={}", tableName,
        Bytes.toString(family));
      return;
    }
    Path mobTableDir = CommonFSUtils.getTableDir(MobUtils.getMobHome(conf), tableName);
    FileSystem fs = storeFiles.get(0).getFileSystem(conf);

    for (Path p : storeFiles) {
      LOG.debug("MOB Cleaner is archiving: {}", p);
      HFileArchiver.archiveStoreFile(conf, fs, MobUtils.getMobRegionInfo(tableName), mobTableDir,
        family, p);
    }
  }
}
