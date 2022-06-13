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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.SetMultimap;

/**
 * The class RSMobFileCleanerChore for running cleaner regularly to remove the obsolete (files which
 * have no active references to) mob files that were referenced from the current RS.
 */
@InterfaceAudience.Private
public class RSMobFileCleanerChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(RSMobFileCleanerChore.class);
  private final HRegionServer rs;

  public RSMobFileCleanerChore(HRegionServer rs) {
    super(rs.getServerName() + "-MobFileCleanerChore", rs,
      rs.getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD),
      Math.round(rs.getConfiguration().getInt(MobConstants.MOB_CLEANER_PERIOD,
        MobConstants.DEFAULT_MOB_CLEANER_PERIOD)
        * ((ThreadLocalRandom.current().nextDouble() + 0.5D))),
      TimeUnit.SECONDS);
    // to prevent a load spike on the fs the initial delay is modified by +/- 50%
    this.rs = rs;
  }

  public RSMobFileCleanerChore() {
    this.rs = null;
  }

  @Override
  protected void chore() {

    long minAgeToArchive = rs.getConfiguration().getLong(MobConstants.MIN_AGE_TO_ARCHIVE_KEY,
      MobConstants.DEFAULT_MIN_AGE_TO_ARCHIVE);
    // We check only those MOB files, which creation time is less
    // than maxCreationTimeToArchive. This is a current time - 1h. 1 hour gap
    // gives us full confidence that all corresponding store files will
    // exist at the time cleaning procedure begins and will be examined.
    // So, if MOB file creation time is greater than this maxTimeToArchive,
    // this will be skipped and won't be archived.
    long maxCreationTimeToArchive = EnvironmentEdgeManager.currentTime() - minAgeToArchive;

    TableDescriptors htds = rs.getTableDescriptors();
    try {
      FileSystem fs = FileSystem.get(rs.getConfiguration());

      Map<String, TableDescriptor> map = null;
      try {
        map = htds.getAll();
      } catch (IOException e) {
        LOG.error("MobFileCleanerChore failed", e);
        return;
      }
      Map<String, Map<String, List<String>>> referencedMOBs = new HashMap<>();
      for (TableDescriptor htd : map.values()) {
        // Now clean obsolete files for a table
        LOG.info("Cleaning obsolete MOB files from table={}", htd.getTableName());
        List<ColumnFamilyDescriptor> list = MobUtils.getMobColumnFamilies(htd);
        List<HRegion> regions = rs.getRegions(htd.getTableName());
        for (HRegion region : regions) {
          for (ColumnFamilyDescriptor hcd : list) {
            HStore store = region.getStore(hcd.getName());
            Collection<HStoreFile> sfs = store.getStorefiles();
            Set<String> regionMobs = new HashSet<String>();
            Path currentPath = null;
            try {
              // collectinng referenced MOBs
              for (HStoreFile sf : sfs) {
                currentPath = sf.getPath();
                sf.initReader();
                byte[] mobRefData = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
                byte[] bulkloadMarkerData = sf.getMetadataValue(HStoreFile.BULKLOAD_TASK_KEY);
                // close store file to avoid memory leaks
                sf.closeStoreFile(true);
                if (mobRefData == null) {
                  if (bulkloadMarkerData == null) {
                    LOG.warn(
                      "Found old store file with no MOB_FILE_REFS: {} - "
                        + "can not proceed until all old files will be MOB-compacted.",
                      currentPath);
                    return;
                  } else {
                    LOG.debug("Skipping file without MOB references (bulkloaded file):{}",
                      currentPath);
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
                  throw new IOException("failure getting mob references for hfile " + sf,
                    exception);
                }
              }
              // collecting files, MOB included currently being written
              regionMobs.addAll(store.getStoreFilesBeingWritten().stream()
                .map(path -> path.getName()).collect(Collectors.toList()));

              referencedMOBs
                .computeIfAbsent(hcd.getNameAsString(), cf -> new HashMap<String, List<String>>())
                .computeIfAbsent(region.getRegionInfo().getEncodedName(), name -> new ArrayList<>())
                .addAll(regionMobs);

            } catch (FileNotFoundException e) {
              LOG.warn(
                "Missing file:{} Starting MOB cleaning cycle from the beginning" + " due to error",
                currentPath, e);
              regionMobs.clear();
              continue;
            } catch (IOException e) {
              LOG.error("Failed to clean the obsolete mob files for table={}",
                htd.getTableName().getNameAsString(), e);
            }
          }
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Found: {} active mob refs for table={}",
            referencedMOBs.values().stream().map(inner -> inner.values())
              .flatMap(lists -> lists.stream()).mapToInt(lists -> lists.size()).sum(),
            htd.getTableName().getNameAsString());
        }
        if (LOG.isTraceEnabled()) {
          referencedMOBs.values().stream().forEach(innerMap -> innerMap.values().stream()
            .forEach(mobFileList -> mobFileList.stream().forEach(LOG::trace)));
        }

        // collect regions referencing MOB files belonging to the current rs
        Set<String> regionsCovered = new HashSet<>();
        referencedMOBs.values().stream()
          .forEach(regionMap -> regionsCovered.addAll(regionMap.keySet()));

        for (ColumnFamilyDescriptor hcd : list) {
          List<Path> toArchive = new ArrayList<Path>();
          String family = hcd.getNameAsString();
          Path dir = MobUtils.getMobFamilyPath(rs.getConfiguration(), htd.getTableName(), family);
          RemoteIterator<LocatedFileStatus> rit = fs.listLocatedStatus(dir);
          while (rit.hasNext()) {
            LocatedFileStatus lfs = rit.next();
            Path p = lfs.getPath();
            String[] mobParts = p.getName().split("_");
            String regionName = mobParts[mobParts.length - 1];

            // skip MOB files not belonging to a region assigned to the current rs
            if (!regionsCovered.contains(regionName)) {
              LOG.trace("MOB file does not belong to current rs: {}", p);
              continue;
            }

            // check active or actively written mob files
            Map<String, List<String>> cfMobs = referencedMOBs.get(hcd.getNameAsString());
            if (
              cfMobs != null && cfMobs.get(regionName) != null
                && cfMobs.get(regionName).contains(p.getName())
            ) {
              LOG.trace("Keeping active MOB file: {}", p);
              continue;
            }

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

          }
          LOG.info(" MOB Cleaner found {} files to archive for table={} family={}",
            toArchive.size(), htd.getTableName().getNameAsString(), family);
          archiveMobFiles(rs.getConfiguration(), htd.getTableName(), family.getBytes(), toArchive);
          LOG.info(" MOB Cleaner archived {} files, table={} family={}", toArchive.size(),
            htd.getTableName().getNameAsString(), family);
        }

        LOG.info("Cleaning obsolete MOB files finished for table={}", htd.getTableName());

      }
    } catch (IOException e) {
      LOG.error("MOB Cleaner failed when trying to access the file system", e);
    }
  }

  /**
   * Archives the mob files.
   * @param conf       The current configuration.
   * @param tableName  The table name.
   * @param family     The name of the column family.
   * @param storeFiles The files to be archived.
   * @throws IOException exception
   */
  public void archiveMobFiles(Configuration conf, TableName tableName, byte[] family,
    List<Path> storeFiles) throws IOException {

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
