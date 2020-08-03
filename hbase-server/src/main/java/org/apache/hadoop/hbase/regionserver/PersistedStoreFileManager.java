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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSortedSet;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * A Storefile manager that is used by {@link PersistedStoreEngine} that persists the in-memory
 * storefile tracking to a persistent table hbase:storefile.
 *
 * We don't override the {@link #clearFiles()} from {@link DefaultStoreFileManager} and persist
 * in-memory storefiles tracking, it will be reused when region reassigns on a different
 * region server.
 */
@InterfaceAudience.Private
public class PersistedStoreFileManager extends DefaultStoreFileManager {
  private static final Logger LOG = LoggerFactory.getLogger(PersistedStoreFileManager.class);
  private final RegionInfo regionInfo;
  private final String tableName;
  private final String regionName;
  private final String storeName;
  private StoreFilePathAccessor accessor;
  private Configuration conf;
  // only uses for warmupHRegion
  private boolean readOnly;

  public PersistedStoreFileManager(CellComparator cellComparator,
    Comparator<HStoreFile> storeFileComparator, Configuration conf,
    CompactionConfiguration compactionConfiguration, HRegionFileSystem regionFs,
    RegionInfo regionInfo, String familyName, StoreFilePathAccessor accessor, boolean readOnly) {
    super(cellComparator, storeFileComparator, conf, compactionConfiguration, regionFs, familyName);
    this.conf = conf;
    this.regionInfo = regionInfo;
    this.tableName = regionInfo.getTable().getNameAsString();
    this.regionName = regionInfo.getEncodedName();
    this.storeName = familyName;
    this.accessor = accessor;
    this.readOnly = readOnly;
  }

  public PersistedStoreFileManager(CellComparator cellComparator,
    Comparator<HStoreFile> storeFileComparator, Configuration conf,
    CompactionConfiguration compactionConfiguration, HRegionFileSystem regionFs,
    RegionInfo regionInfo, String familyName, StoreFilePathAccessor accessor) {
    this(cellComparator, storeFileComparator, conf, compactionConfiguration, regionFs, regionInfo,
      familyName, accessor, false);
  }

  /**
   * Loads the specified storeFiles to the StoreFileManager to be included in reads.
   *
   * @param storeFiles list of storefiles to be loaded, could be an empty list. throws exception
   *                   if it's null.
   */
  @Override
  public void loadFiles(List<HStoreFile> storeFiles) throws IOException {
    Preconditions.checkArgument(storeFiles != null, "store files cannot be "
      + "null when loading");
    if (storeFiles.isEmpty()) {
      LOG.warn("Other than fresh region with no store files, store files should not be empty");
      return;
    }
    ImmutableList<HStoreFile> sortedStorefiles =
      ImmutableList.sortedCopyOf(getStoreFileComparator(), storeFiles);
    setStorefiles(sortedStorefiles);
    updatePathListToTracker(StoreFilePathUpdate.builder().withStoreFiles(sortedStorefiles).build());
  }

  @Override
  public Collection<StoreFileInfo> loadInitialFiles() throws IOException {
    List<Path> pathList = accessor.getIncludedStoreFilePaths(tableName, regionName, storeName);
    boolean isEmptyInPersistedFilePaths = CollectionUtils.isEmpty(pathList);
    if (isEmptyInPersistedFilePaths) {
      // When the path accessor is returning empty result, we scan the
      // the file storage and see if there is any existing HFiles should be loaded.
      // the scan is a one time process when store open during region assignment.
      //
      // this is especially used for region and store open
      // 1. First time migration from a filesystem based e.g. DefaultStoreFileEngine
      // 2. After region split and merge
      // 3. After table clone and create new HFiles directly into data directory
      //
      // Also we don't handle the inconsistency between storefile tracking and file system, which
      // will be handled by a HBCK command
      LOG.info("Cannot find tracking paths ({}) for store {} in region {} of "
          + "table {}, fall back to scan the storage to get a list of storefiles to be opened"
        , isEmptyInPersistedFilePaths, storeName, regionName,
        tableName);
      return getRegionFs().getStoreFiles(getFamilyName());
    }
    ArrayList<StoreFileInfo> storeFiles = new ArrayList<>();
    for (Path storeFilePath : pathList) {
      if (!StoreFileInfo.isValid(getRegionFs().getFileSystem().getFileStatus(storeFilePath))) {
        // TODO add a comment how this file will be removed when we have cleaner
        LOG.warn("Invalid StoreFile: {}", storeFilePath);
        continue;
      }
      StoreFileInfo info = ServerRegionReplicaUtil
        .getStoreFileInfo(conf, getRegionFs().getFileSystem(), regionInfo,
          ServerRegionReplicaUtil.getRegionInfoForFs(regionInfo), getFamilyName(),
          storeFilePath);
      storeFiles.add(info);
    }
    return storeFiles;
  }


  @Override
  public void insertNewFiles(Collection<HStoreFile> sfs) throws IOException {
    // return in case of empty storeFiles as it is a No-op, empty files expected during region close
    if (CollectionUtils.isEmpty(sfs)) {
      return;
    }
    ImmutableList<HStoreFile> storefiles = ImmutableList
      .sortedCopyOf(getStoreFileComparator(), Iterables.concat(getStorefiles(), sfs));
    updatePathListToTracker(StoreFilePathUpdate.builder().withStoreFiles(storefiles).build());
    setStorefiles(storefiles);
  }

  @Override
  public void addCompactionResults(Collection<HStoreFile> newCompactedfiles,
    Collection<HStoreFile> results) throws IOException {
    Preconditions.checkNotNull(newCompactedfiles, "compactedFiles cannot be null");
    Preconditions.checkNotNull(results, "compaction result cannot be null");
    // only allow distinct path to be included, especially rerun after a compaction fails
    ImmutableList<HStoreFile> storefiles = ImmutableList.sortedCopyOf(getStoreFileComparator(),
      Iterables.concat(Iterables.filter(getStorefiles(), sf -> !newCompactedfiles.contains(sf)),
        results)).asList();
    Preconditions.checkArgument(!CollectionUtils.isEmpty(storefiles),
      "storefiles cannot be empty when adding compaction results");

    ImmutableList<HStoreFile> compactedfiles = ImmutableSortedSet
      .copyOf(getStoreFileComparator(),
        Iterables.concat(getCompactedfiles(), newCompactedfiles))
      .asList();
    updatePathListToTracker(StoreFilePathUpdate.builder().withStoreFiles(storefiles).build());
    setStorefiles(storefiles);
    setCompactedfiles(compactedfiles);
    newCompactedfiles.forEach(HStoreFile::markCompactedAway);
  }

  void updatePathListToTracker(StoreFilePathUpdate storeFilePathUpdate) throws IOException {
    try {
      if (!readOnly) {
        accessor.writeStoreFilePaths(tableName, regionName, storeName, storeFilePathUpdate);
      }
    } catch (IOException e) {
      String message = "Failed to update Path list of " + tableName + "-" + regionName +
        "-" + storeName + ", on " + TableName.STOREFILE_STR + ". The new files are not "
        + "persistent and will be removed from " + regionName + "," + storeName +
        ".\nFailed update: " + storeFilePathUpdate;
      LOG.warn(message);
      throw new IOException(message, e);
    }
  }

}
