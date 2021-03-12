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
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

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
  private final StoreFilePathAccessor accessor;
  private final Configuration conf;
  // only uses for warmupHRegion
  private final boolean readOnly;

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

  @Override
  public void loadFiles(List<HStoreFile> storeFiles) throws IOException {
    // update with a sorted store files
    super.loadFiles(storeFiles);
    Preconditions.checkArgument(storeFiles != null, "store files cannot be "
      + "null when loading");
    if (storeFiles.isEmpty()) {
      LOG.warn("Other than fresh region with no store files, store files should not be empty");
      return;
    }
    updatePathListToTracker(StoreFilePathUpdate.builder().withStoreFiles(getStorefiles()).build());
  }

  @Override
  public void insertNewFiles(Collection<HStoreFile> sfs) throws IOException {
    // concatenate the new store files
    super.insertNewFiles(sfs);
    // return in case of empty store files as it is a No-op, here empty files are expected
    // during region close
    if (CollectionUtils.isEmpty(getStorefiles())) {
      return;
    }
    updatePathListToTracker(StoreFilePathUpdate.builder().withStoreFiles(getStorefiles()).build());
  }

  @Override
  protected void addCompactionResultsHook(ImmutableList<HStoreFile> storeFiles)
    throws IOException {
    Preconditions.checkNotNull(storeFiles, "storeFiles cannot be null");
    updatePathListToTracker(StoreFilePathUpdate.builder().withStoreFiles(storeFiles).build());
  }

  @Override
  public Collection<StoreFileInfo> loadInitialFiles() throws IOException {
    // this logic is totally different from the default implementation in DefaultStoreFileManager

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
        LOG.warn("Invalid StoreFile: {}, and archiving it", storeFilePath);
        getRegionFs().removeStoreFile(storeName, storeFilePath);
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

  void updatePathListToTracker(StoreFilePathUpdate storeFilePathUpdate) throws IOException {
    try {
      // if this is not a read only region, update the tracking path
      if (!readOnly) {
        accessor.writeStoreFilePaths(tableName, regionName, storeName, storeFilePathUpdate);
      }
    } catch (IOException e) {
      String message = String.format(
        "Failed to persist tracking paths with key %s-%s-%s to table [%s]. "
          + "%nPaths failed to be updated are: %s",
        regionName, storeName, tableName, TableName.STOREFILE_STR, storeFilePathUpdate);
      LOG.error(message);
      throw new IOException(message, e);
    }
  }

}
