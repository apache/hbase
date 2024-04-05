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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class DataTieringManager {
  private static final Logger LOG = LoggerFactory.getLogger(DataTieringManager.class);
  public static final String DATATIERING_KEY = "hbase.hstore.datatiering.type";
  public static final String DATATIERING_HOT_DATA_AGE_KEY =
    "hbase.hstore.datatiering.hot.age.millis";
  public static final DataTieringType DEFAULT_DATATIERING = DataTieringType.NONE;
  public static final long DEFAULT_DATATIERING_HOT_DATA_AGE = 7 * 24 * 60 * 60 * 1000; // 7 Days
  private static DataTieringManager instance;
  private final Map<String, HRegion> onlineRegions;

  private DataTieringManager(Map<String, HRegion> onlineRegions) {
    this.onlineRegions = onlineRegions;
  }

  public static synchronized void instantiate(Map<String, HRegion> onlineRegions) {
    if (instance == null) {
      instance = new DataTieringManager(onlineRegions);
      LOG.info("DataTieringManager instantiated successfully.");
    } else {
      LOG.warn("DataTieringManager is already instantiated.");
    }
  }

  public static synchronized DataTieringManager getInstance() {
    if (instance == null) {
      throw new IllegalStateException(
        "DataTieringManager has not been instantiated. Call instantiate() first.");
    }
    return instance;
  }

  public boolean isDataTieringEnabled(BlockCacheKey key) throws DataTieringException {
    Path hFilePath = key.getFilePath();
    if (hFilePath == null) {
      throw new DataTieringException("BlockCacheKey Doesn't Contain HFile Path");
    }
    return isDataTieringEnabled(hFilePath);
  }

  public boolean isDataTieringEnabled(Path hFilePath) throws DataTieringException {
    Configuration configuration = getConfiguration(hFilePath);
    DataTieringType dataTieringType = getDataTieringType(configuration);
    return !dataTieringType.equals(DataTieringType.NONE);
  }

  public boolean isHotData(BlockCacheKey key) throws DataTieringException {
    Path hFilePath = key.getFilePath();
    if (hFilePath == null) {
      throw new DataTieringException("BlockCacheKey Doesn't Contain HFile Path");
    }
    return isHotData(hFilePath);
  }

  public boolean isHotData(Path hFilePath) throws DataTieringException {
    Configuration configuration = getConfiguration(hFilePath);
    DataTieringType dataTieringType = getDataTieringType(configuration);

    if (dataTieringType.equals(DataTieringType.TIME_RANGE)) {
      long hotDataAge = getDataTieringHotDataAge(configuration);

      HStoreFile hStoreFile = getHStoreFile(hFilePath);
      if (hStoreFile == null) {
        throw new DataTieringException(
          "HStoreFile corresponding to " + hFilePath + " doesn't exist");
      }
      OptionalLong maxTimestamp = hStoreFile.getMaximumTimestamp();
      if (!maxTimestamp.isPresent()) {
        throw new DataTieringException("Maximum timestamp not present for " + hFilePath);
      }

      long currentTimestamp = EnvironmentEdgeManager.getDelegate().currentTime();
      long diff = currentTimestamp - maxTimestamp.getAsLong();
      return diff <= hotDataAge;
    }
    return false;
  }

  public Set<String> getColdDataFiles(Set<BlockCacheKey> allCachedBlocks)
    throws DataTieringException {
    Set<String> coldHFiles = new HashSet<>();
    for (BlockCacheKey key : allCachedBlocks) {
      if (coldHFiles.contains(key.getHfileName())) {
        continue;
      }
      if (isDataTieringEnabled(key) && !isHotData(key)) {
        coldHFiles.add(key.getHfileName());
      }
    }
    return coldHFiles;
  }

  private HRegion getHRegion(Path hFilePath) throws DataTieringException {
    if (hFilePath.getParent() == null || hFilePath.getParent().getParent() == null) {
      throw new DataTieringException("Incorrect HFile Path: " + hFilePath);
    }
    String regionId = hFilePath.getParent().getParent().getName();
    HRegion hRegion = this.onlineRegions.get(regionId);
    if (hRegion == null) {
      throw new DataTieringException("HRegion corresponding to " + hFilePath + " doesn't exist");
    }
    return hRegion;
  }

  private HStore getHStore(Path hFilePath) throws DataTieringException {
    HRegion hRegion = getHRegion(hFilePath);
    String columnFamily = hFilePath.getParent().getName();
    HStore hStore = hRegion.getStore(Bytes.toBytes(columnFamily));
    if (hStore == null) {
      throw new DataTieringException("HStore corresponding to " + hFilePath + " doesn't exist");
    }
    return hStore;
  }

  private HStoreFile getHStoreFile(Path hFilePath) throws DataTieringException {
    HStore hStore = getHStore(hFilePath);
    for (HStoreFile file : hStore.getStorefiles()) {
      if (file.getPath().equals(hFilePath)) {
        return file;
      }
    }
    return null;
  }

  private Configuration getConfiguration(Path hFilePath) throws DataTieringException {
    HStore hStore = getHStore(hFilePath);
    return hStore.getReadOnlyConfiguration();
  }

  private DataTieringType getDataTieringType(Configuration conf) {
    return DataTieringType.valueOf(conf.get(DATATIERING_KEY, DEFAULT_DATATIERING.name()));
  }

  private long getDataTieringHotDataAge(Configuration conf) {
    return Long.parseLong(
      conf.get(DATATIERING_HOT_DATA_AGE_KEY, String.valueOf(DEFAULT_DATATIERING_HOT_DATA_AGE)));
  }

  /*
   * This API takes the names of files as input and returns a subset of these file names
   * that are cold.
   * @parameter inputFileNames: Input list of file names
   * @return List of names of files that are cold as per data-tiering logic.
   */
  public List<String> getColdFileList(List<String> inputFileNames) {
    List<String> coldFileList = new ArrayList<>();
    for (HRegion r : this.onlineRegions.values()) {
      for (HStore hStore : r.getStores()) {
        Configuration conf = hStore.getReadOnlyConfiguration();
        if (getDataTieringType(conf) != DataTieringType.TIME_RANGE) {
          // Data-Tiering not enabled for the store. Just skip it.
          continue;
        }
        Long hotDataAge = getDataTieringHotDataAge(conf);

        for (HStoreFile hStoreFile : hStore.getStorefiles()) {
          String hFileName =
            hStoreFile.getFileInfo().getHFileInfo().getHFileContext().getHFileName();
          if(inputFileNames.contains(hFileName)) {
            OptionalLong maxTimestamp = hStoreFile.getMaximumTimestamp();
            if (!maxTimestamp.isPresent()) {
              // We could throw from here, But we are already in the critical code-path
              // of freeing space. Hence, we can ignore that file for now
              // Or do we want to include it?
              continue;
            }
            long currentTimestamp = EnvironmentEdgeManager.getDelegate().currentTime();
            long fileAge = currentTimestamp - maxTimestamp.getAsLong();
            if (fileAge > hotDataAge) {
              coldFileList.add(hFileName);
            }
          }
        }
      }
    }
    return coldFileList;
  }
}
