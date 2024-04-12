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

import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DataTieringManager class categorizes data into hot data and cold data based on the specified
 * {@link DataTieringType} when DataTiering is enabled. DataTiering is disabled by default with
 * {@link DataTieringType} set to {@link DataTieringType#NONE}. The {@link DataTieringType}
 * determines the logic for distinguishing data into hot or cold. By default, all data is considered
 * as hot.
 */
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

  /**
   * Initializes the DataTieringManager instance with the provided map of online regions.
   * @param onlineRegions A map containing online regions.
   */
  public static synchronized void instantiate(Map<String, HRegion> onlineRegions) {
    if (instance == null) {
      instance = new DataTieringManager(onlineRegions);
      LOG.info("DataTieringManager instantiated successfully.");
    } else {
      LOG.warn("DataTieringManager is already instantiated.");
    }
  }

  /**
   * Retrieves the instance of DataTieringManager.
   * @return The instance of DataTieringManager.
   * @throws IllegalStateException if DataTieringManager has not been instantiated.
   */
  public static synchronized DataTieringManager getInstance() {
    if (instance == null) {
      throw new IllegalStateException(
        "DataTieringManager has not been instantiated. Call instantiate() first.");
    }
    return instance;
  }

  /**
   * Determines whether data tiering is enabled for the given block cache key.
   * @param key the block cache key
   * @return {@code true} if data tiering is enabled for the HFile associated with the key,
   *         {@code false} otherwise
   * @throws DataTieringException if there is an error retrieving the HFile path or configuration
   */
  public boolean isDataTieringEnabled(BlockCacheKey key) throws DataTieringException {
    Path hFilePath = key.getFilePath();
    if (hFilePath == null) {
      throw new DataTieringException("BlockCacheKey Doesn't Contain HFile Path");
    }
    return isDataTieringEnabled(hFilePath);
  }

  /**
   * Determines whether data tiering is enabled for the given HFile path.
   * @param hFilePath the path to the HFile
   * @return {@code true} if data tiering is enabled, {@code false} otherwise
   * @throws DataTieringException if there is an error retrieving the configuration
   */
  public boolean isDataTieringEnabled(Path hFilePath) throws DataTieringException {
    Configuration configuration = getConfiguration(hFilePath);
    DataTieringType dataTieringType = getDataTieringType(configuration);
    return !dataTieringType.equals(DataTieringType.NONE);
  }

  /**
   * Determines whether the data associated with the given block cache key is considered hot.
   * @param key the block cache key
   * @return {@code true} if the data is hot, {@code false} otherwise
   * @throws DataTieringException if there is an error retrieving data tiering information or the
   *                              HFile maximum timestamp
   */
  public boolean isHotData(BlockCacheKey key) throws DataTieringException {
    Path hFilePath = key.getFilePath();
    if (hFilePath == null) {
      throw new DataTieringException("BlockCacheKey Doesn't Contain HFile Path");
    }
    return isHotData(hFilePath);
  }

  /**
   * Determines whether the data in the HFile at the given path is considered hot based on the
   * configured data tiering type and hot data age.
   * @param hFilePath the path to the HFile
   * @return {@code true} if the data is hot, {@code false} otherwise
   * @throws DataTieringException if there is an error retrieving data tiering information or the
   *                              HFile maximum timestamp
   */
  public boolean isHotData(Path hFilePath) throws DataTieringException {
    Configuration configuration = getConfiguration(hFilePath);
    DataTieringType dataTieringType = getDataTieringType(configuration);

    if (dataTieringType.equals(DataTieringType.TIME_RANGE)) {
      long hotDataAge = getDataTieringHotDataAge(configuration);

      HStoreFile hStoreFile = getHStoreFile(hFilePath);
      if (hStoreFile == null) {
        LOG.error("HStoreFile corresponding to " + hFilePath + " doesn't exist");
        return false;
      }
      OptionalLong maxTimestamp = hStoreFile.getMaximumTimestamp();
      if (!maxTimestamp.isPresent()) {
        throw new DataTieringException("Maximum timestamp not present for " + hFilePath);
      }

      long currentTimestamp = EnvironmentEdgeManager.getDelegate().currentTime();
      long diff = currentTimestamp - maxTimestamp.getAsLong();
      return diff <= hotDataAge;
    }
    // DataTieringType.NONE or other types are considered hot by default
    return true;
  }

  /**
   * Returns a set of cold data filenames from the given set of cached blocks. Cold data is
   * determined by the configured data tiering type and hot data age.
   * @param allCachedBlocks a set of all cached block cache keys
   * @return a set of cold data filenames
   * @throws DataTieringException if there is an error determining whether a block is hot
   */
  public Set<String> getColdDataFiles(Set<BlockCacheKey> allCachedBlocks)
    throws DataTieringException {
    Set<String> coldHFiles = new HashSet<>();
    for (BlockCacheKey key : allCachedBlocks) {
      if (coldHFiles.contains(key.getHfileName())) {
        continue;
      }
      if (!isHotData(key)) {
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
      if (file.getPath().toUri().getPath().toString().equals(hFilePath.toString())) {
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
}
