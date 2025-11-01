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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.BlockCacheKey;
import org.apache.hadoop.hbase.io.hfile.HFileInfo;
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
  public static final String GLOBAL_DATA_TIERING_ENABLED_KEY =
    "hbase.regionserver.datatiering.enable";
  public static final boolean DEFAULT_GLOBAL_DATA_TIERING_ENABLED = false; // disabled by default
  public static final String DATATIERING_KEY = "hbase.hstore.datatiering.type";
  public static final String HSTORE_DATATIERING_GRACE_PERIOD_MILLIS_KEY =
    "hbase.hstore.datatiering.grace.period.millis";
  public static final long DEFAULT_DATATIERING_GRACE_PERIOD = 0;
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
   * Initializes the DataTieringManager instance with the provided map of online regions, only if
   * the configuration "hbase.regionserver.datatiering.enable" is enabled.
   * @param conf          Configuration object.
   * @param onlineRegions A map containing online regions.
   * @return True if the instance is instantiated successfully, false otherwise.
   */
  public static synchronized boolean instantiate(Configuration conf,
    Map<String, HRegion> onlineRegions) {
    if (isDataTieringFeatureEnabled(conf) && instance == null) {
      instance = new DataTieringManager(onlineRegions);
      LOG.info("DataTieringManager instantiated successfully.");
      return true;
    } else {
      LOG.warn("DataTieringManager is already instantiated.");
    }
    return false;
  }

  /**
   * Retrieves the instance of DataTieringManager.
   * @return The instance of DataTieringManager, if instantiated, null otherwise.
   */
  public static synchronized DataTieringManager getInstance() {
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
   * Determines whether the data associated with the given block cache key is considered hot. If the
   * data tiering type is set to {@link DataTieringType#TIME_RANGE} and maximum timestamp is not
   * present, it considers {@code Long.MAX_VALUE} as the maximum timestamp, making the data hot by
   * default.
   * @param key the block cache key
   * @return {@code true} if the data is hot, {@code false} otherwise
   * @throws DataTieringException if there is an error retrieving data tiering information
   */
  public boolean isHotData(BlockCacheKey key) throws DataTieringException {
    Path hFilePath = key.getFilePath();
    if (hFilePath == null) {
      throw new DataTieringException("BlockCacheKey Doesn't Contain HFile Path");
    }
    return isHotData(hFilePath);
  }

  /**
   * Determines whether the data associated with the given time range tracker is considered hot. If
   * the data tiering type is set to {@link DataTieringType#TIME_RANGE}, it uses the maximum
   * timestamp from the time range tracker to determine if the data is hot. Otherwise, it considers
   * the data as hot by default.
   * @param maxTimestamp the maximum timestamp associated with the data.
   * @param conf         The configuration object to use for determining hot data criteria.
   * @return {@code true} if the data is hot, {@code false} otherwise
   */
  public boolean isHotData(long maxTimestamp, Configuration conf) {
    if (isWithinGracePeriod(maxTimestamp, conf)) {
      return true;
    }
    DataTieringType dataTieringType = getDataTieringType(conf);

    if (
      !dataTieringType.equals(DataTieringType.NONE)
        && maxTimestamp != TimeRangeTracker.INITIAL_MAX_TIMESTAMP
    ) {
      return hotDataValidator(maxTimestamp, getDataTieringHotDataAge(conf));
    }
    // DataTieringType.NONE or other types are considered hot by default
    return true;
  }

  /**
   * Determines whether the data in the HFile at the given path is considered hot based on the
   * configured data tiering type and hot data age. If the data tiering type is set to
   * {@link DataTieringType#TIME_RANGE} and maximum timestamp is not present, it considers
   * {@code Long.MAX_VALUE} as the maximum timestamp, making the data hot by default.
   * @param hFilePath the path to the HFile
   * @return {@code true} if the data is hot, {@code false} otherwise
   * @throws DataTieringException if there is an error retrieving data tiering information
   */
  public boolean isHotData(Path hFilePath) throws DataTieringException {
    Configuration configuration = getConfiguration(hFilePath);
    DataTieringType dataTieringType = getDataTieringType(configuration);

    if (!dataTieringType.equals(DataTieringType.NONE)) {
      HStoreFile hStoreFile = getHStoreFile(hFilePath);
      if (hStoreFile == null) {
        throw new DataTieringException(
          "Store file corresponding to " + hFilePath + " doesn't exist");
      }
      long maxTimestamp = dataTieringType.getInstance().getTimestamp(hStoreFile);
      if (isWithinGracePeriod(maxTimestamp, configuration)) {
        return true;
      }
      return hotDataValidator(maxTimestamp, getDataTieringHotDataAge(configuration));
    }
    // DataTieringType.NONE or other types are considered hot by default
    return true;
  }

  /**
   * Determines whether the data in the HFile being read is considered hot based on the configured
   * data tiering type and hot data age. If the data tiering type is set to
   * {@link DataTieringType#TIME_RANGE} and maximum timestamp is not present, it considers
   * {@code Long.MAX_VALUE} as the maximum timestamp, making the data hot by default.
   * @param hFileInfo     Information about the HFile to determine if its data is hot.
   * @param configuration The configuration object to use for determining hot data criteria.
   * @return {@code true} if the data is hot, {@code false} otherwise
   */
  public boolean isHotData(HFileInfo hFileInfo, Configuration configuration) {
    DataTieringType dataTieringType = getDataTieringType(configuration);
    if (hFileInfo != null && !dataTieringType.equals(DataTieringType.NONE)) {
      long maxTimestamp = dataTieringType.getInstance().getTimestamp(hFileInfo);
      if (isWithinGracePeriod(maxTimestamp, configuration)) {
        return true;
      }
      return hotDataValidator(maxTimestamp, getDataTieringHotDataAge(configuration));
    }
    // DataTieringType.NONE or other types are considered hot by default
    return true;
  }

  private boolean isWithinGracePeriod(long maxTimestamp, Configuration conf) {
    long gracePeriod = getDataTieringGracePeriod(conf);
    return gracePeriod > 0 && (getCurrentTimestamp() - maxTimestamp) < gracePeriod;
  }

  private boolean hotDataValidator(long maxTimestamp, long hotDataAge) {
    long currentTimestamp = getCurrentTimestamp();
    long diff = currentTimestamp - maxTimestamp;
    return diff <= hotDataAge;
  }

  private long getCurrentTimestamp() {
    return EnvironmentEdgeManager.getDelegate().currentTime();
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
    String regionId;
    try {
      regionId = HRegionFileSystem.getRegionId(hFilePath);
    } catch (IOException e) {
      throw new DataTieringException(e.getMessage());
    }
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

  private long getDataTieringGracePeriod(Configuration conf) {
    return Long.parseLong(conf.get(HSTORE_DATATIERING_GRACE_PERIOD_MILLIS_KEY,
      String.valueOf(DEFAULT_DATATIERING_GRACE_PERIOD)));
  }

  /*
   * This API traverses through the list of online regions and returns a subset of these files-names
   * that are cold.
   * @return List of names of files with cold data as per data-tiering logic.
   */
  public Map<String, String> getColdFilesList() {
    Map<String, String> coldFiles = new HashMap<>();
    for (HRegion r : this.onlineRegions.values()) {
      for (HStore hStore : r.getStores()) {
        Configuration conf = hStore.getReadOnlyConfiguration();
        DataTieringType dataTieringType = getDataTieringType(conf);
        if (dataTieringType == DataTieringType.NONE) {
          // Data-Tiering not enabled for the store. Just skip it.
          continue;
        }
        Long hotDataAge = getDataTieringHotDataAge(conf);

        for (HStoreFile hStoreFile : hStore.getStorefiles()) {
          String hFileName =
            hStoreFile.getFileInfo().getHFileInfo().getHFileContext().getHFileName();
          long maxTimeStamp = dataTieringType.getInstance().getTimestamp(hStoreFile);
          LOG.debug("Max TS for file {} is {}", hFileName, new Date(maxTimeStamp));
          long currentTimestamp = EnvironmentEdgeManager.getDelegate().currentTime();
          long fileAge = currentTimestamp - maxTimeStamp;
          if (fileAge > hotDataAge) {
            // Values do not matter.
            coldFiles.put(hFileName, null);
          }
        }
      }
    }
    return coldFiles;
  }

  private static boolean isDataTieringFeatureEnabled(Configuration conf) {
    return conf.getBoolean(DataTieringManager.GLOBAL_DATA_TIERING_ENABLED_KEY,
      DataTieringManager.DEFAULT_GLOBAL_DATA_TIERING_ENABLED);
  }

  // Resets the instance to null. To be used only for testing.
  public static void resetForTestingOnly() {
    instance = null;
  }
}
