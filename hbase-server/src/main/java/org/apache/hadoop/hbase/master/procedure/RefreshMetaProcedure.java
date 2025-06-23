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

package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshMetaState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshMetaStateData;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.FSUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@InterfaceAudience.Private
public class RefreshMetaProcedure extends AbstractStateMachineTableProcedure<RefreshMetaState> {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshMetaProcedure.class);

  private List<RegionInfo> currentRegions;
  private List<RegionInfo> latestRegions;
  private static final int MUTATION_BATCH_SIZE = 100;

  public RefreshMetaProcedure() {
    super();
  }

  public RefreshMetaProcedure(MasterProcedureEnv env) {
    super(env);
  }

  @Override
  public TableName getTableName() {
    return TableName.META_TABLE_NAME;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RefreshMetaState refreshMetaState) {
    LOG.info("Executing RefreshMetaProcedure state: {}", refreshMetaState);

    try {
      return switch (refreshMetaState) {
        case REFRESH_META_INIT -> executeInit(env);
        case REFRESH_META_SCAN_STORAGE -> executeScanStorage(env);
        case REFRESH_META_UPDATE -> executeUpdate(env);
        case REFRESH_META_COMPLETE -> executeComplete();
        default -> throw new UnsupportedOperationException("Unhandled state: " + refreshMetaState);
      };
    } catch (Exception ex) {
      LOG.error("Error in RefreshMetaProcedure state {}", refreshMetaState, ex);
      setFailure("RefreshMetaProcedure", ex);
      return Flow.NO_MORE_STATE;
    }
  }

  private Flow executeInit(MasterProcedureEnv env) throws IOException {
    LOG.trace("Getting current regions from hbase:meta table");
    try {
      currentRegions = getCurrentRegions(env.getMasterServices().getConnection());
      LOG.info("Found {} current regions in meta table", currentRegions.size());
      setNextState(RefreshMetaState.REFRESH_META_SCAN_STORAGE);
      return Flow.HAS_MORE_STATE;
    } catch (IOException ioe) {
      LOG.error("Failed to get current regions from meta table", ioe);
      throw ioe;
    }
  }

  private Flow executeScanStorage(MasterProcedureEnv env) throws IOException {
    try {
      latestRegions = scanBackingStorage(env.getMasterServices().getConnection());
      LOG.info("Found {} regions in backing storage", latestRegions.size());
      setNextState(RefreshMetaState.REFRESH_META_UPDATE);
      return Flow.HAS_MORE_STATE;
    } catch (IOException ioe) {
      LOG.error("Failed to scan backing storage", ioe);
      throw ioe;
    }
  }

  private Flow executeUpdate(MasterProcedureEnv env) throws IOException {
    if (currentRegions == null || latestRegions == null) {
      LOG.error("Can not execute update on null lists. "
        + "Meta Table Regions - {}, Storage Regions - {}", currentRegions, latestRegions);
      throw new IOException((currentRegions == null ? "current regions" : "latest regions") + "list is null");
    }
    try {
      LOG.info("Comparing regions. Current regions: {}, Latest regions: {}",
        currentRegions.size(), latestRegions.size());
      compareAndUpdateRegions(
        currentRegions.stream()
          .collect(Collectors.toMap(RegionInfo::getEncodedName, Function.identity())),
        latestRegions.stream()
          .collect(Collectors.toMap(RegionInfo::getEncodedName, Function.identity())),
        env.getMasterServices().getConnection(), env);
      LOG.info("Meta table update completed successfully");
      setNextState(RefreshMetaState.REFRESH_META_COMPLETE);
      return Flow.HAS_MORE_STATE;
    } catch (IOException ioe) {
      LOG.error("Failed to update the hbase:meta table", ioe);
      throw ioe;
    }
  }

  private Flow executeComplete() {
    currentRegions = null;
    latestRegions = null;
    LOG.info("RefreshMetaProcedure completed successfully");
    return Flow.NO_MORE_STATE;
  }

  /** Compares the current regions in hbase:meta with the latest regions from backing storage
   * and applies necessary mutations (additions, deletions, or modifications) to the meta table.
   *
   * @param currentMap Current regions from hbase:meta
   * @param latestMap Latest regions from backing storage
   * @param connection HBase connection to use for meta table operations
   * @param env MasterProcedureEnv for accessing master services
   * @throws IOException If there is an error accessing the meta table or backing storage
   */
  private void compareAndUpdateRegions(Map<String, RegionInfo> currentMap, Map<String, RegionInfo> latestMap,
    Connection connection, MasterProcedureEnv env) throws IOException {

    List<Mutation> mutations = new ArrayList<>();

    for (String regionId : latestMap.keySet()) {
      if (!currentMap.containsKey(regionId)) {
        mutations.add(MetaTableAccessor.makePutFromRegionInfo(latestMap.get(regionId)));
        LOG.debug("Adding the region to meta: {}", latestMap.get(regionId).getRegionNameAsString());
      } else {
        RegionInfo currentRegion = currentMap.get(regionId);
        RegionInfo latestRegion = latestMap.get(regionId);
        if (hasBoundaryChanged(currentRegion, latestRegion)) {
          mutations.add(MetaTableAccessor.makePutFromRegionInfo(latestRegion));
          LOG.debug("Adding a put to update region boundaries in meta: {}",
            latestRegion.getRegionNameAsString());
        }
      }
    }

    for (String regionId : currentMap.keySet()) {
      if (!latestMap.containsKey(regionId)) {
        mutations.add(MetaTableAccessor.makeDeleteFromRegionInfo(currentMap.get(regionId),
          EnvironmentEdgeManager.currentTime()));
        LOG.debug("Removing region from meta: {}", currentMap.get(regionId).getRegionNameAsString());
      }
    }

    if (!mutations.isEmpty()) {
      LOG.info("Applying {} mutations to meta table", mutations.size());
      executeBatchAndRetry(connection, mutations, env);
    } else {
      LOG.info("No update needed - meta table is in sync with backing storage");
    }
  }

  private void executeBatchAndRetry(Connection connection, List<Mutation> mutations,
    MasterProcedureEnv env) throws IOException {
    List<List<Mutation>> chunks = Lists.partition(mutations, MUTATION_BATCH_SIZE);

    for (int i = 0; i < chunks.size(); i++) {
      List<Mutation> chunk = chunks.get(i);
      RetryCounter retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      while (retryCounter.shouldRetry()) {
        try {
          applyMutations(connection, chunk);
          LOG.debug("Successfully processed batch {}/{}", i + 1, chunks.size());
          break;
        } catch (IOException ioe) {
          LOG.warn("Batch {}/{} failed on attempt {}/{}", i + 1, chunks.size(),
            retryCounter.getAttemptTimes() + 1, retryCounter.getMaxAttempts(), ioe);
          if (!retryCounter.shouldRetry()) {
            LOG.error("Exceeded max retries for batch {}/{}. Failing refresh meta procedure.", i + 1, chunks.size());
            throw ioe;
          }
          try {
            retryCounter.sleepUntilNextRetry();
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during retry", ie);
          }
        }
      }
    }
  }

  private void applyMutations(Connection connection, List<Mutation> mutations) throws IOException {
    List<Put> puts = mutations.stream()
      .filter(m -> m instanceof Put)
      .map(m -> (Put) m)
      .collect(Collectors.toList());

    List<Delete> deletes = mutations.stream()
      .filter(m -> m instanceof Delete)
      .map(m -> (Delete) m)
      .collect(Collectors.toList());

    if (!puts.isEmpty()) {
      MetaTableAccessor.putsToMetaTable(connection, puts);
    }
    if (!deletes.isEmpty()) {
      MetaTableAccessor.deleteFromMetaTable(connection, deletes);
    }
  }

  private boolean hasBoundaryChanged(RegionInfo region1, RegionInfo region2) {
    return !Arrays.equals(region1.getStartKey(), region2.getStartKey()) ||
      !Arrays.equals(region1.getEndKey(), region2.getEndKey());
  }

  /** Scans the backing storage for all regions and returns a list of RegionInfo objects.
   * This method scans the filesystem for region directories and reads their .regioninfo files.
   *
   * @param connection The HBase connection to use.
   * @return List of RegionInfo objects found in the backing storage.
   * @throws IOException If there is an error accessing the filesystem or reading region info files.
   */
  private List<RegionInfo> scanBackingStorage(Connection connection) throws IOException {
    List<RegionInfo> regions = new ArrayList<>();
    Configuration conf = connection.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path rootDir = CommonFSUtils.getRootDir(conf);
    Path dataDir = new Path(rootDir, HConstants.BASE_NAMESPACE_DIR);

    LOG.info("Scanning backing storage under: {}", dataDir);

    if (!fs.exists(dataDir)) {
      LOG.warn("Data directory does not exist: {}", dataDir);
      return regions;
    }

    FileStatus[] namespaceDirs = fs.listStatus(dataDir, this::isRelevantDirectory);
    LOG.debug("Found {} namespace directories in data dir", Arrays.stream(namespaceDirs).toList());

    for (FileStatus nsDir : namespaceDirs) {
      try {
        List<RegionInfo> namespaceRegions = scanTablesInNamespace(fs, nsDir.getPath());
        regions.addAll(namespaceRegions);
        LOG.debug("Found {} regions in namespace {}", namespaceRegions.size(), nsDir.getPath().getName());
      } catch (IOException e) {
        LOG.error("Failed to scan namespace directory: {}", nsDir.getPath(), e);
      }
    }
    LOG.info("Scanned backing storage and found {} regions", regions.size());
    return regions;
  }

  private List<RegionInfo> scanTablesInNamespace(FileSystem fs, Path namespacePath) throws IOException {
    LOG.debug("Scanning namespace {})", namespacePath.getName());
    return Arrays.stream(fs.listStatus(namespacePath, this::isValidTableDirectory))
      .parallel()
      .filter(FileStatus::isDirectory)
      .flatMap(tableDir -> {
        try {
          List<Path> regionDirs = FSUtils.getRegionDirs(fs, tableDir.getPath());
          List<RegionInfo> tableRegions = scanRegionsInTable(fs, regionDirs);
          LOG.debug("Found {} regions in table {} in namespace {}",
            tableRegions.size(), tableDir.getPath().getName(), namespacePath.getName());
          return tableRegions.stream();
        } catch (IOException e) {
          LOG.warn("Failed to scan table directory: {} for namespace {}",
            tableDir.getPath(), namespacePath.getName(), e);
          return Stream.empty();
        }
      }).toList();
  }

  private List<RegionInfo> scanRegionsInTable(FileSystem fs, List<Path> regionDirs) throws IOException {
    List<RegionInfo> regions = new ArrayList<>();

    for (Path regionDir : regionDirs) {
      String encodedRegionName = regionDir.getName();
      try {
        Path regionInfoPath = new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE);
        if (fs.exists(regionInfoPath)) {
          RegionInfo ri = readRegionInfo(fs, regionInfoPath);
          if (ri != null && isValidRegionInfo(ri, encodedRegionName)) {
            regions.add(ri);
            LOG.debug("Found region: {} -> {}", encodedRegionName, ri.getRegionNameAsString());
          } else {
            LOG.warn("Invalid RegionInfo in file: {}", regionInfoPath);
          }
        } else {
          LOG.debug("No .regioninfo file found in region directory: {}", regionDir);
        }
      } catch (Exception e) {
        LOG.warn("Failed to read region info from directory: {}", encodedRegionName, e);
      }
    }
    return regions;
  }

  private boolean isValidTableDirectory(Path path) {
    return !(path.getName().matches("^[._-].*")) &&
      !(path.getName().startsWith(TableName.META_TABLE_NAME.getNameAsString()));
  }

  private boolean isValidRegionInfo(RegionInfo regionInfo, String expectedEncodedName) {
    if (!expectedEncodedName.equals(regionInfo.getEncodedName())) {
      LOG.warn("RegionInfo encoded name mismatch: directory={}, regioninfo={}",
        expectedEncodedName, regionInfo.getEncodedName());
      return false;
    }
    return true;
  }

  private boolean isRelevantDirectory(Path path) {
    return !path.getName().matches("^[._-].*");
  }

  private RegionInfo readRegionInfo(FileSystem fs, Path regionInfoPath) {
    try (FSDataInputStream inputStream = fs.open(regionInfoPath);
      DataInputStream dataInputStream = new DataInputStream(inputStream)) {
      return RegionInfo.parseFrom(dataInputStream);
    } catch (Exception e) {
      LOG.warn("Failed to parse .regioninfo file: {}", regionInfoPath, e);
      return null;
    }
  }

  /**
   * Retrieves the current regions from the hbase:meta table.
   *
   * @param connection The HBase connection to use.
   * @return List of RegionInfo objects representing the current regions in meta.
   * @throws IOException If there is an error accessing the meta table.
   */
  List<RegionInfo> getCurrentRegions(Connection connection) throws IOException {
    LOG.info("Getting all regions from meta table");
    return MetaTableAccessor.getAllRegions(connection, true);
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RefreshMetaState refreshMetaState)
    throws IOException, InterruptedException {
    // No specific rollback needed as it is generally safe to re-run the procedure.
    LOG.trace("Rollback not implemented for RefreshMetaProcedure state: {}", refreshMetaState);
  }

  @Override
  protected RefreshMetaState getState(int stateId) {
    return RefreshMetaState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RefreshMetaState refreshMetaState) {
    return refreshMetaState.getNumber();
  }

  @Override
  protected RefreshMetaState getInitialState() {
    return RefreshMetaState.REFRESH_META_INIT;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // For now, we'll use a simple approach since we do not need to store any state data
    RefreshMetaStateData.Builder builder = RefreshMetaStateData.newBuilder();
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // For now, we'll use a simple approach since we do not need to store any state data
    serializer.deserialize(RefreshMetaStateData.class);
  }
}
