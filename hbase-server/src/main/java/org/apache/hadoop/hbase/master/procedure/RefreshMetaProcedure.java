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

import static org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState.WAITING_TIMEOUT;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshMetaState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshMetaStateData;

@InterfaceAudience.Private
public class RefreshMetaProcedure extends AbstractStateMachineTableProcedure<RefreshMetaState> {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshMetaProcedure.class);
  private static final String HIDDEN_DIR_PATTERN = "^[._-].*";

  private List<RegionInfo> currentRegions;
  private List<RegionInfo> latestRegions;
  private List<Mutation> pendingMutations;
  private RetryCounter retryCounter;
  private static final int MUTATION_BATCH_SIZE = 100;
  private List<RegionInfo> newlyAddedRegions;
  private List<TableName> deletedTables;

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
        case REFRESH_META_PREPARE -> executePrepare();
        case REFRESH_META_APPLY -> executeApply(env);
        case REFRESH_META_FOLLOWUP -> executeFollowup(env);
        case REFRESH_META_FINISH -> executeFinish(env);
        default -> throw new UnsupportedOperationException("Unhandled state: " + refreshMetaState);
      };
    } catch (Exception ex) {
      LOG.error("Error in RefreshMetaProcedure state {}", refreshMetaState, ex);
      setFailure("RefreshMetaProcedure", ex);
      return Flow.NO_MORE_STATE;
    }
  }

  private Flow executeInit(MasterProcedureEnv env) throws IOException {
    LOG.trace("Getting current regions from {} table", TableName.META_TABLE_NAME);
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
      setNextState(RefreshMetaState.REFRESH_META_PREPARE);
      return Flow.HAS_MORE_STATE;
    } catch (IOException ioe) {
      LOG.error("Failed to scan backing storage", ioe);
      throw ioe;
    }
  }

  private Flow executePrepare() throws IOException {
    if (currentRegions == null || latestRegions == null) {
      LOG.error(
        "Can not execute update on null lists. " + "Meta Table Regions - {}, Storage Regions - {}",
        currentRegions, latestRegions);
      throw new IOException(
        (currentRegions == null ? "current regions" : "latest regions") + " list is null");
    }
    LOG.info("Comparing regions. Current regions: {}, Latest regions: {}", currentRegions.size(),
      latestRegions.size());

    this.newlyAddedRegions = new ArrayList<>();
    this.deletedTables = new ArrayList<>();

    pendingMutations = prepareMutations(
      currentRegions.stream()
        .collect(Collectors.toMap(RegionInfo::getEncodedName, Function.identity())),
      latestRegions.stream()
        .collect(Collectors.toMap(RegionInfo::getEncodedName, Function.identity())));

    if (pendingMutations.isEmpty()) {
      LOG.info("RefreshMetaProcedure completed, No update needed.");
      setNextState(RefreshMetaState.REFRESH_META_FINISH);
    } else {
      LOG.info("Prepared {} region mutations and {} tables for cleanup.", pendingMutations.size(),
        deletedTables.size());
      setNextState(RefreshMetaState.REFRESH_META_APPLY);
    }
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeApply(MasterProcedureEnv env) throws ProcedureSuspendedException {
    try {
      if (pendingMutations != null && !pendingMutations.isEmpty()) {
        applyMutations(env.getMasterServices().getConnection(), pendingMutations);
        LOG.debug("RefreshMetaProcedure applied {} mutations to meta table",
          pendingMutations.size());
      }
    } catch (IOException ioe) {
      if (retryCounter == null) {
        retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
      }
      long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
      LOG.warn("Failed to apply mutations to meta table, suspending for {} ms", backoff, ioe);
      setTimeout(Math.toIntExact(backoff));
      setState(WAITING_TIMEOUT);
      skipPersistence();
      throw new ProcedureSuspendedException();
    }

    if (
      (this.newlyAddedRegions != null && !this.newlyAddedRegions.isEmpty())
        || (this.deletedTables != null && !this.deletedTables.isEmpty())
    ) {
      setNextState(RefreshMetaState.REFRESH_META_FOLLOWUP);
    } else {
      LOG.info("RefreshMetaProcedure completed. No follow-up actions were required.");
      setNextState(RefreshMetaState.REFRESH_META_FINISH);
    }
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFollowup(MasterProcedureEnv env) throws IOException {

    LOG.info("Submitting assignment for new regions: {}", this.newlyAddedRegions);
    addChildProcedure(env.getAssignmentManager().createAssignProcedures(newlyAddedRegions));

    for (TableName tableName : this.deletedTables) {
      LOG.debug("Submitting deletion for empty table {}", tableName);
      env.getMasterServices().getAssignmentManager().deleteTable(tableName);
      env.getMasterServices().getTableStateManager().setDeletedTable(tableName);
      env.getMasterServices().getTableDescriptors().remove(tableName);
    }
    setNextState(RefreshMetaState.REFRESH_META_FINISH);
    return Flow.HAS_MORE_STATE;
  }

  private Flow executeFinish(MasterProcedureEnv env) {
    invalidateTableDescriptorCache(env);
    LOG.info("RefreshMetaProcedure completed successfully. All follow-up actions finished.");
    currentRegions = null;
    latestRegions = null;
    pendingMutations = null;
    deletedTables = null;
    newlyAddedRegions = null;
    return Flow.NO_MORE_STATE;
  }

  private void invalidateTableDescriptorCache(MasterProcedureEnv env) {
    LOG.debug("Invalidating the table descriptor cache to ensure new tables are discovered");
    env.getMasterServices().getTableDescriptors().invalidateTableDescriptorCache();
  }

  /**
   * Prepares mutations by comparing the current regions in hbase:meta with the latest regions from
   * backing storage. Also populates newlyAddedRegions and deletedTables lists for follow-up
   * actions.
   * @param currentMap Current regions from hbase:meta
   * @param latestMap  Latest regions from backing storage
   * @return List of mutations to apply to the meta table
   * @throws IOException If there is an error creating mutations
   */
  private List<Mutation> prepareMutations(Map<String, RegionInfo> currentMap,
    Map<String, RegionInfo> latestMap) throws IOException {
    List<Mutation> mutations = new ArrayList<>();

    for (String regionId : Stream.concat(currentMap.keySet().stream(), latestMap.keySet().stream())
      .collect(Collectors.toSet())) {
      RegionInfo currentRegion = currentMap.get(regionId);
      RegionInfo latestRegion = latestMap.get(regionId);

      if (latestRegion != null) {
        if (currentRegion == null || hasBoundaryChanged(currentRegion, latestRegion)) {
          mutations.add(MetaTableAccessor.makePutFromRegionInfo(latestRegion));
          newlyAddedRegions.add(latestRegion);
        }
      } else {
        mutations.add(MetaTableAccessor.makeDeleteFromRegionInfo(currentRegion,
          EnvironmentEdgeManager.currentTime()));
      }
    }

    if (!currentMap.isEmpty() || !latestMap.isEmpty()) {
      Set<TableName> currentTables =
        currentMap.values().stream().map(RegionInfo::getTable).collect(Collectors.toSet());
      Set<TableName> latestTables =
        latestMap.values().stream().map(RegionInfo::getTable).collect(Collectors.toSet());

      Set<TableName> tablesToDeleteState = new HashSet<>(currentTables);
      tablesToDeleteState.removeAll(latestTables);
      if (!tablesToDeleteState.isEmpty()) {
        LOG.warn(
          "The following tables have no regions on storage and WILL BE REMOVED from the meta: {}",
          tablesToDeleteState);
        this.deletedTables.addAll(tablesToDeleteState);
      }

      Set<TableName> tablesToRestoreState = new HashSet<>(latestTables);
      tablesToRestoreState.removeAll(currentTables);
      if (!tablesToRestoreState.isEmpty()) {
        LOG.info("Adding missing table:state entry for recovered tables: {}", tablesToRestoreState);
        for (TableName tableName : tablesToRestoreState) {
          TableState tableState = new TableState(tableName, TableState.State.ENABLED);
          mutations.add(MetaTableAccessor.makePutFromTableState(tableState,
            EnvironmentEdgeManager.currentTime()));
        }
      }
    }
    return mutations;
  }

  private void applyMutations(Connection connection, List<Mutation> mutations) throws IOException {
    List<List<Mutation>> chunks = Lists.partition(mutations, MUTATION_BATCH_SIZE);

    for (int i = 0; i < chunks.size(); i++) {
      List<Mutation> chunk = chunks.get(i);

      List<Put> puts =
        chunk.stream().filter(m -> m instanceof Put).map(m -> (Put) m).collect(Collectors.toList());

      List<Delete> deletes = chunk.stream().filter(m -> m instanceof Delete).map(m -> (Delete) m)
        .collect(Collectors.toList());

      if (!puts.isEmpty()) {
        MetaTableAccessor.putsToMetaTable(connection, puts);
      }
      if (!deletes.isEmpty()) {
        MetaTableAccessor.deleteFromMetaTable(connection, deletes);
      }
      LOG.debug("Successfully processed batch {}/{}", i + 1, chunks.size());
    }
  }

  boolean hasBoundaryChanged(RegionInfo region1, RegionInfo region2) {
    return !Arrays.equals(region1.getStartKey(), region2.getStartKey())
      || !Arrays.equals(region1.getEndKey(), region2.getEndKey());
  }

  /**
   * Scans the backing storage for all regions and returns a list of RegionInfo objects. This method
   * scans the filesystem for region directories and reads their .regioninfo files.
   * @param connection The HBase connection to use.
   * @return List of RegionInfo objects found in the backing storage.
   * @throws IOException If there is an error accessing the filesystem or reading region info files.
   */
  List<RegionInfo> scanBackingStorage(Connection connection) throws IOException {
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

    FileStatus[] namespaceDirs =
      fs.listStatus(dataDir, path -> !path.getName().matches(HIDDEN_DIR_PATTERN));
    LOG.debug("Found {} namespace directories in data dir", Arrays.stream(namespaceDirs).toList());

    for (FileStatus nsDir : namespaceDirs) {
      String namespaceName = nsDir.getPath().getName();
      if (NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR.equals(namespaceName)) {
        LOG.info("Skipping system namespace {}", namespaceName);
        continue;
      }
      try {
        List<RegionInfo> namespaceRegions = scanTablesInNamespace(fs, nsDir.getPath());
        regions.addAll(namespaceRegions);
        LOG.debug("Found {} regions in namespace {}", namespaceRegions.size(),
          nsDir.getPath().getName());
      } catch (IOException e) {
        LOG.error("Failed to scan namespace directory: {}", nsDir.getPath(), e);
      }
    }
    LOG.info("Scanned backing storage and found {} regions", regions.size());
    return regions;
  }

  private List<RegionInfo> scanTablesInNamespace(FileSystem fs, Path namespacePath)
    throws IOException {
    LOG.debug("Scanning namespace {}", namespacePath.getName());
    List<Path> tableDirs = FSUtils.getLocalTableDirs(fs, namespacePath);

    return tableDirs.parallelStream().flatMap(tableDir -> {
      try {
        List<RegionInfo> tableRegions = scanRegionsInTable(fs, FSUtils.getRegionDirs(fs, tableDir));
        LOG.debug("Found {} regions in table {} in namespace {}", tableRegions.size(),
          tableDir.getName(), namespacePath.getName());
        return tableRegions.stream();
      } catch (IOException e) {
        LOG.warn("Failed to scan table directory: {} for namespace {}", tableDir,
          namespacePath.getName(), e);
        return Stream.empty();
      }
    }).toList();
  }

  private List<RegionInfo> scanRegionsInTable(FileSystem fs, List<Path> regionDirs)
    throws IOException {
    return regionDirs.stream().map(regionDir -> {
      String encodedRegionName = regionDir.getName();
      try {
        Path regionInfoPath = new Path(regionDir, HRegionFileSystem.REGION_INFO_FILE);
        if (fs.exists(regionInfoPath)) {
          RegionInfo ri = readRegionInfo(fs, regionInfoPath);
          if (ri != null && isValidRegionInfo(ri, encodedRegionName)) {
            LOG.debug("Found region: {} -> {}", encodedRegionName, ri.getRegionNameAsString());
            return ri;
          } else {
            LOG.warn("Invalid RegionInfo in file: {}", regionInfoPath);
          }
        } else {
          LOG.debug("No .regioninfo file found in region directory: {}", regionDir);
        }
      } catch (Exception e) {
        LOG.warn("Failed to read region info from directory: {}", encodedRegionName, e);
      }
      return null;
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  private boolean isValidRegionInfo(RegionInfo regionInfo, String expectedEncodedName) {
    if (!expectedEncodedName.equals(regionInfo.getEncodedName())) {
      LOG.warn("RegionInfo encoded name mismatch: directory={}, regioninfo={}", expectedEncodedName,
        regionInfo.getEncodedName());
      return false;
    }
    return true;
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
   * @param connection The HBase connection to use.
   * @return List of RegionInfo objects representing the current regions in meta.
   * @throws IOException If there is an error accessing the meta table.
   */
  List<RegionInfo> getCurrentRegions(Connection connection) throws IOException {
    LOG.info("Getting all regions from meta table");
    return MetaTableAccessor.getAllRegions(connection, true);
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(
      org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
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
