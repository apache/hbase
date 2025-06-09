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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshMetaState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshMetaStateData;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@InterfaceAudience.Private
public class RefreshMetaProcedure extends AbstractStateMachineTableProcedure<RefreshMetaState> {
  private static final Logger LOG = LoggerFactory.getLogger(RefreshMetaProcedure.class);

  private List<RegionInfo> currentRegions;
  private List<RegionInfo> latestRegions;
  private static final int CHUNK_SIZE = 100;

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
    LOG.trace("Scanning backing storage for region directories");
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
    LOG.trace("Compare and update the current regions with backing storage.");

    if (currentRegions == null || latestRegions == null) {
      LOG.error("Can not execute update on null lists. "
        + "Meta Table Regions - {}, Storage Regions - {}", currentRegions, latestRegions);
      throw new IOException((currentRegions == null ? "current regions" : "latest regions") + "list is null");
    }

    try {
      if (needsUpdate(currentRegions, latestRegions)) {
        LOG.info("Update needed. Current regions: {}, Latest regions: {}",
          currentRegions.size(), latestRegions.size());
        compareAndUpdateRegions(currentRegions, latestRegions,
          env.getMasterServices().getConnection());
        LOG.info("Meta table update completed successfully");
      } else {
        LOG.info("No update needed - meta table is in sync with backing storage");
      }

      setNextState(RefreshMetaState.REFRESH_META_COMPLETE);
      return Flow.HAS_MORE_STATE;
    } catch (IOException ioe) {
      LOG.error("Failed to update the hbase:meta table", ioe);
      throw ioe;
    }
  }

  private Flow executeComplete() {
    LOG.info("RefreshMetaProcedure completed successfully");
    return Flow.NO_MORE_STATE;
  }

  /**
   * Compares the current regions with the latest regions and updates the meta table if necessary.
   */
  void compareAndUpdateRegions(List<RegionInfo> current, List<RegionInfo> latest,
    Connection connection) throws IOException {
    List<Put> puts = new ArrayList<>();
    List<Delete> deletes = new ArrayList<>();
    Set<RegionInfo> currentSet = new HashSet<>(current);
    Set<RegionInfo> latestSet = new HashSet<>(latest);

    // Find regions to add (present in latest but not in current)
    for (RegionInfo ri : latest) {
      if (!currentSet.contains(ri)) {
        puts.add(MetaTableAccessor.makePutFromRegionInfo(ri));
        LOG.debug("Adding the region to meta: {}", ri.getRegionNameAsString());
      }
    }

    // Find regions to remove (present in current but not in latest)
    for (RegionInfo ri : current) {
      if (!latestSet.contains(ri)) {
        deletes.add(MetaTableAccessor.makeDeleteFromRegionInfo(ri,
          EnvironmentEdgeManager.currentTime()));
        LOG.debug("Removing region from meta: {}", ri.getRegionNameAsString());
      }
    }

    // Find regions to update (same region name but different boundaries)
    for (RegionInfo latestRegion : latest) {
      if (currentSet.contains(latestRegion)) {
        RegionInfo currentRegion = current.stream()
          .filter(c -> c.getRegionNameAsString().equals(latestRegion.getRegionNameAsString()))
          .findFirst()
          .orElse(null);

        if (currentRegion != null && hasBoundaryChanged(currentRegion, latestRegion)) {
          puts.add(MetaTableAccessor.makePutFromRegionInfo(latestRegion));
          LOG.debug("Adding a put to update region boundaries in meta: {}",
            latestRegion.getRegionNameAsString());
        }
      }
    }

    if (!puts.isEmpty()) {
      LOG.info("Adding/updating {} regions in meta table", puts.size());
      executeBatchedPuts(connection, puts);
    }

    if (!deletes.isEmpty()) {
      LOG.info("Removing {} regions from meta table", deletes.size());
      executeBatchedDeletes(connection, deletes);
    }

    if (puts.isEmpty() && deletes.isEmpty()) {
      LOG.info("No changes needed in meta table");
    }
  }

  private void executeBatchedPuts(Connection connection, List<Put> puts) throws IOException {
    for (int i = 0; i < puts.size(); i += CHUNK_SIZE) {
      int end = Math.min(puts.size(), i + CHUNK_SIZE);
      List<Put> chunk = puts.subList(i, end);

      for (int attempt = 1; attempt <= 3; attempt++) {
        try {
          MetaTableAccessor.putsToMetaTable(connection, chunk);
          LOG.debug("Successfully processed put batch {}-{}", i, end);
          break;
        } catch (IOException ioe) {
          LOG.warn("Put batch {}-{} failed on attempt {}/3", i, end, attempt, ioe);
          if (attempt == 3) {
            throw ioe;
          }
          try {
            Thread.sleep(100);
          } catch (InterruptedException iex) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during retry", iex);
          }
        }
      }
    }
  }

  private void executeBatchedDeletes(Connection connection, List<Delete> deletes) throws IOException {
    for (int i = 0; i < deletes.size(); i += CHUNK_SIZE) {
      int end = Math.min(deletes.size(), i + CHUNK_SIZE);
      List<Delete> chunk = deletes.subList(i, end);

      for (int attempt = 1; attempt <= 3; attempt++) {
        try {
          MetaTableAccessor.deleteFromMetaTable(connection, chunk);
          LOG.debug("Successfully processed delete batch {}-{}", i, end);
          break;
        } catch (IOException e) {
          LOG.warn("Delete batch {}-{} failed on attempt {}/3", i, end, attempt, e);
          if (attempt == 3) {
            throw e;
          }
          try {
            Thread.sleep(100);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during retry", ie);
          }
        }
      }
    }
  }

  /**
   * Determines if an update is needed by comparing current and latest regions.
   */
  boolean needsUpdate(List<RegionInfo> current, List<RegionInfo> latest) {
    if (current == null || latest == null) {
      LOG.warn("Cannot compare null region lists - current: {}, latest: {}",
        current != null, latest != null);
      return false;
    }

    Set<RegionInfo> currentSet = new HashSet<>(current);
    Set<RegionInfo> latestSet = new HashSet<>(latest);

    // Check for size or set differences
    if (!currentSet.equals(latestSet)) {
      LOG.info("Region mismatch: current={}, latest={}", currentSet.size(), latestSet.size());
      return true;
    }

    // Check for boundary changes
    Map<String, RegionInfo> latestRegionsMap = latest.stream()
      .collect(Collectors.toMap(RegionInfo::getRegionNameAsString, Function.identity()));

    for (RegionInfo cr : current) {
      RegionInfo lr = latestRegionsMap.get(cr.getRegionNameAsString());
      if (lr == null || hasBoundaryChanged(cr, lr)) {
        LOG.info("Region mismatch or boundary changed for {}", cr.getRegionNameAsString());
        return true;
      }
    }
    return false;
  }

  private boolean hasBoundaryChanged(RegionInfo region1, RegionInfo region2) {
    return !Arrays.equals(region1.getStartKey(), region2.getStartKey()) ||
      !Arrays.equals(region1.getEndKey(), region2.getEndKey());
  }

  /**
   * Scans the backing storage for region directories and returns a list of RegionInfo objects.
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

    try {
      // Scan namespace directories
      for (FileStatus nsDir : fs.listStatus(dataDir)) {
        if (isNotRelevantDirectory(nsDir)) {
          continue;
        }

        // Scan table directories within namespace
        for (FileStatus tableDir : fs.listStatus(nsDir.getPath())) {
          if (isNotRelevantDirectory(tableDir)) {
            continue;
          }

          // Scan region directories within table
          for (FileStatus regionDir : fs.listStatus(tableDir.getPath())) {
            if (isNotRelevantDirectory(regionDir)) {
              continue;
            }

            String encodedRegionName = regionDir.getPath().getName();
            try {
              // Read .regioninfo file to get the RegionInfo object
              Path regionInfoPath = new Path(regionDir.getPath(), ".regioninfo");
              if (fs.exists(regionInfoPath)) {
                try (FSDataInputStream inputStream = fs.open(regionInfoPath);
                  DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                  RegionInfo ri = RegionInfo.parseFrom(dataInputStream);
                  if (ri != null) {
                    regions.add(ri);
                    LOG.debug("Found region: {} -> {}", encodedRegionName, ri.getRegionNameAsString());
                  }
                } catch (Exception e) {
                  LOG.warn("Failed to parse .regioninfo file: {}", regionInfoPath, e);
                }
              } else {
                LOG.warn("No .regioninfo file found in region directory: {}", regionDir.getPath());
              }
            } catch (Exception e) {
              LOG.warn("Failed to read region info from directory: {}", encodedRegionName, e);
            }
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Error scanning backing storage", e);
      throw e;
    }

    LOG.info("Scanned backing storage and found {} regions", regions.size());
    return regions;
  }

  private boolean isSystemDirectory(String dirName) {
    return dirName.startsWith(".") || dirName.startsWith("-") || dirName.startsWith("_");
  }

  private  boolean isNotRelevantDirectory(FileStatus fs) {
    return !fs.isDirectory() || isSystemDirectory(fs.getPath().getName());
  }

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
