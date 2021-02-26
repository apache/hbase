/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ReopenTableRegionsStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Used for reopening the regions for a table.
 */
@InterfaceAudience.Private
public class ReopenTableRegionsProcedure
    extends AbstractStateMachineTableProcedure<ReopenTableRegionsState> {

  private static final Logger LOG = LoggerFactory.getLogger(ReopenTableRegionsProcedure.class);

  private TableName tableName;

  // Specify specific regions of a table to reopen.
  // if specified null, all regions of the table will be reopened.
  private List<byte[]> regionNames;

  private List<HRegionLocation> regions = Collections.emptyList();

  private RetryCounter retryCounter;

  public ReopenTableRegionsProcedure() {
    regionNames = Collections.emptyList();
  }

  public ReopenTableRegionsProcedure(TableName tableName) {
    this.tableName = tableName;
    this.regionNames = Collections.emptyList();
  }

  public ReopenTableRegionsProcedure(final TableName tableName,
      final List<byte[]> regionNames) {
    this.tableName = tableName;
    this.regionNames = regionNames;
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  private boolean canSchedule(MasterProcedureEnv env, HRegionLocation loc) {
    if (loc.getSeqNum() < 0) {
      return false;
    }
    RegionStateNode regionNode =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(loc.getRegion());
    // If the region node is null, then at least in the next round we can remove this region to make
    // progress. And the second condition is a normal one, if there are no TRSP with it then we can
    // schedule one to make progress.
    return regionNode == null || !regionNode.isInTransition();
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ReopenTableRegionsState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case REOPEN_TABLE_REGIONS_GET_REGIONS:
        if (!isTableEnabled(env)) {
          LOG.info("Table {} is disabled, give up reopening its regions", tableName);
          return Flow.NO_MORE_STATE;
        }
        List<HRegionLocation> tableRegions = env.getAssignmentManager()
          .getRegionStates().getRegionsOfTableForReopen(tableName);
        regions = getRegionLocationsForReopen(tableRegions);
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_REOPEN_REGIONS:
        for (HRegionLocation loc : regions) {
          RegionStateNode regionNode =
            env.getAssignmentManager().getRegionStates().getRegionStateNode(loc.getRegion());
          // this possible, maybe the region has already been merged or split, see HBASE-20921
          if (regionNode == null) {
            continue;
          }
          TransitRegionStateProcedure proc;
          regionNode.lock();
          try {
            if (regionNode.getProcedure() != null) {
              continue;
            }
            proc = TransitRegionStateProcedure.reopen(env, regionNode.getRegionInfo());
            regionNode.setProcedure(proc);
          } finally {
            regionNode.unlock();
          }
          addChildProcedure(proc);
        }
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_CONFIRM_REOPENED);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_CONFIRM_REOPENED:
        regions = regions.stream().map(env.getAssignmentManager().getRegionStates()::checkReopened)
          .filter(l -> l != null).collect(Collectors.toList());
        if (regions.isEmpty()) {
          return Flow.NO_MORE_STATE;
        }
        if (regions.stream().anyMatch(loc -> canSchedule(env, loc))) {
          retryCounter = null;
          setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
          return Flow.HAS_MORE_STATE;
        }
        // We can not schedule TRSP for all the regions need to reopen, wait for a while and retry
        // again.
        if (retryCounter == null) {
          retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
        }
        long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
        LOG.info(
          "There are still {} region(s) which need to be reopened for table {} are in " +
            "OPENING state, suspend {}secs and try again later",
          regions.size(), tableName, backoff / 1000);
        setTimeout(Math.toIntExact(backoff));
        setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
        skipPersistence();
        throw new ProcedureSuspendedException();
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  private List<HRegionLocation> getRegionLocationsForReopen(
      List<HRegionLocation> tableRegionsForReopen) {

    List<HRegionLocation> regionsToReopen = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(regionNames) &&
      CollectionUtils.isNotEmpty(tableRegionsForReopen)) {
      for (byte[] regionName : regionNames) {
        for (HRegionLocation hRegionLocation : tableRegionsForReopen) {
          if (Bytes.equals(regionName, hRegionLocation.getRegion().getRegionName())) {
            regionsToReopen.add(hRegionLocation);
            break;
          }
        }
      }
    } else {
      regionsToReopen = tableRegionsForReopen;
    }
    return regionsToReopen;
  }

  /**
   * At end of timeout, wake ourselves up so we run again.
   */
  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false; // 'false' means that this procedure handled the timeout
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ReopenTableRegionsState state)
      throws IOException, InterruptedException {
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected ReopenTableRegionsState getState(int stateId) {
    return ReopenTableRegionsState.forNumber(stateId);
  }

  @Override
  protected int getStateId(ReopenTableRegionsState state) {
    return state.getNumber();
  }

  @Override
  protected ReopenTableRegionsState getInitialState() {
    return ReopenTableRegionsState.REOPEN_TABLE_REGIONS_GET_REGIONS;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    ReopenTableRegionsStateData.Builder builder = ReopenTableRegionsStateData.newBuilder()
      .setTableName(ProtobufUtil.toProtoTableName(tableName));
    regions.stream().map(ProtobufUtil::toRegionLocation).forEachOrdered(builder::addRegion);
    if (CollectionUtils.isNotEmpty(regionNames)) {
      // As of this writing, wrapping this statement withing if condition is only required
      // for backward compatibility as we used to have 'regionNames' as null for cases
      // where all regions of given table should be reopened. Now, we have kept emptyList()
      // for 'regionNames' to indicate all regions of given table should be reopened unless
      // 'regionNames' contains at least one specific region, in which case only list of regions
      // that 'regionNames' contain should be reopened, not all regions of given table.
      // Now, we don't need this check since we are not dealing with null 'regionNames' and hence,
      // guarding by this if condition can be removed in HBase 4.0.0.
      regionNames.stream().map(ByteString::copyFrom).forEachOrdered(builder::addRegionNames);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    ReopenTableRegionsStateData data = serializer.deserialize(ReopenTableRegionsStateData.class);
    tableName = ProtobufUtil.toTableName(data.getTableName());
    regions = data.getRegionList().stream().map(ProtobufUtil::toRegionLocation)
      .collect(Collectors.toList());
    if (CollectionUtils.isNotEmpty(data.getRegionNamesList())) {
      regionNames = data.getRegionNamesList().stream().map(ByteString::toByteArray)
        .collect(Collectors.toList());
    } else {
      regionNames = Collections.emptyList();
    }
  }
}
