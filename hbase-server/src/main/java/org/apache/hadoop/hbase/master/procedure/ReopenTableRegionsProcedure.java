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

import com.google.errorprone.annotations.RestrictedApi;
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

  public static final String PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY =
    "hbase.reopen.table.regions.progressive.batch.backoff.ms";
  public static final long PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT = 0L;
  public static final String PROGRESSIVE_BATCH_SIZE_MAX_KEY =
    "hbase.reopen.table.regions.progressive.batch.size.max";
  public static final int PROGRESSIVE_BATCH_SIZE_MAX_DISABLED = -1;
  private static final int PROGRESSIVE_BATCH_SIZE_MAX_DEFAULT_VALUE = Integer.MAX_VALUE;

  // this minimum prevents a max which would break this procedure
  private static final int MINIMUM_BATCH_SIZE_MAX = 1;

  private TableName tableName;

  // Specify specific regions of a table to reopen.
  // if specified null, all regions of the table will be reopened.
  private List<byte[]> regionNames;

  private List<HRegionLocation> regions = Collections.emptyList();

  private List<HRegionLocation> currentRegionBatch = Collections.emptyList();

  private RetryCounter retryCounter;

  private long reopenBatchBackoffMillis;
  private int reopenBatchSize;
  private int reopenBatchSizeMax;
  private long regionsReopened = 0;
  private long batchesProcessed = 0;

  public ReopenTableRegionsProcedure() {
    this(null);
  }

  public ReopenTableRegionsProcedure(TableName tableName) {
    this(tableName, Collections.emptyList());
  }

  public ReopenTableRegionsProcedure(final TableName tableName, final List<byte[]> regionNames) {
    this(tableName, regionNames, PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT,
      PROGRESSIVE_BATCH_SIZE_MAX_DISABLED);
  }

  public ReopenTableRegionsProcedure(final TableName tableName, long reopenBatchBackoffMillis,
    int reopenBatchSizeMax) {
    this(tableName, Collections.emptyList(), reopenBatchBackoffMillis, reopenBatchSizeMax);
  }

  public ReopenTableRegionsProcedure(final TableName tableName, final List<byte[]> regionNames,
    long reopenBatchBackoffMillis, int reopenBatchSizeMax) {
    this.tableName = tableName;
    this.regionNames = regionNames;
    this.reopenBatchBackoffMillis = reopenBatchBackoffMillis;
    if (reopenBatchSizeMax == PROGRESSIVE_BATCH_SIZE_MAX_DISABLED) {
      this.reopenBatchSize = Integer.MAX_VALUE;
      this.reopenBatchSizeMax = Integer.MAX_VALUE;
    } else {
      this.reopenBatchSize = 1;
      this.reopenBatchSizeMax = Math.max(reopenBatchSizeMax, MINIMUM_BATCH_SIZE_MAX);
    }
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public long getRegionsReopened() {
    return regionsReopened;
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  public long getBatchesProcessed() {
    return batchesProcessed;
  }

  @RestrictedApi(explanation = "Should only be called internally or in tests", link = "",
      allowedOnPath = ".*(/src/test/.*|ReopenTableRegionsProcedure).java")
  protected int progressBatchSize() {
    int previousBatchSize = reopenBatchSize;
    reopenBatchSize = Math.min(reopenBatchSizeMax, 2 * reopenBatchSize);
    if (reopenBatchSize < previousBatchSize) {
      // the batch size should never decrease. this must be overflow, so just use max
      reopenBatchSize = reopenBatchSizeMax;
    }
    return reopenBatchSize;
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
        List<HRegionLocation> tableRegions =
          env.getAssignmentManager().getRegionStates().getRegionsOfTableForReopen(tableName);
        regions = getRegionLocationsForReopen(tableRegions);
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_REOPEN_REGIONS:
        // if we didn't finish reopening the last batch yet, let's keep trying until we do.
        // at that point, the batch will be empty and we can generate a new batch
        if (!regions.isEmpty() && currentRegionBatch.isEmpty()) {
          currentRegionBatch = regions.stream().limit(reopenBatchSize).collect(Collectors.toList());
          batchesProcessed++;
        }
        for (HRegionLocation loc : currentRegionBatch) {
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
          regionsReopened++;
        }
        setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_CONFIRM_REOPENED);
        return Flow.HAS_MORE_STATE;
      case REOPEN_TABLE_REGIONS_CONFIRM_REOPENED:
        // update region lists based on what's been reopened
        regions = filterReopened(env, regions);
        currentRegionBatch = filterReopened(env, currentRegionBatch);

        // existing batch didn't fully reopen, so try to resolve that first.
        // since this is a retry, don't do the batch backoff
        if (!currentRegionBatch.isEmpty()) {
          return reopenIfSchedulable(env, currentRegionBatch, false);
        }

        if (regions.isEmpty()) {
          return Flow.NO_MORE_STATE;
        }

        // current batch is finished, schedule more regions
        return reopenIfSchedulable(env, regions, true);
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  private List<HRegionLocation> filterReopened(MasterProcedureEnv env,
    List<HRegionLocation> regionsToCheck) {
    return regionsToCheck.stream().map(env.getAssignmentManager().getRegionStates()::checkReopened)
      .filter(l -> l != null).collect(Collectors.toList());
  }

  private Flow reopenIfSchedulable(MasterProcedureEnv env, List<HRegionLocation> regionsToReopen,
    boolean shouldBatchBackoff) throws ProcedureSuspendedException {
    if (regionsToReopen.stream().anyMatch(loc -> canSchedule(env, loc))) {
      retryCounter = null;
      setNextState(ReopenTableRegionsState.REOPEN_TABLE_REGIONS_REOPEN_REGIONS);
      if (shouldBatchBackoff) {
        progressBatchSize();
      }
      if (shouldBatchBackoff && reopenBatchBackoffMillis > 0) {
        setBackoffState(reopenBatchBackoffMillis);
        throw new ProcedureSuspendedException();
      } else {
        return Flow.HAS_MORE_STATE;
      }
    }

    // We can not schedule TRSP for all the regions need to reopen, wait for a while and retry
    // again.
    if (retryCounter == null) {
      retryCounter = ProcedureUtil.createRetryCounter(env.getMasterConfiguration());
    }
    long backoffMillis = retryCounter.getBackoffTimeAndIncrementAttempts();
    LOG.info(
      "There are still {} region(s) which need to be reopened for table {}. {} are in "
        + "OPENING state, suspend {}secs and try again later",
      regions.size(), tableName, currentRegionBatch.size(), backoffMillis / 1000);
    setBackoffState(backoffMillis);
    throw new ProcedureSuspendedException();
  }

  private void setBackoffState(long millis) {
    setTimeout(Math.toIntExact(millis));
    setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
    skipPersistence();
  }

  private List<HRegionLocation>
    getRegionLocationsForReopen(List<HRegionLocation> tableRegionsForReopen) {

    List<HRegionLocation> regionsToReopen = new ArrayList<>();
    if (
      CollectionUtils.isNotEmpty(regionNames) && CollectionUtils.isNotEmpty(tableRegionsForReopen)
    ) {
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
