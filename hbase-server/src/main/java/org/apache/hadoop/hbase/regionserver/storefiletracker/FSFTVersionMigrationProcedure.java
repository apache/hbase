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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.conf.ConfigKey;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.procedure.GlobalProcedureInterface;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FSFTVersionMigrationState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.FSFTVersionMigrationStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Master-side migration procedure to upgrade FSFT store file list version from V1 to V2 by
 * scheduling {@link FSFTVersionUpgradeRegionProcedure} for regions that still have version &lt; 2
 * in their latest store file list files.
 */
@InterfaceAudience.Private
public class FSFTVersionMigrationProcedure
  extends StateMachineProcedure<MasterProcedureEnv, FSFTVersionMigrationState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(FSFTVersionMigrationProcedure.class);
  private static final long TARGET_VERSION = StoreFileListFile.VERSION;

  public static final String PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY =
    ConfigKey.LONG("hbase.fsft.version.migration.progressive.batch.backoff.ms");
  public static final long PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT = 0L;
  public static final String PROGRESSIVE_BATCH_SIZE_MAX_KEY =
    ConfigKey.INT("hbase.fsft.version.migration.progressive.batch.size.max");
  public static final int PROGRESSIVE_BATCH_SIZE_MAX_DISABLED = -1;
  private static final int MINIMUM_BATCH_SIZE_MAX = 1;

  private List<RegionInfo> regionsToUpgrade = Collections.emptyList();
  private int nextIndex = 0;

  private long batchBackoffMillis = PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT;
  private int batchSize = Integer.MAX_VALUE;
  private int batchSizeMax = Integer.MAX_VALUE;

  private long regionsScheduled = 0;
  private long batchesProcessed = 0;


  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, FSFTVersionMigrationState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case FSFT_VERSION_MIGRATION_PREPARE:
        try {
          initBatchTuning(env);
          regionsToUpgrade = collectRegionsNeedingUpgrade(env);
          nextIndex = 0;
          if (regionsToUpgrade.isEmpty()) {
            LOG.info("No regions need FSFT version migration");
            return Flow.NO_MORE_STATE;
          }
          LOG.info("FSFT version migration found {} regions needing upgrade", regionsToUpgrade.size());
          setNextState(FSFTVersionMigrationState.FSFT_VERSION_MIGRATION_SCHEDULE_REGIONS);
          return Flow.HAS_MORE_STATE;
        } catch (IOException e) {
          long backoff = ProcedureUtil.createRetryCounter(env.getMasterConfiguration())
            .getBackoffTimeAndIncrementAttempts();
          LOG.warn("Failed preparing FSFT version migration, suspend {}secs", backoff / 1000, e);
          throw suspend(Math.toIntExact(backoff), true);
        }
      case FSFT_VERSION_MIGRATION_SCHEDULE_REGIONS:
        if (nextIndex >= regionsToUpgrade.size()) {
          LOG.info("FSFT version migration finished scheduling {} regions in {} batches",
            regionsScheduled, batchesProcessed);
          return Flow.NO_MORE_STATE;
        }

        int endExclusive = Math.min(regionsToUpgrade.size(), nextIndex + batchSize);
        for (int i = nextIndex; i < endExclusive; i++) {
          addChildProcedure(new FSFTVersionUpgradeRegionProcedure(regionsToUpgrade.get(i)));
          regionsScheduled++;
        }
        batchesProcessed++;
        nextIndex = endExclusive;

        // Tune next batch.
        progressBatchSize();

        // If we still have more work, optionally back off between batches.
        if (nextIndex < regionsToUpgrade.size() && batchBackoffMillis > 0) {
          setBackoffState(batchBackoffMillis);
          throw new ProcedureSuspendedException();
        }
        setNextState(FSFTVersionMigrationState.FSFT_VERSION_MIGRATION_SCHEDULE_REGIONS);
        return Flow.HAS_MORE_STATE;
      default:
        throw new UnsupportedOperationException("Unhandled state=" + state);
    }
  }

  private static boolean isFsft(TableDescriptor td) {
    String impl = td.getValue(StoreFileTrackerFactory.TRACKER_IMPL);
    if (impl == null) {
      return false;
    }
    if (impl.equalsIgnoreCase(StoreFileTrackerFactory.Trackers.FILE.name())) {
      return true;
    }
    // Support legacy/lowercase config, or class name.
    return "file".equalsIgnoreCase(impl) || impl.endsWith("FileBasedStoreFileTracker");
  }

  private void initBatchTuning(MasterProcedureEnv env) {
    batchBackoffMillis = env.getMasterConfiguration().getLong(PROGRESSIVE_BATCH_BACKOFF_MILLIS_KEY,
      PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT);
    int configuredMax = env.getMasterConfiguration().getInt(PROGRESSIVE_BATCH_SIZE_MAX_KEY,
      PROGRESSIVE_BATCH_SIZE_MAX_DISABLED);
    if (configuredMax == PROGRESSIVE_BATCH_SIZE_MAX_DISABLED) {
      batchSize = Integer.MAX_VALUE;
      batchSizeMax = Integer.MAX_VALUE;
    } else {
      batchSize = 1;
      batchSizeMax = Math.max(configuredMax, MINIMUM_BATCH_SIZE_MAX);
    }
  }

  private int progressBatchSize() {
    int previous = batchSize;
    batchSize = Math.min(batchSizeMax, 2 * batchSize);
    if (batchSize < previous) {
      batchSize = batchSizeMax;
    }
    return batchSize;
  }

  private void setBackoffState(long millis) {
    setTimeout(Math.toIntExact(millis));
    setState(ProcedureProtos.ProcedureState.WAITING_TIMEOUT);
    skipPersistence();
  }

  private List<RegionInfo> collectRegionsNeedingUpgrade(MasterProcedureEnv env) throws IOException {
    MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    FileSystem fs = mfs.getFileSystem();
    Path rootDir = mfs.getRootDir();

    List<RegionInfo> regionsToUpgrade = new ArrayList<>();
    Map<String, TableDescriptor> all = env.getMasterServices().getTableDescriptors().getAll();
    for (TableDescriptor td : all.values()) {
      if (!isFsft(td)) {
        continue;
      }
      TableName tableName = td.getTableName();
      Collection<RegionInfo> regions = env.getAssignmentManager().getRegionStates().getRegionsOfTable(tableName);
      for (RegionInfo region : regions) {
        Path regionDir = FSUtils.getRegionDirFromRootDir(rootDir, region);
        if (!fs.exists(regionDir)) {
          continue;
        }
        boolean needsUpgrade = false;
        for (Path familyDir : FSUtils.getFamilyDirs(fs, regionDir)) {
          if (FSFTVersionUpgradeUtil.familyDirNeedsUpgrade(fs, familyDir, TARGET_VERSION)) {
            needsUpgrade = true;
            break;
          }
        }
        if (needsUpgrade) {
          regionsToUpgrade.add(region);
        }
      }
    }
    return regionsToUpgrade;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, FSFTVersionMigrationState state)
    throws IOException, InterruptedException {
  }

  @Override
  protected FSFTVersionMigrationState getState(int stateId) {
    return FSFTVersionMigrationState.forNumber(stateId);
  }

  @Override
  protected int getStateId(FSFTVersionMigrationState state) {
    return state.getNumber();
  }

  @Override
  protected FSFTVersionMigrationState getInitialState() {
    return FSFTVersionMigrationState.FSFT_VERSION_MIGRATION_PREPARE;
  }

  @Override
  protected boolean waitInitialized(MasterProcedureEnv env) {
    return env.waitInitialized(this);
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    FSFTVersionMigrationStateData.Builder builder = FSFTVersionMigrationStateData.newBuilder()
      .setNextIndex(nextIndex)
      .setBatchSize(batchSize)
      .setBatchSizeMax(batchSizeMax)
      .setRegionsScheduled(regionsScheduled)
      .setBatchesProcessed(batchesProcessed)
      .setBatchBackoffMillis(batchBackoffMillis);
    regionsToUpgrade.stream().map(ProtobufUtil::toRegionInfo).forEachOrdered(builder::addRegionInfo);
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    FSFTVersionMigrationStateData data =
      serializer.deserialize(FSFTVersionMigrationStateData.class);
    regionsToUpgrade = data.getRegionInfoList().stream().map(ProtobufUtil::toRegionInfo)
      .collect(Collectors.toList());
    nextIndex = data.hasNextIndex() ? data.getNextIndex() : 0;
    batchSize = data.hasBatchSize() ? data.getBatchSize() : Integer.MAX_VALUE;
    batchSizeMax = data.hasBatchSizeMax() ? data.getBatchSizeMax() : Integer.MAX_VALUE;
    regionsScheduled = data.hasRegionsScheduled() ? data.getRegionsScheduled() : 0L;
    batchesProcessed = data.hasBatchesProcessed() ? data.getBatchesProcessed() : 0L;
    batchBackoffMillis = data.hasBatchBackoffMillis() ? data.getBatchBackoffMillis()
      : PROGRESSIVE_BATCH_BACKOFF_MILLIS_DEFAULT;
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }
}

