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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RepairFsftMode;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RepairFsftRegionState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RepairFsftRegionStateData;

/**
 * Online repair flow for a corrupted FILE store-file-tracker manifest.
 *
 * <p>
 * Used for user-table regions and {@code hbase:meta}. Not used for {@code master:store} —
 * the procedure store itself is master:store, so the procedure framework can't help when
 * its own backing region is corrupt; the offline {@code hbase sft --repair} CLI handles
 * that case.
 *
 * <p>
 * The procedure holds the region lock for its entire lifetime (inherited from
 * {@link AbstractStateMachineRegionProcedure}) and runs through the following states:
 *
 * <ol>
 * <li>{@code ENSURE_REGION_ABNORMALLY_CLOSED} — bypass any in-flight TRSP and stamp the
 * region's state in meta as {@code ABNORMALLY_CLOSED} so the next assign treats it as a
 * crash-recovery open.</li>
 * <li>{@code COMPUTE_NEW_MANIFEST} — invoke {@code StoreFileListRepair} (disk-only or
 * lineage-assisted) to derive the authoritative file set; persist the result on the
 * procedure so it survives a master failover.</li>
 * <li>{@code WRITE_NEW_MANIFEST} — write the recomputed {@code .filelist} entry under the
 * store directory (skipped on dry-run).</li>
 * <li>{@code SCHEDULE_REOPEN} — enqueue a child {@link TransitRegionStateProcedure} to
 * assign the region back online.</li>
 * <li>{@code WAIT_FOR_REOPEN} — wait for the child TRSP to finish before returning
 * {@code Flow.NO_MORE_STATE}.</li>
 * </ol>
 */
@InterfaceAudience.Private
public class RepairFsftRegionProcedure
  extends AbstractStateMachineRegionProcedure<RepairFsftRegionState> {

  private static final Logger LOG = LoggerFactory.getLogger(RepairFsftRegionProcedure.class);

  private byte[] family;
  private RepairFsftMode mode;
  private boolean dryRun;

  // Populated by COMPUTE_NEW_MANIFEST, consumed by WRITE_NEW_MANIFEST. Persisted in the
  // procedure state data so a master failover between COMPUTE and WRITE doesn't redo the
  // disk walk (and risk picking up a different file set if compactions sneak in).
  private List<byte[]> computedStoreFileNames = Collections.emptyList();
  private long maxSeqIdSeen = -1L;

  // Set after WRITE; lets resume short-circuit if the procedure crashes between WRITE and
  // SCHEDULE_REOPEN.
  private long writtenSeqId = -1L;

  public RepairFsftRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  public RepairFsftRegionProcedure(MasterProcedureEnv env, RegionInfo hri, byte[] family,
    RepairFsftMode mode, boolean dryRun) {
    super(env, hri);
    this.family = family;
    this.mode = mode;
    this.dryRun = dryRun;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_EDIT;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RepairFsftRegionState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    LOG.debug("{} execute state={}", this, state);
    try {
      switch (state) {
        case REPAIR_FSFT_ENSURE_REGION_ABNORMALLY_CLOSED:
          ensureRegionAbnormallyClosed(env);
          setNextState(RepairFsftRegionState.REPAIR_FSFT_COMPUTE_NEW_MANIFEST);
          return Flow.HAS_MORE_STATE;
        case REPAIR_FSFT_COMPUTE_NEW_MANIFEST:
          computeNewManifest(env);
          setNextState(RepairFsftRegionState.REPAIR_FSFT_WRITE_NEW_MANIFEST);
          return Flow.HAS_MORE_STATE;
        case REPAIR_FSFT_WRITE_NEW_MANIFEST:
          writeNewManifest(env);
          setNextState(RepairFsftRegionState.REPAIR_FSFT_SCHEDULE_REOPEN);
          return Flow.HAS_MORE_STATE;
        case REPAIR_FSFT_SCHEDULE_REOPEN:
          scheduleReopen(env);
          setNextState(RepairFsftRegionState.REPAIR_FSFT_WAIT_FOR_REOPEN);
          return Flow.HAS_MORE_STATE;
        case REPAIR_FSFT_WAIT_FOR_REOPEN:
          if (!isReopenComplete(env)) {
            // The child TRSP we scheduled in SCHEDULE_REOPEN handles its own waits; if we
            // got here while it's still in flight, suspend ourselves.
            throw new ProcedureSuspendedException();
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      // Repair is destructive in spirit (rewriting the manifest) — failures should bubble
      // up rather than retry blindly. Operator can re-run after diagnosing.
      setFailure("master-repair-fsft-region", e);
      return Flow.NO_MORE_STATE;
    }
  }

  /**
   * Bypass any stuck TRSP for the region and stamp meta with
   * {@code RegionState.State.ABNORMALLY_CLOSED} so the eventual reopen path runs as a
   * crash-recovery open.
   *
   * <p>
   * Implementation will:
   * <ul>
   * <li>Use {@code AssignmentManager.getRegionStates().getRegionStateNode(hri)} to acquire
   * the region's state node lock.</li>
   * <li>If the node has an in-flight procedure, mark it bypassed
   * ({@code ProcedureExecutor.bypassProcedure}).</li>
   * <li>Call {@code MetaTableAccessor.updateRegionState(...)} with
   * {@code RegionState.State.ABNORMALLY_CLOSED} (mirroring
   * {@code MasterRpcServices.setRegionStateInMeta}).</li>
   * <li>Refresh the in-memory state via
   * {@code AssignmentManager.populateRegionStatesFromMeta(hri)}.</li>
   * </ul>
   *
   * <p>
   * For {@code hbase:meta}, this updates the in-memory MetaRegionLocator entry rather than
   * meta itself; we'll handle that branch when fleshing this out.
   */
  private void ensureRegionAbnormallyClosed(MasterProcedureEnv env) throws IOException {
    // TODO(HBASE-30137): bypass stuck TRSP, stamp ABNORMALLY_CLOSED in meta, refresh AM.
  }

  /**
   * Run {@code StoreFileListRepair.repair(...)} against the region's store directory.
   *
   * <p>
   * Implementation will build a {@code StoreFileListRepair.Lineage} via
   * {@code StoreFileListFilePrettyPrinter.resolveLineage}-equivalent code (only when
   * {@link #mode} is {@code LINEAGE_ASSISTED}), invoke the repair library in
   * {@code dryRun=true} so nothing is written here, and capture the recomputed file list
   * + max seq id into {@link #computedStoreFileNames} / {@link #maxSeqIdSeen}.
   *
   * <p>
   * Persisting the recomputed list before WRITE means a failover between COMPUTE and WRITE
   * won't redo the disk walk on the new master (and risk seeing a different file set if a
   * compaction snuck in — which shouldn't happen with the region offline, but defence in
   * depth).
   */
  private void computeNewManifest(MasterProcedureEnv env) throws IOException {
    // TODO(HBASE-30137): call StoreFileListRepair.repair(dryRun=true) and stash the result.
  }

  /**
   * Write the recomputed manifest as a fresh {@code .filelist} entry under the store
   * directory.
   *
   * <p>
   * Skipped when {@link #dryRun} is true (still moves the procedure forward to
   * SCHEDULE_REOPEN, which is itself a no-op on dry-run; we don't want to leave the region
   * ABNORMALLY_CLOSED in dry-run mode either, so SCHEDULE_REOPEN reassigns regardless).
   *
   * <p>
   * Implementation will use {@code StoreFileListFile} (or a small writer helper exposed by
   * {@code StoreFileListRepair}) to materialise the file under the store's
   * {@code .filelist} directory using {@link #computedStoreFileNames} +
   * {@link #maxSeqIdSeen}+1 as the new sequence id, then store the chosen seq id in
   * {@link #writtenSeqId}.
   */
  private void writeNewManifest(MasterProcedureEnv env) throws IOException {
    // TODO(HBASE-30137): write the new manifest entry; record the seq id in writtenSeqId.
  }

  /**
   * Enqueue a child {@link TransitRegionStateProcedure} to assign the region.
   *
   * <p>
   * For user-table regions and {@code hbase:meta} we use
   * {@code env.getAssignmentManager().createOneAssignProcedure(getRegion(), true, true)}
   * (override + force) — same pattern that {@code TruncateRegionProcedure} uses to bring
   * the region back online after rewriting its filesystem.
   */
  private void scheduleReopen(MasterProcedureEnv env) throws IOException {
    // TODO(HBASE-30137): addChildProcedure(am.createOneAssignProcedure(getRegion(), true, true)).
  }

  /**
   * Returns true once the child TRSP scheduled in SCHEDULE_REOPEN has finished. The child
   * procedure handles its own retries and timeouts, so we just check the assignment state.
   */
  private boolean isReopenComplete(MasterProcedureEnv env) {
    // TODO(HBASE-30137): check RegionState.isOpened() (or equivalent) on the in-memory node.
    return true;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RepairFsftRegionState state)
    throws IOException, InterruptedException {
    // No rollback. Once we've stamped ABNORMALLY_CLOSED and rewritten the manifest, the
    // only forward direction is to finish the assign. A failure mid-flight leaves the
    // region offline; the operator can re-run the procedure or assign manually.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(RepairFsftRegionState state) {
    return false;
  }

  @Override
  protected RepairFsftRegionState getState(int stateId) {
    return RepairFsftRegionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RepairFsftRegionState state) {
    return state.getNumber();
  }

  @Override
  protected RepairFsftRegionState getInitialState() {
    return RepairFsftRegionState.REPAIR_FSFT_ENSURE_REGION_ABNORMALLY_CLOSED;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RepairFsftRegionStateData.Builder builder = RepairFsftRegionStateData.newBuilder()
      .setRegionInfo(
        org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toRegionInfo(getRegion()))
      .setFamily(ByteString.copyFrom(family)).setMode(mode).setDryRun(dryRun);
    for (byte[] name : computedStoreFileNames) {
      builder.addComputedStoreFileName(ByteString.copyFrom(name));
    }
    if (maxSeqIdSeen >= 0) {
      builder.setMaxSeqIdSeen(maxSeqIdSeen);
    }
    if (writtenSeqId >= 0) {
      builder.setWrittenSeqId(writtenSeqId);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RepairFsftRegionStateData data = serializer.deserialize(RepairFsftRegionStateData.class);
    setRegion(org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.toRegionInfo(data.getRegionInfo()));
    family = data.getFamily().toByteArray();
    mode = data.getMode();
    dryRun = data.getDryRun();
    if (data.getComputedStoreFileNameCount() > 0) {
      List<byte[]> names = new ArrayList<>(data.getComputedStoreFileNameCount());
      for (ByteString bs : data.getComputedStoreFileNameList()) {
        names.add(bs.toByteArray());
      }
      computedStoreFileNames = names;
    } else {
      computedStoreFileNames = Collections.emptyList();
    }
    maxSeqIdSeen = data.hasMaxSeqIdSeen() ? data.getMaxSeqIdSeen() : -1L;
    writtenSeqId = data.hasWrittenSeqId() ? data.getWrittenSeqId() : -1L;
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (region=").append(getRegion().getRegionNameAsString());
    sb.append(", family=").append(family == null ? "<null>"
      : new String(family, java.nio.charset.StandardCharsets.UTF_8));
    sb.append(", mode=").append(mode);
    sb.append(", dryRun=").append(dryRun);
    sb.append(")");
  }
}
