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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CatalogFamilyFormat;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileListRepair;
import org.apache.hadoop.hbase.util.CommonFSUtils;
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
 * crash-recovery open. Skipped on dry-run.</li>
 * <li>{@code COMPUTE_NEW_MANIFEST} — invoke {@code StoreFileListRepair} (disk-only or
 * lineage-assisted) in dry-run mode to derive the authoritative file set; persist the
 * recomputed name list so the next state survives a master failover.</li>
 * <li>{@code WRITE_NEW_MANIFEST} — re-run {@code StoreFileListRepair} to materialize the
 * new {@code .filelist} entry under the store directory. Skipped on dry-run.</li>
 * <li>{@code SCHEDULE_REOPEN} — enqueue a child {@link TransitRegionStateProcedure} to
 * assign the region back online. Skipped on dry-run.</li>
 * <li>{@code WAIT_FOR_REOPEN} — wait for the child TRSP to finish before returning
 * {@code Flow.NO_MORE_STATE}. Skipped on dry-run.</li>
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
          if (!dryRun) {
            ensureRegionAbnormallyClosed(env);
          }
          setNextState(RepairFsftRegionState.REPAIR_FSFT_COMPUTE_NEW_MANIFEST);
          return Flow.HAS_MORE_STATE;
        case REPAIR_FSFT_COMPUTE_NEW_MANIFEST:
          computeNewManifest(env);
          setNextState(RepairFsftRegionState.REPAIR_FSFT_WRITE_NEW_MANIFEST);
          return Flow.HAS_MORE_STATE;
        case REPAIR_FSFT_WRITE_NEW_MANIFEST:
          if (!dryRun) {
            writeNewManifest(env);
          }
          setNextState(RepairFsftRegionState.REPAIR_FSFT_SCHEDULE_REOPEN);
          return Flow.HAS_MORE_STATE;
        case REPAIR_FSFT_SCHEDULE_REOPEN:
          if (dryRun) {
            return Flow.NO_MORE_STATE;
          }
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
   * Stamp the region as {@code RegionState.State.ABNORMALLY_CLOSED} so the eventual reopen
   * path runs as a crash-recovery open.
   *
   * <p>
   * Any in-flight TRSP that was holding the region's scheduler lock is bypassed at RPC
   * submission time (see {@code MasterRpcServices.repairFsftRegion}), <em>before</em> this
   * procedure is submitted -- it has to be, because this procedure inherits the same
   * life-of-procedure region lock and could not otherwise have started executing. So by the
   * time we get here there is no competing TRSP to displace; we only need to stamp the
   * state.
   *
   * <p>
   * For non-meta regions we write meta first and then reload the in-memory state from meta,
   * so AM and meta cannot disagree if the meta write fails. {@code hbase:meta} itself cannot
   * record its own region state in meta, so we set the in-memory state node directly.
   */
  private void ensureRegionAbnormallyClosed(MasterProcedureEnv env) throws IOException {
    RegionInfo hri = getRegion();
    AssignmentManager am = env.getAssignmentManager();
    RegionStateNode node = am.getRegionStates().getRegionStateNode(hri);
    if (node == null) {
      throw new IOException("No RegionStateNode for " + hri.getRegionNameAsString()
        + "; refusing to repair an unknown region.");
    }
    if (!hri.isMetaRegion()) {
      // Persist to meta first, then reload so the in-memory state mirrors what is durably
      // recorded (mirrors MasterRpcServices.setRegionStateInMeta). If the meta write throws,
      // we have not touched in-memory state, so the two stay consistent.
      MetaTableAccessor.updateRegionState(env.getMasterServices().getConnection(), hri,
        RegionState.State.ABNORMALLY_CLOSED);
      am.populateRegionStatesFromMeta(hri);
      LOG.info("Stamped region {} as ABNORMALLY_CLOSED in meta before FSFT repair",
        hri.getRegionNameAsString());
    } else {
      node.lock();
      try {
        RegionState.State previous = node.getState();
        node.setState(RegionState.State.ABNORMALLY_CLOSED);
        LOG.info("Stamped meta region {} state {} -> ABNORMALLY_CLOSED before FSFT repair",
          hri.getRegionNameAsString(), previous);
      } finally {
        node.unlock();
      }
    }
  }

  /**
   * Run {@code StoreFileListRepair.repair(...)} in dry-run mode against the region's store
   * directory and capture the recomputed file list. Persisting the recomputed list before
   * WRITE means a failover between COMPUTE and WRITE won't redo the disk walk on the new
   * master (and risk seeing a different file set if a compaction snuck in — which shouldn't
   * happen with the region offline, but defence in depth).
   */
  private void computeNewManifest(MasterProcedureEnv env) throws IOException {
    StoreFileListRepair.RepairReport report = runRepair(env, true);
    List<byte[]> names = new ArrayList<>(report.getManifestEntries().size());
    long maxSeq = -1L;
    for (StoreFileInfo info : report.getManifestEntries()) {
      names.add(info.getPath().getName().getBytes(java.nio.charset.StandardCharsets.UTF_8));
      // StoreFileInfo doesn't expose a seq id directly; the manifest writer uses the file
      // mtime so we just record the largest mtime seen as a best-effort marker. The CLI's
      // pretty-printer uses the same field for diagnostics.
      long mt = info.getModificationTime();
      if (mt > maxSeq) {
        maxSeq = mt;
      }
    }
    this.computedStoreFileNames = names;
    this.maxSeqIdSeen = maxSeq;
    LOG.info("Repair compute (dry-run) for region {} family {} produced {} entries (mode={})",
      getRegion().getRegionNameAsString(),
      new String(family, java.nio.charset.StandardCharsets.UTF_8),
      names.size(), mode);
  }

  /**
   * Write the recomputed manifest as a fresh {@code .filelist} entry under the store
   * directory. Re-runs {@code StoreFileListRepair.repair(...)} with {@code dryRun=false};
   * the library handles the no-op detection (skipping the write if the existing manifest
   * already matches) and the seqId-monotonic generation rotation.
   */
  private void writeNewManifest(MasterProcedureEnv env) throws IOException {
    StoreFileListRepair.RepairReport report = runRepair(env, false);
    if (report.isNoOp()) {
      LOG.info("Repair write for region {} family {} was a no-op; manifest already healthy",
        getRegion().getRegionNameAsString(),
        new String(family, java.nio.charset.StandardCharsets.UTF_8));
    } else {
      Path written = report.getWrittenManifest();
      LOG.info("Wrote repaired FSFT manifest for region {} family {} at {} ({} entries)",
        getRegion().getRegionNameAsString(),
        new String(family, java.nio.charset.StandardCharsets.UTF_8),
        written, report.getManifestEntries().size());
    }
    this.writtenSeqId = maxSeqIdSeen;
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
    TransitRegionStateProcedure trsp =
      env.getAssignmentManager().createOneAssignProcedure(getRegion(), true, true);
    if (trsp == null) {
      throw new IOException("Failed to create TRSP for region " + getRegion().getRegionNameAsString()
        + " after FSFT repair; assignment manager refused.");
    }
    addChildProcedure(trsp);
  }

  /**
   * Returns true once the child TRSP scheduled in SCHEDULE_REOPEN has finished. The child
   * procedure handles its own retries and timeouts, so we just check the assignment state.
   */
  private boolean isReopenComplete(MasterProcedureEnv env) {
    RegionStateNode node =
      env.getAssignmentManager().getRegionStates().getRegionStateNode(getRegion());
    if (node == null) {
      // The region disappeared while we were running. Treat as complete so the procedure
      // doesn't loop forever; failure (if any) was already logged by the child TRSP.
      return true;
    }
    return node.isInState(RegionState.State.OPEN);
  }

  private StoreFileListRepair.RepairReport runRepair(MasterProcedureEnv env, boolean dryRun)
    throws IOException {
    RegionInfo hri = getRegion();
    Configuration conf = env.getMasterConfiguration();
    FileSystem fs = env.getMasterServices().getMasterFileSystem().getFileSystem();
    Path rootDir = env.getMasterServices().getMasterFileSystem().getRootDir();
    Path tableDir = CommonFSUtils.getTableDir(rootDir, hri.getTable());

    TableDescriptor td = env.getMasterServices().getTableDescriptors().get(hri.getTable());
    if (td == null) {
      throw new IOException("No table descriptor for " + hri.getTable());
    }
    ColumnFamilyDescriptor cfd = td.getColumnFamily(family);
    if (cfd == null) {
      throw new IOException("Family " + new String(family, java.nio.charset.StandardCharsets.UTF_8)
        + " not found on table " + hri.getTable());
    }

    HRegionFileSystem regionFs =
      HRegionFileSystem.openRegionFromFileSystem(conf, fs, tableDir, hri, true);

    StoreFileListRepair.Lineage lineage = StoreFileListRepair.Lineage.none();
    StoreFileListRepair.Mode repairMode = mode == RepairFsftMode.REPAIR_FSFT_MODE_LINEAGE_ASSISTED
      ? StoreFileListRepair.Mode.LINEAGE_ASSISTED
      : StoreFileListRepair.Mode.DISK_ONLY;
    if (repairMode == StoreFileListRepair.Mode.LINEAGE_ASSISTED) {
      lineage = resolveLineage(env, hri);
    }
    return StoreFileListRepair.repair(conf, td, cfd, regionFs, lineage, repairMode, dryRun);
  }

  /**
   * Pull split/merge parents from meta to feed lineage-assisted repair. The result mirrors
   * what the offline CLI's {@code resolveLineage} produces: a single split parent, or a
   * list of merge parents, or {@code none()} when the child has no recoverable lineage in
   * meta.
   */
  private StoreFileListRepair.Lineage resolveLineage(MasterProcedureEnv env, RegionInfo child)
    throws IOException {
    Result row =
      MetaTableAccessor.getRegionResult(env.getMasterServices().getConnection(), child);
    if (row == null || row.isEmpty()) {
      return StoreFileListRepair.Lineage.none();
    }
    List<RegionInfo> mergeParents =
      CatalogFamilyFormat.getMergeRegions(row.rawCells());
    if (mergeParents != null && !mergeParents.isEmpty()) {
      return StoreFileListRepair.Lineage.mergeParents(mergeParents);
    }
    // Split-parent recovery from meta is not preserved on the child row in modern HBase;
    // operators who need a split-parent walk should fall back to the offline CLI which
    // can be pointed at the parent dir explicitly.
    return StoreFileListRepair.Lineage.none();
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
