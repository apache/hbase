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
package org.apache.hadoop.hbase.master.assignment;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.AbstractStateMachineRegionProcedure;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.GCRegionState;

/**
 * GC a Region that is no longer in use. It has been split or merged away.
 * Caller determines if it is GC time. This Procedure does not check.
 * <p>This is a Region StateMachine Procedure. We take a read lock on the Table and then
 * exclusive on the Region.
 */
@InterfaceAudience.Private
public class GCRegionProcedure extends AbstractStateMachineRegionProcedure<GCRegionState> {
  private static final Logger LOG = LoggerFactory.getLogger(GCRegionProcedure.class);

  public GCRegionProcedure(final MasterProcedureEnv env, final RegionInfo hri) {
    super(env, hri);
  }

  public GCRegionProcedure() {
    // Required by the Procedure framework to create the procedure on replay
    super();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.REGION_GC;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, GCRegionState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }
    MasterServices masterServices = env.getMasterServices();
    try {
      switch (state) {
        case GC_REGION_PREPARE:
          // Nothing to do to prepare.
          setNextState(GCRegionState.GC_REGION_ARCHIVE);
          break;
        case GC_REGION_ARCHIVE:
          MasterFileSystem mfs = masterServices.getMasterFileSystem();
          FileSystem fs = mfs.getFileSystem();
          if (HFileArchiver.exists(masterServices.getConfiguration(), fs, getRegion())) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Archiving region=" + getRegion().getShortNameToLog());
            }
            HFileArchiver.archiveRegion(masterServices.getConfiguration(), fs, getRegion());
          }
          FileSystem walFs = mfs.getWALFileSystem();
          // Cleanup the directories on WAL filesystem also
          Path regionWALDir = CommonFSUtils.getWALRegionDir(env.getMasterConfiguration(),
            getRegion().getTable(), getRegion().getEncodedName());
          if (walFs.exists(regionWALDir)) {
            if (!walFs.delete(regionWALDir, true)) {
              LOG.debug("Failed to delete {}", regionWALDir);
            }
          }
          Path wrongRegionWALDir = CommonFSUtils.getWrongWALRegionDir(env.getMasterConfiguration(),
            getRegion().getTable(), getRegion().getEncodedName());
          if (walFs.exists(wrongRegionWALDir)) {
            if (!walFs.delete(wrongRegionWALDir, true)) {
              LOG.debug("Failed to delete {}", regionWALDir);
            }
          }
          setNextState(GCRegionState.GC_REGION_PURGE_METADATA);
          break;
        case GC_REGION_PURGE_METADATA:
          // TODO: Purge metadata before removing from HDFS? This ordering is copied
          // from CatalogJanitor.
          AssignmentManager am = masterServices.getAssignmentManager();
          if (am != null) {
            if (am.getRegionStates() != null) {
              am.getRegionStates().deleteRegion(getRegion());
            }
          }
          MetaTableAccessor.deleteRegionInfo(masterServices.getConnection(), getRegion());
          masterServices.getServerManager().removeRegion(getRegion());
          FavoredNodesManager fnm = masterServices.getFavoredNodesManager();
          if (fnm != null) {
            fnm.deleteFavoredNodesForRegions(Lists.newArrayList(getRegion()));
          }
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException ioe) {
      // TODO: This is going to spew log? Add retry backoff
      LOG.warn("Error trying to GC " + getRegion().getShortNameToLog() + "; retrying...", ioe);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, GCRegionState state) throws IOException, InterruptedException {
    // no-op
  }

  @Override
  protected GCRegionState getState(int stateId) {
    return GCRegionState.forNumber(stateId);
  }

  @Override
  protected int getStateId(GCRegionState state) {
    return state.getNumber();
  }

  @Override
  protected GCRegionState getInitialState() {
    return GCRegionState.GC_REGION_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.serializeStateData(serializer);
    // Double serialization of regionname. Superclass is also serializing. Fix.
    final MasterProcedureProtos.GCRegionStateData.Builder msg =
        MasterProcedureProtos.GCRegionStateData.newBuilder()
        .setRegionInfo(ProtobufUtil.toRegionInfo(getRegion()));
    serializer.serialize(msg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer)
      throws IOException {
    super.deserializeStateData(serializer);
    final MasterProcedureProtos.GCRegionStateData msg =
        serializer.deserialize(MasterProcedureProtos.GCRegionStateData.class);
    setRegion(ProtobufUtil.toRegionInfo(msg.getRegionInfo()));
  }
}
