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
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.MergeRegionException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.CatalogJanitor;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.RegionStates;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.protobuf.generated.MasterProcedureProtos.DispatchMergingRegionsState;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * The procedure to Merge a region in a table.
 */
@InterfaceAudience.Private
public class DispatchMergingRegionsProcedure
    extends AbstractStateMachineTableProcedure<DispatchMergingRegionsState> {
  private static final Log LOG = LogFactory.getLog(DispatchMergingRegionsProcedure.class);

  private final AtomicBoolean aborted = new AtomicBoolean(false);
  private Boolean traceEnabled;
  private AssignmentManager assignmentManager;
  private int timeout;
  private ServerName regionLocation;
  private String regionsToMergeListFullName;
  private String regionsToMergeListEncodedName;

  private TableName tableName;
  private HRegionInfo [] regionsToMerge;
  private boolean forcible;

  public DispatchMergingRegionsProcedure() {
    this.traceEnabled = isTraceEnabled();
    this.assignmentManager = null;
    this.timeout = -1;
    this.regionLocation = null;
    this.regionsToMergeListFullName = null;
    this.regionsToMergeListEncodedName = null;
  }

  public DispatchMergingRegionsProcedure(
      final MasterProcedureEnv env,
      final TableName tableName,
      final HRegionInfo [] regionsToMerge,
      final boolean forcible) {
    super(env);
    this.traceEnabled = isTraceEnabled();
    this.assignmentManager = getAssignmentManager(env);
    this.tableName = tableName;
    // For now, we only merge 2 regions.  It could be extended to more than 2 regions in
    // the future.
    assert(regionsToMerge.length == 2);
    this.regionsToMerge = regionsToMerge;
    this.forcible = forcible;

    this.timeout = -1;
    this.regionsToMergeListFullName = getRegionsToMergeListFullNameString();
    this.regionsToMergeListEncodedName = getRegionsToMergeListEncodedNameString();
  }

  @Override
  protected Flow executeFromState(
      final MasterProcedureEnv env,
      final DispatchMergingRegionsState state) throws InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " execute state=" + state);
    }

    try {
      switch (state) {
      case DISPATCH_MERGING_REGIONS_PREPARE:
        prepareMergeRegion(env);
        setNextState(DispatchMergingRegionsState.DISPATCH_MERGING_REGIONS_PRE_OPERATION);
        break;
      case DISPATCH_MERGING_REGIONS_PRE_OPERATION:
        //Unused for now - reserve to add preMerge coprocessor in the future
        setNextState(DispatchMergingRegionsState.DISPATCH_MERGING_REGIONS_MOVE_REGION_TO_SAME_RS);
        break;
      case DISPATCH_MERGING_REGIONS_MOVE_REGION_TO_SAME_RS:
        if (MoveRegionsToSameRS(env)) {
          setNextState(DispatchMergingRegionsState.DISPATCH_MERGING_REGIONS_DO_MERGE_IN_RS);
        } else {
          LOG.info("Cancel merging regions " + getRegionsToMergeListFullNameString()
            + ", because can't move them to the same RS");
          setNextState(DispatchMergingRegionsState.DISPATCH_MERGING_REGIONS_POST_OPERATION);
        }
        break;
      case DISPATCH_MERGING_REGIONS_DO_MERGE_IN_RS:
        doMergeInRS(env);
        setNextState(DispatchMergingRegionsState.DISPATCH_MERGING_REGIONS_POST_OPERATION);
        break;
      case DISPATCH_MERGING_REGIONS_POST_OPERATION:
        //Unused for now - reserve to add postCompletedMerge coprocessor in the future
        return Flow.NO_MORE_STATE;
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (IOException e) {
      LOG.warn("Error trying to merge regions " + getRegionsToMergeListFullNameString() +
        " in the table " + tableName + " (in state=" + state + ")", e);

      setFailure("master-merge-regions", e);
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(
      final MasterProcedureEnv env,
      final DispatchMergingRegionsState state) throws IOException, InterruptedException {
    if (isTraceEnabled()) {
      LOG.trace(this + " rollback state=" + state);
    }

    try {
      switch (state) {
      case DISPATCH_MERGING_REGIONS_POST_OPERATION:
        break; // nothing to rollback
      case DISPATCH_MERGING_REGIONS_DO_MERGE_IN_RS:
        String msg = this + " We are in the " + state + " state."
            + " It is complicated to rollback the merge operation that region server is working on."
            + " Rollback is not supported and we should let the merge operation to complete";
        LOG.warn(msg);
        break;
      case DISPATCH_MERGING_REGIONS_MOVE_REGION_TO_SAME_RS:
        break; // nothing to rollback
      case DISPATCH_MERGING_REGIONS_PRE_OPERATION:
        break; // nothing to rollback
      case DISPATCH_MERGING_REGIONS_PREPARE:
        break; // nothing to rollback
      default:
        throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    } catch (Exception e) {
      // This will be retried. Unless there is a bug in the code,
      // this should be just a "temporary error" (e.g. network down)
      LOG.warn("Failed rollback attempt step " + state + " for merging the regions "
          + getRegionsToMergeListFullNameString() + " in table " + tableName, e);
      throw e;
    }
  }

  @Override
  protected DispatchMergingRegionsState getState(final int stateId) {
    return DispatchMergingRegionsState.valueOf(stateId);
  }

  @Override
  protected int getStateId(final DispatchMergingRegionsState state) {
    return state.getNumber();
  }

  @Override
  protected DispatchMergingRegionsState getInitialState() {
    return DispatchMergingRegionsState.DISPATCH_MERGING_REGIONS_PREPARE;
  }

  @Override
  protected void setNextState(DispatchMergingRegionsState state) {
    if (aborted.get()) {
      setAbortFailure("merge-table-regions", "abort requested");
    } else {
      super.setNextState(state);
    }
  }

  @Override
  public boolean abort(final MasterProcedureEnv env) {
    aborted.set(true);
    return true;
  }

  @Override
  public void serializeStateData(final OutputStream stream) throws IOException {
    super.serializeStateData(stream);

    MasterProcedureProtos.DispatchMergingRegionsStateData.Builder dispatchMergingRegionsMsg =
        MasterProcedureProtos.DispatchMergingRegionsStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setTableName(ProtobufUtil.toProtoTableName(tableName))
        .setForcible(forcible);
    for (HRegionInfo hri: regionsToMerge) {
      dispatchMergingRegionsMsg.addRegionInfo(HRegionInfo.convert(hri));
    }
    dispatchMergingRegionsMsg.build().writeDelimitedTo(stream);
  }

  @Override
  public void deserializeStateData(final InputStream stream) throws IOException {
    super.deserializeStateData(stream);

    MasterProcedureProtos.DispatchMergingRegionsStateData dispatchMergingRegionsMsg =
        MasterProcedureProtos.DispatchMergingRegionsStateData.parseDelimitedFrom(stream);
    setUser(MasterProcedureUtil.toUserInfo(dispatchMergingRegionsMsg.getUserInfo()));
    tableName = ProtobufUtil.toTableName(dispatchMergingRegionsMsg.getTableName());

    assert(dispatchMergingRegionsMsg.getRegionInfoCount() == 2);
    regionsToMerge = new HRegionInfo[dispatchMergingRegionsMsg.getRegionInfoCount()];
    for (int i = 0; i < regionsToMerge.length; i++) {
      regionsToMerge[i] = HRegionInfo.convert(dispatchMergingRegionsMsg.getRegionInfo(i));
    }
  }

  @Override
  public void toStringClassDetails(StringBuilder sb) {
    sb.append(getClass().getSimpleName());
    sb.append(" (table=");
    sb.append(tableName);
    sb.append(" regions=");
    sb.append(getRegionsToMergeListFullNameString());
    sb.append(" forcible=");
    sb.append(forcible);
    sb.append(")");
  }

  @Override
  protected boolean acquireLock(final MasterProcedureEnv env) {
    return !env.getProcedureQueue().waitRegions(
      this, getTableName(), regionsToMerge[0], regionsToMerge[1]);
  }

  @Override
  protected void releaseLock(final MasterProcedureEnv env) {
    env.getProcedureQueue().wakeRegions(this, getTableName(), regionsToMerge[0], regionsToMerge[1]);
  }

  @Override
  public TableName getTableName() {
    return tableName;
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.MERGE;
  }

  /**
   * Prepare merge and do some check
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void prepareMergeRegion(final MasterProcedureEnv env) throws IOException {
    // Note: the following logic assumes that we only have 2 regions to merge.  In the future,
    // if we want to extend to more than 2 regions, the code needs to modify a little bit.
    //
    CatalogJanitor catalogJanitor = env.getMasterServices().getCatalogJanitor();
    boolean regionAHasMergeQualifier = !catalogJanitor.cleanMergeQualifier(regionsToMerge[0]);
    if (regionAHasMergeQualifier
        || !catalogJanitor.cleanMergeQualifier(regionsToMerge[1])) {
      String msg = "Skip merging regions " + regionsToMerge[0].getRegionNameAsString()
          + ", " + regionsToMerge[1].getRegionNameAsString() + ", because region "
          + (regionAHasMergeQualifier ? regionsToMerge[0].getEncodedName() : regionsToMerge[1]
              .getEncodedName()) + " has merge qualifier";
      LOG.info(msg);
      throw new MergeRegionException(msg);
    }

      RegionStates regionStates = getAssignmentManager(env).getRegionStates();
      RegionState regionStateA = regionStates.getRegionState(regionsToMerge[0].getEncodedName());
      RegionState regionStateB = regionStates.getRegionState(regionsToMerge[1].getEncodedName());
      if (regionStateA == null || regionStateB == null) {
        throw new UnknownRegionException(
          regionStateA == null ?
              regionsToMerge[0].getEncodedName() : regionsToMerge[1].getEncodedName());
      }

      if (!regionStateA.isOpened() || !regionStateB.isOpened()) {
        throw new MergeRegionException(
          "Unable to merge regions not online " + regionStateA + ", " + regionStateB);
      }

      if (regionsToMerge[0].getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID ||
          regionsToMerge[1].getReplicaId() != HRegionInfo.DEFAULT_REPLICA_ID) {
        throw new MergeRegionException("Can't merge non-default replicas");
      }

      if (!forcible && !HRegionInfo.areAdjacent(regionsToMerge[0], regionsToMerge[1])) {
        throw new MergeRegionException(
          "Unable to merge not adjacent regions "
            + regionsToMerge[0].getRegionNameAsString() + ", "
            + regionsToMerge[1].getRegionNameAsString()
            + " where forcible = " + forcible);
      }
  }

  /**
   * Move all regions to the same region server
   * @param env MasterProcedureEnv
   * @return whether target regions hosted by the same RS
   * @throws IOException
   */
  private boolean MoveRegionsToSameRS(final MasterProcedureEnv env) throws IOException {
    // Make sure regions are on the same regionserver before send merge
    // regions request to region server.
    //
    boolean onSameRS = isRegionsOnTheSameServer(env);
    if (!onSameRS) {
      // Note: the following logic assumes that we only have 2 regions to merge.  In the future,
      // if we want to extend to more than 2 regions, the code needs to modify a little bit.
      //
      RegionStates regionStates = getAssignmentManager(env).getRegionStates();
      ServerName regionLocation2 = regionStates.getRegionServerOfRegion(regionsToMerge[1]);

      RegionLoad loadOfRegionA = getRegionLoad(env, regionLocation, regionsToMerge[0]);
      RegionLoad loadOfRegionB = getRegionLoad(env, regionLocation2, regionsToMerge[1]);
      if (loadOfRegionA != null && loadOfRegionB != null
          && loadOfRegionA.getRequestsCount() < loadOfRegionB.getRequestsCount()) {
        // switch regionsToMerge[0] and regionsToMerge[1]
        HRegionInfo tmpRegion = this.regionsToMerge[0];
        this.regionsToMerge[0] = this.regionsToMerge[1];
        this.regionsToMerge[1] = tmpRegion;
        ServerName tmpLocation = regionLocation;
        regionLocation = regionLocation2;
        regionLocation2 = tmpLocation;
      }

      long startTime = EnvironmentEdgeManager.currentTime();

      RegionPlan regionPlan = new RegionPlan(regionsToMerge[1], regionLocation2, regionLocation);
      LOG.info("Moving regions to same server for merge: " + regionPlan.toString());
      getAssignmentManager(env).balance(regionPlan);
      do {
        try {
          Thread.sleep(20);
          // Make sure check RIT first, then get region location, otherwise
          // we would make a wrong result if region is online between getting
          // region location and checking RIT
          boolean isRIT = regionStates.isRegionInTransition(regionsToMerge[1]);
          regionLocation2 = regionStates.getRegionServerOfRegion(regionsToMerge[1]);
          onSameRS = regionLocation.equals(regionLocation2);
          if (onSameRS || !isRIT) {
            // Regions are on the same RS, or regionsToMerge[1] is not in
            // RegionInTransition any more
            break;
          }
        } catch (InterruptedException e) {
          InterruptedIOException iioe = new InterruptedIOException();
          iioe.initCause(e);
          throw iioe;
        }
      } while ((EnvironmentEdgeManager.currentTime() - startTime) <= getTimeout(env));
    }
    return onSameRS;
  }

  /**
   * Do the real merge operation in the region server that hosts regions
   * @param env MasterProcedureEnv
   * @throws IOException
   */
  private void doMergeInRS(final MasterProcedureEnv env) throws IOException {
    long duration = 0;
    long startTime = EnvironmentEdgeManager.currentTime();
    do {
      try {
        if (getServerName(env) == null) {
          // The merge probably already happen. Check
          RegionState regionState = getAssignmentManager(env).getRegionStates().getRegionState(
            regionsToMerge[0].getEncodedName());
          if (regionState.isMerging() || regionState.isMerged()) {
            LOG.info("Merge regions " +  getRegionsToMergeListEncodedNameString() +
              " is in progress or completed.  No need to send a new request.");
          } else {
            LOG.warn("Cannot sending merge to hosting server of the regions " +
              getRegionsToMergeListEncodedNameString() + " as the server is unknown");
          }
          return;
        }
        // TODO: the following RPC call is not idempotent.  Multiple calls (eg. after master
        // failover, re-execute this step) could result in some exception thrown that does not
        // paint the correct picture.  This behavior is on-par with old releases.  Improvement
        // could happen in the future.
        env.getMasterServices().getServerManager().sendRegionsMerge(
          getServerName(env),
          regionsToMerge[0],
          regionsToMerge[1],
          forcible,
          getUser());
        LOG.info("Sent merge to server " + getServerName(env) + " for region " +
            getRegionsToMergeListEncodedNameString() + ", focible=" + forcible);
        return;
      } catch (RegionOpeningException roe) {
        // Do a retry since region should be online on RS immediately
        LOG.warn("Failed mergering regions in " + getServerName(env) + ", retrying...", roe);
      } catch (Exception ie) {
        LOG.warn("Failed sending merge to " + getServerName(env) + " for regions " +
            getRegionsToMergeListEncodedNameString() + ", focible=" + forcible, ie);
        return;
      }
    } while ((duration = EnvironmentEdgeManager.currentTime() - startTime) <= getTimeout(env));

    // If we reaches here, it means that we get timed out.
    String msg = "Failed sending merge to " + getServerName(env) + " after " + duration + "ms";
    LOG.warn(msg);
    throw new IOException(msg);
  }

  private RegionLoad getRegionLoad(
      final MasterProcedureEnv env,
      final ServerName sn,
      final HRegionInfo hri) {
    ServerManager serverManager =  env.getMasterServices().getServerManager();
    ServerLoad load = serverManager.getLoad(sn);
    if (load != null) {
      Map<byte[], RegionLoad> regionsLoad = load.getRegionsLoad();
      if (regionsLoad != null) {
        return regionsLoad.get(hri.getRegionName());
      }
    }
    return null;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return whether target regions hosted by the same RS
   */
  private boolean isRegionsOnTheSameServer(final MasterProcedureEnv env) throws IOException{
    Boolean onSameRS = true;
    int i = 0;
    RegionStates regionStates = getAssignmentManager(env).getRegionStates();
    regionLocation = regionStates.getRegionServerOfRegion(regionsToMerge[i]);
    if (regionLocation != null) {
      for(i = 1; i < regionsToMerge.length; i++) {
        ServerName regionLocation2 = regionStates.getRegionServerOfRegion(regionsToMerge[i]);
        if (regionLocation2 != null) {
          if (onSameRS) {
            onSameRS = regionLocation.equals(regionLocation2);
          }
        } else {
          // At least one region is not online, merge will fail, no need to continue.
          break;
        }
      }
      if (i == regionsToMerge.length) {
        // Finish checking all regions, return the result;
        return onSameRS;
      }
    }

    // If reaching here, at least one region is not online.
    String msg = "Skip merging regions " + getRegionsToMergeListFullNameString() +
        ", because region " + regionsToMerge[i].getEncodedName() + " is not online now.";
    LOG.warn(msg);
    throw new IOException(msg);
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return assignmentManager
   */
  private AssignmentManager getAssignmentManager(final MasterProcedureEnv env) {
    if (assignmentManager == null) {
      assignmentManager = env.getMasterServices().getAssignmentManager();
    }
    return assignmentManager;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return timeout value
   */
  private int getTimeout(final MasterProcedureEnv env) {
    if (timeout == -1) {
      timeout = env.getMasterConfiguration().getInt(
        "hbase.master.regionmerge.timeout", regionsToMerge.length * 60 * 1000);
    }
    return timeout;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param env MasterProcedureEnv
   * @return serverName
   */
  private ServerName getServerName(final MasterProcedureEnv env) {
    if (regionLocation == null) {
      regionLocation =
          getAssignmentManager(env).getRegionStates().getRegionServerOfRegion(regionsToMerge[0]);
    }
    return regionLocation;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @param fullName whether return only encoded name
   * @return region names in a list
   */
  private String getRegionsToMergeListFullNameString() {
    if (regionsToMergeListFullName == null) {
      StringBuilder sb = new StringBuilder("[");
      int i = 0;
      while(i < regionsToMerge.length - 1) {
        sb.append(regionsToMerge[i].getRegionNameAsString() + ", ");
        i++;
      }
      sb.append(regionsToMerge[i].getRegionNameAsString() + " ]");
      regionsToMergeListFullName = sb.toString();
    }
    return regionsToMergeListFullName;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return encoded region names
   */
  private String getRegionsToMergeListEncodedNameString() {
    if (regionsToMergeListEncodedName == null) {
      StringBuilder sb = new StringBuilder("[");
      int i = 0;
      while(i < regionsToMerge.length - 1) {
        sb.append(regionsToMerge[i].getEncodedName() + ", ");
        i++;
      }
      sb.append(regionsToMerge[i].getEncodedName() + " ]");
      regionsToMergeListEncodedName = sb.toString();
    }
    return regionsToMergeListEncodedName;
  }

  /**
   * The procedure could be restarted from a different machine. If the variable is null, we need to
   * retrieve it.
   * @return traceEnabled
   */
  private Boolean isTraceEnabled() {
    if (traceEnabled == null) {
      traceEnabled = LOG.isTraceEnabled();
    }
    return traceEnabled;
  }
}
