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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.assignment.RegionStateNode;
import org.apache.hadoop.hbase.master.assignment.RegionStateStore;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Acts like the super class in all cases except when no Regions found in the
 * current Master in-memory context. In this latter case, when the call to
 * super#getRegionsOnCrashedServer returns nothing, this SCP will scan
 * hbase:meta for references to the passed ServerName. If any found, we'll
 * clean them up.
 *
 * <p>This version of SCP is for external invocation as part of fix-up (e.g. HBCK2's
 * scheduleRecoveries); the super class is used during normal recovery operations.
 * It is for the case where meta has references to 'Unknown Servers',
 * servers that are in hbase:meta but not in live-server or dead-server lists; i.e. Master
 * and hbase:meta content have deviated. It should never happen in normal running
 * cluster but if we do drop accounting of servers, we need a means of fix-up.
 * Eventually, as part of normal CatalogJanitor task, rather than just identify
 * these 'Unknown Servers', it would make repair, queuing something like this
 * HBCKSCP to do cleanup, reassigning them so Master and hbase:meta are aligned again.
 *
 * <p>NOTE that this SCP is costly to run; does a full scan of hbase:meta.</p>
 */
@InterfaceAudience.Private
public class HBCKServerCrashProcedure extends ServerCrashProcedure {
  private static final Logger LOG = LoggerFactory.getLogger(HBCKServerCrashProcedure.class);

  /**
   * @param serverName Name of the crashed server.
   * @param shouldSplitWal True if we should split WALs as part of crashed server processing.
   * @param carryingMeta True if carrying hbase:meta table region.
   */
  public HBCKServerCrashProcedure(final MasterProcedureEnv env, final ServerName serverName,
                              final boolean shouldSplitWal, final boolean carryingMeta) {
    super(env, serverName, shouldSplitWal, carryingMeta);
  }

  /**
   * Used when deserializing from a procedure store; we'll construct one of these then call
   * #deserializeStateData(InputStream). Do not use directly.
   */
  public HBCKServerCrashProcedure() {}

  /**
   * If no Regions found in Master context, then we will search hbase:meta for references
   * to the passed server. Operator may have passed ServerName because they have found
   * references to 'Unknown Servers'. They are using HBCKSCP to clear them out.
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH_EXCEPTION",
    justification="FindBugs seems confused on ps in below.")
  List<RegionInfo> getRegionsOnCrashedServer(MasterProcedureEnv env) {
    // Super will return an immutable list (empty if nothing on this server).
    List<RegionInfo> ris = super.getRegionsOnCrashedServer(env);
    if (!ris.isEmpty()) {
      return ris;
    }
    // Nothing in in-master context. Check for Unknown Server! in hbase:meta.
    // If super list is empty, then allow that an operator scheduled an SCP because they are trying
    // to purge 'Unknown Servers' -- servers that are neither online nor in dead servers
    // list but that ARE in hbase:meta and so showing as unknown in places like 'HBCK Report'.
    // This mis-accounting does not happen in normal circumstance but may arise in-extremis
    // when cluster has been damaged in operation.
    UnknownServerVisitor visitor =
        new UnknownServerVisitor(env.getMasterServices().getConnection(), getServerName());
    try {
      MetaTableAccessor.scanMetaForTableRegions(env.getMasterServices().getConnection(),
          visitor, null);
    } catch (IOException ioe) {
      LOG.warn("Failed scan of hbase:meta for 'Unknown Servers'", ioe);
      return ris;
    }
    LOG.info("Found {} mentions of {} in hbase:meta of OPEN/OPENING Regions: {}",
        visitor.getReassigns().size(), getServerName(),
        visitor.getReassigns().stream().map(RegionInfo::getEncodedName).
            collect(Collectors.joining(",")));
    return visitor.getReassigns();
  }

  /**
   * Visitor for hbase:meta that 'fixes' Unknown Server issues. Collects
   * a List of Regions to reassign as 'result'.
   */
  private static class UnknownServerVisitor implements MetaTableAccessor.Visitor {
    private final List<RegionInfo> reassigns = new ArrayList<>();
    private final ServerName unknownServerName;
    private final Connection connection;

    private UnknownServerVisitor(Connection connection, ServerName unknownServerName) {
      this.connection = connection;
      this.unknownServerName = unknownServerName;
    }

    @Override
    public boolean visit(Result result) throws IOException {
      RegionLocations rls = MetaTableAccessor.getRegionLocations(result);
      if (rls == null) {
        return true;
      }
      for (HRegionLocation hrl: rls.getRegionLocations()) {
        if (hrl == null) {
          continue;
        }
        if (hrl.getRegion() == null) {
          continue;
        }
        if (hrl.getServerName() == null) {
          continue;
        }
        if (!hrl.getServerName().equals(this.unknownServerName)) {
          continue;
        }
        RegionState.State state = RegionStateStore.getRegionState(result, hrl.getRegion());
        RegionState rs = new RegionState(hrl.getRegion(), state, hrl.getServerName());
        if (rs.isClosing()) {
          // Move region to CLOSED in hbase:meta.
          LOG.info("Moving {} from CLOSING to CLOSED in hbase:meta",
              hrl.getRegion().getRegionNameAsString());
          try {
            MetaTableAccessor.updateRegionState(this.connection, hrl.getRegion(),
                RegionState.State.CLOSED);
          } catch (IOException ioe) {
            LOG.warn("Failed moving {} from CLOSING to CLOSED",
              hrl.getRegion().getRegionNameAsString(), ioe);
          }
        } else if (rs.isOpening() || rs.isOpened()) {
          this.reassigns.add(hrl.getRegion());
        } else {
          LOG.info("Passing {}", rs);
        }
      }
      return true;
    }

    private List<RegionInfo> getReassigns() {
      return this.reassigns;
    }
  }

  /**
   * The RegionStateNode will not have a location if a confirm of an OPEN fails. On fail,
   * the RegionStateNode regionLocation is set to null. This is 'looser' than the test done
   * in the superclass. The HBCKSCP has been scheduled by an operator via hbck2 probably at the
   * behest of a report of an 'Unknown Server' in the 'HBCK Report'. Let the operators operation
   * succeed even in case where the region location in the RegionStateNode is null.
   */
  @Override
  protected boolean isMatchingRegionLocation(RegionStateNode rsn) {
    return super.isMatchingRegionLocation(rsn) || rsn.getRegionLocation() == null;
  }
}
