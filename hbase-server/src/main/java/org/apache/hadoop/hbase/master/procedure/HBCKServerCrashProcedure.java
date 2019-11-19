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

import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SCP that differs from default only in how it gets the list of
 * Regions hosted on the crashed-server; it also reads hbase:meta directly rather
 * than rely solely on Master memory for list of Regions that were on crashed server.
 * This version of SCP is for external invocation as part of fix-up (e.g. HBCK2's
 * scheduleRecoveries). It is for the case where meta has references to 'Unknown Servers',
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
   * Adds Regions found by super method any found scanning hbase:meta.
   */
  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="NP_NULL_ON_SOME_PATH_EXCEPTION",
    justification="FindBugs seems confused on ps in below.")
  List<RegionInfo> getRegionsOnCrashedServer(MasterProcedureEnv env) {
    // Super can return immutable emptyList.
    List<RegionInfo> ris = super.getRegionsOnCrashedServer(env);
    List<Pair<RegionInfo, ServerName>> ps = null;
    try {
      ps = MetaTableAccessor.getTableRegionsAndLocations(env.getMasterServices().getConnection(),
              null, false);
    } catch (IOException ioe) {
      LOG.warn("Failed get of all regions; continuing", ioe);
    }
    if (ps == null || ps.isEmpty()) {
      LOG.warn("No regions found in hbase:meta");
      return ris;
    }
    List<RegionInfo> aggregate = ris == null || ris.isEmpty()?
        new ArrayList<>(): new ArrayList<>(ris);
    int before = aggregate.size();
    ps.stream().filter(p -> p.getSecond() != null && p.getSecond().equals(getServerName())).
        forEach(p -> aggregate.add(p.getFirst()));
    LOG.info("Found {} mentions of {} in hbase:meta", aggregate.size() - before, getServerName());
    return aggregate;
  }
}
