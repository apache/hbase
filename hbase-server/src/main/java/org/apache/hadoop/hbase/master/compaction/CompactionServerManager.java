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
package org.apache.hadoop.hbase.master.compaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.SwitchCompactionOffloadProcedure;
import org.apache.hadoop.hbase.regionserver.CompactionOffloadSwitchStorage;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCompactionOffloadEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCompactionOffloadEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchCompactionOffloadRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchCompactionOffloadResponse;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;

@InterfaceAudience.Private
public class CompactionServerManager {
  private final MasterServices masterServices;
  /** Map of registered servers to their current load */
  private final Cache<ServerName, ServerMetrics> onlineServers;
  private CompactionOffloadSwitchStorage compactionOffloadSwitchStorage;
  private static final Logger LOG = LoggerFactory.getLogger(CompactionServerManager.class.getName());

  public CompactionServerManager(final MasterServices master) {
    this.masterServices = master;
    int compactionServerMsgInterval =
        master.getConfiguration().getInt(HConstants.COMPACTION_SERVER_MSG_INTERVAL, 3 * 1000);
    int compactionServerExpiredFactor =
        master.getConfiguration().getInt("hbase.compaction.server.expired.factor", 2);
    this.onlineServers = CacheBuilder.newBuilder().expireAfterWrite(
      compactionServerMsgInterval * compactionServerExpiredFactor, TimeUnit.MILLISECONDS).build();
    this.compactionOffloadSwitchStorage =
      new CompactionOffloadSwitchStorage(masterServices.getZooKeeper(), masterServices.getConfiguration());
  }

  public void compactionServerReport(ServerName sn, ServerMetrics sl) {
    this.onlineServers.put(sn, sl);
  }

  /**
   * @return A copy of the internal list of online servers.
   */
  public List<ServerName> getOnlineServersList() {
    return new ArrayList<>(this.onlineServers.asMap().keySet());
  }

  /**
   * @return Read-only map of servers to serverinfo
   */
  public Map<ServerName, ServerMetrics> getOnlineServers() {
    return Collections.unmodifiableMap(this.onlineServers.asMap());
  }

  /**
   * May return "0.0.0" when server is not online
   */
  public String getVersion(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.asMap().get(serverName);
    return serverMetrics != null ? serverMetrics.getVersion() : "0.0.0";
  }

  public int getInfoPort(ServerName serverName) {
    ServerMetrics serverMetrics = onlineServers.asMap().get(serverName);
    return serverMetrics != null ? serverMetrics.getInfoServerPort() : 0;
  }

  public IsCompactionOffloadEnabledResponse
      isCompactionOffloadEnabled(IsCompactionOffloadEnabledRequest request) throws IOException {
    masterServices.getMasterCoprocessorHost().preIsCompactionOffloadEnabled();
    boolean enabled = compactionOffloadSwitchStorage.isCompactionOffloadEnabled();
    IsCompactionOffloadEnabledResponse response = IsCompactionOffloadEnabledResponse.newBuilder()
        .setCompactionOffloadEnabled(enabled).build();
    masterServices.getMasterCoprocessorHost().postIsCompactionOffloadEnabled(enabled);
    return response;
  }

  public SwitchCompactionOffloadResponse
      switchCompactionOffload(SwitchCompactionOffloadRequest request) throws IOException {
    boolean compactionOffloadEnabled = request.getCompactionOffloadEnabled();
    masterServices.getMasterCoprocessorHost().preSwitchCompactionOffload(compactionOffloadEnabled);
    boolean oldCompactionOffloadEnable =
        compactionOffloadSwitchStorage.isCompactionOffloadEnabled();
    if (compactionOffloadEnabled != oldCompactionOffloadEnable) {
      LOG.info("{} switch compaction offload from {} to {}",
        masterServices.getClientIdAuditPrefix(), oldCompactionOffloadEnable,
        compactionOffloadEnabled);
      ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
      SwitchCompactionOffloadProcedure procedure =
          new SwitchCompactionOffloadProcedure(compactionOffloadSwitchStorage,
              compactionOffloadEnabled, masterServices.getServerName(), latch);
      masterServices.getMasterProcedureExecutor().submitProcedure(procedure);
      latch.await();
    } else {
      LOG.warn("Skip switch compaction offload to {} because it's the same with old value",
        compactionOffloadEnabled);
    }
    SwitchCompactionOffloadResponse response = SwitchCompactionOffloadResponse.newBuilder()
        .setPreviousCompactionOffloadEnabled(oldCompactionOffloadEnable).build();
    masterServices.getMasterCoprocessorHost()
        .postSwitchCompactionOffload(oldCompactionOffloadEnable, compactionOffloadEnabled);
    return response;
  }
}
