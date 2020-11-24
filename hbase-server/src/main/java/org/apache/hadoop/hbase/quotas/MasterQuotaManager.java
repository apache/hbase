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

package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.RegionStateListener;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.SwitchRpcThrottleProcedure;
import org.apache.hadoop.hbase.namespace.NamespaceAuditor;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.FileArchiveNotificationRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos.FileArchiveNotificationRequest.FileWithSize;

/**
 * Master Quota Manager.
 * It is responsible for initialize the quota table on the first-run and
 * provide the admin operations to interact with the quota table.
 *
 * TODO: FUTURE: The master will be responsible to notify each RS of quota changes
 * and it will do the "quota aggregation" when the QuotaScope is CLUSTER.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterQuotaManager implements RegionStateListener {
  private static final Logger LOG = LoggerFactory.getLogger(MasterQuotaManager.class);
  private static final Map<RegionInfo, Long> EMPTY_MAP = Collections.unmodifiableMap(
      new HashMap<>());

  private final MasterServices masterServices;
  private NamedLock<String> namespaceLocks;
  private NamedLock<TableName> tableLocks;
  private NamedLock<String> userLocks;
  private NamedLock<String> regionServerLocks;
  private boolean initialized = false;
  private NamespaceAuditor namespaceQuotaManager;
  private ConcurrentHashMap<RegionInfo, SizeSnapshotWithTimestamp> regionSizes;
  // Storage for quota rpc throttle
  private RpcThrottleStorage rpcThrottleStorage;

  public MasterQuotaManager(final MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  public void start() throws IOException {
    // If the user doesn't want the quota support skip all the initializations.
    if (!QuotaUtil.isQuotaEnabled(masterServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    // Create the quota table if missing
    if (!masterServices.getTableDescriptors().exists(QuotaUtil.QUOTA_TABLE_NAME)) {
      LOG.info("Quota table not found. Creating...");
      createQuotaTable();
    }

    LOG.info("Initializing quota support");
    namespaceLocks = new NamedLock<>();
    tableLocks = new NamedLock<>();
    userLocks = new NamedLock<>();
    regionServerLocks = new NamedLock<>();
    regionSizes = new ConcurrentHashMap<>();

    namespaceQuotaManager = new NamespaceAuditor(masterServices);
    namespaceQuotaManager.start();
    initialized = true;

    rpcThrottleStorage =
        new RpcThrottleStorage(masterServices.getZooKeeper(), masterServices.getConfiguration());
  }

  public void stop() {
  }

  public boolean isQuotaInitialized() {
    return initialized && namespaceQuotaManager.isInitialized();
  }

  /* ==========================================================================
   *  Admin operations to manage the quota table
   */
  public SetQuotaResponse setQuota(final SetQuotaRequest req)
      throws IOException, InterruptedException {
    checkQuotaSupport();

    if (req.hasUserName()) {
      userLocks.lock(req.getUserName());
      try {
        if (req.hasTableName()) {
          setUserQuota(req.getUserName(), ProtobufUtil.toTableName(req.getTableName()), req);
        } else if (req.hasNamespace()) {
          setUserQuota(req.getUserName(), req.getNamespace(), req);
        } else {
          setUserQuota(req.getUserName(), req);
        }
      } finally {
        userLocks.unlock(req.getUserName());
      }
    } else if (req.hasTableName()) {
      TableName table = ProtobufUtil.toTableName(req.getTableName());
      tableLocks.lock(table);
      try {
        setTableQuota(table, req);
      } finally {
        tableLocks.unlock(table);
      }
    } else if (req.hasNamespace()) {
      namespaceLocks.lock(req.getNamespace());
      try {
        setNamespaceQuota(req.getNamespace(), req);
      } finally {
        namespaceLocks.unlock(req.getNamespace());
      }
    } else if (req.hasRegionServer()) {
      regionServerLocks.lock(req.getRegionServer());
      try {
        setRegionServerQuota(req.getRegionServer(), req);
      } finally {
        regionServerLocks.unlock(req.getRegionServer());
      }
    } else {
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "a user, a table, a namespace or region server must be specified"));
    }
    return SetQuotaResponse.newBuilder().build();
  }

  public void setUserQuota(final String userName, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public GlobalQuotaSettingsImpl fetch() throws IOException {
        return new GlobalQuotaSettingsImpl(req.getUserName(), null, null, null,
            QuotaUtil.getUserQuota(masterServices.getConnection(), userName));
      }
      @Override
      public void update(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        QuotaUtil.addUserQuota(masterServices.getConnection(), userName, quotaPojo.toQuotas());
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConnection(), userName);
      }
      @Override
      public void preApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, quotaPojo);
      }
      @Override
      public void postApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, quotaPojo);
      }
    });
  }

  public void setUserQuota(final String userName, final TableName table,
      final SetQuotaRequest req) throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public GlobalQuotaSettingsImpl fetch() throws IOException {
        return new GlobalQuotaSettingsImpl(userName, table, null, null,
            QuotaUtil.getUserQuota(masterServices.getConnection(), userName, table));
      }
      @Override
      public void update(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        QuotaUtil.addUserQuota(masterServices.getConnection(), userName, table,
            quotaPojo.toQuotas());
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConnection(), userName, table);
      }
      @Override
      public void preApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(userName, table, quotaPojo);
      }
      @Override
      public void postApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(userName, table, quotaPojo);
      }
    });
  }

  public void setUserQuota(final String userName, final String namespace,
      final SetQuotaRequest req) throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public GlobalQuotaSettingsImpl fetch() throws IOException {
        return new GlobalQuotaSettingsImpl(userName, null, namespace, null,
            QuotaUtil.getUserQuota(masterServices.getConnection(), userName, namespace));
      }
      @Override
      public void update(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        QuotaUtil.addUserQuota(masterServices.getConnection(), userName, namespace,
            quotaPojo.toQuotas());
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConnection(), userName, namespace);
      }
      @Override
      public void preApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetUserQuota(
            userName, namespace, quotaPojo);
      }
      @Override
      public void postApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetUserQuota(
            userName, namespace, quotaPojo);
      }
    });
  }

  public void setTableQuota(final TableName table, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public GlobalQuotaSettingsImpl fetch() throws IOException {
        return new GlobalQuotaSettingsImpl(null, table, null, null,
            QuotaUtil.getTableQuota(masterServices.getConnection(), table));
      }
      @Override
      public void update(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        QuotaUtil.addTableQuota(masterServices.getConnection(), table, quotaPojo.toQuotas());
      }
      @Override
      public void delete() throws IOException {
        SpaceQuotaSnapshot currSnapshotOfTable =
            QuotaTableUtil.getCurrentSnapshotFromQuotaTable(masterServices.getConnection(), table);
        QuotaUtil.deleteTableQuota(masterServices.getConnection(), table);
        if (currSnapshotOfTable != null) {
          SpaceQuotaStatus quotaStatus = currSnapshotOfTable.getQuotaStatus();
          if (SpaceViolationPolicy.DISABLE == quotaStatus.getPolicy().orElse(null)
              && quotaStatus.isInViolation()) {
            QuotaUtil.enableTableIfNotEnabled(masterServices.getConnection(), table);
          }
        }
      }
      @Override
      public void preApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetTableQuota(table, quotaPojo);
      }
      @Override
      public void postApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetTableQuota(table, quotaPojo);
      }
    });
  }

  public void setNamespaceQuota(final String namespace, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public GlobalQuotaSettingsImpl fetch() throws IOException {
        return new GlobalQuotaSettingsImpl(null, null, namespace, null,
            QuotaUtil.getNamespaceQuota(masterServices.getConnection(), namespace));
      }
      @Override
      public void update(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        QuotaUtil.addNamespaceQuota(masterServices.getConnection(), namespace,
          quotaPojo.toQuotas());
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteNamespaceQuota(masterServices.getConnection(), namespace);
      }
      @Override
      public void preApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetNamespaceQuota(namespace, quotaPojo);
      }
      @Override
      public void postApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetNamespaceQuota(namespace, quotaPojo);
      }
    });
  }

  public void setRegionServerQuota(final String regionServer, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    setQuota(req, new SetQuotaOperations() {
      @Override
      public GlobalQuotaSettingsImpl fetch() throws IOException {
        return new GlobalQuotaSettingsImpl(null, null, null, regionServer,
            QuotaUtil.getRegionServerQuota(masterServices.getConnection(), regionServer));
      }

      @Override
      public void update(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        QuotaUtil.addRegionServerQuota(masterServices.getConnection(), regionServer,
          quotaPojo.toQuotas());
      }

      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteRegionServerQuota(masterServices.getConnection(), regionServer);
      }

      @Override
      public void preApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().preSetRegionServerQuota(regionServer, quotaPojo);
      }

      @Override
      public void postApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException {
        masterServices.getMasterCoprocessorHost().postSetRegionServerQuota(regionServer, quotaPojo);
      }
    });
  }

  public void setNamespaceQuota(NamespaceDescriptor desc) throws IOException {
    if (initialized) {
      this.namespaceQuotaManager.addNamespace(desc);
    }
  }

  public void removeNamespaceQuota(String namespace) throws IOException {
    if (initialized) {
      this.namespaceQuotaManager.deleteNamespace(namespace);
    }
  }

  public SwitchRpcThrottleResponse switchRpcThrottle(SwitchRpcThrottleRequest request)
      throws IOException {
    boolean rpcThrottle = request.getRpcThrottleEnabled();
    if (initialized) {
      masterServices.getMasterCoprocessorHost().preSwitchRpcThrottle(rpcThrottle);
      boolean oldRpcThrottle = rpcThrottleStorage.isRpcThrottleEnabled();
      if (rpcThrottle != oldRpcThrottle) {
        LOG.info("{} switch rpc throttle from {} to {}", masterServices.getClientIdAuditPrefix(),
          oldRpcThrottle, rpcThrottle);
        ProcedurePrepareLatch latch = ProcedurePrepareLatch.createBlockingLatch();
        SwitchRpcThrottleProcedure procedure = new SwitchRpcThrottleProcedure(rpcThrottleStorage,
            rpcThrottle, masterServices.getServerName(), latch);
        masterServices.getMasterProcedureExecutor().submitProcedure(procedure);
        latch.await();
      } else {
        LOG.warn("Skip switch rpc throttle to {} because it's the same with old value",
          rpcThrottle);
      }
      SwitchRpcThrottleResponse response = SwitchRpcThrottleResponse.newBuilder()
          .setPreviousRpcThrottleEnabled(oldRpcThrottle).build();
      masterServices.getMasterCoprocessorHost().postSwitchRpcThrottle(oldRpcThrottle, rpcThrottle);
      return response;
    } else {
      LOG.warn("Skip switch rpc throttle to {} because rpc quota is disabled", rpcThrottle);
      return SwitchRpcThrottleResponse.newBuilder().setPreviousRpcThrottleEnabled(false).build();
    }
  }

  public IsRpcThrottleEnabledResponse isRpcThrottleEnabled(IsRpcThrottleEnabledRequest request)
      throws IOException {
    if (initialized) {
      masterServices.getMasterCoprocessorHost().preIsRpcThrottleEnabled();
      boolean enabled = isRpcThrottleEnabled();
      IsRpcThrottleEnabledResponse response =
          IsRpcThrottleEnabledResponse.newBuilder().setRpcThrottleEnabled(enabled).build();
      masterServices.getMasterCoprocessorHost().postIsRpcThrottleEnabled(enabled);
      return response;
    } else {
      LOG.warn("Skip get rpc throttle because rpc quota is disabled");
      return IsRpcThrottleEnabledResponse.newBuilder().setRpcThrottleEnabled(false).build();
    }
  }

  public boolean isRpcThrottleEnabled() throws IOException {
    return initialized ? rpcThrottleStorage.isRpcThrottleEnabled() : false;
  }

  public SwitchExceedThrottleQuotaResponse
      switchExceedThrottleQuota(SwitchExceedThrottleQuotaRequest request) throws IOException {
    boolean enabled = request.getExceedThrottleQuotaEnabled();
    if (initialized) {
      masterServices.getMasterCoprocessorHost().preSwitchExceedThrottleQuota(enabled);
      boolean previousEnabled =
          QuotaUtil.isExceedThrottleQuotaEnabled(masterServices.getConnection());
      if (previousEnabled == enabled) {
        LOG.warn("Skip switch exceed throttle quota to {} because it's the same with old value",
          enabled);
      } else {
        QuotaUtil.switchExceedThrottleQuota(masterServices.getConnection(), enabled);
        LOG.info("{} switch exceed throttle quota from {} to {}",
          masterServices.getClientIdAuditPrefix(), previousEnabled, enabled);
      }
      SwitchExceedThrottleQuotaResponse response = SwitchExceedThrottleQuotaResponse.newBuilder()
          .setPreviousExceedThrottleQuotaEnabled(previousEnabled).build();
      masterServices.getMasterCoprocessorHost().postSwitchExceedThrottleQuota(previousEnabled,
        enabled);
      return response;
    } else {
      LOG.warn("Skip switch exceed throttle quota to {} because quota is disabled", enabled);
      return SwitchExceedThrottleQuotaResponse.newBuilder()
          .setPreviousExceedThrottleQuotaEnabled(false).build();
    }
  }

  public boolean isExceedThrottleQuotaEnabled() throws IOException {
    return initialized ? QuotaUtil.isExceedThrottleQuotaEnabled(masterServices.getConnection())
        : false;
  }

  private void setQuota(final SetQuotaRequest req, final SetQuotaOperations quotaOps)
      throws IOException, InterruptedException {
    if (req.hasRemoveAll() && req.getRemoveAll() == true) {
      quotaOps.preApply(null);
      quotaOps.delete();
      quotaOps.postApply(null);
      return;
    }

    // Apply quota changes
    GlobalQuotaSettingsImpl currentQuota = quotaOps.fetch();
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Current quota for request(" + TextFormat.shortDebugString(req)
              + "): " + currentQuota);
    }
    // Call the appropriate "pre" CP hook with the current quota value (may be null)
    quotaOps.preApply(currentQuota);
    // Translate the protobuf request back into a POJO
    QuotaSettings newQuota = QuotaSettings.buildFromProto(req);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Deserialized quota from request: " + newQuota);
    }

    // Merge the current quota settings with the new quota settings the user provided.
    //
    // NB: while SetQuotaRequest technically allows for multi types of quotas to be set in one
    // message, the Java API (in Admin/AsyncAdmin) does not. Assume there is only one type.
    GlobalQuotaSettingsImpl mergedQuota = currentQuota.merge(newQuota);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Computed merged quota from current quota and user request: " + mergedQuota);
    }

    // Submit new changes
    if (mergedQuota == null) {
      quotaOps.delete();
    } else {
      quotaOps.update(mergedQuota);
    }
    // Advertise the final result via the "post" CP hook
    quotaOps.postApply(mergedQuota);
  }

  public void checkNamespaceTableAndRegionQuota(TableName tName, int regions) throws IOException {
    if (initialized) {
      namespaceQuotaManager.checkQuotaToCreateTable(tName, regions);
    }
  }

  public void checkAndUpdateNamespaceRegionQuota(TableName tName, int regions) throws IOException {
    if (initialized) {
      namespaceQuotaManager.checkQuotaToUpdateRegion(tName, regions);
    }
  }

  /**
   * @return cached region count, or -1 if quota manager is disabled or table status not found
  */
  public int getRegionCountOfTable(TableName tName) throws IOException {
    if (initialized) {
      return namespaceQuotaManager.getRegionCountOfTable(tName);
    }
    return -1;
  }

  @Override
  public void onRegionMerged(RegionInfo mergedRegion) throws IOException {
    if (initialized) {
      namespaceQuotaManager.updateQuotaForRegionMerge(mergedRegion);
    }
  }

  @Override
  public void onRegionSplit(RegionInfo hri) throws IOException {
    if (initialized) {
      namespaceQuotaManager.checkQuotaToSplitRegion(hri);
    }
  }

  /**
   * Remove table from namespace quota.
   *
   * @param tName - The table name to update quota usage.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void removeTableFromNamespaceQuota(TableName tName) throws IOException {
    if (initialized) {
      namespaceQuotaManager.removeFromNamespaceUsage(tName);
    }
  }

  public NamespaceAuditor getNamespaceQuotaManager() {
    return this.namespaceQuotaManager;
  }

  /**
   * Encapsulates CRUD quota operations for some subject.
   */
  private static interface SetQuotaOperations {
    /**
     * Fetches the current quota settings for the subject.
     */
    GlobalQuotaSettingsImpl fetch() throws IOException;
    /**
     * Deletes the quota for the subject.
     */
    void delete() throws IOException;
    /**
     * Persist the given quota for the subject.
     */
    void update(GlobalQuotaSettingsImpl quotaPojo) throws IOException;
    /**
     * Performs some action before {@link #update(GlobalQuotaSettingsImpl)} with the current
     * quota for the subject.
     */
    void preApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException;
    /**
     * Performs some action after {@link #update(GlobalQuotaSettingsImpl)} with the resulting
     * quota from the request action for the subject.
     */
    void postApply(GlobalQuotaSettingsImpl quotaPojo) throws IOException;
  }

  /* ==========================================================================
   *  Helpers
   */

  private void checkQuotaSupport() throws IOException {
    if (!QuotaUtil.isQuotaEnabled(masterServices.getConfiguration())) {
      throw new DoNotRetryIOException(
        new UnsupportedOperationException("quota support disabled"));
    }
    if (!initialized) {
      long maxWaitTime = masterServices.getConfiguration().getLong(
        "hbase.master.wait.for.quota.manager.init", 30000); // default is 30 seconds.
      long startTime = EnvironmentEdgeManager.currentTime();
      do {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for Quota Manager to be initialized.");
          break;
        }
      } while (!initialized && (EnvironmentEdgeManager.currentTime() - startTime) < maxWaitTime);
      if (!initialized) {
        throw new IOException("Quota manager is uninitialized, please retry later.");
      }
    }
  }

  private void createQuotaTable() throws IOException {
    masterServices.createSystemTable(QuotaUtil.QUOTA_TABLE_DESC);
  }

  private static class NamedLock<T> {
    private final HashSet<T> locks = new HashSet<>();

    public void lock(final T name) throws InterruptedException {
      synchronized (locks) {
        while (locks.contains(name)) {
          locks.wait();
        }
        locks.add(name);
      }
    }

    public void unlock(final T name) {
      synchronized (locks) {
        locks.remove(name);
        locks.notifyAll();
      }
    }
  }

  @Override
  public void onRegionSplitReverted(RegionInfo hri) throws IOException {
    if (initialized) {
      this.namespaceQuotaManager.removeRegionFromNamespaceUsage(hri);
    }
  }

  /**
   * Holds the size of a region at the given time, millis since the epoch.
   */
  private static class SizeSnapshotWithTimestamp {
    private final long size;
    private final long time;

    public SizeSnapshotWithTimestamp(long size, long time) {
      this.size = size;
      this.time = time;
    }

    public long getSize() {
      return size;
    }

    public long getTime() {
      return time;
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof SizeSnapshotWithTimestamp) {
        SizeSnapshotWithTimestamp other = (SizeSnapshotWithTimestamp) o;
        return size == other.size && time == other.time;
      }
      return false;
    }

    @Override
    public int hashCode() {
      HashCodeBuilder hcb = new HashCodeBuilder();
      return hcb.append(size).append(time).toHashCode();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder(32);
      sb.append("SizeSnapshotWithTimestamp={size=").append(size).append("B, ");
      sb.append("time=").append(time).append("}");
      return sb.toString();
    }
  }

  void initializeRegionSizes() {
    assert regionSizes == null;
    this.regionSizes = new ConcurrentHashMap<>();
  }

  public void addRegionSize(RegionInfo hri, long size, long time) {
    if (regionSizes == null) {
      return;
    }
    regionSizes.put(hri, new SizeSnapshotWithTimestamp(size, time));
  }

  public Map<RegionInfo, Long> snapshotRegionSizes() {
    if (regionSizes == null) {
      return EMPTY_MAP;
    }

    Map<RegionInfo, Long> copy = new HashMap<>();
    for (Entry<RegionInfo, SizeSnapshotWithTimestamp> entry : regionSizes.entrySet()) {
      copy.put(entry.getKey(), entry.getValue().getSize());
    }
    return copy;
  }

  int pruneEntriesOlderThan(long timeToPruneBefore, QuotaObserverChore quotaObserverChore) {
    if (regionSizes == null) {
      return 0;
    }
    int numEntriesRemoved = 0;
    Iterator<Entry<RegionInfo, SizeSnapshotWithTimestamp>> iterator =
        regionSizes.entrySet().iterator();
    while (iterator.hasNext()) {
      RegionInfo regionInfo = iterator.next().getKey();
      long currentEntryTime = regionSizes.get(regionInfo).getTime();
      // do not prune the entries if table is in violation and
      // violation policy is disable to avoid cycle of enable/disable.
      // Please refer HBASE-22012 for more details.
      // prune entries older than time.
      if (currentEntryTime < timeToPruneBefore && !isInViolationAndPolicyDisable(
          regionInfo.getTable(), quotaObserverChore)) {
        iterator.remove();
        numEntriesRemoved++;
      }
    }
    return numEntriesRemoved;
  }

  /**
   * Method to check if a table is in violation and policy set on table is DISABLE.
   *
   * @param tableName          tableName to check.
   * @param quotaObserverChore QuotaObserverChore instance
   * @return returns true if table is in violation and policy is disable else false.
   */
  private boolean isInViolationAndPolicyDisable(TableName tableName,
      QuotaObserverChore quotaObserverChore) {
    boolean isInViolationAtTable = false;
    boolean isInViolationAtNamespace = false;
    SpaceViolationPolicy tablePolicy = null;
    SpaceViolationPolicy namespacePolicy = null;
    // Get Current Snapshot for the given table
    SpaceQuotaSnapshot tableQuotaSnapshot = quotaObserverChore.getTableQuotaSnapshot(tableName);
    SpaceQuotaSnapshot namespaceQuotaSnapshot =
        quotaObserverChore.getNamespaceQuotaSnapshot(tableName.getNamespaceAsString());
    if (tableQuotaSnapshot != null) {
      // check if table in violation
      isInViolationAtTable = tableQuotaSnapshot.getQuotaStatus().isInViolation();
      Optional<SpaceViolationPolicy> policy = tableQuotaSnapshot.getQuotaStatus().getPolicy();
      if (policy.isPresent()) {
        tablePolicy = policy.get();
      }
    }
    if (namespaceQuotaSnapshot != null) {
      // check namespace in violation
      isInViolationAtNamespace = namespaceQuotaSnapshot.getQuotaStatus().isInViolation();
      Optional<SpaceViolationPolicy> policy = namespaceQuotaSnapshot.getQuotaStatus().getPolicy();
      if (policy.isPresent()) {
        namespacePolicy = policy.get();
      }
    }
    return (tablePolicy == SpaceViolationPolicy.DISABLE && isInViolationAtTable) || (
        namespacePolicy == SpaceViolationPolicy.DISABLE && isInViolationAtNamespace);
  }

  /**
   * Removes each region size entry where the RegionInfo references the provided TableName.
   *
   * @param tableName tableName.
   */
  public void removeRegionSizesForTable(TableName tableName) {
    regionSizes.keySet().removeIf(regionInfo -> regionInfo.getTable().equals(tableName));
  }

  public void processFileArchivals(FileArchiveNotificationRequest request, Connection conn,
      Configuration conf, FileSystem fs) throws IOException {
    final HashMultimap<TableName,Entry<String,Long>> archivedFilesByTable = HashMultimap.create();
    // Group the archived files by table
    for (FileWithSize fileWithSize : request.getArchivedFilesList()) {
      TableName tn = ProtobufUtil.toTableName(fileWithSize.getTableName());
      archivedFilesByTable.put(
          tn, Maps.immutableEntry(fileWithSize.getName(), fileWithSize.getSize()));
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Grouped archived files by table: " + archivedFilesByTable);
    }
    // Report each set of files to the appropriate object
    for (TableName tn : archivedFilesByTable.keySet()) {
      final Set<Entry<String,Long>> filesWithSize = archivedFilesByTable.get(tn);
      final FileArchiverNotifier notifier = FileArchiverNotifierFactoryImpl.getInstance().get(
          conn, conf, fs, tn);
      notifier.addArchivedFiles(filesWithSize);
    }
  }
}

