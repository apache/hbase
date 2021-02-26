/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map.Entry;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.TextFormat;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RegionServerStatusProtos;

/**
 * A manager for filesystem space quotas in the RegionServer.
 *
 * This class is the centralized point for what a RegionServer knows about space quotas
 * on tables. For each table, it tracks two different things: the {@link SpaceQuotaSnapshot}
 * and a {@link SpaceViolationPolicyEnforcement} (which may be null when a quota is not
 * being violated). Both of these are sensitive on when they were last updated. The
 * {link SpaceQutoaViolationPolicyRefresherChore} periodically runs and updates
 * the state on <code>this</code>.
 */
@InterfaceAudience.Private
public class RegionServerSpaceQuotaManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerSpaceQuotaManager.class);

  private final RegionServerServices rsServices;

  private SpaceQuotaRefresherChore spaceQuotaRefresher;
  private AtomicReference<Map<TableName, SpaceQuotaSnapshot>> currentQuotaSnapshots;
  private boolean started = false;
  private final ConcurrentHashMap<TableName,SpaceViolationPolicyEnforcement> enforcedPolicies;
  private SpaceViolationPolicyEnforcementFactory factory;
  private RegionSizeStore regionSizeStore;
  private RegionSizeReportingChore regionSizeReporter;

  public RegionServerSpaceQuotaManager(RegionServerServices rsServices) {
    this(rsServices, SpaceViolationPolicyEnforcementFactory.getInstance());
  }

  RegionServerSpaceQuotaManager(
      RegionServerServices rsServices, SpaceViolationPolicyEnforcementFactory factory) {
    this.rsServices = Objects.requireNonNull(rsServices);
    this.factory = factory;
    this.enforcedPolicies = new ConcurrentHashMap<>();
    this.currentQuotaSnapshots = new AtomicReference<>(new HashMap<>());
    // Initialize the size store to not track anything -- create the real one if we're start()'ed
    this.regionSizeStore = NoOpRegionSizeStore.getInstance();
  }

  public synchronized void start() throws IOException {
    if (!QuotaUtil.isQuotaEnabled(rsServices.getConfiguration())) {
      LOG.info("Quota support disabled, not starting space quota manager.");
      return;
    }

    if (started) {
      LOG.warn("RegionServerSpaceQuotaManager has already been started!");
      return;
    }
    // Start the chores
    this.spaceQuotaRefresher = new SpaceQuotaRefresherChore(this, rsServices.getClusterConnection());
    rsServices.getChoreService().scheduleChore(spaceQuotaRefresher);
    this.regionSizeReporter = new RegionSizeReportingChore(rsServices);
    rsServices.getChoreService().scheduleChore(regionSizeReporter);
    // Instantiate the real RegionSizeStore
    this.regionSizeStore = RegionSizeStoreFactory.getInstance().createStore();
    started = true;
  }

  public synchronized void stop() {
    if (spaceQuotaRefresher != null) {
      spaceQuotaRefresher.shutdown();
      spaceQuotaRefresher = null;
    }
    if (regionSizeReporter != null) {
      regionSizeReporter.shutdown();
      regionSizeReporter = null;
    }
    started = false;
  }

  /**
   * @return if the {@code Chore} has been started.
   */
  public boolean isStarted() {
    return started;
  }

  /**
   * Copies the last {@link SpaceQuotaSnapshot}s that were recorded. The current view
   * of what the RegionServer thinks the table's utilization is.
   */
  public Map<TableName,SpaceQuotaSnapshot> copyQuotaSnapshots() {
    return new HashMap<>(currentQuotaSnapshots.get());
  }

  /**
   * Updates the current {@link SpaceQuotaSnapshot}s for the RegionServer.
   *
   * @param newSnapshots The space quota snapshots.
   */
  public void updateQuotaSnapshot(Map<TableName,SpaceQuotaSnapshot> newSnapshots) {
    currentQuotaSnapshots.set(Objects.requireNonNull(newSnapshots));
  }

  /**
   * Creates an object well-suited for the RegionServer to use in verifying active policies.
   */
  public ActivePolicyEnforcement getActiveEnforcements() {
    return new ActivePolicyEnforcement(copyActiveEnforcements(), copyQuotaSnapshots(), rsServices);
  }

  /**
   * Converts a map of table to {@link SpaceViolationPolicyEnforcement}s into
   * {@link SpaceViolationPolicy}s.
   */
  public Map<TableName, SpaceQuotaSnapshot> getActivePoliciesAsMap() {
    final Map<TableName, SpaceViolationPolicyEnforcement> enforcements =
        copyActiveEnforcements();
    final Map<TableName, SpaceQuotaSnapshot> policies = new HashMap<>();
    for (Entry<TableName, SpaceViolationPolicyEnforcement> entry : enforcements.entrySet()) {
      final SpaceQuotaSnapshot snapshot = entry.getValue().getQuotaSnapshot();
      if (snapshot != null) {
        policies.put(entry.getKey(), snapshot);
      }
    }
    return policies;
  }

  /**
   * Enforces the given violationPolicy on the given table in this RegionServer.
   */
  public void enforceViolationPolicy(TableName tableName, SpaceQuotaSnapshot snapshot) {
    SpaceQuotaStatus status = snapshot.getQuotaStatus();
    if (!status.isInViolation()) {
      throw new IllegalStateException(
          tableName + " is not in violation. Violation policy should not be enabled.");
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Enabling violation policy enforcement on " + tableName
          + " with policy " + status.getPolicy());
    }
    // Construct this outside of the lock
    final SpaceViolationPolicyEnforcement enforcement = getFactory().create(
        getRegionServerServices(), tableName, snapshot);
    // "Enables" the policy
    // HBASE-XXXX: Should this synchronize on the actual table name instead of the map? That would
    // allow policy enable/disable on different tables to happen concurrently. As written now, only
    // one table will be allowed to transition at a time. This is probably OK, but not sure if
    // it would become a bottleneck at large clusters/number of tables.
    synchronized (enforcedPolicies) {
      try {
        enforcement.enable();
      } catch (IOException e) {
        LOG.error("Failed to enable space violation policy for " + tableName
            + ". This table will not enter violation.", e);
        return;
      }
      enforcedPolicies.put(tableName, enforcement);
    }
  }

  /**
   * Disables enforcement on any violation policy on the given <code>tableName</code>.
   */
  public void disableViolationPolicyEnforcement(TableName tableName) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Disabling violation policy enforcement on " + tableName);
    }
    // "Disables" the policy
    synchronized (enforcedPolicies) {
      SpaceViolationPolicyEnforcement enforcement = enforcedPolicies.remove(tableName);
      if (enforcement != null) {
        try {
          enforcement.disable();
        } catch (IOException e) {
          LOG.error("Failed to disable space violation policy for " + tableName
              + ". This table will remain in violation.", e);
          enforcedPolicies.put(tableName, enforcement);
        }
      }
    }
  }

  /**
   * Returns whether or not compactions should be disabled for the given <code>tableName</code> per
   * a space quota violation policy. A convenience method.
   *
   * @param tableName The table to check
   * @return True if compactions should be disabled for the table, false otherwise.
   */
  public boolean areCompactionsDisabled(TableName tableName) {
    SpaceViolationPolicyEnforcement enforcement = this.enforcedPolicies.get(Objects.requireNonNull(tableName));
    if (enforcement != null) {
      return enforcement.areCompactionsDisabled();
    }
    return false;
  }

  /**
   * Returns the {@link RegionSizeStore} tracking filesystem utilization by each region.
   *
   * @return A {@link RegionSizeStore} implementation.
   */
  public RegionSizeStore getRegionSizeStore() {
    return regionSizeStore;
  }

  /**
   * Builds the protobuf message to inform the Master of files being archived.
   *
   * @param tn The table the files previously belonged to.
   * @param archivedFiles The files and their size in bytes that were archived.
   * @return The protobuf representation
   */
  public RegionServerStatusProtos.FileArchiveNotificationRequest buildFileArchiveRequest(
      TableName tn, Collection<Entry<String,Long>> archivedFiles) {
    RegionServerStatusProtos.FileArchiveNotificationRequest.Builder builder =
        RegionServerStatusProtos.FileArchiveNotificationRequest.newBuilder();
    HBaseProtos.TableName protoTn = ProtobufUtil.toProtoTableName(tn);
    for (Entry<String,Long> archivedFile : archivedFiles) {
      RegionServerStatusProtos.FileArchiveNotificationRequest.FileWithSize fws =
          RegionServerStatusProtos.FileArchiveNotificationRequest.FileWithSize.newBuilder()
              .setName(archivedFile.getKey())
              .setSize(archivedFile.getValue())
              .setTableName(protoTn)
              .build();
      builder.addArchivedFiles(fws);
    }
    final RegionServerStatusProtos.FileArchiveNotificationRequest request = builder.build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Reporting file archival to Master: " + TextFormat.shortDebugString(request));
    }
    return request;
  }

  /**
   * Returns the collection of tables which have quota violation policies enforced on
   * this RegionServer.
   */
  Map<TableName,SpaceViolationPolicyEnforcement> copyActiveEnforcements() {
    // Allows reads to happen concurrently (or while the map is being updated)
    return new HashMap<>(this.enforcedPolicies);
  }

  RegionServerServices getRegionServerServices() {
    return rsServices;
  }

  Connection getConnection() {
    return rsServices.getConnection();
  }

  SpaceViolationPolicyEnforcementFactory getFactory() {
    return factory;
  }
}
