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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MetricsMaster;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.HashMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Multimap;

import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

/**
 * Reads the currently received Region filesystem-space use reports and acts on those which
 * violate a defined quota.
 */
@InterfaceAudience.Private
public class QuotaObserverChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaObserverChore.class);
  static final String QUOTA_OBSERVER_CHORE_PERIOD_KEY =
      "hbase.master.quotas.observer.chore.period";
  static final int QUOTA_OBSERVER_CHORE_PERIOD_DEFAULT = 1000 * 60 * 1; // 1 minutes in millis

  static final String QUOTA_OBSERVER_CHORE_DELAY_KEY =
      "hbase.master.quotas.observer.chore.delay";
  static final long QUOTA_OBSERVER_CHORE_DELAY_DEFAULT = 1000L * 15L; // 15 seconds in millis

  static final String QUOTA_OBSERVER_CHORE_TIMEUNIT_KEY =
      "hbase.master.quotas.observer.chore.timeunit";
  static final String QUOTA_OBSERVER_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  static final String QUOTA_OBSERVER_CHORE_REPORT_PERCENT_KEY =
      "hbase.master.quotas.observer.report.percent";
  static final double QUOTA_OBSERVER_CHORE_REPORT_PERCENT_DEFAULT= 0.95;

  static final String REGION_REPORT_RETENTION_DURATION_KEY =
      "hbase.master.quotas.region.report.retention.millis";
  static final long REGION_REPORT_RETENTION_DURATION_DEFAULT =
      1000 * 60 * 10; // 10 minutes

  private final Connection conn;
  private final Configuration conf;
  private final MasterQuotaManager quotaManager;
  private final MetricsMaster metrics;
  /*
   * Callback that changes in quota snapshots are passed to.
   */
  private final SpaceQuotaSnapshotNotifier snapshotNotifier;

  /*
   * Preserves the state of quota snapshots for tables and namespaces
   */
  private final Map<TableName,SpaceQuotaSnapshot> tableQuotaSnapshots;
  private final Map<TableName,SpaceQuotaSnapshot> readOnlyTableQuotaSnapshots;
  private final Map<String,SpaceQuotaSnapshot> namespaceQuotaSnapshots;
  private final Map<String,SpaceQuotaSnapshot> readOnlyNamespaceSnapshots;

  // The time, in millis, that region reports should be kept by the master
  private final long regionReportLifetimeMillis;

  /*
   * Encapsulates logic for tracking the state of a table/namespace WRT space quotas
   */
  private QuotaSnapshotStore<TableName> tableSnapshotStore;
  private QuotaSnapshotStore<String> namespaceSnapshotStore;

  public QuotaObserverChore(HMaster master, MetricsMaster metrics) {
    this(
        master.getConnection(), master.getConfiguration(),
        master.getSpaceQuotaSnapshotNotifier(), master.getMasterQuotaManager(),
        master, metrics);
  }

  QuotaObserverChore(
      Connection conn, Configuration conf, SpaceQuotaSnapshotNotifier snapshotNotifier,
      MasterQuotaManager quotaManager, Stoppable stopper, MetricsMaster metrics) {
    super(
        QuotaObserverChore.class.getSimpleName(), stopper, getPeriod(conf),
        getInitialDelay(conf), getTimeUnit(conf));
    this.conn = conn;
    this.conf = conf;
    this.metrics = metrics;
    this.quotaManager = quotaManager;
    this.snapshotNotifier = Objects.requireNonNull(snapshotNotifier);
    this.tableQuotaSnapshots = new ConcurrentHashMap<>();
    this.readOnlyTableQuotaSnapshots = Collections.unmodifiableMap(tableQuotaSnapshots);
    this.namespaceQuotaSnapshots = new ConcurrentHashMap<>();
    this.readOnlyNamespaceSnapshots = Collections.unmodifiableMap(namespaceQuotaSnapshots);
    this.regionReportLifetimeMillis = conf.getLong(
        REGION_REPORT_RETENTION_DURATION_KEY, REGION_REPORT_RETENTION_DURATION_DEFAULT);
  }

  @Override
  protected void chore() {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Refreshing space quotas in RegionServer");
      }
      long start = System.nanoTime();
      _chore();
      if (metrics != null) {
        metrics.incrementQuotaObserverTime((System.nanoTime() - start) / 1_000_000);
      }
    } catch (IOException e) {
      LOG.warn("Failed to process quota reports and update quota state. Will retry.", e);
    }
  }

  void _chore() throws IOException {
    // Get the total set of tables that have quotas defined. Includes table quotas
    // and tables included by namespace quotas.
    TablesWithQuotas tablesWithQuotas = fetchAllTablesWithQuotasDefined();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Found following tables with quotas: " + tablesWithQuotas);
    }

    if (metrics != null) {
      // Set the number of namespaces and tables with quotas defined
      metrics.setNumSpaceQuotas(tablesWithQuotas.getTableQuotaTables().size()
          + tablesWithQuotas.getNamespacesWithQuotas().size());
    }

    // The current "view" of region space use. Used henceforth.
    final Map<RegionInfo,Long> reportedRegionSpaceUse = quotaManager.snapshotRegionSizes();
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          "Using " + reportedRegionSpaceUse.size() + " region space use reports: " +
          reportedRegionSpaceUse);
    }

    // Remove the "old" region reports
    pruneOldRegionReports();

    // Create the stores to track table and namespace snapshots
    initializeSnapshotStores(reportedRegionSpaceUse);
    // Report the number of (non-expired) region size reports
    if (metrics != null) {
      metrics.setNumRegionSizeReports(reportedRegionSpaceUse.size());
    }

    // Filter out tables for which we don't have adequate regionspace reports yet.
    // Important that we do this after we instantiate the stores above
    // This gives us a set of Tables which may or may not be violating their quota.
    // To be safe, we want to make sure that these are not in violation.
    Set<TableName> tablesInLimbo = tablesWithQuotas.filterInsufficientlyReportedTables(
        tableSnapshotStore);

    if (LOG.isTraceEnabled()) {
      LOG.trace("Filtered insufficiently reported tables, left with " +
          reportedRegionSpaceUse.size() + " regions reported");
    }

    for (TableName tableInLimbo : tablesInLimbo) {
      final SpaceQuotaSnapshot currentSnapshot = tableSnapshotStore.getCurrentState(tableInLimbo);
      SpaceQuotaStatus currentStatus = currentSnapshot.getQuotaStatus();
      if (currentStatus.isInViolation()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Moving " + tableInLimbo + " out of violation because fewer region sizes were"
              + " reported than required.");
        }
        SpaceQuotaSnapshot targetSnapshot = new SpaceQuotaSnapshot(
            SpaceQuotaStatus.notInViolation(), currentSnapshot.getUsage(),
            currentSnapshot.getLimit());
        this.snapshotNotifier.transitionTable(tableInLimbo, targetSnapshot);
        // Update it in the Table QuotaStore so that memory is consistent with no violation.
        tableSnapshotStore.setCurrentState(tableInLimbo, targetSnapshot);
        // In case of Disable SVP, we need to enable the table as it moves out of violation
        if (SpaceViolationPolicy.DISABLE == currentStatus.getPolicy().orElse(null)) {
          QuotaUtil.enableTableIfNotEnabled(conn, tableInLimbo);
        }
      }
    }

    // Transition each table to/from quota violation based on the current and target state.
    // Only table quotas are enacted.
    final Set<TableName> tablesWithTableQuotas = tablesWithQuotas.getTableQuotaTables();
    processTablesWithQuotas(tablesWithTableQuotas);

    // For each Namespace quota, transition each table in the namespace in or out of violation
    // only if a table quota violation policy has not already been applied.
    final Set<String> namespacesWithQuotas = tablesWithQuotas.getNamespacesWithQuotas();
    final Multimap<String,TableName> tablesByNamespace = tablesWithQuotas.getTablesByNamespace();
    processNamespacesWithQuotas(namespacesWithQuotas, tablesByNamespace);
  }

  void initializeSnapshotStores(Map<RegionInfo,Long> regionSizes) {
    Map<RegionInfo,Long> immutableRegionSpaceUse = Collections.unmodifiableMap(regionSizes);
    if (tableSnapshotStore == null) {
      tableSnapshotStore = new TableQuotaSnapshotStore(conn, this, immutableRegionSpaceUse);
    } else {
      tableSnapshotStore.setRegionUsage(immutableRegionSpaceUse);
    }
    if (namespaceSnapshotStore == null) {
      namespaceSnapshotStore = new NamespaceQuotaSnapshotStore(
          conn, this, immutableRegionSpaceUse);
    } else {
      namespaceSnapshotStore.setRegionUsage(immutableRegionSpaceUse);
    }
  }

  /**
   * Processes each {@code TableName} which has a quota defined and moves it in or out of
   * violation based on the space use.
   *
   * @param tablesWithTableQuotas The HBase tables which have quotas defined
   */
  void processTablesWithQuotas(final Set<TableName> tablesWithTableQuotas) throws IOException {
    long numTablesInViolation = 0L;
    for (TableName table : tablesWithTableQuotas) {
      final SpaceQuota spaceQuota = tableSnapshotStore.getSpaceQuota(table);
      if (spaceQuota == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Unexpectedly did not find a space quota for " + table
              + ", maybe it was recently deleted.");
        }
        continue;
      }
      final SpaceQuotaSnapshot currentSnapshot = tableSnapshotStore.getCurrentState(table);
      final SpaceQuotaSnapshot targetSnapshot = tableSnapshotStore.getTargetState(table, spaceQuota);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Processing " + table + " with current=" + currentSnapshot + ", target="
            + targetSnapshot);
      }
      updateTableQuota(table, currentSnapshot, targetSnapshot);

      if (targetSnapshot.getQuotaStatus().isInViolation()) {
        numTablesInViolation++;
      }
    }
    // Report the number of tables in violation
    if (metrics != null) {
      metrics.setNumTableInSpaceQuotaViolation(numTablesInViolation);
    }
  }

  /**
   * Processes each namespace which has a quota defined and moves all of the tables contained
   * in that namespace into or out of violation of the quota. Tables which are already in
   * violation of a quota at the table level which <em>also</em> have a reside in a namespace
   * with a violated quota will not have the namespace quota enacted. The table quota takes
   * priority over the namespace quota.
   *
   * @param namespacesWithQuotas The set of namespaces that have quotas defined
   * @param tablesByNamespace A mapping of namespaces and the tables contained in those namespaces
   */
  void processNamespacesWithQuotas(
      final Set<String> namespacesWithQuotas,
      final Multimap<String,TableName> tablesByNamespace) throws IOException {
    long numNamespacesInViolation = 0L;
    for (String namespace : namespacesWithQuotas) {
      // Get the quota definition for the namespace
      final SpaceQuota spaceQuota = namespaceSnapshotStore.getSpaceQuota(namespace);
      if (spaceQuota == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Could not get Namespace space quota for " + namespace
              + ", maybe it was recently deleted.");
        }
        continue;
      }
      final SpaceQuotaSnapshot currentSnapshot = namespaceSnapshotStore.getCurrentState(namespace);
      final SpaceQuotaSnapshot targetSnapshot = namespaceSnapshotStore.getTargetState(
          namespace, spaceQuota);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Processing " + namespace + " with current=" + currentSnapshot + ", target="
            + targetSnapshot);
      }
      updateNamespaceQuota(namespace, currentSnapshot, targetSnapshot, tablesByNamespace);

      if (targetSnapshot.getQuotaStatus().isInViolation()) {
        numNamespacesInViolation++;
      }
    }

    // Report the number of namespaces in violation
    if (metrics != null) {
      metrics.setNumNamespacesInSpaceQuotaViolation(numNamespacesInViolation);
    }
  }

  /**
   * Updates the hbase:quota table with the new quota policy for this <code>table</code>
   * if necessary.
   *
   * @param table The table being checked
   * @param currentSnapshot The state of the quota on this table from the previous invocation.
   * @param targetSnapshot The state the quota should be in for this table.
   */
  void updateTableQuota(
      TableName table, SpaceQuotaSnapshot currentSnapshot, SpaceQuotaSnapshot targetSnapshot)
          throws IOException {
    final SpaceQuotaStatus currentStatus = currentSnapshot.getQuotaStatus();
    final SpaceQuotaStatus targetStatus = targetSnapshot.getQuotaStatus();

    // If we're changing something, log it.
    if (!currentSnapshot.equals(targetSnapshot)) {
      this.snapshotNotifier.transitionTable(table, targetSnapshot);
      // Update it in memory
      tableSnapshotStore.setCurrentState(table, targetSnapshot);

      // If the target is none, we're moving out of violation. Update the hbase:quota table
      SpaceViolationPolicy currPolicy = currentStatus.getPolicy().orElse(null);
      SpaceViolationPolicy targetPolicy = targetStatus.getPolicy().orElse(null);
      if (!targetStatus.isInViolation()) {
        // In case of Disable SVP, we need to enable the table as it moves out of violation
        if (isDisableSpaceViolationPolicy(currPolicy, targetPolicy)) {
          QuotaUtil.enableTableIfNotEnabled(conn, table);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(table + " moved into observance of table space quota.");
        }
      } else {
        // We're either moving into violation or changing violation policies
        if (currPolicy != targetPolicy && SpaceViolationPolicy.DISABLE == currPolicy) {
          // In case of policy switch, we need to enable the table if current policy is Disable SVP
          QuotaUtil.enableTableIfNotEnabled(conn, table);
        } else if (SpaceViolationPolicy.DISABLE == targetPolicy) {
          // In case of Disable SVP, we need to disable the table as it moves into violation
          QuotaUtil.disableTableIfNotDisabled(conn, table);
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            table + " moved into violation of table space quota with policy of " + targetPolicy);
        }
      }
    } else if (LOG.isTraceEnabled()) {
      // Policies are the same, so we have nothing to do except log this. Don't need to re-update
      // the quota table
      if (!currentStatus.isInViolation()) {
        LOG.trace(table + " remains in observance of quota.");
      } else {
        LOG.trace(table + " remains in violation of quota.");
      }
    }
  }

  /**
   * Method to check whether we are dealing with DISABLE {@link SpaceViolationPolicy}. In such a
   * case, currPolicy or/and targetPolicy will be having DISABLE policy.
   * @param currPolicy currently set space violation policy
   * @param targetPolicy new space violation policy
   * @return true if is DISABLE space violation policy; otherwise false
   */
  private boolean isDisableSpaceViolationPolicy(final SpaceViolationPolicy currPolicy,
      final SpaceViolationPolicy targetPolicy) {
    return SpaceViolationPolicy.DISABLE == currPolicy
        || SpaceViolationPolicy.DISABLE == targetPolicy;
  }

  /**
   * Updates the hbase:quota table with the target quota policy for this <code>namespace</code>
   * if necessary.
   *
   * @param namespace The namespace being checked
   * @param currentSnapshot The state of the quota on this namespace from the previous invocation
   * @param targetSnapshot The state the quota should be in for this namespace
   * @param tablesByNamespace A mapping of tables in namespaces.
   */
  void updateNamespaceQuota(
      String namespace, SpaceQuotaSnapshot currentSnapshot, SpaceQuotaSnapshot targetSnapshot,
      final Multimap<String,TableName> tablesByNamespace) throws IOException {
    final SpaceQuotaStatus targetStatus = targetSnapshot.getQuotaStatus();

    // When the policies differ, we need to move into or out of violation
    if (!currentSnapshot.equals(targetSnapshot)) {
      // We want to have a policy of "NONE", moving out of violation
      if (!targetStatus.isInViolation()) {
        for (TableName tableInNS : tablesByNamespace.get(namespace)) {
          // If there is a quota on this table in violation
          if (tableSnapshotStore.getCurrentState(tableInNS).getQuotaStatus().isInViolation()) {
            // Table-level quota violation policy is being applied here.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Not activating Namespace violation policy because a Table violation"
                  + " policy is already in effect for " + tableInNS);
            }
          } else {
            LOG.info(tableInNS + " moving into observance of namespace space quota");
            this.snapshotNotifier.transitionTable(tableInNS, targetSnapshot);
          }
        }
      // We want to move into violation at the NS level
      } else {
        // Moving tables in the namespace into violation or to a different violation policy
        for (TableName tableInNS : tablesByNamespace.get(namespace)) {
          final SpaceQuotaSnapshot tableQuotaSnapshot =
                tableSnapshotStore.getCurrentState(tableInNS);
          final boolean hasTableQuota =
              !Objects.equals(QuotaSnapshotStore.NO_QUOTA, tableQuotaSnapshot);
          if (hasTableQuota && tableQuotaSnapshot.getQuotaStatus().isInViolation()) {
            // Table-level quota violation policy is being applied here.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Not activating Namespace violation policy because a Table violation"
                  + " policy is already in effect for " + tableInNS);
            }
          } else {
            // No table quota present or a table quota present that is not in violation
            LOG.info(tableInNS + " moving into violation of namespace space quota with policy "
                + targetStatus.getPolicy());
            this.snapshotNotifier.transitionTable(tableInNS, targetSnapshot);
          }
        }
      }
      // Update the new state in memory for this namespace
      namespaceSnapshotStore.setCurrentState(namespace, targetSnapshot);
    } else {
      // Policies are the same
      if (!targetStatus.isInViolation()) {
        // Both are NONE, so we remain in observance
        if (LOG.isTraceEnabled()) {
          LOG.trace(namespace + " remains in observance of quota.");
        }
      } else {
        // Namespace quota is still in violation, need to enact if the table quota is not
        // taking priority.
        for (TableName tableInNS : tablesByNamespace.get(namespace)) {
          // Does a table policy exist
          if (tableSnapshotStore.getCurrentState(tableInNS).getQuotaStatus().isInViolation()) {
            // Table-level quota violation policy is being applied here.
            if (LOG.isTraceEnabled()) {
              LOG.trace("Not activating Namespace violation policy because Table violation"
                  + " policy is already in effect for " + tableInNS);
            }
          } else {
            // No table policy, so enact namespace policy
            LOG.info(tableInNS + " moving into violation of namespace space quota");
            this.snapshotNotifier.transitionTable(tableInNS, targetSnapshot);
          }
        }
      }
    }
  }

  /**
   * Removes region reports over a certain age.
   */
  void pruneOldRegionReports() {
    final long now = EnvironmentEdgeManager.currentTime();
    final long pruneTime = now - regionReportLifetimeMillis;
    final int numRemoved = quotaManager.pruneEntriesOlderThan(pruneTime,this);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Removed " + numRemoved + " old region size reports that were older than "
          + pruneTime + ".");
    }
  }

  /**
   * Computes the set of all tables that have quotas defined. This includes tables with quotas
   * explicitly set on them, in addition to tables that exist namespaces which have a quota
   * defined.
   */
  TablesWithQuotas fetchAllTablesWithQuotasDefined() throws IOException {
    final Scan scan = QuotaTableUtil.makeScan(null);
    final TablesWithQuotas tablesWithQuotas = new TablesWithQuotas(conn, conf);
    try (final QuotaRetriever scanner = new QuotaRetriever()) {
      scanner.init(conn, scan);
      for (QuotaSettings quotaSettings : scanner) {
        // Only one of namespace and tablename should be 'null'
        final String namespace = quotaSettings.getNamespace();
        final TableName tableName = quotaSettings.getTableName();
        if (QuotaType.SPACE != quotaSettings.getQuotaType()) {
          continue;
        }

        if (namespace != null) {
          assert tableName == null;
          // Collect all of the tables in the namespace
          TableName[] tablesInNS = conn.getAdmin().listTableNamesByNamespace(namespace);
          for (TableName tableUnderNs : tablesInNS) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Adding " + tableUnderNs + " under " +  namespace
                  + " as having a namespace quota");
            }
            tablesWithQuotas.addNamespaceQuotaTable(tableUnderNs);
          }
        } else {
          assert tableName != null;
          if (LOG.isTraceEnabled()) {
            LOG.trace("Adding " + tableName + " as having table quota.");
          }
          // namespace is already null, must be a non-null tableName
          tablesWithQuotas.addTableQuotaTable(tableName);
        }
      }
      return tablesWithQuotas;
    }
  }

  QuotaSnapshotStore<TableName> getTableSnapshotStore() {
    return tableSnapshotStore;
  }

  QuotaSnapshotStore<String> getNamespaceSnapshotStore() {
    return namespaceSnapshotStore;
  }

  /**
   * Returns an unmodifiable view over the current {@link SpaceQuotaSnapshot} objects
   * for each HBase table with a quota defined.
   */
  public Map<TableName,SpaceQuotaSnapshot> getTableQuotaSnapshots() {
    return readOnlyTableQuotaSnapshots;
  }

  /**
   * Returns an unmodifiable view over the current {@link SpaceQuotaSnapshot} objects
   * for each HBase namespace with a quota defined.
   */
  public Map<String,SpaceQuotaSnapshot> getNamespaceQuotaSnapshots() {
    return readOnlyNamespaceSnapshots;
  }

  /**
   * Fetches the {@link SpaceQuotaSnapshot} for the given table.
   */
  SpaceQuotaSnapshot getTableQuotaSnapshot(TableName table) {
    SpaceQuotaSnapshot state = this.tableQuotaSnapshots.get(table);
    if (state == null) {
      // No tracked state implies observance.
      return QuotaSnapshotStore.NO_QUOTA;
    }
    return state;
  }

  /**
   * Stores the quota state for the given table.
   */
  void setTableQuotaSnapshot(TableName table, SpaceQuotaSnapshot snapshot) {
    this.tableQuotaSnapshots.put(table, snapshot);
  }

  /**
   * Fetches the {@link SpaceQuotaSnapshot} for the given namespace from this chore.
   */
  SpaceQuotaSnapshot getNamespaceQuotaSnapshot(String namespace) {
    SpaceQuotaSnapshot state = this.namespaceQuotaSnapshots.get(namespace);
    if (state == null) {
      // No tracked state implies observance.
      return QuotaSnapshotStore.NO_QUOTA;
    }
    return state;
  }

  /**
   * Stores the given {@code snapshot} for the given {@code namespace} in this chore.
   */
  void setNamespaceQuotaSnapshot(String namespace, SpaceQuotaSnapshot snapshot) {
    this.namespaceQuotaSnapshots.put(namespace, snapshot);
  }

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value in the given timeunit.
   * @see #getTimeUnit(Configuration)
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(QUOTA_OBSERVER_CHORE_PERIOD_KEY,
        QUOTA_OBSERVER_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value in the given timeunit.
   * @see #getTimeUnit(Configuration)
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(QUOTA_OBSERVER_CHORE_DELAY_KEY,
        QUOTA_OBSERVER_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The
   * configuration value for {@link #QUOTA_OBSERVER_CHORE_TIMEUNIT_KEY} must correspond to
   * a {@link TimeUnit} value.
   *
   * @param conf The configuration object.
   * @return The configured time unit for the chore period and initial delay or the default value.
   */
  static TimeUnit getTimeUnit(Configuration conf) {
    return TimeUnit.valueOf(conf.get(QUOTA_OBSERVER_CHORE_TIMEUNIT_KEY,
        QUOTA_OBSERVER_CHORE_TIMEUNIT_DEFAULT));
  }

  /**
   * Extracts the percent of Regions for a table to have been reported to enable quota violation
   * state change.
   *
   * @param conf The configuration object.
   * @return The percent of regions reported to use.
   */
  static Double getRegionReportPercent(Configuration conf) {
    return conf.getDouble(QUOTA_OBSERVER_CHORE_REPORT_PERCENT_KEY,
        QUOTA_OBSERVER_CHORE_REPORT_PERCENT_DEFAULT);
  }

  /**
   * A container which encapsulates the tables that have either a table quota or are contained in a
   * namespace which have a namespace quota.
   */
  static class TablesWithQuotas {
    private final Set<TableName> tablesWithTableQuotas = new HashSet<>();
    private final Set<TableName> tablesWithNamespaceQuotas = new HashSet<>();
    private final Connection conn;
    private final Configuration conf;

    public TablesWithQuotas(Connection conn, Configuration conf) {
      this.conn = Objects.requireNonNull(conn);
      this.conf = Objects.requireNonNull(conf);
    }

    Configuration getConfiguration() {
      return conf;
    }

    /**
     * Adds a table with a table quota.
     */
    public void addTableQuotaTable(TableName tn) {
      tablesWithTableQuotas.add(tn);
    }

    /**
     * Adds a table with a namespace quota.
     */
    public void addNamespaceQuotaTable(TableName tn) {
      tablesWithNamespaceQuotas.add(tn);
    }

    /**
     * Returns true if the given table has a table quota.
     */
    public boolean hasTableQuota(TableName tn) {
      return tablesWithTableQuotas.contains(tn);
    }

    /**
     * Returns true if the table exists in a namespace with a namespace quota.
     */
    public boolean hasNamespaceQuota(TableName tn) {
      return tablesWithNamespaceQuotas.contains(tn);
    }

    /**
     * Returns an unmodifiable view of all tables with table quotas.
     */
    public Set<TableName> getTableQuotaTables() {
      return Collections.unmodifiableSet(tablesWithTableQuotas);
    }

    /**
     * Returns an unmodifiable view of all tables in namespaces that have
     * namespace quotas.
     */
    public Set<TableName> getNamespaceQuotaTables() {
      return Collections.unmodifiableSet(tablesWithNamespaceQuotas);
    }

    public Set<String> getNamespacesWithQuotas() {
      Set<String> namespaces = new HashSet<>();
      for (TableName tn : tablesWithNamespaceQuotas) {
        namespaces.add(tn.getNamespaceAsString());
      }
      return namespaces;
    }

    /**
     * Returns a view of all tables that reside in a namespace with a namespace
     * quota, grouped by the namespace itself.
     */
    public Multimap<String,TableName> getTablesByNamespace() {
      Multimap<String,TableName> tablesByNS = HashMultimap.create();
      for (TableName tn : tablesWithNamespaceQuotas) {
        tablesByNS.put(tn.getNamespaceAsString(), tn);
      }
      return tablesByNS;
    }

    /**
     * Filters out all tables for which the Master currently doesn't have enough region space
     * reports received from RegionServers yet.
     */
    public Set<TableName> filterInsufficientlyReportedTables(
        QuotaSnapshotStore<TableName> tableStore) throws IOException {
      final double percentRegionsReportedThreshold = getRegionReportPercent(getConfiguration());
      Set<TableName> tablesToRemove = new HashSet<>();
      for (TableName table : Iterables.concat(tablesWithTableQuotas, tablesWithNamespaceQuotas)) {
        // Don't recompute a table we've already computed
        if (tablesToRemove.contains(table)) {
          continue;
        }
        final int numRegionsInTable = getNumRegions(table);
        // If the table doesn't exist (no regions), bail out.
        if (numRegionsInTable == 0) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Filtering " + table + " because no regions were reported");
          }
          tablesToRemove.add(table);
          continue;
        }
        final int reportedRegionsInQuota = getNumReportedRegions(table, tableStore);
        final double ratioReported = ((double) reportedRegionsInQuota) / numRegionsInTable;
        if (ratioReported < percentRegionsReportedThreshold) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Filtering " + table + " because " + reportedRegionsInQuota  + " of " +
                numRegionsInTable + " regions were reported.");
          }
          tablesToRemove.add(table);
        } else if (LOG.isTraceEnabled()) {
          LOG.trace("Retaining " + table + " because " + reportedRegionsInQuota  + " of " +
              numRegionsInTable + " regions were reported.");
        }
      }
      for (TableName tableToRemove : tablesToRemove) {
        tablesWithTableQuotas.remove(tableToRemove);
        tablesWithNamespaceQuotas.remove(tableToRemove);
      }
      return tablesToRemove;
    }

    /**
     * Computes the total number of regions in a table.
     */
    int getNumRegions(TableName table) throws IOException {
      List<RegionInfo> regions = this.conn.getAdmin().getRegions(table);
      if (regions == null) {
        return 0;
      }
      // Filter the region replicas if any and return the original number of regions for a table.
      RegionReplicaUtil.removeNonDefaultRegions(regions);
      return regions.size();
    }

    /**
     * Computes the number of regions reported for a table.
     */
    int getNumReportedRegions(TableName table, QuotaSnapshotStore<TableName> tableStore)
        throws IOException {
      return Iterables.size(tableStore.filterBySubject(table));
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder(32);
      sb.append(getClass().getSimpleName())
          .append(": tablesWithTableQuotas=")
          .append(this.tablesWithTableQuotas)
          .append(", tablesWithNamespaceQuotas=")
          .append(this.tablesWithNamespaceQuotas);
      return sb.toString();
    }
  }
}
