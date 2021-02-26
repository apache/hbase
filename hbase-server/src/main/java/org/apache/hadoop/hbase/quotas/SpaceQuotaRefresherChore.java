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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ScheduledChore} which periodically updates the {@link RegionServerSpaceQuotaManager}
 * with information from the hbase:quota.
 */
@InterfaceAudience.Private
public class SpaceQuotaRefresherChore extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(SpaceQuotaRefresherChore.class);

  static final String POLICY_REFRESHER_CHORE_PERIOD_KEY =
      "hbase.regionserver.quotas.policy.refresher.chore.period";
  static final int POLICY_REFRESHER_CHORE_PERIOD_DEFAULT = 1000 * 60 * 1; // 1 minute in millis

  static final String POLICY_REFRESHER_CHORE_DELAY_KEY =
      "hbase.regionserver.quotas.policy.refresher.chore.delay";
  static final long POLICY_REFRESHER_CHORE_DELAY_DEFAULT = 1000L * 15L; // 15 seconds in millis

  static final String POLICY_REFRESHER_CHORE_TIMEUNIT_KEY =
      "hbase.regionserver.quotas.policy.refresher.chore.timeunit";
  static final String POLICY_REFRESHER_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  static final String POLICY_REFRESHER_CHORE_REPORT_PERCENT_KEY =
      "hbase.regionserver.quotas.policy.refresher.report.percent";
  static final double POLICY_REFRESHER_CHORE_REPORT_PERCENT_DEFAULT= 0.95;

  private final RegionServerSpaceQuotaManager manager;
  private final Connection conn;
  private boolean quotaTablePresent = false;

  public SpaceQuotaRefresherChore(RegionServerSpaceQuotaManager manager, Connection conn) {
    super(SpaceQuotaRefresherChore.class.getSimpleName(),
        manager.getRegionServerServices(),
        getPeriod(manager.getRegionServerServices().getConfiguration()),
        getInitialDelay(manager.getRegionServerServices().getConfiguration()),
        getTimeUnit(manager.getRegionServerServices().getConfiguration()));
    this.manager = manager;
    this.conn = conn;
  }

  @Override
  protected void chore() {
    try {
      // check whether quotaTable is present or not.
      if (!quotaTablePresent && !checkQuotaTableExists()) {
        LOG.info("Quota table not found, skipping quota manager cache refresh.");
        return;
      }
      // since quotaTable is present so setting the flag as true.
      quotaTablePresent = true;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Reading current quota snapshots from hbase:quota.");
      }
      // Get the snapshots that the quota manager is currently aware of
      final Map<TableName, SpaceQuotaSnapshot> currentSnapshots =
          getManager().copyQuotaSnapshots();
      // Read the new snapshots from the quota table
      final Map<TableName, SpaceQuotaSnapshot> newSnapshots = fetchSnapshotsFromQuotaTable();
      if (LOG.isTraceEnabled()) {
        LOG.trace(currentSnapshots.size() + " table quota snapshots are collected, "
            + "read " + newSnapshots.size() + " from the quota table.");
      }
      // Iterate over each new quota snapshot
      for (Entry<TableName, SpaceQuotaSnapshot> entry : newSnapshots.entrySet()) {
        final TableName tableName = entry.getKey();
        final SpaceQuotaSnapshot newSnapshot = entry.getValue();
        // May be null!
        final SpaceQuotaSnapshot currentSnapshot = currentSnapshots.get(tableName);
        if (LOG.isTraceEnabled()) {
          LOG.trace(tableName + ": current=" + currentSnapshot + ", new=" + newSnapshot);
        }
        if (!newSnapshot.equals(currentSnapshot)) {
          // We have a new snapshot.
          // We might need to enforce it or disable the enforcement or switch policy
          boolean currInViolation = isInViolation(currentSnapshot);
          boolean newInViolation = newSnapshot.getQuotaStatus().isInViolation();
          if (!currInViolation && newInViolation) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Enabling " + newSnapshot + " on " + tableName);
            }
            getManager().enforceViolationPolicy(tableName, newSnapshot);
          } else if (currInViolation && !newInViolation) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Removing quota violation policy on " + tableName);
            }
            getManager().disableViolationPolicyEnforcement(tableName);
          } else if (currInViolation && newInViolation) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Switching quota violation policy on " + tableName + " from "
                  + currentSnapshot + " to " + newSnapshot);
            }
            getManager().enforceViolationPolicy(tableName, newSnapshot);
          }
        }
      }

      // Disable violation policy for all such tables which have been removed in new snapshot
      for (TableName tableName : currentSnapshots.keySet()) {
        // check whether table was removed in new snapshot
        if (!newSnapshots.containsKey(tableName)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Removing quota violation policy on " + tableName);
          }
          getManager().disableViolationPolicyEnforcement(tableName);
        }
      }

      // We're intentionally ignoring anything extra with the currentSnapshots. If we were missing
      // information from the RegionServers to create an accurate SpaceQuotaSnapshot in the Master,
      // the Master will generate a new SpaceQuotaSnapshot which represents this state. This lets
      // us avoid having to do anything special with currentSnapshots here.

      // Update the snapshots in the manager
      getManager().updateQuotaSnapshot(newSnapshots);
    } catch (IOException e) {
      LOG.warn(
          "Caught exception while refreshing enforced quota violation policies, will retry.", e);
    }
  }

  /**
   * Checks if hbase:quota exists in hbase:meta
   *
   * @return true if hbase:quota table is in meta, else returns false.
   * @throws IOException throws IOException
   */
  boolean checkQuotaTableExists() throws IOException {
    try (Admin admin = getConnection().getAdmin()) {
      return admin.tableExists(QuotaUtil.QUOTA_TABLE_NAME);
    }
  }

  /**
   * Checks if the given <code>snapshot</code> is in violation, allowing the snapshot to be null.
   * If the snapshot is null, this is interpreted as no snapshot which implies not in violation.
   *
   * @param snapshot The snapshot to operate on.
   * @return true if the snapshot is in violation, false otherwise.
   */
  boolean isInViolation(SpaceQuotaSnapshot snapshot) {
    if (snapshot == null) {
      return false;
    }
    return snapshot.getQuotaStatus().isInViolation();
  }

  /**
   * Reads all quota snapshots from the quota table.
   *
   * @return The current "view" of space use by each table.
   */
  public Map<TableName, SpaceQuotaSnapshot> fetchSnapshotsFromQuotaTable() throws IOException {
    try (Table quotaTable = getConnection().getTable(QuotaUtil.QUOTA_TABLE_NAME);
        ResultScanner scanner = quotaTable.getScanner(QuotaTableUtil.makeQuotaSnapshotScan())) {
      Map<TableName,SpaceQuotaSnapshot> snapshots = new HashMap<>();
      for (Result result : scanner) {
        try {
          extractQuotaSnapshot(result, snapshots);
        } catch (IllegalArgumentException e) {
          final String msg = "Failed to parse result for row " + Bytes.toString(result.getRow());
          LOG.error(msg, e);
          throw new IOException(msg, e);
        }
      }
      return snapshots;
    }
  }

  /**
   * Wrapper around {@link QuotaTableUtil#extractQuotaSnapshot(Result, Map)} for testing.
   */
  void extractQuotaSnapshot(Result result, Map<TableName,SpaceQuotaSnapshot> snapshots) {
    QuotaTableUtil.extractQuotaSnapshot(result, snapshots);
  }

  Connection getConnection() {
    return conn;
  }

  RegionServerSpaceQuotaManager getManager() {
    return manager;
  }

  /**
   * Extracts the period for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore period or the default value.
   */
  static int getPeriod(Configuration conf) {
    return conf.getInt(POLICY_REFRESHER_CHORE_PERIOD_KEY,
        POLICY_REFRESHER_CHORE_PERIOD_DEFAULT);
  }

  /**
   * Extracts the initial delay for the chore from the configuration.
   *
   * @param conf The configuration object.
   * @return The configured chore initial delay or the default value.
   */
  static long getInitialDelay(Configuration conf) {
    return conf.getLong(POLICY_REFRESHER_CHORE_DELAY_KEY,
        POLICY_REFRESHER_CHORE_DELAY_DEFAULT);
  }

  /**
   * Extracts the time unit for the chore period and initial delay from the configuration. The
   * configuration value for {@link #POLICY_REFRESHER_CHORE_TIMEUNIT_KEY} must correspond to
   * a {@link TimeUnit} value.
   *
   * @param conf The configuration object.
   * @return The configured time unit for the chore period and initial delay or the default value.
   */
  static TimeUnit getTimeUnit(Configuration conf) {
    return TimeUnit.valueOf(conf.get(POLICY_REFRESHER_CHORE_TIMEUNIT_KEY,
        POLICY_REFRESHER_CHORE_TIMEUNIT_DEFAULT));
  }

  /**
   * Extracts the percent of Regions for a table to have been reported to enable quota violation
   * state change.
   *
   * @param conf The configuration object.
   * @return The percent of regions reported to use.
   */
  static Double getRegionReportPercent(Configuration conf) {
    return conf.getDouble(POLICY_REFRESHER_CHORE_REPORT_PERCENT_KEY,
        POLICY_REFRESHER_CHORE_REPORT_PERCENT_DEFAULT);
  }
}
