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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A {@link ScheduledChore} which periodically updates a local copy of tables which have
 * space quota violation policies enacted on them.
 */
@InterfaceAudience.Private
public class SpaceQuotaViolationPolicyRefresherChore extends ScheduledChore {
  private static final Log LOG = LogFactory.getLog(SpaceQuotaViolationPolicyRefresherChore.class);

  static final String POLICY_REFRESHER_CHORE_PERIOD_KEY =
      "hbase.regionserver.quotas.policy.refresher.chore.period";
  static final int POLICY_REFRESHER_CHORE_PERIOD_DEFAULT = 1000 * 60 * 5; // 5 minutes in millis

  static final String POLICY_REFRESHER_CHORE_DELAY_KEY =
      "hbase.regionserver.quotas.policy.refresher.chore.delay";
  static final long POLICY_REFRESHER_CHORE_DELAY_DEFAULT = 1000L * 60L; // 1 minute

  static final String POLICY_REFRESHER_CHORE_TIMEUNIT_KEY =
      "hbase.regionserver.quotas.policy.refresher.chore.timeunit";
  static final String POLICY_REFRESHER_CHORE_TIMEUNIT_DEFAULT = TimeUnit.MILLISECONDS.name();

  static final String POLICY_REFRESHER_CHORE_REPORT_PERCENT_KEY =
      "hbase.regionserver.quotas.policy.refresher.report.percent";
  static final double POLICY_REFRESHER_CHORE_REPORT_PERCENT_DEFAULT= 0.95;

  private final RegionServerSpaceQuotaManager manager;

  public SpaceQuotaViolationPolicyRefresherChore(RegionServerSpaceQuotaManager manager) {
    super(SpaceQuotaViolationPolicyRefresherChore.class.getSimpleName(),
        manager.getRegionServerServices(),
        getPeriod(manager.getRegionServerServices().getConfiguration()),
        getInitialDelay(manager.getRegionServerServices().getConfiguration()),
        getTimeUnit(manager.getRegionServerServices().getConfiguration()));
    this.manager = manager;
  }

  @Override
  protected void chore() {
    // Tables with a policy currently enforced
    final Map<TableName, SpaceViolationPolicy> activeViolationPolicies;
    // Tables with policies that should be enforced
    final Map<TableName, SpaceViolationPolicy> violationPolicies;
    try {
      // Tables with a policy currently enforced
      activeViolationPolicies = manager.getActiveViolationPolicyEnforcements();
      // Tables with policies that should be enforced
      violationPolicies = manager.getViolationPoliciesToEnforce();
    } catch (IOException e) {
      LOG.warn("Failed to fetch enforced quota violation policies, will retry.", e);
      return;
    }
    // Ensure each policy which should be enacted is enacted.
    for (Entry<TableName, SpaceViolationPolicy> entry : violationPolicies.entrySet()) {
      final TableName tableName = entry.getKey();
      final SpaceViolationPolicy policyToEnforce = entry.getValue();
      final SpaceViolationPolicy currentPolicy = activeViolationPolicies.get(tableName);
      if (currentPolicy != policyToEnforce) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Enabling " + policyToEnforce + " on " + tableName);
        }
        manager.enforceViolationPolicy(tableName, policyToEnforce);
      }
    }
    // Remove policies which should no longer be enforced
    Iterator<TableName> iter = activeViolationPolicies.keySet().iterator();
    while (iter.hasNext()) {
      final TableName localTableWithPolicy = iter.next();
      if (!violationPolicies.containsKey(localTableWithPolicy)) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Removing quota violation policy on " + localTableWithPolicy);
        }
        manager.disableViolationPolicyEnforcement(localTableWithPolicy);
        iter.remove();
      }
    }
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
