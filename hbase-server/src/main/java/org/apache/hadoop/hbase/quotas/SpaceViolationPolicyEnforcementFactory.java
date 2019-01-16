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

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.policies.DefaultViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.DisableTableViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.MissingSnapshotViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoInsertsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesCompactionsViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.NoWritesViolationPolicyEnforcement;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * A factory class for instantiating {@link SpaceViolationPolicyEnforcement} instances.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SpaceViolationPolicyEnforcementFactory {

  private static final SpaceViolationPolicyEnforcementFactory INSTANCE =
      new SpaceViolationPolicyEnforcementFactory();

  private SpaceViolationPolicyEnforcementFactory() {}

  /**
   * Returns an instance of this factory.
   */
  public static SpaceViolationPolicyEnforcementFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Constructs the appropriate {@link SpaceViolationPolicyEnforcement} for tables that are
   * in violation of their space quota.
   */
  public SpaceViolationPolicyEnforcement create(
      RegionServerServices rss, TableName tableName, SpaceQuotaSnapshot snapshot) {
    SpaceViolationPolicyEnforcement enforcement;
    SpaceQuotaStatus status = snapshot.getQuotaStatus();
    if (!status.isInViolation()) {
      throw new IllegalArgumentException(tableName + " is not in violation. Snapshot=" + snapshot);
    }
    switch (status.getPolicy().get()) {
      case DISABLE:
        enforcement = new DisableTableViolationPolicyEnforcement();
        break;
      case NO_WRITES_COMPACTIONS:
        enforcement = new NoWritesCompactionsViolationPolicyEnforcement();
        break;
      case NO_WRITES:
        enforcement = new NoWritesViolationPolicyEnforcement();
        break;
      case NO_INSERTS:
        enforcement = new NoInsertsViolationPolicyEnforcement();
        break;
      default:
        throw new IllegalArgumentException("Unhandled SpaceViolationPolicy: " + status.getPolicy());
    }
    enforcement.initialize(rss, tableName, snapshot);
    return enforcement;
  }

  /**
   * Creates the "default" {@link SpaceViolationPolicyEnforcement} for a table that isn't in
   * violation. This is used to have uniform policy checking for tables in and not quotas. This
   * policy will still verify that new bulk loads do not exceed the configured quota limit.
   *
   * @param rss RegionServerServices instance the policy enforcement should use.
   * @param tableName The target HBase table.
   * @param snapshot The current quota snapshot for the {@code tableName}, can be null.
   */
  public SpaceViolationPolicyEnforcement createWithoutViolation(
      RegionServerServices rss, TableName tableName, SpaceQuotaSnapshot snapshot) {
    if (snapshot == null) {
      // If we have no snapshot, this is equivalent to no quota for this table.
      // We should do use the (singleton instance) of this policy to do nothing.
      return MissingSnapshotViolationPolicyEnforcement.getInstance();
    }
    // We have a snapshot which means that there is a quota set on this table, but it's not in
    // violation of that quota. We need to construct a policy for this table.
    SpaceQuotaStatus status = snapshot.getQuotaStatus();
    if (status.isInViolation()) {
      throw new IllegalArgumentException(
          tableName + " is in violation. Logic error. Snapshot=" + snapshot);
    }
    // We have a unique size snapshot to use. Create an instance for this tablename + snapshot.
    DefaultViolationPolicyEnforcement enforcement = new DefaultViolationPolicyEnforcement();
    enforcement.initialize(rss, tableName, snapshot);
    return enforcement;
  }
}
