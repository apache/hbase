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
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot.SpaceQuotaStatus;
import org.apache.hadoop.hbase.quotas.policies.BulkLoadVerifyingViolationPolicyEnforcement;
import org.apache.hadoop.hbase.quotas.policies.DisableTableViolationPolicyEnforcement;
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
    switch (status.getPolicy()) {
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
   * violation. This is used to have uniform policy checking for tables in and not quotas.
   */
  public SpaceViolationPolicyEnforcement createWithoutViolation(
      RegionServerServices rss, TableName tableName, SpaceQuotaSnapshot snapshot) {
    SpaceQuotaStatus status = snapshot.getQuotaStatus();
    if (status.isInViolation()) {
      throw new IllegalArgumentException(
          tableName + " is in violation. Logic error. Snapshot=" + snapshot);
    }
    BulkLoadVerifyingViolationPolicyEnforcement enforcement = new BulkLoadVerifyingViolationPolicyEnforcement();
    enforcement.initialize(rss, tableName, snapshot);
    return enforcement;
  }
}
