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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * A class to ease dealing with tables that have and do not have violation policies
 * being enforced in a uniform manner. Immutable.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ActivePolicyEnforcement {
  private final Map<TableName,SpaceViolationPolicyEnforcement> activePolicies;
  private final Map<TableName,SpaceQuotaSnapshot> snapshots;
  private final RegionServerServices rss;

  public ActivePolicyEnforcement(Map<TableName,SpaceViolationPolicyEnforcement> activePolicies,
      Map<TableName,SpaceQuotaSnapshot> snapshots, RegionServerServices rss) {
    this.activePolicies = activePolicies;
    this.snapshots = snapshots;
    this.rss = rss;
  }

  /**
   * Returns the proper {@link SpaceViolationPolicyEnforcement} implementation for the given table.
   * If the given table does not have a violation policy enforced, a "no-op" policy will
   * be returned which always allows an action.
   *
   * @see #getPolicyEnforcement(TableName)
   */
  public SpaceViolationPolicyEnforcement getPolicyEnforcement(Region r) {
    return getPolicyEnforcement(Objects.requireNonNull(r).getTableDesc().getTableName());
  }

  /**
   * Returns the proper {@link SpaceViolationPolicyEnforcement} implementation for the given table.
   * If the given table does not have a violation policy enforced, a "no-op" policy will
   * be returned which always allows an action.
   *
   * @param tableName The table to fetch the policy for.
   * @return A non-null {@link SpaceViolationPolicyEnforcement} instance.
   */
  public SpaceViolationPolicyEnforcement getPolicyEnforcement(TableName tableName) {
    SpaceViolationPolicyEnforcement policy = activePolicies.get(Objects.requireNonNull(tableName));
    if (null == policy) {
      synchronized (activePolicies) {
        // If we've never seen a snapshot, assume no use, and infinite limit
        SpaceQuotaSnapshot snapshot = snapshots.get(tableName);
        if (null == snapshot) {
          snapshot = SpaceQuotaSnapshot.getNoSuchSnapshot();
        }
        // Create the default policy and cache it
        return SpaceViolationPolicyEnforcementFactory.getInstance().createWithoutViolation(
            rss, tableName, snapshot);
      }
    }
    return policy;
  }

  /**
   * Returns an unmodifiable version of the active {@link SpaceViolationPolicyEnforcement}s.
   */
  public Map<TableName,SpaceViolationPolicyEnforcement> getPolicies() {
    return Collections.unmodifiableMap(activePolicies);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + activePolicies;
  }
}
