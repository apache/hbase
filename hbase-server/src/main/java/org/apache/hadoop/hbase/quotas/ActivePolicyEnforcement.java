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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * A class to ease dealing with tables that have and do not have violation policies
 * being enforced. This class is immutable, expect for {@code locallyCachedPolicies}.
 *
 * The {@code locallyCachedPolicies} are mutable given the current {@code activePolicies}
 * and {@code snapshots}. It is expected that when a new instance of this class is
 * instantiated, we also want to invalidate those previously cached policies (as they
 * may now be invalidate if we received new quota usage information).
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ActivePolicyEnforcement {
  private final Map<TableName,SpaceViolationPolicyEnforcement> activePolicies;
  private final Map<TableName,SpaceQuotaSnapshot> snapshots;
  private final RegionServerServices rss;
  private final SpaceViolationPolicyEnforcementFactory factory;
  private final Map<TableName,SpaceViolationPolicyEnforcement> locallyCachedPolicies;

  public ActivePolicyEnforcement(Map<TableName,SpaceViolationPolicyEnforcement> activePolicies,
      Map<TableName,SpaceQuotaSnapshot> snapshots, RegionServerServices rss) {
    this(activePolicies, snapshots, rss, SpaceViolationPolicyEnforcementFactory.getInstance());
  }

  public ActivePolicyEnforcement(Map<TableName,SpaceViolationPolicyEnforcement> activePolicies,
      Map<TableName,SpaceQuotaSnapshot> snapshots, RegionServerServices rss,
      SpaceViolationPolicyEnforcementFactory factory) {
    this.activePolicies = activePolicies;
    this.snapshots = snapshots;
    this.rss = rss;
    this.factory = factory;
    // Mutable!
    this.locallyCachedPolicies = new HashMap<>();
  }

  /**
   * Returns the proper {@link SpaceViolationPolicyEnforcement} implementation for the given table.
   * If the given table does not have a violation policy enforced, a "no-op" policy will
   * be returned which always allows an action.
   *
   * @see #getPolicyEnforcement(TableName)
   */
  public SpaceViolationPolicyEnforcement getPolicyEnforcement(Region r) {
    return getPolicyEnforcement(Objects.requireNonNull(r).getTableDescriptor().getTableName());
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
    if (policy == null) {
      synchronized (locallyCachedPolicies) {
        // When we don't have an policy enforcement for the table, there could be one of two cases:
        //  1) The table has no quota defined
        //  2) The table is not in violation of its quota
        // In both of these cases, we want to make sure that access remains fast and we minimize
        // object creation. We can accomplish this by locally caching policies instead of creating
        // a new instance of the policy each time.
        policy = locallyCachedPolicies.get(tableName);
        // We have already created/cached the enforcement, use it again. `activePolicies` and
        // `snapshots` are immutable, thus this policy is valid for the lifetime of `this`.
        if (policy != null) {
          return policy;
        }
        // Create a PolicyEnforcement for this table and snapshot. The snapshot may be null
        // which is OK.
        policy = factory.createWithoutViolation(rss, tableName, snapshots.get(tableName));
        // Cache the policy we created
        locallyCachedPolicies.put(tableName, policy);
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

  /**
   * Returns an unmodifiable version of the policy enforcements that were cached because they are
   * not in violation of their quota.
   */
  Map<TableName,SpaceViolationPolicyEnforcement> getLocallyCachedPolicies() {
    return Collections.unmodifiableMap(locallyCachedPolicies);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ": " + activePolicies;
  }
}
