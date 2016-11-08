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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota;

/**
 * A common interface for computing and storing space quota observance/violation for entities.
 *
 * An entity is presently a table or a namespace.
 */
@InterfaceAudience.Private
public interface QuotaViolationStore<T> {

  /**
   * The current state of a table with respect to the policy set forth by a quota.
   */
  @InterfaceAudience.Private
  public enum ViolationState {
    IN_VIOLATION,
    IN_OBSERVANCE,
  }

  /**
   * Fetch the Quota for the given {@code subject}. May be null.
   *
   * @param subject The object for which the quota should be fetched
   */
  SpaceQuota getSpaceQuota(T subject) throws IOException;

  /**
   * Returns the current {@link ViolationState} for the given {@code subject}.
   *
   * @param subject The object which the quota violation state should be fetched
   */
  ViolationState getCurrentState(T subject);

  /**
   * Computes the target {@link ViolationState} for the given {@code subject} and
   * {@code spaceQuota}.
   *
   * @param subject The object which to determine the target quota violation state of
   * @param spaceQuota The quota "definition" for the {@code subject}
   */
  ViolationState getTargetState(T subject, SpaceQuota spaceQuota);

  /**
   * Filters the provided <code>regions</code>, returning those which match the given
   * <code>subject</code>.
   *
   * @param subject The filter criteria. Only regions belonging to this parameter will be returned
   */
  Iterable<Entry<HRegionInfo,Long>> filterBySubject(T subject);

  /**
   * Persists the current {@link ViolationState} for the {@code subject}.
   *
   * @param subject The object which the {@link ViolationState} is being persisted for
   * @param state The current {@link ViolationState} of the {@code subject}
   */
  void setCurrentState(T subject, ViolationState state);

  /**
   * Updates {@code this} with the latest snapshot of filesystem use by region.
   *
   * @param regionUsage A map of {@code HRegionInfo} objects to their filesystem usage in bytes
   */
  void setRegionUsage(Map<HRegionInfo,Long> regionUsage);
}
