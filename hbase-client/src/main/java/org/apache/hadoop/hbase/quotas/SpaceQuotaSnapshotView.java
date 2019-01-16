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

import java.util.Optional;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A point-in-time view of a space quota on a table, read only.
 */
@InterfaceAudience.Public
public interface SpaceQuotaSnapshotView {

  /**
   * Encapsulates the state of a quota on a table. The quota may or may not be in violation. If the
   * quota is not in violation, the violation may not be presented. If the quota is in violation,
   * there is guaranteed to be presented.
   */
  @InterfaceAudience.Public
  interface SpaceQuotaStatusView {
    /**
     * Returns the violation policy, which may not be presented. It is guaranteed to be presented if
     * {@link #isInViolation()} is {@code true}, but may not be presented otherwise.
     */
    Optional<SpaceViolationPolicy> getPolicy();

    /**
     * @return {@code true} if the quota is being violated, {@code false} otherwise.
     */
    boolean isInViolation();
  }

  /**
   * Returns the status of the quota.
   */
  SpaceQuotaStatusView getQuotaStatus();

  /**
   * Returns the current usage, in bytes, of the target (e.g. table, namespace).
   */
  long getUsage();

  /**
   * Returns the limit, in bytes, of the target (e.g. table, namespace).
   */
  long getLimit();
}
