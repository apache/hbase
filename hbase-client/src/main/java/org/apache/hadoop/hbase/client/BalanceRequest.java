/*
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
package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Encapsulates options for executing a run of the Balancer.
 */
@InterfaceAudience.Public
public final class BalanceRequest {
  private static final BalanceRequest DEFAULT = BalanceRequest.newBuilder().build();

  /**
   * Builder for constructing a {@link BalanceRequest}
   */
  @InterfaceAudience.Public
  public final static class Builder {
    private boolean dryRun = false;
    private boolean ignoreRegionsInTransition = false;

    private Builder() {
    }

    /**
     * Updates BalancerRequest to run the balancer in dryRun mode. In this mode, the balancer will
     * try to find a plan but WILL NOT execute any region moves or call any coprocessors. You can
     * run in dryRun mode regardless of whether the balancer switch is enabled or disabled, but
     * dryRun mode will not run over an existing request or chore. Dry run is useful for testing out
     * new balance configs. See the logs on the active HMaster for the results of the dry run.
     */
    public Builder setDryRun(boolean dryRun) {
      this.dryRun = dryRun;
      return this;
    }

    /**
     * Updates BalancerRequest to run the balancer even if there are regions in transition. WARNING:
     * Advanced usage only, this could cause more issues than it fixes.
     */
    public Builder setIgnoreRegionsInTransition(boolean ignoreRegionsInTransition) {
      this.ignoreRegionsInTransition = ignoreRegionsInTransition;
      return this;
    }

    /**
     * Build the {@link BalanceRequest}
     */
    public BalanceRequest build() {
      return new BalanceRequest(dryRun, ignoreRegionsInTransition);
    }
  }

  /**
   * Create a builder to construct a custom {@link BalanceRequest}.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Get a BalanceRequest for a default run of the balancer. The default mode executes any moves
   * calculated and will not run if regions are already in transition.
   */
  public static BalanceRequest defaultInstance() {
    return DEFAULT;
  }

  private final boolean dryRun;
  private final boolean ignoreRegionsInTransition;

  private BalanceRequest(boolean dryRun, boolean ignoreRegionsInTransition) {
    this.dryRun = dryRun;
    this.ignoreRegionsInTransition = ignoreRegionsInTransition;
  }

  /**
   * Returns true if the balancer should run in dry run mode, otherwise false. In dry run mode,
   * moves will be calculated but not executed.
   */
  public boolean isDryRun() {
    return dryRun;
  }

  /**
   * Returns true if the balancer should execute even if regions are in transition, otherwise false.
   * This is an advanced usage feature, as it can cause more issues than it fixes.
   */
  public boolean isIgnoreRegionsInTransition() {
    return ignoreRegionsInTransition;
  }
}
