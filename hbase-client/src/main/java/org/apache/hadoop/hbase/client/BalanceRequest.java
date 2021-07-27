/*
 *
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
import org.apache.yetus.audience.InterfaceStability;

/**
 * Encapsulates options for executing an unscheduled run of the Balancer.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BalanceRequest {
  private static final BalanceRequest DEFAULT = BalanceRequest.newBuilder().build();

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public final static class Builder {
    private boolean dryRun = false;
    private boolean ignoreRegionsInTransition = false;

    private Builder() {}

    /**
     * Creates a BalancerRequest which runs the balancer in dryRun mode.
     * In this mode, the balancer will try to find a plan but WILL NOT
     * execute any region moves or call any coprocessors.
     *
     * You can run in dryRun mode regardless of whether the balancer switch
     * is enabled or disabled, but dryRun mode will not run over an existing
     * request or chore.
     *
     * Dry run is useful for testing out new balance configs. See the logs
     * on the active HMaster for the results of the dry run.
     */
    public Builder setDryRun(boolean dryRun) {
      this.dryRun = dryRun;
      return this;
    }

    /**
     * Creates a BalancerRequest to cause the balancer to run even if there
     * are regions in transition.
     *
     * WARNING: Advanced usage only, this could cause more issues than it fixes.
     */
    public Builder setIgnoreRegionsInTransition(boolean ignoreRegionsInTransition) {
      this.ignoreRegionsInTransition = ignoreRegionsInTransition;
      return this;
    }

    public BalanceRequest build() {
      return new BalanceRequest(dryRun, ignoreRegionsInTransition);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static BalanceRequest defaultInstance() {
    return DEFAULT;
  }

  private final boolean dryRun;
  private final boolean ignoreRegionsInTransition;

  private BalanceRequest(boolean dryRun, boolean ignoreRegionsInTransition) {
    this.dryRun = dryRun;
    this.ignoreRegionsInTransition = ignoreRegionsInTransition;
  }

  public boolean isDryRun() {
    return dryRun;
  }

  public boolean isIgnoreRegionsInTransition() {
    return ignoreRegionsInTransition;
  }
}
