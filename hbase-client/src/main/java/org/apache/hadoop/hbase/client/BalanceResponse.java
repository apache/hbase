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
 * Response returned from a balancer invocation
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class BalanceResponse {

  @InterfaceAudience.Public
  @InterfaceStability.Evolving
  public final static class Builder {
    private boolean balancerRan;
    private int movesCalculated;
    private int movesExecuted;

    private Builder() {}

    public Builder setBalancerRan(boolean balancerRan) {
      this.balancerRan = balancerRan;
      return this;
    }

    public Builder setMovesCalculated(int movesCalculated) {
      this.movesCalculated = movesCalculated;
      return this;
    }

    public Builder setMovesExecuted(int movesExecuted) {
      this.movesExecuted = movesExecuted;
      return this;
    }

    public BalanceResponse build() {
      return new BalanceResponse(balancerRan, movesCalculated, movesExecuted);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  private final boolean balancerRan;
  private final int movesCalculated;
  private final int movesExecuted;

  private BalanceResponse(boolean balancerRan, int movesCalculated, int movesExecuted) {
    this.balancerRan = balancerRan;
    this.movesCalculated = movesCalculated;
    this.movesExecuted = movesExecuted;
  }

  /**
   * Determines whether the balancer ran or not. The balancer may not run for a variety of reasons,
   * such as: another balance is running, there are regions in transition, the cluster is in
   * maintenance mode, etc.
   */
  public boolean isBalancerRan() {
    return balancerRan;
  }

  /**
   * The number of moves calculated by the balancer if it ran. This may be zero if
   * no better balance could be found.
   */
  public int getMovesCalculated() {
    return movesCalculated;
  }

  /**
   * The number of moves actually executed by the balancer if it ran. This will be
   * zero if {@link #getMovesCalculated()} is zero or if {@link BalanceRequest#isDryRun()}
   * was true. It may also not be equal to {@link #getMovesCalculated()} if the balancer
   * was interrupted midway through executing  the moves due to max run time.
   */
  public int getMovesExecuted() {
    return movesExecuted;
  }
}
