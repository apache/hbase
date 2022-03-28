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
 * Response returned from a balancer invocation
 */
@InterfaceAudience.Public
public final class BalanceResponse {

  /**
   * Used in HMaster to build a {@link BalanceResponse} for returning results of a balance
   * invocation to callers
   */
  @InterfaceAudience.Private
  public final static class Builder {
    private boolean balancerRan;
    private int movesCalculated;
    private int movesExecuted;

    private Builder() {
    }

    /**
     * Set true if the balancer ran, otherwise false. The balancer may not run in some
     * circumstances, such as if a balance is already running or there are regions already in
     * transition.
     * @param balancerRan true if balancer ran, false otherwise
     */
    public Builder setBalancerRan(boolean balancerRan) {
      this.balancerRan = balancerRan;
      return this;
    }

    /**
     * Set how many moves were calculated by the balancer. This will be zero if the cluster is
     * already balanced.
     * @param movesCalculated moves calculated by the balance run
     */
    public Builder setMovesCalculated(int movesCalculated) {
      this.movesCalculated = movesCalculated;
      return this;
    }

    /**
     * Set how many of the calculated moves were actually executed by the balancer. This should be
     * zero if the balancer is run with {@link BalanceRequest#isDryRun()}. It may also not equal
     * movesCalculated if the balancer ran out of time while executing the moves.
     * @param movesExecuted moves executed by the balance run
     */
    public Builder setMovesExecuted(int movesExecuted) {
      this.movesExecuted = movesExecuted;
      return this;
    }

    /**
     * Build the {@link BalanceResponse}
     */
    public BalanceResponse build() {
      return new BalanceResponse(balancerRan, movesCalculated, movesExecuted);
    }
  }

  /**
   * Creates a new {@link BalanceResponse.Builder}
   */
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
   * Returns true if the balancer ran, otherwise false. The balancer may not run for a variety of
   * reasons, such as: another balance is running, there are regions in transition, the cluster is
   * in maintenance mode, etc.
   */
  public boolean isBalancerRan() {
    return balancerRan;
  }

  /**
   * The number of moves calculated by the balancer if {@link #isBalancerRan()} is true. This will
   * be zero if no better balance could be found.
   */
  public int getMovesCalculated() {
    return movesCalculated;
  }

  /**
   * The number of moves actually executed by the balancer if it ran. This will be zero if
   * {@link #getMovesCalculated()} is zero or if {@link BalanceRequest#isDryRun()} was true. It may
   * also not be equal to {@link #getMovesCalculated()} if the balancer was interrupted midway
   * through executing the moves due to max run time.
   */
  public int getMovesExecuted() {
    return movesExecuted;
  }
}
