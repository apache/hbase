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

package org.apache.hadoop.hbase.chaos.factories;

import org.apache.hadoop.hbase.chaos.actions.UnbalanceKillAndRebalanceAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;
import org.apache.hadoop.hbase.chaos.policies.Policy;

public class UnbalanceMonkeyFactory extends MonkeyFactory {
  /** How often to introduce the chaos. If too frequent, sequence of kills on minicluster
   * can cause test to fail when Put runs out of retries. */
  private long chaosEveryMilliSec;
  private long waitForUnbalanceMilliSec;
  private long waitForKillMilliSec;
  private long waitAfterBalanceMilliSec;

  @Override
  public ChaosMonkey build() {
    loadProperties();
    Policy chaosPolicy = new PeriodicRandomActionPolicy(chaosEveryMilliSec,
        new UnbalanceKillAndRebalanceAction(waitForUnbalanceMilliSec, waitForKillMilliSec,
            waitAfterBalanceMilliSec));

    return new PolicyBasedChaosMonkey(util, chaosPolicy);
  }

  private void loadProperties() {
    chaosEveryMilliSec = Long.parseLong(this.properties.getProperty(
      MonkeyConstants.UNBALANCE_CHAOS_EVERY_MS,
      MonkeyConstants.DEFAULT_UNBALANCE_CHAOS_EVERY_MS + ""));
    waitForUnbalanceMilliSec = Long.parseLong(this.properties.getProperty(
      MonkeyConstants.UNBALANCE_WAIT_FOR_UNBALANCE_MS,
      MonkeyConstants.DEFAULT_UNBALANCE_WAIT_FOR_UNBALANCE_MS + ""));
    waitForKillMilliSec = Long.parseLong(this.properties.getProperty(
      MonkeyConstants.UNBALANCE_WAIT_FOR_KILLS_MS,
      MonkeyConstants.DEFAULT_UNBALANCE_WAIT_FOR_KILLS_MS + ""));
    waitAfterBalanceMilliSec = Long.parseLong(this.properties.getProperty(
      MonkeyConstants.UNBALANCE_WAIT_AFTER_BALANCE_MS,
      MonkeyConstants.DEFAULT_UNBALANCE_WAIT_AFTER_BALANCE_MS + ""));
  }
}
