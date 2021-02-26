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

import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.chaos.actions.CorruptDataFilesAction;
import org.apache.hadoop.hbase.chaos.actions.DeleteDataFilesAction;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

/**
 * A chaos monkey to delete and corrupt regionserver data, requires a user with
 * passwordless ssh access to the cluster and sudo privileges.
 * Highly destructive
 */
public class DataIssuesMonkeyFactory extends MonkeyFactory {

  private long action1Period;
  private long action2Period;

  private float chanceToAct;

  @Override
  public ChaosMonkey build() {
    loadProperties();

    // Highly destructive actions to mess things around.
    Action[] actions1 = new Action[] {
      new DeleteDataFilesAction(chanceToAct),
      new CorruptDataFilesAction(chanceToAct)
    };

    // Action to log more info for debugging
    Action[] actions2 = new Action[] {
      new DumpClusterStatusAction()
    };

    return new PolicyBasedChaosMonkey(properties, util,
      new PeriodicRandomActionPolicy(action1Period, actions1),
      new PeriodicRandomActionPolicy(action2Period, actions2));
  }

  private void loadProperties() {
    action1Period = Long.parseLong(this.properties.getProperty(
      MonkeyConstants.PERIODIC_ACTION1_PERIOD,
      MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD + ""));
    action2Period = Long.parseLong(this.properties.getProperty(
      MonkeyConstants.PERIODIC_ACTION2_PERIOD,
      MonkeyConstants.DEFAULT_PERIODIC_ACTION2_PERIOD + ""));
    chanceToAct = Float.parseFloat(this.properties.getProperty(
        MonkeyConstants.DATA_ISSUE_CHANCE,
        MonkeyConstants.DEFAULT_DATA_ISSUE_CHANCE+ ""));
  }
}
