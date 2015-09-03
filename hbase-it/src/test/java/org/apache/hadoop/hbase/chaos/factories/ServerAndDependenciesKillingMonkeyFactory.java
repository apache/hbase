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
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.ForceBalancerAction;
import org.apache.hadoop.hbase.chaos.actions.RestartActiveMasterAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomDataNodeAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomRsExceptMetaAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomZKNodeAction;
import org.apache.hadoop.hbase.chaos.actions.RollingBatchRestartRsExceptMetaAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.chaos.policies.DoActionsOncePolicy;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

/**
 * Creates ChaosMonkeys for doing server restart actions, but not
 * flush / compact / snapshot kind of actions.
 */
public class ServerAndDependenciesKillingMonkeyFactory extends MonkeyFactory {

  @Override
  public ChaosMonkey build() {

    // Destructive actions to mess things around. Cannot run batch restart.
    Action[] actions1 = new Action[]{
      new RestartRandomRsExceptMetaAction(60000),
      new RestartActiveMasterAction(5000),
      new RollingBatchRestartRsExceptMetaAction(5000, 1.0f, 2), // only allow 2 servers to be dead.
      new ForceBalancerAction(),
      new RestartRandomDataNodeAction(60000),
      new RestartRandomZKNodeAction(60000)
    };

    // Action to log more info for debugging
    Action[] actions2 = new Action[]{
      new DumpClusterStatusAction()
    };

    return new PolicyBasedChaosMonkey(util,
      new CompositeSequentialPolicy(
        new DoActionsOncePolicy(60 * 1000, actions1),
        new PeriodicRandomActionPolicy(60 * 1000, actions1)),
      new PeriodicRandomActionPolicy(60 * 1000, actions2));
  }
}
