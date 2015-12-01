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
import org.apache.hadoop.hbase.chaos.actions.AddColumnAction;
import org.apache.hadoop.hbase.chaos.actions.BatchRestartRsAction;
import org.apache.hadoop.hbase.chaos.actions.CompactRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.CompactTableAction;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.FlushRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.FlushTableAction;
import org.apache.hadoop.hbase.chaos.actions.MergeRandomAdjacentRegionsOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.MoveMetaAction;
import org.apache.hadoop.hbase.chaos.actions.MoveRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.MoveRegionsOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.RemoveColumnAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomRsAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRsHoldingMetaAction;
import org.apache.hadoop.hbase.chaos.actions.RollingBatchRestartRsAction;
import org.apache.hadoop.hbase.chaos.actions.SplitRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.chaos.policies.DoActionsOncePolicy;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

public class StressAssignmentManagerMonkeyFactory extends MonkeyFactory {
  @Override
  public ChaosMonkey build() {

    // Actions that could slow down region movement.
    // These could also get regions stuck if there are issues.
    Action[] actions1 = new Action[] {
        new CompactTableAction(tableName, 0.5f),
        new CompactRandomRegionOfTableAction(tableName, 0.6f),
        new FlushTableAction(tableName),
        new FlushRandomRegionOfTableAction(tableName),
        new MoveMetaAction()
    };

    Action[] actions2 = new Action[] {
        new SplitRandomRegionOfTableAction(tableName),
        new MergeRandomAdjacentRegionsOfTableAction(tableName),
        new AddColumnAction(tableName),
        new RemoveColumnAction(tableName, columnFamilies),
        new MoveRegionsOfTableAction(800, 1600, tableName),
        new MoveRandomRegionOfTableAction(800, tableName),
        new RestartRandomRsAction(60000),
        new BatchRestartRsAction(5000, 0.5f),
        new RollingBatchRestartRsAction(5000, 1.0f),
        new RestartRsHoldingMetaAction(35000),
        new MoveMetaAction()
    };

    // Action to log more info for debugging
    Action[] actions3 = new Action[] {
        new DumpClusterStatusAction()
    };

    return new PolicyBasedChaosMonkey(util,
        new PeriodicRandomActionPolicy(90 * 1000, actions1),
        new CompositeSequentialPolicy(
            new DoActionsOncePolicy(90 * 1000, actions2),
            new PeriodicRandomActionPolicy(90 * 1000, actions2)),
        new PeriodicRandomActionPolicy(90 * 1000, actions3)
    );
  }
}
