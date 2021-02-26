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
import org.apache.hadoop.hbase.chaos.actions.ChangeBloomFilterAction;
import org.apache.hadoop.hbase.chaos.actions.ChangeCompressionAction;
import org.apache.hadoop.hbase.chaos.actions.ChangeEncodingAction;
import org.apache.hadoop.hbase.chaos.actions.ChangeVersionsAction;
import org.apache.hadoop.hbase.chaos.actions.CompactMobAction;
import org.apache.hadoop.hbase.chaos.actions.CompactRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.CompactTableAction;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.FlushRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.FlushTableAction;
import org.apache.hadoop.hbase.chaos.actions.MergeRandomAdjacentRegionsOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.MoveRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.MoveRegionsOfTableAction;
import org.apache.hadoop.hbase.chaos.actions.RemoveColumnAction;
import org.apache.hadoop.hbase.chaos.actions.SnapshotTableAction;
import org.apache.hadoop.hbase.chaos.actions.SplitRandomRegionOfTableAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;
import org.apache.hadoop.hbase.chaos.policies.TwoConcurrentActionPolicy;

/**
 * This is a copy of NoKillMonkeyFactory that also does mob compactions.
 */
public class MobNoKillMonkeyFactory extends MonkeyFactory {
  @Override public ChaosMonkey build() {
    Action[] actions1 = new Action[] {
      new CompactMobAction(tableName, MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD),
      new CompactTableAction(tableName, MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD),
      new CompactRandomRegionOfTableAction(tableName,
        MonkeyConstants.DEFAULT_COMPACT_RANDOM_REGION_RATIO),
      new FlushTableAction(tableName),
      new FlushRandomRegionOfTableAction(tableName),
      new MoveRandomRegionOfTableAction(tableName)
    };

    Action[] actions2 = new Action[] {
      new SplitRandomRegionOfTableAction(tableName),
      new MergeRandomAdjacentRegionsOfTableAction(tableName),
      new SnapshotTableAction(tableName),
      new AddColumnAction(tableName),
      new RemoveColumnAction(tableName, columnFamilies),
      new ChangeEncodingAction(tableName),
      new ChangeCompressionAction(tableName),
      new ChangeBloomFilterAction(tableName),
      new ChangeVersionsAction(tableName)
    };

    Action[] actions3 = new Action[] {
      new MoveRegionsOfTableAction(MonkeyConstants.DEFAULT_MOVE_REGIONS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_MOVE_REGIONS_MAX_TIME, tableName),
      new MoveRandomRegionOfTableAction(MonkeyConstants.DEFAULT_RESTART_ACTIVE_MASTER_SLEEP_TIME,
        tableName), };

    Action[] actions4 = new Action[] { new DumpClusterStatusAction() };

    return new PolicyBasedChaosMonkey(properties, util,
      new TwoConcurrentActionPolicy(MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD, actions1,
        actions2),
      new PeriodicRandomActionPolicy(MonkeyConstants.DEFAULT_PERIODIC_ACTION2_PERIOD,actions3),
      new PeriodicRandomActionPolicy(MonkeyConstants.DEFAULT_PERIODIC_ACTION4_PERIOD,actions4));
  }
}
