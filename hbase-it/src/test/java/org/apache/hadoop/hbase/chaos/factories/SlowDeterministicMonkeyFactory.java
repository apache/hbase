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

import org.apache.hadoop.hbase.chaos.actions.*;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.chaos.policies.DoActionsOncePolicy;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

public class SlowDeterministicMonkeyFactory extends MonkeyFactory {

  private long action1Period;
  private long action2Period;
  private long action3Period;
  private long action4Period;
  private long moveRegionsMaxTime;
  private long moveRegionsSleepTime;
  private long moveRandomRegionSleepTime;
  private long restartRandomRSSleepTime;
  private long batchRestartRSSleepTime;
  private float batchRestartRSRatio;
  private long restartActiveMasterSleepTime;
  private long rollingBatchRestartRSSleepTime;
  private float rollingBatchRestartRSRatio;
  private long restartRsHoldingMetaSleepTime;
  private float compactTableRatio;
  private float compactRandomRegionRatio;
  private long decreaseHFileSizeSleepTime;

  @Override
  public ChaosMonkey build() {

    loadProperties();
    // Actions such as compact/flush a table/region,
    // move one region around. They are not so destructive,
    // can be executed more frequently.
    Action[] actions1 = new Action[] {
        new CompactTableAction(tableName, compactTableRatio),
        new CompactRandomRegionOfTableAction(tableName, compactRandomRegionRatio),
        new FlushTableAction(tableName),
        new FlushRandomRegionOfTableAction(tableName),
        new MoveRandomRegionOfTableAction(tableName)
    };

    // Actions such as split/merge/snapshot.
    // They should not cause data loss, or unreliability
    // such as region stuck in transition.
    Action[] actions2 = new Action[] {
        new SplitRandomRegionOfTableAction(tableName),
        new MergeRandomAdjacentRegionsOfTableAction(tableName),
        new SnapshotTableAction(tableName),
        new AddColumnAction(tableName),
        new RemoveColumnAction(tableName, columnFamilies),
        new ChangeEncodingAction(tableName),
        new ChangeCompressionAction(tableName),
        new ChangeBloomFilterAction(tableName),
        new ChangeVersionsAction(tableName),
        new ChangeSplitPolicyAction(tableName),
    };

    // Destructive actions to mess things around.
    Action[] actions3 = new Action[] {
        new MoveRegionsOfTableAction(moveRegionsSleepTime, moveRegionsMaxTime,
            tableName),
        new MoveRandomRegionOfTableAction(moveRandomRegionSleepTime, tableName),
        new RestartRandomRsAction(restartRandomRSSleepTime),
        new BatchRestartRsAction(batchRestartRSSleepTime, batchRestartRSRatio),
        new RestartActiveMasterAction(restartActiveMasterSleepTime),
        new RollingBatchRestartRsAction(rollingBatchRestartRSSleepTime,
            rollingBatchRestartRSRatio),
        new RestartRsHoldingMetaAction(restartRsHoldingMetaSleepTime),
        new DecreaseMaxHFileSizeAction(decreaseHFileSizeSleepTime, tableName),
        new SplitAllRegionOfTableAction(tableName),
    };

    // Action to log more info for debugging
    Action[] actions4 = new Action[] {
        new DumpClusterStatusAction()
    };

    return new PolicyBasedChaosMonkey(util,
        new PeriodicRandomActionPolicy(action1Period, actions1),
        new PeriodicRandomActionPolicy(action2Period, actions2),
        new CompositeSequentialPolicy(
            new DoActionsOncePolicy(action3Period, actions3),
            new PeriodicRandomActionPolicy(action3Period, actions3)),
        new PeriodicRandomActionPolicy(action4Period, actions4));
  }

  private void loadProperties() {

      action1Period = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.PERIODIC_ACTION1_PERIOD,
        MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD + ""));
      action2Period = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.PERIODIC_ACTION2_PERIOD,
        MonkeyConstants.DEFAULT_PERIODIC_ACTION2_PERIOD + ""));
      action3Period = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.COMPOSITE_ACTION3_PERIOD,
        MonkeyConstants.DEFAULT_COMPOSITE_ACTION3_PERIOD + ""));
      action4Period = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.PERIODIC_ACTION4_PERIOD,
        MonkeyConstants.DEFAULT_PERIODIC_ACTION4_PERIOD + ""));
      moveRegionsMaxTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.MOVE_REGIONS_MAX_TIME,
        MonkeyConstants.DEFAULT_MOVE_REGIONS_MAX_TIME + ""));
      moveRegionsSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.MOVE_REGIONS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_MOVE_REGIONS_SLEEP_TIME + ""));
      moveRandomRegionSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.MOVE_RANDOM_REGION_SLEEP_TIME,
        MonkeyConstants.DEFAULT_MOVE_RANDOM_REGION_SLEEP_TIME + ""));
      restartRandomRSSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.RESTART_RANDOM_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RANDOM_RS_SLEEP_TIME + ""));
      batchRestartRSSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.BATCH_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_BATCH_RESTART_RS_SLEEP_TIME + ""));
      restartActiveMasterSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.RESTART_ACTIVE_MASTER_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_ACTIVE_MASTER_SLEEP_TIME + ""));
      rollingBatchRestartRSSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.ROLLING_BATCH_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_RESTART_RS_SLEEP_TIME + ""));
      rollingBatchRestartRSRatio = Float.parseFloat(this.properties.getProperty(
        MonkeyConstants.ROLLING_BATCH_RESTART_RS_RATIO,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_RESTART_RS_RATIO + ""));
      restartRsHoldingMetaSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.RESTART_RS_HOLDING_META_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RS_HOLDING_META_SLEEP_TIME + ""));
      compactTableRatio = Float.parseFloat(this.properties.getProperty(
        MonkeyConstants.COMPACT_TABLE_ACTION_RATIO,
        MonkeyConstants.DEFAULT_COMPACT_TABLE_ACTION_RATIO + ""));
      compactRandomRegionRatio = Float.parseFloat(this.properties.getProperty(
        MonkeyConstants.COMPACT_RANDOM_REGION_RATIO,
        MonkeyConstants.DEFAULT_COMPACT_RANDOM_REGION_RATIO + ""));
    decreaseHFileSizeSleepTime = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.DECREASE_HFILE_SIZE_SLEEP_TIME,
        MonkeyConstants.DEFAULT_DECREASE_HFILE_SIZE_SLEEP_TIME + ""));
  }
}
