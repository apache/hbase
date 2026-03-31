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
package org.apache.hadoop.hbase.chaos.factories;

import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.DumpHdfsClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.ForceBalancerAction;
import org.apache.hadoop.hbase.chaos.actions.GracefulRollingRestartRsAction;
import org.apache.hadoop.hbase.chaos.actions.RestartActiveMasterAction;
import org.apache.hadoop.hbase.chaos.actions.RestartActiveNameNodeAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomDataNodeAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomJournalNodeAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomRsExceptMetaAction;
import org.apache.hadoop.hbase.chaos.actions.RestartRandomZKNodeAction;
import org.apache.hadoop.hbase.chaos.actions.RollingBatchRestartRsAction;
import org.apache.hadoop.hbase.chaos.actions.RollingBatchSuspendResumeRsAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.CompositeSequentialPolicy;
import org.apache.hadoop.hbase.chaos.policies.DoActionsOncePolicy;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

/**
 * Creates ChaosMonkeys for doing server restart actions, but not flush / compact / snapshot kind of
 * actions.
 */
public class ServerAndDependenciesKillingMonkeyFactory extends MonkeyFactory {

  private long restartRandomRsExceptMetaSleepTime;
  private long restartActiveMasterSleepTime;
  private long rollingBatchRestartRSSleepTime;
  private long restartActiveNameNodeSleepTime;
  private long restartRandomDataNodeSleepTime;
  private long restartRandomJournalNodeSleepTime;
  private long restartRandomZKNodeSleepTime;
  private long gracefulRollingRestartTSSLeepTime;
  private long rollingBatchSuspendRSSleepTime;
  private float rollingBatchSuspendtRSRatio;
  private long action1Period;

  @Override
  public ChaosMonkey build() {
    loadProperties();

    // Destructive actions to mess things around. Cannot run batch restart.
    // @formatter:off
    Action[] actions1 = new Action[] {
      new RestartRandomRsExceptMetaAction(restartRandomRsExceptMetaSleepTime),
      new RestartActiveMasterAction(restartActiveMasterSleepTime),
      // only allow 2 servers to be dead.
      new RollingBatchRestartRsAction(rollingBatchRestartRSSleepTime, 1.0f, 2, true),
      new ForceBalancerAction(),
      new RestartActiveNameNodeAction(restartActiveNameNodeSleepTime),
      new RestartRandomDataNodeAction(restartRandomDataNodeSleepTime),
      new RestartRandomJournalNodeAction(restartRandomJournalNodeSleepTime),
      new RestartRandomZKNodeAction(restartRandomZKNodeSleepTime),
      new GracefulRollingRestartRsAction(gracefulRollingRestartTSSLeepTime),
      new RollingBatchSuspendResumeRsAction(rollingBatchSuspendRSSleepTime,
          rollingBatchSuspendtRSRatio)
    };
    // @formatter:on

    // Action to log more info for debugging
    Action[] actions2 =
      new Action[] { new DumpClusterStatusAction(), new DumpHdfsClusterStatusAction() };

    return new PolicyBasedChaosMonkey(properties, util,
      new CompositeSequentialPolicy(new DoActionsOncePolicy(action1Period, actions1),
        new PeriodicRandomActionPolicy(action1Period, actions1)),
      new PeriodicRandomActionPolicy(action1Period, actions2));
  }

  private void loadProperties() {
    restartRandomRsExceptMetaSleepTime = Long
      .parseLong(this.properties.getProperty(MonkeyConstants.RESTART_RANDOM_RS_EXCEPTION_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RANDOM_RS_EXCEPTION_SLEEP_TIME + ""));
    restartActiveMasterSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.RESTART_ACTIVE_MASTER_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_ACTIVE_MASTER_SLEEP_TIME + ""));
    rollingBatchRestartRSSleepTime = Long
      .parseLong(this.properties.getProperty(MonkeyConstants.ROLLING_BATCH_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_RESTART_RS_SLEEP_TIME + ""));
    restartActiveNameNodeSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.RESTART_ACTIVE_NAMENODE_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_ACTIVE_NAMENODE_SLEEP_TIME + ""));
    restartRandomDataNodeSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.RESTART_RANDOM_DATANODE_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RANDOM_DATANODE_SLEEP_TIME + ""));
    restartRandomJournalNodeSleepTime = Long
      .parseLong(this.properties.getProperty(MonkeyConstants.RESTART_RANDOM_JOURNALNODE_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RANDOM_JOURNALNODE_SLEEP_TIME + ""));
    restartRandomZKNodeSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.RESTART_RANDOM_ZKNODE_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RANDOM_ZKNODE_SLEEP_TIME + ""));
    gracefulRollingRestartTSSLeepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.GRACEFUL_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_GRACEFUL_RESTART_RS_SLEEP_TIME + ""));
    rollingBatchSuspendRSSleepTime = Long
      .parseLong(this.properties.getProperty(MonkeyConstants.ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME + ""));
    rollingBatchSuspendtRSRatio =
      Float.parseFloat(this.properties.getProperty(MonkeyConstants.ROLLING_BATCH_SUSPEND_RS_RATIO,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_SUSPEND_RS_RATIO + ""));
    action1Period =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.PERIODIC_ACTION1_PERIOD,
        MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD + ""));
  }
}
