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
import org.apache.hadoop.hbase.chaos.actions.AddCPULoadAction;
import org.apache.hadoop.hbase.chaos.actions.CorruptPacketsCommandAction;
import org.apache.hadoop.hbase.chaos.actions.DelayPacketsCommandAction;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.DuplicatePacketsCommandAction;
import org.apache.hadoop.hbase.chaos.actions.FillDiskCommandAction;
import org.apache.hadoop.hbase.chaos.actions.LosePacketsCommandAction;
import org.apache.hadoop.hbase.chaos.actions.ReorderPacketsCommandAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

/**
 * A chaos monkey to create distributed cluster related issues, requires a user with passwordless
 * ssh access to the cluster and sudo privileges.
 */
public class DistributedIssuesMonkeyFactory extends MonkeyFactory {

  @Override
  public ChaosMonkey build() {
    loadProperties();

    Action[] actions1 =
      new Action[] { new AddCPULoadAction(cpuLoadDuration, cpuLoadProcesses, networkIssueTimeout),
        new CorruptPacketsCommandAction(networkIssueRation, networkIssueDuration,
          networkIssueTimeout, networkIssueInterface),
        new DuplicatePacketsCommandAction(networkIssueRation, networkIssueDuration,
          networkIssueTimeout, networkIssueInterface),
        new LosePacketsCommandAction(networkIssueRation, networkIssueDuration, networkIssueTimeout,
          networkIssueInterface),
        new DelayPacketsCommandAction(networkIssueDelay, networkIssueDuration, networkIssueTimeout,
          networkIssueInterface),
        new ReorderPacketsCommandAction(networkIssueRation, networkIssueDuration, networkIssueDelay,
          networkIssueTimeout, networkIssueInterface),
        new FillDiskCommandAction(fillDiskFileSize, fillDiskIssueduration, fillDiskPath,
          fillDiskTimeout) };

    // Action to log more info for debugging
    Action[] actions2 = new Action[] { new DumpClusterStatusAction() };

    return new PolicyBasedChaosMonkey(properties, util,
      new PeriodicRandomActionPolicy(action1Period, actions1),
      new PeriodicRandomActionPolicy(action2Period, actions2));
  }
}
