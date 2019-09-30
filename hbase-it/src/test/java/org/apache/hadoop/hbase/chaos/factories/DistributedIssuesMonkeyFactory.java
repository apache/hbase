/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.chaos.factories;

import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.chaos.actions.AddCPULoadAction;
import org.apache.hadoop.hbase.chaos.actions.CorruptPackagesCommandAction;
import org.apache.hadoop.hbase.chaos.actions.DelayPackagesCommandAction;
import org.apache.hadoop.hbase.chaos.actions.DumpClusterStatusAction;
import org.apache.hadoop.hbase.chaos.actions.DuplicatePackagesCommandAction;
import org.apache.hadoop.hbase.chaos.actions.FillDiskCommandAction;
import org.apache.hadoop.hbase.chaos.actions.LosePackagesCommandAction;
import org.apache.hadoop.hbase.chaos.actions.ReorderPackagesCommandAction;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.chaos.monkies.PolicyBasedChaosMonkey;
import org.apache.hadoop.hbase.chaos.policies.PeriodicRandomActionPolicy;

/**
 * A chaos monkey to create distributed cluster related issues, requires a user with
 * passwordless ssh access to the cluster and sudo privileges.
 */
public class DistributedIssuesMonkeyFactory extends MonkeyFactory {

  private long action1Period;
  private long action2Period;

  private long cpuLoadDuration;
  private long cpuLoadProcesses;
  private long networkIssueTimeout;
  private long networkIssueDuration;
  private float networkIssueRation;
  private long networkIssueDelay;
  private long fillDiskTimeout;
  private String fillDiskPath;
  private long fillDiskFileSize;
  private long fillDiskIssueduration;

  @Override public ChaosMonkey build() {
    loadProperties();

    Action[] actions1 = new Action[] {
      new AddCPULoadAction(cpuLoadDuration, cpuLoadProcesses, networkIssueTimeout),
      new CorruptPackagesCommandAction(networkIssueRation, networkIssueDuration,
          networkIssueTimeout),
      new DuplicatePackagesCommandAction(networkIssueRation, networkIssueDuration,
          networkIssueTimeout),
      new LosePackagesCommandAction(networkIssueRation, networkIssueDuration,
          networkIssueTimeout),
      new DelayPackagesCommandAction(networkIssueDelay, networkIssueDuration,
          networkIssueTimeout),
      new ReorderPackagesCommandAction(networkIssueRation, networkIssueDuration,
          networkIssueDelay, networkIssueTimeout),
      new FillDiskCommandAction(fillDiskFileSize, fillDiskIssueduration, fillDiskPath,
          fillDiskTimeout)};

    // Action to log more info for debugging
    Action[] actions2 = new Action[] {new DumpClusterStatusAction()};

    return new PolicyBasedChaosMonkey(util, new PeriodicRandomActionPolicy(action1Period, actions1),
        new PeriodicRandomActionPolicy(action2Period, actions2));
  }

  private void loadProperties() {
    action1Period = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.PERIODIC_ACTION1_PERIOD,
            MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD + ""));
    action2Period = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.PERIODIC_ACTION2_PERIOD,
            MonkeyConstants.DEFAULT_PERIODIC_ACTION2_PERIOD + ""));
    cpuLoadDuration = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.CPU_LOAD_DURATION,
        MonkeyConstants.DEFAULT_CPU_LOAD_DURATION + ""));
    cpuLoadProcesses = Long.parseLong(this.properties.getProperty(
        MonkeyConstants.CPU_LOAD_PROCESSES,
        MonkeyConstants.DEFAULT_CPU_LOAD_PROCESSES + ""));
    networkIssueTimeout = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.NETWORK_ISSUE_COMMAND_TIMEOUT,
            MonkeyConstants.DEFAULT_NETWORK_ISSUE_COMMAND_TIMEOUT + ""));
    networkIssueDuration = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.NETWORK_ISSUE_DURATION,
            MonkeyConstants.DEFAULT_NETWORK_ISSUE_DURATION + ""));
    networkIssueRation = Float.parseFloat(this.properties
        .getProperty(MonkeyConstants.NETWORK_ISSUE_RATIO,
            MonkeyConstants.DEFAULT_NETWORK_ISSUE_RATIO + ""));
    networkIssueDelay = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.NETWORK_ISSUE_DELAY,
            MonkeyConstants.DEFAULT_NETWORK_ISSUE_DELAY + ""));
    fillDiskTimeout = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.FILL_DISK_COMMAND_TIMEOUT,
            MonkeyConstants.DEFAULT_FILL_DISK_COMMAND_TIMEOUT + ""));
    fillDiskPath = this.properties
        .getProperty(MonkeyConstants.FILL_DISK_PATH,
            MonkeyConstants.DEFAULT_FILL_DISK_PATH + "");
    fillDiskFileSize = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.FILL_DISK_FILE_SIZE,
            MonkeyConstants.DEFAULT_FILL_DISK_FILE_SIZE + ""));
    fillDiskIssueduration = Long.parseLong(this.properties
        .getProperty(MonkeyConstants.FILL_DISK_ISSUE_DURATION,
            MonkeyConstants.DEFAULT_FILL_DISK_ISSUE_DURATION + ""));
  }
}
