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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public interface MonkeyConstants {

  String PERIODIC_ACTION1_PERIOD = "sdm.action1.period";
  String PERIODIC_ACTION2_PERIOD = "sdm.action2.period";
  String PERIODIC_ACTION4_PERIOD = "sdm.action4.period";
  String COMPOSITE_ACTION3_PERIOD = "sdm.action3.period";
  String MOVE_REGIONS_MAX_TIME = "move.regions.max.time";
  String MOVE_REGIONS_SLEEP_TIME = "move.regions.sleep.time";
  String MOVE_RANDOM_REGION_SLEEP_TIME = "move.randomregion.sleep.time";
  String RESTART_RANDOM_RS_SLEEP_TIME = "restart.random.rs.sleep.time";
  String BATCH_RESTART_RS_SLEEP_TIME = "batch.restart.rs.sleep.time";
  String BATCH_RESTART_RS_RATIO = "batch.restart.rs.ratio";
  String RESTART_ACTIVE_MASTER_SLEEP_TIME = "restart.active.master.sleep.time";
  String ROLLING_BATCH_RESTART_RS_SLEEP_TIME = "rolling.batch.restart.rs.sleep.time";
  String ROLLING_BATCH_RESTART_RS_RATIO = "rolling.batch.restart.rs.ratio";
  String RESTART_RS_HOLDING_META_SLEEP_TIME = "restart.rs.holding.meta.sleep.time";
  String COMPACT_TABLE_ACTION_RATIO = "compact.table.ratio";
  String COMPACT_RANDOM_REGION_RATIO = "compact.random.region.ratio";
  String UNBALANCE_CHAOS_EVERY_MS = "unbalance.chaos.period";
  String UNBALANCE_WAIT_FOR_UNBALANCE_MS = "unbalance.action.wait.period";
  String UNBALANCE_WAIT_FOR_KILLS_MS = "unbalance.action.kill.period";
  String UNBALANCE_WAIT_AFTER_BALANCE_MS = "unbalance.action.wait.after.period";
  String UNBALANCE_KILL_META_RS = "unbalance.action.kill.meta.rs";
  String DECREASE_HFILE_SIZE_SLEEP_TIME = "decrease.hfile.size.sleep.time";
  String GRACEFUL_RESTART_RS_SLEEP_TIME = "graceful.restart.rs.sleep.time";
  String ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME = "rolling.batch.suspend.rs.sleep.time";
  String ROLLING_BATCH_SUSPEND_RS_RATIO = "rolling.batch.suspend.rs.ratio";
  String SKIP_META_RS = "skip.meta.rs";
  String CPU_LOAD_DURATION = "cpu.load.duration";
  String CPU_LOAD_PROCESSES = "cpu.load.processes";
  String NETWORK_ISSUE_COMMAND_TIMEOUT = "network.issue.command.timeout";
  String NETWORK_ISSUE_DURATION = "network.issueduration";
  String NETWORK_ISSUE_RATIO = "network.issue.ratio";
  String NETWORK_ISSUE_DELAY = "network.issue.delay";
  String NETWORK_ISSUE_INTERFACE = "network.issue.interface";
  //should be higher than the usual timeout because the target machine might respond slowly
  String FILL_DISK_COMMAND_TIMEOUT = "fill.disk.command.timeout";
  String FILL_DISK_PATH = "fill.disk.path";
  String FILL_DISK_FILE_SIZE = "fill.disk.file.size";
  String FILL_DISK_ISSUE_DURATION = "fill.disk.issue.duration";
  String DATA_ISSUE_CHANCE = "data.issue.chance";

  /**
   * A Set of prefixes which encompasses all of the configuration properties for the ChaosMonky.
   */
  Set<String> MONKEY_CONFIGURATION_KEY_PREFIXES = new HashSet<>(
      Arrays.asList("sdm.", "move.", "restart.", "batch.", "rolling.", "compact.", "unbalance.",
          "decrease.", "decrease.", "graceful.", "cpu.", "network.", "fill.", "data.", "skip"));

  long DEFAULT_PERIODIC_ACTION1_PERIOD = 60 * 1000;
  long DEFAULT_PERIODIC_ACTION2_PERIOD = 90 * 1000;
  long DEFAULT_PERIODIC_ACTION4_PERIOD = 90 * 1000;
  long DEFAULT_COMPOSITE_ACTION3_PERIOD = 150 * 1000;
  long DEFAULT_MOVE_REGIONS_MAX_TIME = 10 * 60 * 1000;
  long DEFAULT_MOVE_REGIONS_SLEEP_TIME = 800;
  long DEFAULT_MOVE_RANDOM_REGION_SLEEP_TIME = 800;
  long DEFAULT_RESTART_RANDOM_RS_SLEEP_TIME = 60000;
  long DEFAULT_BATCH_RESTART_RS_SLEEP_TIME = 5000;
  float DEFAULT_BATCH_RESTART_RS_RATIO = 0.5f;
  long DEFAULT_RESTART_ACTIVE_MASTER_SLEEP_TIME = 5000;
  long DEFAULT_ROLLING_BATCH_RESTART_RS_SLEEP_TIME = 5000;
  float DEFAULT_ROLLING_BATCH_RESTART_RS_RATIO = 1.0f;
  long DEFAULT_RESTART_RS_HOLDING_META_SLEEP_TIME = 35000;
  float DEFAULT_COMPACT_TABLE_ACTION_RATIO = 0.5f;
  float DEFAULT_COMPACT_RANDOM_REGION_RATIO = 0.6f;
  long DEFAULT_UNBALANCE_CHAOS_EVERY_MS = 65 * 1000;
  long DEFAULT_UNBALANCE_WAIT_FOR_UNBALANCE_MS = 2 * 1000;
  long DEFAULT_UNBALANCE_WAIT_FOR_KILLS_MS = 2 * 1000;
  long DEFAULT_UNBALANCE_WAIT_AFTER_BALANCE_MS = 5 * 1000;
  boolean DEFAULT_UNBALANCE_KILL_META_RS = true;
  long DEFAULT_DECREASE_HFILE_SIZE_SLEEP_TIME = 30 * 1000;
  long DEFAULT_GRACEFUL_RESTART_RS_SLEEP_TIME = 5000;
  long DEFAULT_ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME = 30 * 1000;
  float DEFAULT_ROLLING_BATCH_SUSPEND_RS_RATIO = 1.0f;
  boolean DEFAULT_SKIP_META_RS = false;
  long DEFAULT_CPU_LOAD_DURATION = 5 * 60 * 1000;
  long DEFAULT_CPU_LOAD_PROCESSES = 2;
  long DEFAULT_NETWORK_ISSUE_COMMAND_TIMEOUT = 30 * 1000;
  long DEFAULT_NETWORK_ISSUE_DURATION = 60 * 1000;
  float DEFAULT_NETWORK_ISSUE_RATIO = 0.1f;
  long DEFAULT_NETWORK_ISSUE_DELAY = 100;
  String DEFAULT_NETWORK_ISSUE_INTERFACE = "eth0";
  long DEFAULT_FILL_DISK_COMMAND_TIMEOUT = 60 * 1000;
  String DEFAULT_FILL_DISK_PATH = "/tmp";
  long DEFAULT_FILL_DISK_FILE_SIZE = 0;
  long DEFAULT_FILL_DISK_ISSUE_DURATION = 5 * 60 * 1000;
  float DEFAULT_DATA_ISSUE_CHANCE = 0.01f;
}
