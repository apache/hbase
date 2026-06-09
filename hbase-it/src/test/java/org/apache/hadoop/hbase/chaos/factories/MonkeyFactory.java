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

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.chaos.monkies.ChaosMonkey;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

/**
 * Base class of the factory that will create a ChaosMonkey.
 */
public abstract class MonkeyFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MonkeyFactory.class);

  protected TableName tableName;
  protected Set<String> columnFamilies;
  protected IntegrationTestingUtility util;
  protected Properties properties = new Properties();

  protected long action1Period;
  protected long action2Period;
  protected long action3Period;
  protected long action4Period;
  protected long moveRegionsMaxTime;
  protected long moveRegionsSleepTime;
  protected long moveRandomRegionSleepTime;
  protected long restartRandomRSSleepTime;
  protected long batchRestartRSSleepTime;
  protected float batchRestartRSRatio;
  protected long restartActiveMasterSleepTime;
  protected long rollingBatchRestartRSSleepTime;
  protected float rollingBatchRestartRSRatio;
  protected long restartRsHoldingMetaSleepTime;
  protected float compactTableRatio;
  protected float compactRandomRegionRatio;
  protected long decreaseHFileSizeSleepTime;
  protected long decreaseHFileSizeMinHFileSize;
  protected float decreaseHFileSizeHFileSizeJitter;
  protected long gracefulRollingRestartTSSLeepTime;
  protected long rollingBatchSuspendRSSleepTime;
  protected float rollingBatchSuspendRSRatio;
  protected long snapshotTableTtl;

  protected long cpuLoadDuration;
  protected long cpuLoadProcesses;
  protected long networkIssueTimeout;
  protected long networkIssueDuration;
  protected float networkIssueRation;
  protected long networkIssueDelay;
  protected String networkIssueInterface;
  protected long fillDiskTimeout;
  protected String fillDiskPath;
  protected long fillDiskFileSize;
  protected long fillDiskIssueduration;

  protected long restartRandomRsExceptMetaSleepTime;
  protected long restartActiveNameNodeSleepTime;
  protected long restartRandomDataNodeSleepTime;
  protected long restartRandomJournalNodeSleepTime;
  protected long restartRandomZKNodeSleepTime;

  /**
   * How often to introduce the chaos. If too frequent, sequence of kills on minicluster can cause
   * test to fail when Put runs out of retries.
   */
  protected long chaosEveryMilliSec;
  protected long waitForUnbalanceMilliSec;
  protected long waitForKillMilliSec;
  protected long waitAfterBalanceMilliSec;
  protected boolean killMetaRs;

  public MonkeyFactory setTableName(TableName tableName) {
    this.tableName = tableName;
    return this;
  }

  public MonkeyFactory setColumnFamilies(Set<String> columnFamilies) {
    this.columnFamilies = columnFamilies;
    return this;
  }

  public MonkeyFactory setUtil(IntegrationTestingUtility util) {
    this.util = util;
    return this;
  }

  public MonkeyFactory setProperties(Properties props) {
    if (props != null) {
      this.properties = props;
    }
    return this;
  }

  protected final void loadProperties() {
    action1Period =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.PERIODIC_ACTION1_PERIOD,
        MonkeyConstants.DEFAULT_PERIODIC_ACTION1_PERIOD + ""));
    action2Period =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.PERIODIC_ACTION2_PERIOD,
        MonkeyConstants.DEFAULT_PERIODIC_ACTION2_PERIOD + ""));
    action3Period =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.COMPOSITE_ACTION3_PERIOD,
        MonkeyConstants.DEFAULT_COMPOSITE_ACTION3_PERIOD + ""));
    action4Period =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.PERIODIC_ACTION4_PERIOD,
        MonkeyConstants.DEFAULT_PERIODIC_ACTION4_PERIOD + ""));
    moveRegionsMaxTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.MOVE_REGIONS_MAX_TIME,
        MonkeyConstants.DEFAULT_MOVE_REGIONS_MAX_TIME + ""));
    moveRegionsSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.MOVE_REGIONS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_MOVE_REGIONS_SLEEP_TIME + ""));
    moveRandomRegionSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.MOVE_RANDOM_REGION_SLEEP_TIME,
        MonkeyConstants.DEFAULT_MOVE_RANDOM_REGION_SLEEP_TIME + ""));
    restartRandomRSSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.RESTART_RANDOM_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RANDOM_RS_SLEEP_TIME + ""));
    batchRestartRSSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.BATCH_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_BATCH_RESTART_RS_SLEEP_TIME + ""));
    batchRestartRSRatio =
      Float.parseFloat(this.properties.getProperty(MonkeyConstants.BATCH_RESTART_RS_RATIO,
        MonkeyConstants.DEFAULT_BATCH_RESTART_RS_RATIO + ""));
    restartActiveMasterSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.RESTART_ACTIVE_MASTER_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_ACTIVE_MASTER_SLEEP_TIME + ""));
    rollingBatchRestartRSSleepTime = Long
      .parseLong(this.properties.getProperty(MonkeyConstants.ROLLING_BATCH_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_RESTART_RS_SLEEP_TIME + ""));
    rollingBatchRestartRSRatio =
      Float.parseFloat(this.properties.getProperty(MonkeyConstants.ROLLING_BATCH_RESTART_RS_RATIO,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_RESTART_RS_RATIO + ""));
    restartRsHoldingMetaSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.RESTART_RS_HOLDING_META_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RS_HOLDING_META_SLEEP_TIME + ""));
    compactTableRatio =
      Float.parseFloat(this.properties.getProperty(MonkeyConstants.COMPACT_TABLE_ACTION_RATIO,
        MonkeyConstants.DEFAULT_COMPACT_TABLE_ACTION_RATIO + ""));
    compactRandomRegionRatio =
      Float.parseFloat(this.properties.getProperty(MonkeyConstants.COMPACT_RANDOM_REGION_RATIO,
        MonkeyConstants.DEFAULT_COMPACT_RANDOM_REGION_RATIO + ""));
    decreaseHFileSizeSleepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.DECREASE_HFILE_SIZE_SLEEP_TIME,
        MonkeyConstants.DEFAULT_DECREASE_HFILE_SIZE_SLEEP_TIME + ""));
    decreaseHFileSizeMinHFileSize =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.DECREASE_HFILE_SIZE_MIN_HFILE_SIZE,
        MonkeyConstants.DEFAULT_DECREASE_HFILE_SIZE_MIN_HFILE_SIZE + ""));
    decreaseHFileSizeHFileSizeJitter = Float
      .parseFloat(this.properties.getProperty(MonkeyConstants.DECREASE_HFILE_SIZE_HFILE_SIZE_JITTER,
        MonkeyConstants.DEFAULT_DECREASE_HFILE_SIZE_HFILE_SIZE_JITTER + ""));
    gracefulRollingRestartTSSLeepTime =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.GRACEFUL_RESTART_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_GRACEFUL_RESTART_RS_SLEEP_TIME + ""));
    rollingBatchSuspendRSSleepTime = Long
      .parseLong(this.properties.getProperty(MonkeyConstants.ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_SUSPEND_RS_SLEEP_TIME + ""));
    rollingBatchSuspendRSRatio =
      Float.parseFloat(this.properties.getProperty(MonkeyConstants.ROLLING_BATCH_SUSPEND_RS_RATIO,
        MonkeyConstants.DEFAULT_ROLLING_BATCH_SUSPEND_RS_RATIO + ""));
    snapshotTableTtl =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.SNAPSHOT_TABLE_TTL,
        MonkeyConstants.DEFAULT_SNAPSHOT_TABLE_TTL + ""));

    cpuLoadDuration = Long.parseLong(this.properties.getProperty(MonkeyConstants.CPU_LOAD_DURATION,
      MonkeyConstants.DEFAULT_CPU_LOAD_DURATION + ""));
    cpuLoadProcesses =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.CPU_LOAD_PROCESSES,
        MonkeyConstants.DEFAULT_CPU_LOAD_PROCESSES + ""));
    networkIssueTimeout =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.NETWORK_ISSUE_COMMAND_TIMEOUT,
        MonkeyConstants.DEFAULT_NETWORK_ISSUE_COMMAND_TIMEOUT + ""));
    networkIssueDuration =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.NETWORK_ISSUE_DURATION,
        MonkeyConstants.DEFAULT_NETWORK_ISSUE_DURATION + ""));
    networkIssueRation =
      Float.parseFloat(this.properties.getProperty(MonkeyConstants.NETWORK_ISSUE_RATIO,
        MonkeyConstants.DEFAULT_NETWORK_ISSUE_RATIO + ""));
    networkIssueDelay =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.NETWORK_ISSUE_DELAY,
        MonkeyConstants.DEFAULT_NETWORK_ISSUE_DELAY + ""));
    networkIssueInterface = this.properties.getProperty(MonkeyConstants.NETWORK_ISSUE_INTERFACE,
      MonkeyConstants.DEFAULT_NETWORK_ISSUE_INTERFACE + "");
    fillDiskTimeout =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.FILL_DISK_COMMAND_TIMEOUT,
        MonkeyConstants.DEFAULT_FILL_DISK_COMMAND_TIMEOUT + ""));
    fillDiskPath = this.properties.getProperty(MonkeyConstants.FILL_DISK_PATH,
      MonkeyConstants.DEFAULT_FILL_DISK_PATH + "");
    fillDiskFileSize =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.FILL_DISK_FILE_SIZE,
        MonkeyConstants.DEFAULT_FILL_DISK_FILE_SIZE + ""));
    fillDiskIssueduration =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.FILL_DISK_ISSUE_DURATION,
        MonkeyConstants.DEFAULT_FILL_DISK_ISSUE_DURATION + ""));

    restartRandomRsExceptMetaSleepTime = Long
      .parseLong(this.properties.getProperty(MonkeyConstants.RESTART_RANDOM_RS_EXCEPTION_SLEEP_TIME,
        MonkeyConstants.DEFAULT_RESTART_RANDOM_RS_EXCEPTION_SLEEP_TIME + ""));
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

    chaosEveryMilliSec =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.UNBALANCE_CHAOS_EVERY_MS,
        MonkeyConstants.DEFAULT_UNBALANCE_CHAOS_EVERY_MS + ""));
    waitForUnbalanceMilliSec =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.UNBALANCE_WAIT_FOR_UNBALANCE_MS,
        MonkeyConstants.DEFAULT_UNBALANCE_WAIT_FOR_UNBALANCE_MS + ""));
    waitForKillMilliSec =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.UNBALANCE_WAIT_FOR_KILLS_MS,
        MonkeyConstants.DEFAULT_UNBALANCE_WAIT_FOR_KILLS_MS + ""));
    waitAfterBalanceMilliSec =
      Long.parseLong(this.properties.getProperty(MonkeyConstants.UNBALANCE_WAIT_AFTER_BALANCE_MS,
        MonkeyConstants.DEFAULT_UNBALANCE_WAIT_AFTER_BALANCE_MS + ""));
    killMetaRs =
      Boolean.parseBoolean(this.properties.getProperty(MonkeyConstants.UNBALANCE_KILL_META_RS,
        MonkeyConstants.DEFAULT_UNBALANCE_KILL_META_RS + ""));
  }

  public abstract ChaosMonkey build();

  public static final String CALM = "calm";
  // TODO: the name has become a misnomer since the default (not-slow) monkey has been removed
  public static final String SLOW_DETERMINISTIC = "slowDeterministic";
  public static final String UNBALANCE = "unbalance";
  public static final String SERVER_KILLING = "serverKilling";
  public static final String STRESS_AM = "stressAM";
  public static final String NO_KILL = "noKill";
  public static final String MASTER_KILLING = "masterKilling";
  public static final String MOB_NO_KILL = "mobNoKill";
  public static final String MOB_SLOW_DETERMINISTIC = "mobSlowDeterministic";
  public static final String SERVER_AND_DEPENDENCIES_KILLING = "serverAndDependenciesKilling";
  public static final String DISTRIBUTED_ISSUES = "distributedIssues";
  public static final String DATA_ISSUES = "dataIssues";
  public static final String CONFIGURABLE_SLOW_DETERMINISTIC = "configurableSlowDeterministic";

  public static Map<String, MonkeyFactory> FACTORIES = ImmutableMap
    .<String, MonkeyFactory> builder().put(CALM, new CalmMonkeyFactory())
    .put(SLOW_DETERMINISTIC, new SlowDeterministicMonkeyFactory())
    .put(UNBALANCE, new UnbalanceMonkeyFactory())
    .put(SERVER_KILLING, new ServerKillingMonkeyFactory())
    .put(STRESS_AM, new StressAssignmentManagerMonkeyFactory())
    .put(NO_KILL, new NoKillMonkeyFactory()).put(MASTER_KILLING, new MasterKillingMonkeyFactory())
    .put(MOB_NO_KILL, new MobNoKillMonkeyFactory())
    .put(MOB_SLOW_DETERMINISTIC, new MobNoKillMonkeyFactory())
    .put(SERVER_AND_DEPENDENCIES_KILLING, new ServerAndDependenciesKillingMonkeyFactory())
    .put(DISTRIBUTED_ISSUES, new DistributedIssuesMonkeyFactory())
    .put(DATA_ISSUES, new DataIssuesMonkeyFactory())
    .put(CONFIGURABLE_SLOW_DETERMINISTIC, new ConfigurableSlowDeterministicMonkeyFactory()).build();

  public static MonkeyFactory getFactory(String factoryName) {
    MonkeyFactory fact = FACTORIES.get(factoryName);
    if (fact == null && factoryName != null && !factoryName.isEmpty()) {
      Class<? extends MonkeyFactory> klass = null;
      try {
        klass = Class.forName(factoryName).asSubclass(MonkeyFactory.class);
        if (klass != null) {
          LOG.info("Instantiating {}", klass.getName());
          fact = ReflectionUtils.newInstance(klass);
        }
      } catch (Exception e) {
        LOG.error("Error trying to create " + factoryName + " could not load it by class name");
        return null;
      }
    }
    return fact;
  }
}
