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

package org.apache.hadoop.hbase.master.migrate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.ProcedureSyncWait;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.MigrateStoreFileTrackerProcedure;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * To avoid too many migrating/upgrade threads to be submitted at the time during master
 * initialization, RollingUpgradeChore handler all rolling-upgrade tasks.
 * */
@InterfaceAudience.Private
public class RollingUpgradeChore extends ScheduledChore {

  public static final String ROLLING_UPGRADE_CHORE_ENABLED_KEY =
    "hbase.master.rolling.upgrade.chore.enabled";
  public static final boolean DEFAULT_ROLLING_UPGRADE_CHORE_ENABLED = true;

  static final String ROLLING_UPGRADE_CHORE_TIMEUNIT_KEY =
    "hbase.master.rolling.upgrade.chore.timeunit";
  static final String DEFAULT_ROLLING_UPGRADE_CHORE_TIMEUNIT_KEY = TimeUnit.MILLISECONDS.name();

  static final String ROLLING_UPGRADE_CHORE_PERIOD_KEY = "hbase.master.rolling.upgrade.chore.period";
  static final int DFAULT_ROLLING_UPGRADE_CHORE_PERIOD = 1000 * 60 * 5; // 5 minutes in millis

  static final String ROLLING_UPGRADE_CHORE_DELAY_KEY = "hbase.master.mob.cleaner.period";
  static final long DEFAULT_ROLLING_UPGRADE_CHORE_DELAY = 1000L * 60 * 10; // 10 minutes in millis

  private final static Logger LOG = LoggerFactory.getLogger(RollingUpgradeChore.class);
  ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private TableDescriptors tableDescriptors;

  public RollingUpgradeChore(MasterServices masterServices) {
    this(masterServices.getConfiguration(), masterServices.getMasterProcedureExecutor(), masterServices.getTableDescriptors(), masterServices);
  }

  private RollingUpgradeChore(Configuration conf, ProcedureExecutor<MasterProcedureEnv> procedureExecutor, TableDescriptors tableDescriptors, Stoppable stopper){
    super(RollingUpgradeChore.class.getSimpleName(), stopper,
      conf.getInt(ROLLING_UPGRADE_CHORE_PERIOD_KEY, DFAULT_ROLLING_UPGRADE_CHORE_PERIOD),
      conf.getLong(ROLLING_UPGRADE_CHORE_DELAY_KEY, DEFAULT_ROLLING_UPGRADE_CHORE_DELAY),
      TimeUnit.valueOf(conf.get(ROLLING_UPGRADE_CHORE_TIMEUNIT_KEY, DEFAULT_ROLLING_UPGRADE_CHORE_TIMEUNIT_KEY)));
    this.procedureExecutor = procedureExecutor;
    this.tableDescriptors = tableDescriptors;
  }

  @Override
  protected void chore() {
    Map<String, TableDescriptor> migrateSFTTables;
    try {
      migrateSFTTables = tableDescriptors.getAll().entrySet().stream().filter(entry -> {
        TableDescriptor td = entry.getValue();
        return StringUtils.isEmpty(td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
      }).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    } catch (IOException e) {
      LOG.warn("Failed to migrate StoreFileTracker", e);
      return;
    }

    if (migrateSFTTables.isEmpty()) {
      LOG.info("There is no table to migrate StoreFileTracker, shutdown RollingUpgradeChore!");
      shutdown();
      return;
    }

    List<MigrateStoreFileTrackerProcedure> procs = new ArrayList<>();
    for (Map.Entry<String, TableDescriptor> entry : migrateSFTTables.entrySet()) {
      TableDescriptor tableDescriptor = entry.getValue();
      MigrateStoreFileTrackerProcedure proc =
        new MigrateStoreFileTrackerProcedure(procedureExecutor.getEnvironment(), tableDescriptor);
      procedureExecutor.submitProcedure(proc);
      procs.add(proc);
    }

    for (MigrateStoreFileTrackerProcedure proc : procs) {
      try {
        ProcedureSyncWait.waitForProcedureToComplete(procedureExecutor, proc, 60000);
      } catch (IOException e) {
        LOG.warn("Failed to migrate StoreFileTracker for table {}", proc.getTableName());
      }
    }
  }
}
