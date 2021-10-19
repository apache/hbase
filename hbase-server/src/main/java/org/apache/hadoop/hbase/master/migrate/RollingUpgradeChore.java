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
import java.util.Iterator;
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
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.MigrateStoreFileTrackerProcedure;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To avoid too many migrating/upgrade threads to be submitted at the time during master
 * initialization, RollingUpgradeChore handles all rolling-upgrade tasks.
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

  static final String ROLLING_UPGRADE_CHORE_DELAY_KEY = "hbase.master.rolling.upgrade.chore.delay";
  static final long DEFAULT_ROLLING_UPGRADE_CHORE_DELAY = 1000L * 60 * 10; // 10 minutes in millis

  private final static Logger LOG = LoggerFactory.getLogger(RollingUpgradeChore.class);
  ProcedureExecutor<MasterProcedureEnv> procedureExecutor;
  private TableDescriptors tableDescriptors;
  private List<MigrateStoreFileTrackerProcedure> processingProcs = new ArrayList<>();
  private boolean isMigratingDone;

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
    migrateStoreFileTracker(5);
    if (isMigratingDone) {
      LOG.info("All Rolling-Upgrade tasks are complete, shutdown RollingUpgradeChore!");
      shutdown();
    }
  }

  private void migrateStoreFileTracker(int concurrentCount){
    Iterator<MigrateStoreFileTrackerProcedure> iter = processingProcs.iterator();
    while(iter.hasNext()){
      MigrateStoreFileTrackerProcedure proc = iter.next();
      if(procedureExecutor.isFinished(proc.getProcId())){
        iter.remove();
      }
    }
    // No new migration procedures will be submitted until
    // all procedures executed last time are completed.
    if (!processingProcs.isEmpty()) {
      return;
    }

    Map<String, TableDescriptor> migrateSFTTables;
    try {
      migrateSFTTables = tableDescriptors.getAll().entrySet().stream().filter(entry -> {
        TableDescriptor td = entry.getValue();
        return StringUtils.isEmpty(td.getValue(StoreFileTrackerFactory.TRACKER_IMPL));
      }).limit(concurrentCount).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    } catch (IOException e) {
      LOG.warn("Failed to migrate StoreFileTracker", e);
      return;
    }

    if (migrateSFTTables.isEmpty()) {
      LOG.info("There is no table to migrate StoreFileTracker!");
      isMigratingDone = true;
      return;
    }

    for (Map.Entry<String, TableDescriptor> entry : migrateSFTTables.entrySet()) {
      TableDescriptor tableDescriptor = entry.getValue();
      MigrateStoreFileTrackerProcedure proc =
        new MigrateStoreFileTrackerProcedure(procedureExecutor.getEnvironment(), tableDescriptor);
      procedureExecutor.submitProcedure(proc);
      processingProcs.add(proc);
    }
  }
}
