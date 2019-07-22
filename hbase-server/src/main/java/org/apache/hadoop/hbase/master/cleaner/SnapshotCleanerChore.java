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

package org.apache.hadoop.hbase.master.cleaner;


import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos;

/**
 * This chore, every time it runs, will try to delete snapshots that are expired based on TTL in
 * seconds configured for each Snapshot
 */
@InterfaceAudience.Private
public class SnapshotCleanerChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotCleanerChore.class);
  private static final String SNAPSHOT_CLEANER_CHORE_NAME = "SnapshotCleaner";
  private static final String SNAPSHOT_CLEANER_INTERVAL = "hbase.master.cleaner.snapshot.interval";
  private static final int SNAPSHOT_CLEANER_DEFAULT_INTERVAL = 1800 * 1000; // Default 30 min
  private static final String DELETE_SNAPSHOT_EVENT =
          "Eligible Snapshot for cleanup due to expired TTL.";

  private final SnapshotManager snapshotManager;

  /**
   * Construct Snapshot Cleaner Chore with parameterized constructor
   *
   * @param stopper When {@link Stoppable#isStopped()} is true, this chore will cancel and cleanup
   * @param configuration The configuration to set
   * @param snapshotManager SnapshotManager instance to manage lifecycle of snapshot
   */
  public SnapshotCleanerChore(Stoppable stopper, Configuration configuration,
          SnapshotManager snapshotManager) {
    super(SNAPSHOT_CLEANER_CHORE_NAME, stopper, configuration.getInt(SNAPSHOT_CLEANER_INTERVAL,
            SNAPSHOT_CLEANER_DEFAULT_INTERVAL));
    this.snapshotManager = snapshotManager;
  }

  @Override
  protected void chore() {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Snapshot Cleaner Chore is starting up...");
    }
    try {
      List<SnapshotProtos.SnapshotDescription> completedSnapshotsList =
              this.snapshotManager.getCompletedSnapshots();
      for (SnapshotProtos.SnapshotDescription snapshotDescription : completedSnapshotsList) {
        long snapshotCreatedTime = snapshotDescription.getCreationTime();
        long snapshotTtl = snapshotDescription.getTtl();
        /*
         * Backward compatibility after the patch deployment on HMaster
         * Any snapshot with ttl 0 is to be considered as snapshot to keep FOREVER
         * Default ttl value specified by {@HConstants.DEFAULT_SNAPSHOT_TTL}
         */
        if (snapshotCreatedTime > 0 && snapshotTtl > 0 &&
                snapshotTtl < TimeUnit.MILLISECONDS.toSeconds(Long.MAX_VALUE)) {
          long currentTime = EnvironmentEdgeManager.currentTime();
          if ((snapshotCreatedTime + TimeUnit.SECONDS.toMillis(snapshotTtl)) < currentTime) {
            LOG.info("Event: {} Name: {}, CreatedTime: {}, TTL: {}, currentTime: {}",
                    DELETE_SNAPSHOT_EVENT, snapshotDescription.getName(), snapshotCreatedTime,
                    snapshotTtl, currentTime);
            deleteExpiredSnapshot(snapshotDescription);
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Error while cleaning up Snapshots...", e);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Snapshot Cleaner Chore is closing...");
    }
  }

  private void deleteExpiredSnapshot(SnapshotProtos.SnapshotDescription snapshotDescription) {
    try {
      this.snapshotManager.deleteSnapshot(snapshotDescription);
    } catch (Exception e) {
      LOG.error("Error while deleting Snapshot: {}", snapshotDescription.getName(), e);
    }
  }

}
