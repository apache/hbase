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
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationBulkLoadEventTracker;
import org.apache.hadoop.hbase.replication.regionserver.ZKReplicationBulkLoadEventTracker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cleans completed replicated bulk load event markers after they are old enough.
 */
@InterfaceAudience.Private
public class ReplicationBulkLoadEventCleaner extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(ReplicationBulkLoadEventCleaner.class);

  public static final String PERIOD_MS_KEY =
    "hbase.master.cleaner.replication.bulkload.event.period.ms";
  public static final int PERIOD_MS_DEFAULT = (int) TimeUnit.MINUTES.toMillis(10);
  public static final String DONE_TTL_MS_KEY = "hbase.replication.bulkload.event.done.ttl.ms";
  public static final long DONE_TTL_MS_DEFAULT = TimeUnit.DAYS.toMillis(1);

  private final ReplicationBulkLoadEventTracker tracker;
  private final long doneTtlMs;

  public ReplicationBulkLoadEventCleaner(Configuration conf, Stoppable stopper, ZKWatcher zkw) {
    super("ReplicationBulkLoadEventCleaner", stopper,
      conf.getInt(PERIOD_MS_KEY, PERIOD_MS_DEFAULT));
    this.tracker = new ZKReplicationBulkLoadEventTracker(conf, zkw);
    this.doneTtlMs = conf.getLong(DONE_TTL_MS_KEY, DONE_TTL_MS_DEFAULT);
  }

  @Override
  protected void chore() {
    try {
      int deleted = tracker.cleanDoneMarkers(doneTtlMs);
      if (deleted > 0) {
        LOG.info("Cleaned {} replicated bulkload event marker(s)", deleted);
      }
    } catch (IOException e) {
      LOG.warn("Failed to clean replicated bulkload event markers", e);
    }
  }
}
