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
package org.apache.hadoop.hbase.replication.regionserver;

import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * The Class MetricsReplicationSourceRefresherChore for 
 * refreshing age related replication source metrics
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MetricsReplicationSourceRefresherChore extends ScheduledChore {

  private ReplicationSource replicationSource;

  private MetricsSource metrics;

  public static final String DURATION = "hbase.metrics.replication.source.refresher.duration";
  public static final int DEFAULT_DURATION_MILLISECONDS = 60000;

  public MetricsReplicationSourceRefresherChore(Stoppable stopper,
      ReplicationSource replicationSource) {
    this(DEFAULT_DURATION_MILLISECONDS, stopper, replicationSource);
  }

  public MetricsReplicationSourceRefresherChore(int duration, Stoppable stopper,
      ReplicationSource replicationSource) {
    super("MetricsSourceRefresherChore", stopper, duration);
    this.replicationSource = replicationSource;
    this.metrics = this.replicationSource.getSourceMetrics();
  }

  @Override
  protected void chore() {
    this.metrics.setOldestWalAge(getOldestWalAge());
  }

  /*
   * Returns the age of oldest wal.
   */
  long getOldestWalAge() {
    long now = EnvironmentEdgeManager.currentTime();
    long timestamp = getOldestWalTimestamp();
    if (timestamp == Long.MAX_VALUE) {
      // If there are no wals in the queue then set the oldest wal timestamp to current time
      // so that the oldest wal age will be 0.
      timestamp = now;
    }
    long age = now - timestamp;
    return age;
  }

  /*
   * Get the oldest wal timestamp from all the queues.
   */
  private long getOldestWalTimestamp() {
    long oldestWalTimestamp = Long.MAX_VALUE;
    for (Map.Entry<String, PriorityBlockingQueue<Path>> entry : this.replicationSource.getQueues()
        .entrySet()) {
      PriorityBlockingQueue<Path> queue = entry.getValue();
      Path path = queue.peek();
      // Can path ever be null ?
      if (path != null) {
        oldestWalTimestamp =
            Math.min(oldestWalTimestamp, AbstractFSWALProvider.WALStartTimeComparator.getTS(path));
      }
    }
    return oldestWalTimestamp;
  }
}
