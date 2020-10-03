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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.regionserver.ReplicationSinkService;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;

@InterfaceAudience.Private
public class ReplicationSinkServiceImpl implements ReplicationSinkService {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationSinkServiceImpl.class);

  private Configuration conf;

  private Server server;

  private ReplicationSink replicationSink;

  // ReplicationLoad to access replication metrics
  private ReplicationLoad replicationLoad;

  private int statsPeriodInSecond;

  @Override
  public void replicateLogEntries(List<AdminProtos.WALEntry> entries, CellScanner cells,
    String replicationClusterId, String sourceBaseNamespaceDirPath,
    String sourceHFileArchiveDirPath) throws IOException {
    this.replicationSink.replicateEntries(entries, cells, replicationClusterId,
      sourceBaseNamespaceDirPath, sourceHFileArchiveDirPath);
  }

  @Override
  public void initialize(Server server, FileSystem fs, Path logdir, Path oldLogDir,
    WALFactory walFactory) throws IOException {
    this.server = server;
    this.conf = server.getConfiguration();
    this.statsPeriodInSecond =
      this.conf.getInt("replication.stats.thread.period.seconds", 5 * 60);
    this.replicationLoad = new ReplicationLoad();
  }

  @Override
  public void startReplicationService() throws IOException {
    this.replicationSink = new ReplicationSink(this.conf);
    this.server.getChoreService().scheduleChore(
        new ReplicationStatisticsChore("ReplicationSinkStatistics", server,
            (int) TimeUnit.SECONDS.toMillis(statsPeriodInSecond)));
  }

  @Override
  public void stopReplicationService() {
    if (this.replicationSink != null) {
      this.replicationSink.stopReplicationSinkServices();
    }
  }

  @Override
  public ReplicationLoad refreshAndGetReplicationLoad() {
    if (replicationLoad == null) {
      return null;
    }
    // always build for latest data
    replicationLoad.buildReplicationLoad(Collections.emptyList(), replicationSink.getSinkMetrics());
    return replicationLoad;
  }

  private final class ReplicationStatisticsChore extends ScheduledChore {

    ReplicationStatisticsChore(String name, Stoppable stopper, int period) {
      super(name, stopper, period);
    }

    @Override
    protected void chore() {
      printStats(replicationSink.getStats());
    }

    private void printStats(String stats) {
      if (!stats.isEmpty()) {
        LOG.info(stats);
      }
    }
  }
}
