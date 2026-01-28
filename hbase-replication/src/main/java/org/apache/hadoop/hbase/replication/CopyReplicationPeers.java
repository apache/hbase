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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tool for copying replication peer data across different replication peer storages.
 * <p/>
 * Notice that we will not delete the replication peer data from the source storage, as this tool
 * can also be used by online migration. See HBASE-27110 for the whole design.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class CopyReplicationPeers extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(CopyReplicationPeers.class);

  public static final String NAME = "copyreppeers";

  public CopyReplicationPeers(Configuration conf) {
    super(conf);
  }

  private ReplicationPeerStorage create(String type, FileSystem fs, ZKWatcher zk) {
    Configuration conf = new Configuration(getConf());
    conf.set(ReplicationStorageFactory.REPLICATION_PEER_STORAGE_IMPL, type);
    return ReplicationStorageFactory.getReplicationPeerStorage(fs, zk, conf);
  }

  private ZKWatcher createZKWatcher() throws IOException {
    return new ZKWatcher(getConf(), getClass().getSimpleName(), new Abortable() {

      private volatile boolean aborted;

      @Override
      public boolean isAborted() {
        return aborted;
      }

      @Override
      public void abort(String why, Throwable e) {
        aborted = true;
        LOG.error(why, e);
        ExitHandler.getInstance().exit(1);
      }
    });
  }

  private void migrate(ReplicationPeerStorage src, ReplicationPeerStorage dst)
    throws ReplicationException {
    LOG.info("Start migrating from {} to {}", src.getClass().getSimpleName(),
      dst.getClass().getSimpleName());
    for (String peerId : src.listPeerIds()) {
      LOG.info("Going to migrate {}", peerId);
      ReplicationPeerConfig peerConfig = src.getPeerConfig(peerId);
      boolean enabled = src.isPeerEnabled(peerId);
      SyncReplicationState syncState = src.getPeerSyncReplicationState(peerId);
      SyncReplicationState newSyncState = src.getPeerNewSyncReplicationState(peerId);
      if (newSyncState != SyncReplicationState.NONE) {
        throw new IllegalStateException("Can not migrate peer " + peerId
          + " as it is in an intermediate state, syncReplicationState is " + syncState
          + " while newSyncReplicationState is " + newSyncState);
      }
      dst.addPeer(peerId, peerConfig, enabled, syncState);
      LOG.info("Migrated peer {}, peerConfig = '{}', enabled = {}, syncReplicationState = {}",
        peerId, peerConfig, enabled, syncState);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: bin/hbase " + NAME
        + " <SRC_REPLICATION_PEER_STORAGE> <DST_REPLICATION_PEER_STORAGE>");
      System.err.println("The possible values for replication storage type:");
      for (ReplicationPeerStorageType type : ReplicationPeerStorageType.values()) {
        System.err.println("  " + type.name().toLowerCase());
      }
      return -1;
    }
    FileSystem fs = FileSystem.get(getConf());
    try (ZKWatcher zk = createZKWatcher()) {
      ReplicationPeerStorage src = create(args[0], fs, zk);
      ReplicationPeerStorage dst = create(args[1], fs, zk);
      migrate(src, dst);
    }
    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new CopyReplicationPeers(conf), args);
    ExitHandler.getInstance().exit(ret);
  }
}
