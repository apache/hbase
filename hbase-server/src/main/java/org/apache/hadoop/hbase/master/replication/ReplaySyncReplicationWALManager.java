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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class ReplaySyncReplicationWALManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(ReplaySyncReplicationWALManager.class);

  private static final String REPLAY_SUFFIX = "-replay";

  private final MasterServices services;

  private final Configuration conf;

  private final FileSystem fs;

  private final Path walRootDir;

  private final Path remoteWALDir;

  private final Map<String, BlockingQueue<ServerName>> availServers = new HashMap<>();

  public ReplaySyncReplicationWALManager(MasterServices services) {
    this.services = services;
    this.conf = services.getConfiguration();
    this.fs = services.getMasterFileSystem().getWALFileSystem();
    this.walRootDir = services.getMasterFileSystem().getWALRootDir();
    this.remoteWALDir = new Path(this.walRootDir, ReplicationUtils.REMOTE_WAL_DIR_NAME);
  }

  public Path getPeerRemoteWALDir(String peerId) {
    return new Path(this.remoteWALDir, peerId);
  }

  private Path getPeerReplayWALDir(String peerId) {
    return getPeerRemoteWALDir(peerId).suffix(REPLAY_SUFFIX);
  }

  public void createPeerRemoteWALDir(String peerId) throws IOException {
    Path peerRemoteWALDir = getPeerRemoteWALDir(peerId);
    if (!fs.exists(peerRemoteWALDir) && !fs.mkdirs(peerRemoteWALDir)) {
      throw new IOException("Unable to mkdir " + peerRemoteWALDir);
    }
  }

  public void renamePeerRemoteWALDir(String peerId) throws IOException {
    Path peerRemoteWALDir = getPeerRemoteWALDir(peerId);
    Path peerReplayWALDir = peerRemoteWALDir.suffix(REPLAY_SUFFIX);
    if (fs.exists(peerRemoteWALDir)) {
      if (!fs.rename(peerRemoteWALDir, peerReplayWALDir)) {
        throw new IOException("Failed rename remote wal dir from " + peerRemoteWALDir + " to "
            + peerReplayWALDir + " for peer id=" + peerId);
      }
      LOG.info("Rename remote wal dir from {} to {} for peer id={}", remoteWALDir, peerReplayWALDir,
        peerId);
    } else if (!fs.exists(peerReplayWALDir)) {
      throw new IOException("Remote wal dir " + peerRemoteWALDir + " and replay wal dir "
          + peerReplayWALDir + " not exist for peer id=" + peerId);
    }
  }

  public List<Path> getReplayWALs(String peerId) throws IOException {
    Path peerReplayWALDir = getPeerReplayWALDir(peerId);
    List<Path> replayWals = new ArrayList<>();
    RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(peerReplayWALDir, false);
    while (iterator.hasNext()) {
      replayWals.add(iterator.next().getPath());
    }
    return replayWals;
  }

  public void removePeerReplayWALDir(String peerId) throws IOException {
    Path peerReplayWALDir = getPeerReplayWALDir(peerId);
    if (fs.exists(peerReplayWALDir) && !fs.delete(peerReplayWALDir, true)) {
      throw new IOException(
          "Failed to remove replay wals dir " + peerReplayWALDir + " for peer id=" + peerId);
    }
  }

  public void initPeerWorkers(String peerId) {
    BlockingQueue<ServerName> servers = new LinkedBlockingQueue<>();
    services.getServerManager().getOnlineServers().keySet()
        .forEach(server -> servers.offer(server));
    availServers.put(peerId, servers);
  }

  public ServerName getAvailServer(String peerId, long timeout, TimeUnit unit)
      throws InterruptedException {
    return availServers.get(peerId).poll(timeout, unit);
  }

  public void addAvailServer(String peerId, ServerName server) {
    availServers.get(peerId).offer(server);
  }

  public String removeWALRootPath(Path path) {
    String pathStr = path.toString();
    // remove the "/" too.
    return pathStr.substring(walRootDir.toString().length() + 1);
  }
}
