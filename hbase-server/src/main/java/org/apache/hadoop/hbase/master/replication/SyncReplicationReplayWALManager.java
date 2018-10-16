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

import static org.apache.hadoop.hbase.replication.ReplicationUtils.REMOTE_WAL_REPLAY_SUFFIX;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.getPeerRemoteWALDir;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.getPeerReplayWALDir;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.getPeerSnapshotWALDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class SyncReplicationReplayWALManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(SyncReplicationReplayWALManager.class);

  private final MasterServices services;

  private final FileSystem fs;

  private final Path walRootDir;

  private final Path remoteWALDir;

  private final ZKSyncReplicationReplayWALWorkerStorage workerStorage;

  private final Map<String, Set<ServerName>> workers = new HashMap<>();

  private final Object workerLock = new Object();

  public SyncReplicationReplayWALManager(MasterServices services)
      throws IOException, ReplicationException {
    this.services = services;
    this.fs = services.getMasterFileSystem().getWALFileSystem();
    this.walRootDir = services.getMasterFileSystem().getWALRootDir();
    this.remoteWALDir = new Path(this.walRootDir, ReplicationUtils.REMOTE_WAL_DIR_NAME);
    this.workerStorage = new ZKSyncReplicationReplayWALWorkerStorage(services.getZooKeeper(),
        services.getConfiguration());
    checkReplayingWALDir();
  }

  private void checkReplayingWALDir() throws IOException, ReplicationException {
    FileStatus[] files = fs.listStatus(remoteWALDir);
    for (FileStatus file : files) {
      String name = file.getPath().getName();
      if (name.endsWith(REMOTE_WAL_REPLAY_SUFFIX)) {
        String peerId = name.substring(0, name.length() - REMOTE_WAL_REPLAY_SUFFIX.length());
        workers.put(peerId, workerStorage.getPeerWorkers(peerId));
      }
    }
  }

  public void registerPeer(String peerId) throws ReplicationException {
    workers.put(peerId, new HashSet<>());
    workerStorage.addPeer(peerId);
  }

  public void unregisterPeer(String peerId) throws ReplicationException {
    workers.remove(peerId);
    workerStorage.removePeer(peerId);
  }

  public ServerName getPeerWorker(String peerId) throws ReplicationException {
    Optional<ServerName> worker = Optional.empty();
    ServerName workerServer = null;
    synchronized (workerLock) {
      worker = services.getServerManager().getOnlineServers().keySet().stream()
          .filter(server -> !workers.get(peerId).contains(server)).findFirst();
      if (worker.isPresent()) {
        workerServer = worker.get();
        workers.get(peerId).add(workerServer);
      }
    }
    if (workerServer != null) {
      workerStorage.addPeerWorker(peerId, workerServer);
    }
    return workerServer;
  }

  public void removePeerWorker(String peerId, ServerName worker) throws ReplicationException {
    synchronized (workerLock) {
      workers.get(peerId).remove(worker);
    }
    workerStorage.removePeerWorker(peerId, worker);
  }
  public void createPeerRemoteWALDir(String peerId) throws IOException {
    Path peerRemoteWALDir = getPeerRemoteWALDir(remoteWALDir, peerId);
    if (!fs.exists(peerRemoteWALDir) && !fs.mkdirs(peerRemoteWALDir)) {
      throw new IOException("Unable to mkdir " + peerRemoteWALDir);
    }
  }

  private void rename(Path src, Path dst, String peerId) throws IOException {
    if (fs.exists(src)) {
      deleteDir(dst, peerId);
      if (!fs.rename(src, dst)) {
        throw new IOException(
            "Failed to rename dir from " + src + " to " + dst + " for peer id=" + peerId);
      }
      LOG.info("Renamed dir from {} to {} for peer id={}", src, dst, peerId);
    } else if (!fs.exists(dst)) {
      throw new IOException(
          "Want to rename from " + src + " to " + dst + ", but they both do not exist");
    }
  }

  public void renameToPeerReplayWALDir(String peerId) throws IOException {
    rename(getPeerRemoteWALDir(remoteWALDir, peerId), getPeerReplayWALDir(remoteWALDir, peerId),
        peerId);
  }

  public void renameToPeerSnapshotWALDir(String peerId) throws IOException {
    rename(getPeerReplayWALDir(remoteWALDir, peerId), getPeerSnapshotWALDir(remoteWALDir, peerId),
        peerId);
  }

  public List<Path> getReplayWALsAndCleanUpUnusedFiles(String peerId) throws IOException {
    Path peerReplayWALDir = getPeerReplayWALDir(remoteWALDir, peerId);
    for (FileStatus status : fs.listStatus(peerReplayWALDir,
      p -> p.getName().endsWith(ReplicationUtils.RENAME_WAL_SUFFIX))) {
      Path src = status.getPath();
      String srcName = src.getName();
      String dstName =
          srcName.substring(0, srcName.length() - ReplicationUtils.RENAME_WAL_SUFFIX.length());
      FSUtils.renameFile(fs, src, new Path(src.getParent(), dstName));
    }
    List<Path> wals = new ArrayList<>();
    for (FileStatus status : fs.listStatus(peerReplayWALDir)) {
      Path path = status.getPath();
      if (path.getName().endsWith(ReplicationUtils.SYNC_WAL_SUFFIX)) {
        wals.add(path);
      } else {
        if (!fs.delete(path, true)) {
          LOG.warn("Can not delete unused file: " + path);
        }
      }
    }
    return wals;
  }

  public void snapshotPeerReplayWALDir(String peerId) throws IOException {
    Path peerReplayWALDir = getPeerReplayWALDir(remoteWALDir, peerId);
    if (fs.exists(peerReplayWALDir) && !fs.delete(peerReplayWALDir, true)) {
      throw new IOException(
          "Failed to remove replay wals dir " + peerReplayWALDir + " for peer id=" + peerId);
    }
  }

  private void deleteDir(Path dir, String peerId) throws IOException {
    if (!fs.delete(dir, true) && fs.exists(dir)) {
      throw new IOException("Failed to remove dir " + dir + " for peer id=" + peerId);
    }
  }

  public void removePeerRemoteWALs(String peerId) throws IOException {
    deleteDir(getPeerRemoteWALDir(remoteWALDir, peerId), peerId);
    deleteDir(getPeerReplayWALDir(remoteWALDir, peerId), peerId);
    deleteDir(getPeerSnapshotWALDir(remoteWALDir, peerId), peerId);
  }

  public String removeWALRootPath(Path path) {
    String pathStr = path.toString();
    // remove the "/" too.
    return pathStr.substring(walRootDir.toString().length() + 1);
  }

  public void finishReplayWAL(String wal) throws IOException {
    Path walPath = new Path(walRootDir, wal);
    fs.truncate(walPath, 0);
  }

  public boolean isReplayWALFinished(String wal) throws IOException {
    Path walPath = new Path(walRootDir, wal);
    return fs.getFileStatus(walPath).getLen() == 0;
  }

  @VisibleForTesting
  public Path getRemoteWALDir() {
    return remoteWALDir;
  }
}
