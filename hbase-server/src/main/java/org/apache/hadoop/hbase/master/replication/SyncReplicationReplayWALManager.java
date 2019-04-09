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

import static org.apache.hadoop.hbase.replication.ReplicationUtils.getPeerRemoteWALDir;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.getPeerReplayWALDir;
import static org.apache.hadoop.hbase.replication.ReplicationUtils.getPeerSnapshotWALDir;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.ServerListener;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureScheduler;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * The manager for replaying remote wal.
 * <p/>
 * First, it will be used to balance the replay work across all the region servers. We will record
 * the region servers which have already been used for replaying wal, and prevent sending new replay
 * work to it, until the previous replay work has been done, where we will remove the region server
 * from the used worker set. See the comment for {@code UsedReplayWorkersForPeer} for more details.
 * <p/>
 * Second, the logic for managing the remote wal directory is kept here. Before replaying the wals,
 * we will rename the remote wal directory, the new name is called 'replay' directory, see
 * {@link #renameToPeerReplayWALDir(String)}. This is used to prevent further writing of remote
 * wals, which is very important for keeping consistency. And then we will start replaying all the
 * wals, once a wal has been replayed, we will truncate the file, so that if there are crashes
 * happen, we do not need to replay all the wals again, see {@link #finishReplayWAL(String)} and
 * {@link #isReplayWALFinished(String)}. After replaying all the wals, we will rename the 'replay'
 * directory, the new name is called 'snapshot' directory. In the directory, we will keep all the
 * names for the wals being replayed, since all the files should have been truncated. When we
 * transitting original the ACTIVE cluster to STANDBY later, and there are region server crashes, we
 * will see the wals in this directory to determine whether a wal should be split and replayed or
 * not. You can see the code in {@link org.apache.hadoop.hbase.regionserver.SplitLogWorker} for more
 * details.
 */
@InterfaceAudience.Private
public class SyncReplicationReplayWALManager {

  private static final Logger LOG = LoggerFactory.getLogger(SyncReplicationReplayWALManager.class);

  private final ServerManager serverManager;

  private final FileSystem fs;

  private final Path walRootDir;

  private final Path remoteWALDir;

  /**
   * This class is used to record the used workers(region servers) for a replication peer. For
   * balancing the replaying remote wal job, we will only schedule one remote replay procedure each
   * time. So when acquiring a worker, we will first get all the region servers for this cluster,
   * and then filter out the used ones.
   * <p/>
   * The {@link ProcedureEvent} is used for notifying procedures that there are available workers
   * now. We used to use sleeping and retrying before, but if the interval is too large, for
   * example, exponential backoff, then it is not effective, but if the interval is too small, then
   * we will flood the procedure wal.
   * <p/>
   * The states are only kept in memory, so when restarting, we need to reconstruct these
   * information, using the information stored in related procedures. See the {@code afterReplay}
   * method in {@link RecoverStandbyProcedure} and {@link SyncReplicationReplayWALProcedure} for
   * more details.
   */
  private static final class UsedReplayWorkersForPeer {

    private final Set<ServerName> usedWorkers = new HashSet<ServerName>();

    private final ProcedureEvent<?> event;

    public UsedReplayWorkersForPeer(String peerId) {
      this.event = new ProcedureEvent<>(peerId);
    }

    public void used(ServerName worker) {
      usedWorkers.add(worker);
    }

    public Optional<ServerName> acquire(ServerManager serverManager) {
      Optional<ServerName> worker = serverManager.getOnlineServers().keySet().stream()
        .filter(server -> !usedWorkers.contains(server)).findAny();
      worker.ifPresent(usedWorkers::add);
      return worker;
    }

    public void release(ServerName worker) {
      usedWorkers.remove(worker);
    }

    public void suspend(Procedure<?> proc) {
      event.suspend();
      event.suspendIfNotReady(proc);
    }

    public void wake(MasterProcedureScheduler scheduler) {
      if (!event.isReady()) {
        event.wake(scheduler);
      }
    }
  }

  private final ConcurrentMap<String, UsedReplayWorkersForPeer> usedWorkersByPeer =
    new ConcurrentHashMap<>();

  public SyncReplicationReplayWALManager(MasterServices services)
      throws IOException, ReplicationException {
    this.serverManager = services.getServerManager();
    this.fs = services.getMasterFileSystem().getWALFileSystem();
    this.walRootDir = services.getMasterFileSystem().getWALRootDir();
    this.remoteWALDir = new Path(this.walRootDir, ReplicationUtils.REMOTE_WAL_DIR_NAME);
    serverManager.registerListener(new ServerListener() {

      @Override
      public void serverAdded(ServerName serverName) {
        MasterProcedureScheduler scheduler =
          services.getMasterProcedureExecutor().getEnvironment().getProcedureScheduler();
        for (UsedReplayWorkersForPeer usedWorkers : usedWorkersByPeer.values()) {
          synchronized (usedWorkers) {
            usedWorkers.wake(scheduler);
          }
        }
      }
    });
  }

  public void registerPeer(String peerId) {
    usedWorkersByPeer.put(peerId, new UsedReplayWorkersForPeer(peerId));
  }

  public void unregisterPeer(String peerId) {
    usedWorkersByPeer.remove(peerId);
  }

  /**
   * Get a worker for replaying remote wal for a give peer. If no worker available, i.e, all the
   * region servers have been used by others, a {@link ProcedureSuspendedException} will be thrown
   * to suspend the procedure. And it will be woken up later when there are available workers,
   * either by others release a worker, or there is a new region server joins the cluster.
   */
  public ServerName acquirePeerWorker(String peerId, Procedure<?> proc)
      throws ProcedureSuspendedException {
    UsedReplayWorkersForPeer usedWorkers = usedWorkersByPeer.get(peerId);
    synchronized (usedWorkers) {
      Optional<ServerName> worker = usedWorkers.acquire(serverManager);
      if (worker.isPresent()) {
        return worker.get();
      }
      // no worker available right now, suspend the procedure
      usedWorkers.suspend(proc);
    }
    throw new ProcedureSuspendedException();
  }

  public void releasePeerWorker(String peerId, ServerName worker,
      MasterProcedureScheduler scheduler) {
    UsedReplayWorkersForPeer usedWorkers = usedWorkersByPeer.get(peerId);
    synchronized (usedWorkers) {
      usedWorkers.release(worker);
      usedWorkers.wake(scheduler);
    }
  }

  /**
   * Will only be called when loading procedures, where we need to construct the used worker set for
   * each peer.
   */
  public void addUsedPeerWorker(String peerId, ServerName worker) {
    usedWorkersByPeer.get(peerId).used(worker);
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
