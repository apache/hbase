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
package org.apache.hadoop.hbase.replication.master;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.master.procedure.ServerCrashProcedure;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationOffsetUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Predicate;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for replication before
 * deleting it when its TTL is over.
 * <p/>
 * The logic is a bit complicated after we switch to use table based replication queue storage, see
 * the design doc in HBASE-27109 and the comments in HBASE-27214 for more details.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationLogCleaner extends BaseLogCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogCleaner.class);
  private Set<ServerName> notFullyDeadServers;
  private Set<String> peerIds;
  // ServerName -> PeerId -> WalGroup -> Offset
  // Here the server name is the source server name, so we can make sure that there is only one
  // queue for a given peer, that why we can use a String peerId as key instead of
  // ReplicationQueueId.
  private Map<ServerName, Map<String, Map<String, ReplicationGroupOffset>>> replicationOffsets;
  private MasterServices masterService;
  private ReplicationLogCleanerBarrier barrier;
  private ReplicationPeerManager rpm;
  private Supplier<Set<ServerName>> getNotFullyDeadServers;

  private boolean canFilter;
  private boolean stopped = false;

  @Override
  public void preClean() {
    if (this.getConf() == null || isAsyncClusterConnectionClosedOrNull()) {
      LOG.warn(
        "Skipping preClean because configuration is null or asyncClusterConnection is unavailable.");
      return;
    }

    try {
      if (!rpm.getQueueStorage().hasData()) {
        return;
      }
    } catch (ReplicationException e) {
      LOG.error("Error occurred while executing queueStorage.hasData()", e);
      return;
    }
    canFilter = barrier.start();
    if (canFilter) {
      notFullyDeadServers = getNotFullyDeadServers.get();
      peerIds = rpm.listPeers(null).stream().map(ReplicationPeerDescription::getPeerId)
        .collect(Collectors.toSet());
      // must get the not fully dead servers first and then get the replication queue data, in this
      // way we can make sure that, we should have added the missing replication queues for the dead
      // region servers recorded in the above set, otherwise the logic in the
      // filterForDeadRegionServer method may lead us delete wal still in use.
      List<ReplicationQueueData> allQueueData;
      try {
        allQueueData = rpm.getQueueStorage().listAllQueues();
      } catch (ReplicationException e) {
        LOG.error("Can not list all replication queues, give up cleaning", e);
        barrier.stop();
        canFilter = false;
        notFullyDeadServers = null;
        peerIds = null;
        return;
      }
      replicationOffsets = new HashMap<>();
      for (ReplicationQueueData queueData : allQueueData) {
        ReplicationQueueId queueId = queueData.getId();
        ServerName serverName = queueId.getServerWALsBelongTo();
        Map<String, Map<String, ReplicationGroupOffset>> peerId2Offsets =
          replicationOffsets.computeIfAbsent(serverName, k -> new HashMap<>());
        Map<String, ReplicationGroupOffset> offsets =
          peerId2Offsets.computeIfAbsent(queueId.getPeerId(), k -> new HashMap<>());
        offsets.putAll(queueData.getOffsets());
      }
    } else {
      LOG.info("Skip replication log cleaner because an AddPeerProcedure is running");
    }
  }

  @Override
  public void postClean() {
    if (canFilter) {
      barrier.stop();
      canFilter = false;
      // release memory
      notFullyDeadServers = null;
      peerIds = null;
      replicationOffsets = null;
    }
  }

  private boolean shouldDelete(ReplicationGroupOffset offset, FileStatus file) {
    return !ReplicationOffsetUtil.shouldReplicate(offset, file.getPath().getName());
  }

  private boolean filterForLiveRegionServer(ServerName serverName, FileStatus file) {
    Map<String, Map<String, ReplicationGroupOffset>> peerId2Offsets =
      replicationOffsets.get(serverName);
    if (peerId2Offsets == null) {
      // if there are replication queues missing, we can not delete the wal
      return false;
    }
    for (String peerId : peerIds) {
      Map<String, ReplicationGroupOffset> offsets = peerId2Offsets.get(peerId);
      // if no replication queue for a peer, we can not delete the wal
      if (offsets == null) {
        return false;
      }
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(file.getPath().getName());
      ReplicationGroupOffset offset = offsets.get(walGroupId);
      // if a replication queue still need to replicate this wal, we can not delete it
      if (!shouldDelete(offset, file)) {
        return false;
      }
    }
    // if all replication queues have already finished replicating this wal, we can delete it.
    return true;
  }

  private boolean filterForDeadRegionServer(ServerName serverName, FileStatus file) {
    Map<String, Map<String, ReplicationGroupOffset>> peerId2Offsets =
      replicationOffsets.get(serverName);
    if (peerId2Offsets == null) {
      // no replication queue for this dead rs, we can delete all wal files for it
      return true;
    }
    for (String peerId : peerIds) {
      Map<String, ReplicationGroupOffset> offsets = peerId2Offsets.get(peerId);
      if (offsets == null) {
        // for dead server, we only care about existing replication queues, as we will delete a
        // queue after we finish replicating it.
        continue;
      }
      String walGroupId = AbstractFSWALProvider.getWALPrefixFromWALName(file.getPath().getName());
      ReplicationGroupOffset offset = offsets.get(walGroupId);
      // if a replication queue still need to replicate this wal, we can not delete it
      if (!shouldDelete(offset, file)) {
        return false;
      }
    }
    // if all replication queues have already finished replicating this wal, we can delete it.
    return true;
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    // all members of this class are null if replication is disabled,
    // so we cannot filter the files
    if (this.getConf() == null) {
      return files;
    }

    if (isAsyncClusterConnectionClosedOrNull()) {
      LOG.warn("Skip getting deletable files because asyncClusterConnection is unavailable.");
      // asyncClusterConnection is unavailable, we shouldn't delete any files.
      return Collections.emptyList();
    }

    try {
      if (!rpm.getQueueStorage().hasData()) {
        return files;
      }
    } catch (ReplicationException e) {
      LOG.error("Error occurred while executing queueStorage.hasData()", e);
      return Collections.emptyList();
    }
    if (!canFilter) {
      // We can not delete anything if there are AddPeerProcedure running at the same time
      // See HBASE-27214 for more details.
      return Collections.emptyList();
    }

    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        // just for overriding the findbugs NP warnings, as the parameter is marked as Nullable in
        // the guava Predicate.
        if (file == null) {
          return false;
        }
        if (peerIds.isEmpty()) {
          // no peer, can always delete
          return true;
        }
        // not a valid wal file name, delete
        if (!AbstractFSWALProvider.validateWALFilename(file.getPath().getName())) {
          return true;
        }
        // meta wal is always deletable as we will never replicate it
        if (AbstractFSWALProvider.isMetaFile(file.getPath())) {
          return true;
        }
        ServerName serverName =
          AbstractFSWALProvider.parseServerNameFromWALName(file.getPath().getName());
        if (notFullyDeadServers.contains(serverName)) {
          return filterForLiveRegionServer(serverName, file);
        } else {
          return filterForDeadRegionServer(serverName, file);
        }
      }
    });
  }

  private Set<ServerName> getNotFullyDeadServers(MasterServices services) {
    List<ServerName> onlineServers = services.getServerManager().getOnlineServersList();
    return Stream.concat(onlineServers.stream(),
      services.getMasterProcedureExecutor().getProcedures().stream()
        .filter(p -> p instanceof ServerCrashProcedure).filter(p -> !p.isFinished())
        .map(p -> ((ServerCrashProcedure) p).getServerName()))
      .collect(Collectors.toSet());
  }

  @Override
  public void init(Map<String, Object> params) {
    super.init(params);
    if (MapUtils.isNotEmpty(params)) {
      Object master = params.get(HMaster.MASTER);
      if (master instanceof MasterServices) {
        masterService = (MasterServices) master;
        barrier = masterService.getReplicationLogCleanerBarrier();
        rpm = masterService.getReplicationPeerManager();
        getNotFullyDeadServers = () -> getNotFullyDeadServers(masterService);
        return;
      }
    }
    throw new IllegalArgumentException("Missing " + HMaster.MASTER + " parameter");
  }

  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  /**
   * Check if asyncClusterConnection is null or closed.
   * @return true if asyncClusterConnection is null or is closed, false otherwise
   */
  private boolean isAsyncClusterConnectionClosedOrNull() {
    AsyncClusterConnection asyncClusterConnection = masterService.getAsyncClusterConnection();
    return asyncClusterConnection == null || asyncClusterConnection.isClosed();
  }
}
