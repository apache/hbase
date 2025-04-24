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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.io.WALLink;
import org.apache.hadoop.hbase.procedure2.util.StringUtils;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationOffsetUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.AtomicLongMap;

/**
 * <p/>
 * Provides information about the existing states of replication, replication peers and queues.
 * Usage: hbase org.apache.hadoop.hbase.replication.regionserver.DumpReplicationQueues [args]
 * Arguments: --distributed Polls each RS to dump information about the queue --hdfs Reports HDFS
 * usage by the replication queues (note: can be overestimated). In the new version, we
 * reimplemented the DumpReplicationQueues tool to support obtaining information from replication
 * table.
 */
@InterfaceAudience.Private
public class DumpReplicationQueues extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(DumpReplicationQueues.class.getName());

  private List<String> deadRegionServers;
  private List<String> deletedQueues;
  private AtomicLongMap<String> peersQueueSize;
  private long totalSizeOfWALs;
  private long numWalsNotFound;

  public DumpReplicationQueues() {
    deadRegionServers = new ArrayList<>();
    deletedQueues = new ArrayList<>();
    peersQueueSize = AtomicLongMap.create();
    totalSizeOfWALs = 0;
    numWalsNotFound = 0;
  }

  static class DumpOptions {
    boolean hdfs = false;
    boolean distributed = false;

    public DumpOptions() {
    }

    public DumpOptions(DumpOptions that) {
      this.hdfs = that.hdfs;
      this.distributed = that.distributed;
    }

    boolean isHdfs() {
      return hdfs;
    }

    boolean isDistributed() {
      return distributed;
    }

    void setHdfs(boolean hdfs) {
      this.hdfs = hdfs;
    }

    void setDistributed(boolean distributed) {
      this.distributed = distributed;
    }
  }

  static DumpOptions parseOpts(Queue<String> args) {
    DumpOptions opts = new DumpOptions();

    String cmd = null;
    while ((cmd = args.poll()) != null) {
      if (cmd.equals("-h") || cmd.equals("--h") || cmd.equals("--help")) {
        // place item back onto queue so that caller knows parsing was incomplete
        args.add(cmd);
        break;
      }
      final String hdfs = "--hdfs";
      if (cmd.equals(hdfs)) {
        opts.setHdfs(true);
        continue;
      }
      final String distributed = "--distributed";
      if (cmd.equals(distributed)) {
        opts.setDistributed(true);
        continue;
      } else {
        printUsageAndExit("ERROR: Unrecognized option/command: " + cmd, -1);
      }
      // check that --distributed is present when --hdfs is in the arguments
      if (!opts.isDistributed() && opts.isHdfs()) {
        printUsageAndExit("ERROR: --hdfs option can only be used with --distributed: " + cmd, -1);
      }
    }
    return opts;
  }

  /**
   * Main
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new DumpReplicationQueues(), args);
    ExitHandler.getInstance().exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {

    int errCode = -1;
    LinkedList<String> argv = new LinkedList<>();
    argv.addAll(Arrays.asList(args));
    DumpOptions opts = parseOpts(argv);

    // args remaining, print help and exit
    if (!argv.isEmpty()) {
      errCode = 0;
      printUsage();
      return errCode;
    }
    return dumpReplicationQueues(opts);
  }

  protected void printUsage() {
    printUsage(this.getClass().getName(), null);
  }

  protected static void printUsage(final String message) {
    printUsage(DumpReplicationQueues.class.getName(), message);
  }

  protected static void printUsage(final String className, final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: hbase " + className + " \\");
    System.err.println("  <OPTIONS> [-D<property=value>]*");
    System.err.println();
    System.err.println("General Options:");
    System.err.println(" -h|--h|--help  Show this help and exit.");
    System.err.println(" --distributed  Poll each RS and print its own replication queue. "
      + "Default only polls replication table.");
    System.err.println(" --hdfs         Use HDFS to calculate usage of WALs by replication."
      + " It could be overestimated if replicating to multiple peers."
      + " --distributed flag is also needed.");
  }

  protected static void printUsageAndExit(final String message, final int exitCode) {
    printUsage(message);
    ExitHandler.getInstance().exit(exitCode);
  }

  private int dumpReplicationQueues(DumpOptions opts) throws Exception {
    Configuration conf = getConf();
    Connection connection = ConnectionFactory.createConnection(conf);
    Admin admin = connection.getAdmin();

    try {
      List<TableCFs> replicatedTableCFs = admin.listReplicatedTableCFs();
      if (replicatedTableCFs.isEmpty()) {
        LOG.info("No tables with a configured replication peer were found.");
        return (0);
      } else {
        LOG.info("Replicated Tables: " + replicatedTableCFs);
      }

      List<ReplicationPeerDescription> peers = admin.listReplicationPeers();

      if (peers.isEmpty()) {
        LOG.info("Replication is enabled but no peer configuration was found.");
      }

      System.out.println("Dumping replication peers and configurations:");
      System.out.println(dumpPeersState(peers));

      if (opts.isDistributed()) {
        LOG.info("Found [--distributed], will poll each RegionServer.");
        Set<String> peerIds =
          peers.stream().map((peer) -> peer.getPeerId()).collect(Collectors.toSet());
        System.out.println(dumpQueues(connection, peerIds, opts.isHdfs(), conf));
        System.out.println(dumpReplicationSummary());
      } else {
        // use replication table instead
        System.out.println("Dumping replication info via replication table.");
        System.out.println(dumpReplicationViaTable(connection, conf));
      }
      return (0);
    } catch (IOException e) {
      return (-1);
    } finally {
      connection.close();
    }
  }

  public String dumpReplicationViaTable(Connection connection, Configuration conf)
    throws ReplicationException, IOException {
    StringBuilder sb = new StringBuilder();
    ReplicationQueueStorage queueStorage =
      ReplicationStorageFactory.getReplicationQueueStorage(connection, conf);

    // The dump info format is as follows:
    // peers:
    // peers/1: zk1:2181:/hbase
    // peers/1/peer-state: ENABLED
    // rs:
    // rs/rs1,16020,1664092120094/1/rs1%2C16020%2C1664092120094.1664096778778: 123
    // rs/rs2,16020,1664092120094/2/rs1%2C16020%2C1664092120094.1664096778778: 321
    // hfile-refs:
    // hfile-refs/1/hfile1,hfile2
    // hfile-refs/2/hfile3,hfile4
    String peersKey = "peers";
    sb.append(peersKey).append(": ").append("\n");
    List<ReplicationPeerDescription> repPeerDescs = connection.getAdmin().listReplicationPeers();
    for (ReplicationPeerDescription repPeerDesc : repPeerDescs) {
      sb.append(peersKey).append("/").append(repPeerDesc.getPeerId()).append(": ")
        .append(repPeerDesc.getPeerConfig().getClusterKey()).append("\n");
      sb.append(peersKey).append("/").append(repPeerDesc.getPeerId()).append("/peer-state: ")
        .append(repPeerDesc.isEnabled() ? "ENABLED" : "DISABLED").append("\n");
    }

    List<ReplicationQueueData> repQueueDataList = queueStorage.listAllQueues();
    String rsKey = "rs";
    sb.append(rsKey).append(": ").append("\n");
    for (ReplicationQueueData repQueueData : repQueueDataList) {
      String peerId = repQueueData.getId().getPeerId();
      for (ImmutableMap.Entry<String, ReplicationGroupOffset> entry : repQueueData.getOffsets()
        .entrySet()) {
        sb.append(rsKey).append("/").append(entry.getKey()).append("/").append(peerId).append("/")
          .append(entry.getValue().getWal()).append(": ").append(entry.getValue().getOffset())
          .append("\n");
      }
    }

    List<String> peerIds = queueStorage.getAllPeersFromHFileRefsQueue();
    String hfileKey = "hfile-refs";
    sb.append(hfileKey).append(": ").append("\n");
    for (String peerId : peerIds) {
      List<String> hfiles = queueStorage.getReplicableHFiles(peerId);
      sb.append(hfileKey).append("/").append(peerId).append("/").append(String.join(",", hfiles))
        .append("\n");
    }

    return sb.toString();
  }

  public String dumpReplicationSummary() {
    StringBuilder sb = new StringBuilder();
    if (!deletedQueues.isEmpty()) {
      sb.append("Found " + deletedQueues.size() + " deleted queues"
        + ", run hbck -fixReplication in order to remove the deleted replication queues\n");
      for (String deletedQueue : deletedQueues) {
        sb.append("    " + deletedQueue + "\n");
      }
    }
    if (!deadRegionServers.isEmpty()) {
      sb.append("Found " + deadRegionServers.size() + " dead regionservers"
        + ", restart one regionserver to transfer the queues of dead regionservers\n");
      for (String deadRs : deadRegionServers) {
        sb.append("    " + deadRs + "\n");
      }
    }
    if (!peersQueueSize.isEmpty()) {
      sb.append("Dumping all peers's number of WALs in replication queue\n");
      for (Map.Entry<String, Long> entry : peersQueueSize.asMap().entrySet()) {
        sb.append(
          "    PeerId: " + entry.getKey() + " , sizeOfLogQueue: " + entry.getValue() + "\n");
      }
    }
    sb.append("    Total size of WALs on HDFS: " + StringUtils.humanSize(totalSizeOfWALs) + "\n");
    if (numWalsNotFound > 0) {
      sb.append("    ERROR: There are " + numWalsNotFound + " WALs not found!!!\n");
    }
    return sb.toString();
  }

  public String dumpPeersState(List<ReplicationPeerDescription> peers) throws Exception {
    Map<String, String> currentConf;
    StringBuilder sb = new StringBuilder();
    for (ReplicationPeerDescription peer : peers) {
      ReplicationPeerConfig peerConfig = peer.getPeerConfig();
      sb.append("Peer: " + peer.getPeerId() + "\n");
      sb.append("    " + "State: " + (peer.isEnabled() ? "ENABLED" : "DISABLED") + "\n");
      sb.append("    " + "Cluster Name: " + peerConfig.getClusterKey() + "\n");
      sb.append("    " + "Replication Endpoint: " + peerConfig.getReplicationEndpointImpl() + "\n");
      currentConf = peerConfig.getConfiguration();
      // Only show when we have a custom configuration for the peer
      if (currentConf.size() > 1) {
        sb.append("    " + "Peer Configuration: " + currentConf + "\n");
      }
      sb.append("    " + "Peer Table CFs: " + peerConfig.getTableCFsMap() + "\n");
      sb.append("    " + "Peer Namespaces: " + peerConfig.getNamespaces() + "\n");
    }
    return sb.toString();
  }

  public String dumpQueues(Connection connection, Set<String> peerIds, boolean hdfs,
    Configuration conf) throws Exception {
    StringBuilder sb = new StringBuilder();
    ReplicationQueueStorage queueStorage =
      ReplicationStorageFactory.getReplicationQueueStorage(connection, conf);

    Set<ServerName> liveRegionServers =
      connection.getAdmin().getClusterMetrics().getLiveServerMetrics().keySet();

    List<ServerName> regionServers = queueStorage.listAllReplicators();
    if (regionServers == null || regionServers.isEmpty()) {
      return sb.toString();
    }
    for (ServerName regionServer : regionServers) {
      List<ReplicationQueueId> queueIds = queueStorage.listAllQueueIds(regionServer);

      if (!liveRegionServers.contains(regionServer)) {
        deadRegionServers.add(regionServer.getServerName());
      }
      for (ReplicationQueueId queueId : queueIds) {
        List<String> tmpWals = new ArrayList<>();
        // wals
        AbstractFSWALProvider
          .getWALFiles(connection.getConfiguration(), queueId.getServerWALsBelongTo()).stream()
          .map(Path::toString).forEach(tmpWals::add);

        // old wals
        AbstractFSWALProvider.getArchivedWALFiles(connection.getConfiguration(),
          queueId.getServerWALsBelongTo(), URLEncoder
            .encode(queueId.getServerWALsBelongTo().toString(), StandardCharsets.UTF_8.name()))
          .stream().map(Path::toString).forEach(tmpWals::add);

        Map<String, ReplicationGroupOffset> offsets = queueStorage.getOffsets(queueId);
        // filter out the wal files that should replicate
        List<String> wals = new ArrayList<>();
        for (Map.Entry<String, ReplicationGroupOffset> entry : offsets.entrySet()) {
          ReplicationGroupOffset offset = entry.getValue();
          for (String wal : tmpWals) {
            if (ReplicationOffsetUtil.shouldReplicate(offset, wal)) {
              wals.add(wal);
            }
          }
        }
        Collections.sort(wals, Comparator.comparingLong(AbstractFSWALProvider::getTimestamp));
        if (!peerIds.contains(queueId.getPeerId())) {
          deletedQueues.add(regionServer + "/" + queueId);
          sb.append(formatQueue(regionServer, offsets, wals, queueId, true, hdfs));
        } else {
          sb.append(formatQueue(regionServer, offsets, wals, queueId, false, hdfs));
        }
      }
    }
    return sb.toString();
  }

  private String formatQueue(ServerName regionServer, Map<String, ReplicationGroupOffset> offsets,
    List<String> wals, ReplicationQueueId queueId, boolean isDeleted, boolean hdfs)
    throws Exception {
    StringBuilder sb = new StringBuilder();

    sb.append("Dumping replication queue info for RegionServer: [" + regionServer + "]" + "\n");
    sb.append("    Queue id: " + queueId + "\n");
    sb.append("    PeerID: " + queueId.getPeerId() + "\n");
    sb.append("    Recovered: " + queueId.isRecovered() + "\n");
    // In new version, we only record the first dead RegionServer in queueId.
    if (queueId.getSourceServerName().isPresent()) {
      sb.append("    Dead RegionServer: " + queueId.getSourceServerName().get() + "\n");
    } else {
      sb.append("    No dead RegionServer found in this queue." + "\n");
    }
    sb.append("    Was deleted: " + isDeleted + "\n");
    sb.append("    Number of WALs in replication queue: " + wals.size() + "\n");
    peersQueueSize.addAndGet(queueId.getPeerId(), wals.size());

    for (Map.Entry<String, ReplicationGroupOffset> entry : offsets.entrySet()) {
      String walGroup = entry.getKey();
      ReplicationGroupOffset offset = entry.getValue();
      for (String wal : wals) {
        long position = 0;
        if (offset.getWal().equals(wal)) {
          position = offset.getOffset();
        }
        sb.append(
          " Replication position for " + (walGroup != null ? walGroup + "/" + wal : wal) + ": ");
        if (position == 0) {
          sb.append("0 (not started or nothing to replicate)");
        } else if (position > 0) {
          sb.append(position);
        }
        sb.append("\n");
      }
    }

    if (hdfs) {
      FileSystem fs = FileSystem.get(getConf());
      sb.append("    Total size of WALs on HDFS for this queue: "
        + StringUtils.humanSize(getTotalWALSize(fs, wals, regionServer)) + "\n");
    }
    return sb.toString();
  }

  /**
   * return total size in bytes from a list of WALs
   */
  private long getTotalWALSize(FileSystem fs, List<String> wals, ServerName server) {
    long size = 0;
    FileStatus fileStatus;

    for (String wal : wals) {
      try {
        fileStatus = (new WALLink(getConf(), server.getServerName(), wal)).getFileStatus(fs);
      } catch (IOException e) {
        if (e instanceof FileNotFoundException) {
          numWalsNotFound++;
          LOG.warn("WAL " + wal + " couldn't be found, skipping", e);
        } else {
          LOG.warn("Can't get file status of WAL " + wal + ", skipping", e);
        }
        continue;
      }
      size += fileStatus.getLen();
    }

    totalSizeOfWALs += size;
    return size;
  }
}
