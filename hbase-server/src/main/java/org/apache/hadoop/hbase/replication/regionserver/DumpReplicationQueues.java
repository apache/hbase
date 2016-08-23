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
package org.apache.hadoop.hbase.replication.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.io.WALLink;
import org.apache.hadoop.hbase.replication.*;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.*;

/**
 * Provides information about the existing states of replication, replication peers and queues.
 *
 * Usage: hbase org.apache.hadoop.hbase.replication.regionserver.DumpReplicationQueues [args]
 * Arguments: --distributed    Polls each RS to dump information about the queue
 *            --hdfs           Reports HDFS usage by the replication queues (note: can be overestimated).
 */

public class DumpReplicationQueues extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(DumpReplicationQueues.class.getName());

  static class DumpOptions {
    boolean hdfs = false;
    boolean distributed = false;

    public DumpOptions() {
    }

    public DumpOptions(DumpOptions that) {
      this.hdfs = that.hdfs;
      this.distributed = that.distributed;
    }

    boolean isHdfs () {
      return hdfs;
    }

    boolean isDistributed() {
      return distributed;
    }

    void setHdfs (boolean hdfs) {
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
      if (!opts.isDistributed()  && opts.isHdfs()) {
        printUsageAndExit("ERROR: --hdfs option can only be used with --distributed: " + cmd, -1);
      }
    }
    return opts;
  }

  /**
   * Main
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new DumpReplicationQueues(), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {

    int errCode = -1;
    LinkedList<String> argv = new LinkedList<String>();
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
    System.err.println("Usage: java " + className + " \\");
    System.err.println("  <OPTIONS> [-D<property=value>]*");
    System.err.println();
    System.err.println("General Options:");
    System.err.println(" distributed  Poll each RS and print its own replication queue. "
        + "Default only polls ZooKeeper");
    System.err.println(" hdfs         Use HDFS to calculate usage of WALs by replication. It could be overestimated"
        + " if replicating to multiple peers. --distributed flag is also needed.");
  }

  protected static void printUsageAndExit(final String message, final int exitCode) {
    printUsage(message);
    System.exit(exitCode);
  }

  private int dumpReplicationQueues(DumpOptions opts) throws Exception {

    Configuration conf = getConf();
    HBaseAdmin.checkHBaseAvailable(conf);
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf);
    HConnection connection = HConnectionManager.createConnection(conf);
    long deleted = 0;

    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "DumpReplicationQueues" + System.currentTimeMillis(),
        new WarnOnlyAbortable(), true);

    try {
      // Our zk watcher
      LOG.info("Our Quorum: " + zkw.getQuorum());
      List<HashMap<String, String>> replicatedTables = replicationAdmin.listReplicated();
      if (replicatedTables.isEmpty()) {
        LOG.info("No tables with a configured replication peer were found.");
        return(0);
      } else {
        LOG.info("Replicated Tables: " + replicatedTables);
      }

      Map<String, ReplicationPeerConfig> peerConfigs = replicationAdmin.listPeerConfigs();

      if (peerConfigs.isEmpty()) {
        LOG.info("Replication is enabled but no peer configuration was found.");
      }

      System.out.println("Dumping replication peers and configurations:");
      System.out.println(dumpPeersState(replicationAdmin, peerConfigs));

      if (opts.isDistributed()) {
        LOG.info("Found [--distributed], will poll each RegionServer.");
        System.out.println(dumpQueues(connection, zkw, opts.isHdfs(), deleted));
        if (deleted > 0) {
          LOG.warn("Found " + deleted +" deleted queues"
              + ", run hbck -fixReplication in order to remove the deleted replication queues");
        }
      } else {
        // use ZK instead
        System.out.print("Dumping replication znodes via ZooKeeper:");
        System.out.println(ZKUtil.getReplicationZnodesDump(zkw));
      }
      return (0);
    } catch (IOException e) {
      return (-1);
    } finally {
      zkw.close();
    }
  }

  public String dumpPeersState(ReplicationAdmin replicationAdmin,
                              Map<String, ReplicationPeerConfig> peerConfigs) throws Exception {
    Map<String, String> currentConf;
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, ReplicationPeerConfig> peer : peerConfigs.entrySet()) {
      try {
        sb.append("Peer: " + peer.getKey() + "\n");
        sb.append("    " + "State: " + (replicationAdmin.getPeerState(peer.getKey()) ? "ENABLED" : "DISABLED") + "\n");
        sb.append("    " + "Cluster Name: " + peer.getValue() + "\n");
        currentConf = peer.getValue().getConfiguration();
        // Only show when we have a custom configuration for the peer
        if (currentConf.size() > 1) {
          sb.append("    " + "Peer Configuration: " + currentConf + "\n");
        }
        sb.append("    " + "Peer Table CFs: " + replicationAdmin.getPeerTableCFs(peer.getKey()) + "\n");
      } catch (ReplicationException re) {
        sb.append("Got an exception while invoking ReplicationAdmin: " + re + "\n");
      }
    }
    return sb.toString();
  }

  public String dumpQueues(HConnection connection, ZooKeeperWatcher zkw, boolean hdfs, long deleted)
      throws Exception {
    ReplicationQueuesClient queuesClient;
    ReplicationPeers replicationPeers;
    ReplicationQueues replicationQueues;

    StringBuilder sb = new StringBuilder();

    queuesClient = ReplicationFactory.getReplicationQueuesClient(zkw, getConf(), connection);
    queuesClient.init();
    replicationQueues = ReplicationFactory.getReplicationQueues(zkw, getConf(), connection);
    replicationPeers = ReplicationFactory.getReplicationPeers(zkw, getConf(), connection);
    replicationPeers.init();

    // Loops each peer on each RS and dumps the queues

    Set<String> peerIds = new HashSet<String>(replicationPeers.getAllPeerIds());
    try {
      List<String> regionservers = queuesClient.getListOfReplicators();
      for (String regionserver : regionservers) {
        List<String> queueIds = queuesClient.getAllQueues(regionserver);
        replicationQueues.init(regionserver);
        for (String queueId : queueIds) {
          ReplicationQueueInfo queueInfo = new ReplicationQueueInfo(queueId);
          List<String> wals = queuesClient.getLogsInQueue(regionserver, queueId);
          if (!peerIds.contains(queueInfo.getPeerId())) {
            deleted++;
            sb.append(formatQueue(regionserver, replicationQueues, queueInfo, queueId, wals, true, hdfs));
          } else {
            sb.append(formatQueue(regionserver, replicationQueues, queueInfo, queueId, wals, false, hdfs));
          }
        }
      }
    } catch (KeeperException ke) {
      throw new IOException(ke);
    }
    return sb.toString();
  }

  private String formatQueue(String regionserver, ReplicationQueues replicationQueues, ReplicationQueueInfo queueInfo,
                           String queueId, List<String> wals, boolean isDeleted, boolean hdfs) throws Exception {

    StringBuilder sb = new StringBuilder();

    List<String> deadServers ;

    sb.append("Dumping replication queue info for RegionServer: [" + regionserver + "]" + "\n");
    sb.append("    PeerID: " + queueInfo.getPeerId() + "\n");
    sb.append("    Recovered: " + queueInfo.isQueueRecovered() + "\n");
    deadServers = queueInfo.getDeadRegionServers();
    if (deadServers.isEmpty()) {
      sb.append("    No dead RegionServers found in this queue." + "\n");
    } else {
      sb.append("    Dead RegionServers: " + deadServers + "\n");
    }
    sb.append("    Was deleted: " + isDeleted + "\n");
    sb.append("    Number of WALs in replication queue: " + wals.size() + "\n");
    for (String wal : wals) {
      long position = replicationQueues.getLogPosition(queueInfo.getPeerId(), wal);
      sb.append("    Replication position for " + wal + ": " + (position > 0 ? position : "0"
          + " (not started or nothing to replicate)") + "\n");
    }

    if (hdfs) {
      FileSystem fs = FileSystem.get(getConf());
      sb.append("    Total size of WALs on HDFS for this queue: "
          + StringUtils.humanReadableInt(getTotalWALSize(fs, wals, regionserver)) + "\n");
    }
    return sb.toString();
  }
  /**
   *  return total size in bytes from a list of WALs
   */
  private long getTotalWALSize(FileSystem fs, List<String> wals, String server) throws IOException {
    int size = 0;
    FileStatus fileStatus;

    for (String wal : wals) {
      try {
        fileStatus = (new WALLink(getConf(), server, wal)).getFileStatus(fs);
      } catch (IOException e) {
        LOG.warn("WAL " + wal + " couldn't be found, skipping");
        break;
      }
      size += fileStatus.getLen();
    }
    return size;
  }

  private static class WarnOnlyAbortable implements Abortable {
    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("DumpReplicationQueue received abort, ignoring.  Reason: " + why);
      if (LOG.isDebugEnabled()) {
        LOG.debug(e);
      }
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }

  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      LOG.error(errorMsg);
    }
  }
}
