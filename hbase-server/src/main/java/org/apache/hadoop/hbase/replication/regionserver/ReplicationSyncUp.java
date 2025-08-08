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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.AsyncClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.master.replication.OfflineTableReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExitHandler;
import org.apache.hadoop.hbase.util.JsonMapper;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;

/**
 * In a scenario of Replication based Disaster/Recovery, when hbase Master-Cluster crashes, this
 * tool is used to sync-up the delta from Master to Slave using the info from ZooKeeper. The tool
 * will run on Master-Cluster, and assume ZK, Filesystem and NetWork still available after hbase
 * crashes
 *
 * <pre>
 * hbase org.apache.hadoop.hbase.replication.regionserver.ReplicationSyncUp
 * </pre>
 */
@InterfaceAudience.Private
public class ReplicationSyncUp extends Configured implements Tool {

  public static class ReplicationSyncUpToolInfo {

    private long startTimeMs;

    public ReplicationSyncUpToolInfo() {
    }

    public ReplicationSyncUpToolInfo(long startTimeMs) {
      this.startTimeMs = startTimeMs;
    }

    public long getStartTimeMs() {
      return startTimeMs;
    }

    public void setStartTimeMs(long startTimeMs) {
      this.startTimeMs = startTimeMs;
    }
  }

  // For storing the information used to skip replicating some wals after the cluster is back online
  public static final String INFO_DIR = "ReplicationSyncUp";

  public static final String INFO_FILE = "info";

  private static final long SLEEP_TIME = 10000;

  /**
   * Main program
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(HBaseConfiguration.create(), new ReplicationSyncUp(), args);
    ExitHandler.getInstance().exit(ret);
  }

  // Find region servers under wal directory
  // Here we only care about the region servers which may still be alive, as we need to add
  // replications for them if missing. The dead region servers which have already been processed
  // fully do not need to add their replication queues again, as the operation has already been done
  // in SCP.
  private Set<ServerName> listRegionServers(FileSystem walFs, Path walDir) throws IOException {
    FileStatus[] statuses;
    try {
      statuses = walFs.listStatus(walDir);
    } catch (FileNotFoundException e) {
      System.out.println("WAL directory " + walDir + " does not exists, ignore");
      return Collections.emptySet();
    }
    Set<ServerName> regionServers = new HashSet<>();
    for (FileStatus status : statuses) {
      // All wal files under the walDir is within its region server's directory
      if (!status.isDirectory()) {
        continue;
      }
      ServerName sn = AbstractFSWALProvider.getServerNameFromWALDirectoryName(status.getPath());
      if (sn != null) {
        regionServers.add(sn);
      }
    }
    return regionServers;
  }

  private void addMissingReplicationQueues(ReplicationQueueStorage storage, ServerName regionServer,
    Set<String> peerIds) throws ReplicationException {
    Set<String> existingQueuePeerIds = new HashSet<>();
    List<ReplicationQueueId> queueIds = storage.listAllQueueIds(regionServer);
    for (Iterator<ReplicationQueueId> iter = queueIds.iterator(); iter.hasNext();) {
      ReplicationQueueId queueId = iter.next();
      if (!queueId.isRecovered()) {
        existingQueuePeerIds.add(queueId.getPeerId());
      }
    }

    for (String peerId : peerIds) {
      if (!existingQueuePeerIds.contains(peerId)) {
        ReplicationQueueId queueId = new ReplicationQueueId(regionServer, peerId);
        System.out.println("Add replication queue " + queueId + " for claiming");
        storage.setOffset(queueId, regionServer.toString(), ReplicationGroupOffset.BEGIN,
          Collections.emptyMap());
      }
    }
  }

  private void addMissingReplicationQueues(ReplicationQueueStorage storage,
    Set<ServerName> regionServers, Set<String> peerIds) throws ReplicationException {
    for (ServerName regionServer : regionServers) {
      addMissingReplicationQueues(storage, regionServer, peerIds);
    }
  }

  // When using this tool, usually the source cluster is unhealthy, so we should try to claim the
  // replication queues for the dead region servers first and then replicate the data out.
  private void claimReplicationQueues(ReplicationSourceManager mgr, Set<ServerName> regionServers)
    throws ReplicationException, KeeperException, IOException {
    // union the region servers from both places, i.e, from the wal directory, and the records in
    // replication queue storage.
    Set<ServerName> replicators = new HashSet<>(regionServers);
    ReplicationQueueStorage queueStorage = mgr.getQueueStorage();
    replicators.addAll(queueStorage.listAllReplicators());
    FileSystem fs = CommonFSUtils.getCurrentFileSystem(getConf());
    Path infoDir = new Path(CommonFSUtils.getRootDir(getConf()), INFO_DIR);
    for (ServerName sn : replicators) {
      List<ReplicationQueueId> replicationQueues = queueStorage.listAllQueueIds(sn);
      System.out.println(sn + " is dead, claim its replication queues: " + replicationQueues);
      // record the rs name, so when master restarting, we will skip claiming its replication queue
      fs.createNewFile(new Path(infoDir, sn.getServerName()));
      for (ReplicationQueueId queueId : replicationQueues) {
        mgr.claimQueue(queueId, true);
      }
    }
  }

  private void writeInfoFile(FileSystem fs, boolean isForce) throws IOException {
    // Record the info of this run. Currently only record the time we run the job. We will use this
    // timestamp to clean up the data for last sequence ids and hfile refs in replication queue
    // storage. See ReplicationQueueStorage.removeLastSequenceIdsAndHFileRefsBefore.
    ReplicationSyncUpToolInfo info =
      new ReplicationSyncUpToolInfo(EnvironmentEdgeManager.currentTime());
    String json = JsonMapper.writeObjectAsString(info);
    Path infoDir = new Path(CommonFSUtils.getRootDir(getConf()), INFO_DIR);
    try (FSDataOutputStream out = fs.create(new Path(infoDir, INFO_FILE), isForce)) {
      out.write(Bytes.toBytes(json));
    }
  }

  private static boolean parseOpts(String args[]) {
    LinkedList<String> argv = new LinkedList<>();
    argv.addAll(Arrays.asList(args));
    String cmd = null;
    while ((cmd = argv.poll()) != null) {
      if (cmd.equals("-h") || cmd.equals("--h") || cmd.equals("--help")) {
        printUsageAndExit(null, 0);
      }
      if (cmd.equals("-f")) {
        return true;
      }
      if (!argv.isEmpty()) {
        printUsageAndExit("ERROR: Unrecognized option/command: " + cmd, -1);
      }
    }
    return false;
  }

  private static void printUsageAndExit(final String message, final int exitCode) {
    printUsage(message);
    ExitHandler.getInstance().exit(exitCode);
  }

  private static void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: hbase " + ReplicationSyncUp.class.getName() + " \\");
    System.err.println("  <OPTIONS> [-D<property=value>]*");
    System.err.println();
    System.err.println("General Options:");
    System.err.println(" -h|--h|--help  Show this help and exit.");
    System.err
      .println(" -f Start a new ReplicationSyncUp after the previous ReplicationSyncUp failed. "
        + "See HBASE-27623 for details.");
  }

  @Override
  public int run(String[] args) throws Exception {
    Abortable abortable = new Abortable() {

      private volatile boolean abort = false;

      @Override
      public void abort(String why, Throwable e) {
        if (isAborted()) {
          return;
        }
        abort = true;
        System.err.println("Aborting because of " + why);
        e.printStackTrace();
        ExitHandler.getInstance().exit(1);
      }

      @Override
      public boolean isAborted() {
        return abort;
      }
    };
    boolean isForce = parseOpts(args);
    Configuration conf = getConf();
    try (ZKWatcher zkw = new ZKWatcher(conf,
      "syncupReplication" + EnvironmentEdgeManager.currentTime(), abortable, true)) {
      Path walRootDir = CommonFSUtils.getWALRootDir(conf);
      FileSystem fs = CommonFSUtils.getWALFileSystem(conf);
      Path oldLogDir = new Path(walRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
      Path logDir = new Path(walRootDir, HConstants.HREGION_LOGDIR_NAME);

      System.out.println("Start Replication Server");
      writeInfoFile(fs, isForce);
      Replication replication = new Replication();
      // use offline table replication queue storage
      getConf().setClass(ReplicationStorageFactory.REPLICATION_QUEUE_IMPL,
        OfflineTableReplicationQueueStorage.class, ReplicationQueueStorage.class);
      DummyServer server = new DummyServer(getConf(), zkw);
      replication
        .initialize(server, fs, new Path(logDir, server.toString()), oldLogDir,
          new WALFactory(conf,
            ServerName.valueOf(
              getClass().getSimpleName() + ",16010," + EnvironmentEdgeManager.currentTime()),
            null));
      ReplicationSourceManager manager = replication.getReplicationManager();
      manager.init();
      Set<ServerName> regionServers = listRegionServers(fs, logDir);
      addMissingReplicationQueues(manager.getQueueStorage(), regionServers,
        manager.getReplicationPeers().getAllPeerIds());
      claimReplicationQueues(manager, regionServers);
      while (manager.activeFailoverTaskCount() > 0) {
        Thread.sleep(SLEEP_TIME);
      }
      while (manager.getOldSources().size() > 0) {
        Thread.sleep(SLEEP_TIME);
      }
      manager.join();
    } catch (InterruptedException e) {
      System.err.println("didn't wait long enough:" + e);
      return -1;
    }
    return 0;
  }

  private static final class DummyServer implements Server {
    private final Configuration conf;
    private final String hostname;
    private final ZKWatcher zkw;
    private volatile boolean abort = false;

    DummyServer(Configuration conf, ZKWatcher zkw) {
      // a unique name in case the first run fails
      hostname = EnvironmentEdgeManager.currentTime() + ".SyncUpTool.replication.org";
      this.conf = conf;
      this.zkw = zkw;
    }

    @Override
    public Configuration getConfiguration() {
      return conf;
    }

    @Override
    public ZKWatcher getZooKeeper() {
      return zkw;
    }

    @Override
    public CoordinatedStateManager getCoordinatedStateManager() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf(hostname, 1234, 1L);
    }

    @Override
    public void abort(String why, Throwable e) {
      if (isAborted()) {
        return;
      }
      abort = true;
      System.err.println("Aborting because of " + why);
      e.printStackTrace();
      ExitHandler.getInstance().exit(1);
    }

    @Override
    public boolean isAborted() {
      return abort;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }

    @Override
    public Connection getConnection() {
      return null;
    }

    @Override
    public ChoreService getChoreService() {
      return null;
    }

    @Override
    public FileSystem getFileSystem() {
      return null;
    }

    @Override
    public boolean isStopping() {
      return false;
    }

    @Override
    public Connection createConnection(Configuration conf) throws IOException {
      return null;
    }

    @Override
    public AsyncClusterConnection getAsyncClusterConnection() {
      return null;
    }
  }
}
