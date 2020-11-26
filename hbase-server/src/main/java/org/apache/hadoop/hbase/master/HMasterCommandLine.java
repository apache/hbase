/**
 *
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
package org.apache.hadoop.hbase.master;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.trace.TraceUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.ServerCommandLine;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.GnuParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

@InterfaceAudience.Private
public class HMasterCommandLine extends ServerCommandLine {
  private static final Logger LOG = LoggerFactory.getLogger(HMasterCommandLine.class);

  private static final String USAGE =
    "Usage: Master [opts] start|stop|clear\n" +
    " start  Start Master. If local mode, start Master and RegionServer in same JVM\n" +
    " stop   Start cluster shutdown; Master signals RegionServer shutdown\n" +
    " clear  Delete the master znode in ZooKeeper after a master crashes\n "+
    " where [opts] are:\n" +
    "   --minRegionServers=<servers>   Minimum RegionServers needed to host user tables.\n" +
    "   --localRegionServers=<servers> " +
      "RegionServers to start in master process when in standalone mode.\n" +
    "   --masters=<servers>            Masters to start in this process.\n" +
    "   --backup                       Master should start in backup mode\n" +
    "   --shutDownCluster                    " +
      "Start Cluster shutdown; Master signals RegionServer shutdown";

  private final Class<? extends HMaster> masterClass;

  public HMasterCommandLine(Class<? extends HMaster> masterClass) {
    this.masterClass = masterClass;
  }

  @Override
  protected String getUsage() {
    return USAGE;
  }

  @Override
  public int run(String args[]) throws Exception {
    boolean shutDownCluster = false;
    Options opt = new Options();
    opt.addOption("localRegionServers", true,
      "RegionServers to start in master process when running standalone");
    opt.addOption("masters", true, "Masters to start in this process");
    opt.addOption("minRegionServers", true, "Minimum RegionServers needed to host user tables");
    opt.addOption("backup", false, "Do not try to become HMaster until the primary fails");
    opt.addOption("shutDownCluster", false, "`hbase master stop --shutDownCluster` shuts down cluster");

    CommandLine cmd;
    try {
      cmd = new GnuParser().parse(opt, args);
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      usage(null);
      return 1;
    }


    if (cmd.hasOption("minRegionServers")) {
      String val = cmd.getOptionValue("minRegionServers");
      getConf().setInt("hbase.regions.server.count.min",
                  Integer.parseInt(val));
      LOG.debug("minRegionServers set to " + val);
    }

    // minRegionServers used to be minServers.  Support it too.
    if (cmd.hasOption("minServers")) {
      String val = cmd.getOptionValue("minServers");
      getConf().setInt("hbase.regions.server.count.min", Integer.parseInt(val));
      LOG.debug("minServers set to " + val);
    }

    // check if we are the backup master - override the conf if so
    if (cmd.hasOption("backup")) {
      getConf().setBoolean(HConstants.MASTER_TYPE_BACKUP, true);
    }

    // How many regionservers to startup in this process (we run regionservers in same process as
    // master when we are in local/standalone mode. Useful testing)
    if (cmd.hasOption("localRegionServers")) {
      String val = cmd.getOptionValue("localRegionServers");
      getConf().setInt("hbase.regionservers", Integer.parseInt(val));
      LOG.debug("localRegionServers set to " + val);
    }
    // How many masters to startup inside this process; useful testing
    if (cmd.hasOption("masters")) {
      String val = cmd.getOptionValue("masters");
      getConf().setInt("hbase.masters", Integer.parseInt(val));
      LOG.debug("masters set to " + val);
    }

    // Checking whether to shut down cluster or not
    if (cmd.hasOption("shutDownCluster")) {
      shutDownCluster = true;
    }

    @SuppressWarnings("unchecked")
    List<String> remainingArgs = cmd.getArgList();
    if (remainingArgs.size() != 1) {
      usage(null);
      return 1;
    }

    String command = remainingArgs.get(0);

    if ("start".equals(command)) {
      return startMaster();
    } else if ("stop".equals(command)) {
      if (shutDownCluster) {
        return stopMaster();
      }
      System.err.println(
        "To shutdown the master run " +
        "hbase-daemon.sh stop master or send a kill signal to " +
        "the HMaster pid, " +
        "and to stop HBase Cluster run \"stop-hbase.sh\" or \"hbase master stop --shutDownCluster\"");
      return 1;
    } else if ("clear".equals(command)) {
      return (ZNodeClearer.clear(getConf()) ? 0 : 1);
    } else {
      usage("Invalid command: " + command);
      return 1;
    }
  }

  private int startMaster() {
    Configuration conf = getConf();
    TraceUtil.initTracer(conf);

    try {
      // If 'local', defer to LocalHBaseCluster instance.  Starts master
      // and regionserver both in the one JVM.
      if (LocalHBaseCluster.isLocal(conf)) {
        DefaultMetricsSystem.setMiniClusterMode(true);
        final MiniZooKeeperCluster zooKeeperCluster = new MiniZooKeeperCluster(conf);
        File zkDataPath = new File(conf.get(HConstants.ZOOKEEPER_DATA_DIR));

        // find out the default client port
        int zkClientPort = 0;

        // If the zookeeper client port is specified in server quorum, use it.
        String zkserver = conf.get(HConstants.ZOOKEEPER_QUORUM);
        if (zkserver != null) {
          String[] zkservers = zkserver.split(",");

          if (zkservers.length > 1) {
            // In local mode deployment, we have the master + a region server and zookeeper server
            // started in the same process. Therefore, we only support one zookeeper server.
            String errorMsg = "Could not start ZK with " + zkservers.length +
                " ZK servers in local mode deployment. Aborting as clients (e.g. shell) will not "
                + "be able to find this ZK quorum.";
              System.err.println(errorMsg);
              throw new IOException(errorMsg);
          }

          String[] parts = zkservers[0].split(":");

          if (parts.length == 2) {
            // the second part is the client port
            zkClientPort = Integer.parseInt(parts [1]);
          }
        }
        // If the client port could not be find in server quorum conf, try another conf
        if (zkClientPort == 0) {
          zkClientPort = conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, 0);
          // The client port has to be set by now; if not, throw exception.
          if (zkClientPort == 0) {
            throw new IOException("No config value for " + HConstants.ZOOKEEPER_CLIENT_PORT);
          }
        }
        zooKeeperCluster.setDefaultClientPort(zkClientPort);
        // set the ZK tick time if specified
        int zkTickTime = conf.getInt(HConstants.ZOOKEEPER_TICK_TIME, 0);
        if (zkTickTime > 0) {
          zooKeeperCluster.setTickTime(zkTickTime);
        }

        // login the zookeeper server principal (if using security)
        ZKUtil.loginServer(conf, HConstants.ZK_SERVER_KEYTAB_FILE,
          HConstants.ZK_SERVER_KERBEROS_PRINCIPAL, null);
        int localZKClusterSessionTimeout =
          conf.getInt(HConstants.ZK_SESSION_TIMEOUT + ".localHBaseCluster", 10*1000);
        conf.setInt(HConstants.ZK_SESSION_TIMEOUT, localZKClusterSessionTimeout);
        LOG.info("Starting a zookeeper cluster");
        int clientPort = zooKeeperCluster.startup(zkDataPath);
        if (clientPort != zkClientPort) {
          String errorMsg = "Could not start ZK at requested port of " +
            zkClientPort + ".  ZK was started at port: " + clientPort +
            ".  Aborting as clients (e.g. shell) will not be able to find " +
            "this ZK quorum.";
          System.err.println(errorMsg);
          throw new IOException(errorMsg);
        }
        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));

        // Need to have the zk cluster shutdown when master is shutdown.
        // Run a subclass that does the zk cluster shutdown on its way out.
        int mastersCount = conf.getInt("hbase.masters", 1);
        int regionServersCount = conf.getInt("hbase.regionservers", 1);
        // Set start timeout to 5 minutes for cmd line start operations
        conf.setIfUnset("hbase.master.start.timeout.localHBaseCluster", "300000");
        LOG.info("Starting up instance of localHBaseCluster; master=" + mastersCount +
          ", regionserversCount=" + regionServersCount);
        LocalHBaseCluster cluster = new LocalHBaseCluster(conf, mastersCount, regionServersCount,
          LocalHMaster.class, HRegionServer.class);
        ((LocalHMaster)cluster.getMaster(0)).setZKCluster(zooKeeperCluster);
        cluster.startup();
        waitOnMasterThreads(cluster);
      } else {
        logProcessInfo(getConf());
        HMaster master = HMaster.constructMaster(masterClass, conf);
        if (master.isStopped()) {
          LOG.info("Won't bring the Master up as a shutdown is requested");
          return 1;
        }
        master.start();
        master.join();
        if(master.isAborted())
          throw new RuntimeException("HMaster Aborted");
      }
    } catch (Throwable t) {
      LOG.error("Master exiting", t);
      return 1;
    }
    return 0;
  }

  @SuppressWarnings("resource")
  private int stopMaster() {
    Configuration conf = getConf();
    // Don't try more than once
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Admin admin = connection.getAdmin()) {
        admin.shutdown();
      } catch (Throwable t) {
        LOG.error("Failed to stop master", t);
        return 1;
      }
    } catch (MasterNotRunningException e) {
      LOG.error("Master not running");
      return 1;
    } catch (ZooKeeperConnectionException e) {
      LOG.error("ZooKeeper not available");
      return 1;
    } catch (IOException e) {
      LOG.error("Got IOException: " +e.getMessage(), e);
      return 1;
    }
    return 0;
  }

  private void waitOnMasterThreads(LocalHBaseCluster cluster) throws InterruptedException{
    List<JVMClusterUtil.MasterThread> masters = cluster.getMasters();
    List<JVMClusterUtil.RegionServerThread> regionservers = cluster.getRegionServers();

    if (masters != null) {
      for (JVMClusterUtil.MasterThread t : masters) {
        t.join();
        if(t.getMaster().isAborted()) {
          closeAllRegionServerThreads(regionservers);
          throw new RuntimeException("HMaster Aborted");
        }
      }
    }
  }

  private static void closeAllRegionServerThreads(
      List<JVMClusterUtil.RegionServerThread> regionservers) {
    for(JVMClusterUtil.RegionServerThread t : regionservers){
      t.getRegionServer().stop("HMaster Aborted; Bringing down regions servers");
    }
  }

  /*
   * Version of master that will shutdown the passed zk cluster on its way out.
   */
  public static class LocalHMaster extends HMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public LocalHMaster(Configuration conf)
    throws IOException, KeeperException, InterruptedException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          LOG.error("Failed to shutdown MiniZooKeeperCluster", e);
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }
}
