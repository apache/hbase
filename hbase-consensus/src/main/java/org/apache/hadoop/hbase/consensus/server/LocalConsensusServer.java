package org.apache.hadoop.hbase.consensus.server;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import io.airlift.units.Duration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.consensus.client.QuorumClient;
import org.apache.hadoop.hbase.consensus.quorum.AggregateTimer;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.consensus.quorum.RaftQuorumContext;
import org.apache.hadoop.hbase.consensus.util.RaftUtil;
import org.apache.hadoop.hbase.util.DaemonThreadFactory;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.serial.AsyncSerialExecutorServiceImpl;
import org.apache.hadoop.hbase.util.serial.SerialExecutorService;
import org.apache.log4j.Level;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.weakref.jmx.MBeanExporter;

import com.facebook.nifty.codec.DefaultThriftFrameCodecFactory;
import com.facebook.nifty.codec.ThriftFrameCodecFactory;
import com.facebook.nifty.core.NiftyTimer;
import com.facebook.nifty.duplex.TDuplexProtocolFactory;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftEventHandler;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

public class LocalConsensusServer {
  public static final ImmutableMap<String,TDuplexProtocolFactory>
    SWIFT_PROTOCOL_FACTORIES = ImmutableMap.of(
    "binary", TDuplexProtocolFactory.fromSingleFactory(
      new TBinaryProtocol.Factory()),
    "compact", TDuplexProtocolFactory.fromSingleFactory(
      new TCompactProtocol.Factory())
  );

  public static final ImmutableMap<String,ThriftFrameCodecFactory>
    SWIFT_FRAME_CODEC_FACTORIES = ImmutableMap.<String, ThriftFrameCodecFactory>of(
    "buffered", new DefaultThriftFrameCodecFactory(),
    "framed", new DefaultThriftFrameCodecFactory()
  );

  private static final Logger LOG = LoggerFactory.getLogger(
    LocalConsensusServer.class);
  private final Configuration conf;
  private ThriftServer server;
  private final ConsensusService handler;
  private final List<ThriftEventHandler> eventHandlers;

  private InfoServer infoServer;

  private ThriftServerConfig config;

  private ExecutorService execServiceForThriftClients;

  public final AggregateTimer aggregateTimer = new AggregateTimer();
  public SerialExecutorService serialExecutorService;

  @Inject
  public LocalConsensusServer(final ConsensusService handler,
                              final List<ThriftEventHandler> eventHandlers,
                              final Configuration conf) {
    this.eventHandlers = eventHandlers;
    this.handler = handler;
    this.conf = conf;

    this.execServiceForThriftClients =
        Executors.newFixedThreadPool(
          HConstants.DEFAULT_QUORUM_CLIENT_NUM_WORKERS,
          new DaemonThreadFactory("QuorumClient-"));
  }

  public void initialize(final ThriftServerConfig config) {
    if (!(InternalLoggerFactory.getDefaultFactory()
            instanceof Slf4JLoggerFactory)) {
      InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    // Set the transport protocol
    config.setProtocolName(conf.get(HConstants.CONSENSUS_TRANSPORT_PROTOCOL_KEY,
      HConstants.CONSENSUS_TRANSPORT_PROTOCOL_DEFAULT));

    // Set the codec factory
    config.setTransportName(conf.get(HConstants.CONSENSUS_TRANSPORT_CODEC_KEY,
      HConstants.CONSENSUS_TRANSPORT_CODEC_DEFAULT));

    // "ConsensusAsyncSerialExecutor"
    int numThreads = conf.getInt(HConstants.FSM_MUX_THREADPOOL_SIZE_KEY,
      HConstants.DEFAULT_FSM_MUX_THREADPOOL_SIZE);
    serialExecutorService =
        new AsyncSerialExecutorServiceImpl(numThreads, "AsyncSerialExecutor");

    ThriftServer newServer = new ThriftServer(
      new ThriftServiceProcessor(
        new ThriftCodecManager(), eventHandlers, handler),
      config,
      new NiftyTimer("thrift"),
      SWIFT_FRAME_CODEC_FACTORIES,
      SWIFT_PROTOCOL_FACTORIES,
      ThriftServer.DEFAULT_WORKER_EXECUTORS,
      ThriftServer.DEFAULT_SECURITY_FACTORY);

    LOG.info("Start the thrift server with a unbounded queue !");

    synchronized (this) {
      server = newServer;
      this.config = config;
    }
  }

  public void startService() {
    LOG.info("Starting the consensus service at port : " + config.getPort());
    server.start();
    LOG.info("Started the consensus service at port : " + config.getPort());
  }

  public void restartService() {
    LOG.info("Restarting the consensus service at port : " + config.getPort());
    getThriftServer().close();
    initialize(this.config);
    getThriftServer().start();
    LOG.info("Restarted the consensus service at port: " + config.getPort());
  }

  public void stopService() {
    LOG.info("Stopping the consensus service at port : " + config.getPort());
    LOG.info(
      com.google.common.base.Throwables.getStackTraceAsString(new Throwable()));
    handler.stopService();
    server.close();
    execServiceForThriftClients.shutdownNow();
  }

  public ConsensusService getHandler() {
    return handler;
  }

  public synchronized ThriftServer getThriftServer() {
    return server;
  }

  public void startInfoServer(HServerAddress local, Configuration conf) throws IOException {
    this.infoServer = new InfoServer(HConstants.QUORUM_MONITORING_PAGE_KEY,
      local.getBindAddress(), server.getPort() + 1, false,
                                     conf);
    this.infoServer.setAttribute(HConstants.QUORUM_MONITORING_PAGE_KEY, this);
    this.infoServer.start();
  }

  public void shutdownInfoServer() throws Exception {
    this.infoServer.stop();
  }

  public QuorumClient getQuorumClient(QuorumInfo quorumInfo) throws IOException {
    return new QuorumClient(quorumInfo, this.conf, execServiceForThriftClients);
  }

  @Override
  public String toString() {
    return "LocalConsensusServer@" + config.getPort()
      + "[" + (server.isRunning() ? "RUNNING" : "OFFLINE") + "@" + server.getPort() + "]";
  }

  public static void main(String[] args) {
    LOG.info("Starting Consensus service server");
    final Configuration configuration = HBaseConfiguration.create();

    Options opt = new Options();
    opt.addOption("region", true, "the region id for the load test");
    opt.addOption("servers", true, "A list of quorum server delimited by ,");
    opt.addOption("localIndex", true, "The index of the local quorum server from the server list");
    opt.addOption("debug", false, "Set the log4j as debug level." );
    opt.addOption("jetty", false, "Enable the jetty server" );

    String regionId = null;
    String serverList;
    String servers[] = null;
    HServerAddress localHost = null;
    int localIndex = -1;
    HashMap<HServerAddress, Integer> peers = new HashMap<>();
    int rank = 5;
    boolean enableJetty = false;

    try {
      CommandLine cmd = new GnuParser().parse(opt, args);

      if (cmd.hasOption("region")) {
        regionId = cmd.getOptionValue("region");
      }

      if (cmd.hasOption("servers")) {
        serverList = cmd.getOptionValue("servers");
        servers = serverList.split(",");
      }

      if (cmd.hasOption("localIndex")) {
        localIndex = Integer.parseInt(cmd.getOptionValue("localIndex"));
      }

      if (!cmd.hasOption("debug")) {
        org.apache.log4j.Logger.getLogger(
                "org.apache.hadoop.hbase.consensus").setLevel(Level.INFO);
      }


      if (cmd.hasOption("jetty")) {
        enableJetty = true;
      }

      if (regionId == null || regionId.isEmpty() || servers == null || servers.length == 0) {
        LOG.error("Wrong args !");
        printHelp(opt);
        System.exit(-1);
      }

      System.out.println("*******************");
      System.out.println("region: " + regionId);
      String hostname = null;

      try {
        hostname = InetAddress.getLocalHost().getHostName();
        System.out.println("Current host name is " + hostname);
      } catch (UnknownHostException e) {
        System.out.println("Not able to retrieve the local host name");
        System.exit(-1);
      }

      for (int i = 0; i < servers.length; i++) {
        String localConsensusServerAddress = servers[i];

        HServerAddress cur = new HServerAddress(localConsensusServerAddress);
        peers.put(RaftUtil.getHRegionServerAddress(cur), rank--);
        System.out.println("Quorum server " + localConsensusServerAddress);

        if (localIndex != -1) { // local mode
          if (localIndex == i) {
            localHost = cur;
          }
        } else if (localConsensusServerAddress.contains(hostname)) { // remote mode
          localHost = cur;
        }
      }
      if (localHost == null) {
        System.out.println("Error: no local server");
        printHelp(opt);
        System.exit(-1);
      }
      System.out.println("Local Quorum server " + localHost);
      System.out.println("*******************");
    } catch (Exception e) {
      e.printStackTrace();
      printHelp(opt);
      System.exit(-1);
    }

    if (localIndex != -1) {
      // overwrite the log directory for the local mode
      configuration.set("hbase.consensus.log.path",  "/tmp/wal/" + localHost.getHostAddressWithPort());
    }

    // Start the local consensus server
    ThriftServerConfig config =  new ThriftServerConfig().
      setWorkerThreads(configuration.getInt(
        HConstants.CONSENSUS_SERVER_WORKER_THREAD,
        HConstants.DEFAULT_CONSENSUS_SERVER_WORKER_THREAD)).
      setIoThreadCount(configuration.getInt(
        HConstants.CONSENSUS_SERVER_IO_THREAD,
        HConstants.DEFAULT_CONSENSUS_SERVER_IO_THREAD)).
      setPort(localHost.getPort()).
      setIdleConnectionTimeout(new Duration(1, TimeUnit.DAYS));

    // Set the max progress timeout as 30 sec;
    // The actual progress timeout will be the max progress timeout/rank
    // And the heartbeat timeout will be the the max timeout/20;
    configuration.setInt(HConstants.PROGRESS_TIMEOUT_INTERVAL_KEY, 30 * 1000);

    MBeanExporter mbeanExporter = MBeanExporter.withPlatformMBeanServer();

    LocalConsensusServer consensusServer = new LocalConsensusServer(
      ConsensusServiceImpl.createConsensusServiceImpl(),
      new ArrayList<ThriftEventHandler>(),
      configuration);
    consensusServer.initialize(config);
    consensusServer.startService();
    LOG.info("Started Consensus Service with port:" + config.getPort());

    if (enableJetty) {
      try {
        consensusServer.startInfoServer(localHost, configuration);
        System.out.println("Start the jetty server at " +
          localHost.getBindAddress() + ":" + localHost.getPort() + 1);
      } catch (Exception e) {
        LOG.error("Unable to start the jetty server ", e);
      }
    }

    // Set the region with the peers
    QuorumInfo quorumInfo = RaftUtil.createDummyQuorumInfo(regionId, peers);

    // Create the RaftQuorumContext
    RaftQuorumContext context = new RaftQuorumContext(quorumInfo,
      configuration, localHost,
      (regionId + "."),
      consensusServer.aggregateTimer,
      consensusServer.serialExecutorService,
      consensusServer.execServiceForThriftClients
    );
    context.getConsensusMetrics().export(mbeanExporter);

    // Initialize the raft context
    context.initializeAll(HConstants.UNDEFINED_TERM_INDEX);

    // Register the raft context into the consensus server
    consensusServer.getHandler().addRaftQuorumContext(context);
    LOG.info("Registered the region " + regionId + " with the consensus server");
  }

  private static void printHelp(Options opt) {
    new HelpFormatter().printHelp(
      "QuorumLoadTestClient -region regionID -servers h1:port,h2:port," +
        "h3:port...] [-localIndex index]", opt);
  }

  public ExecutorService getExecServiceForThriftClients() {
    return execServiceForThriftClients;
  }

}
