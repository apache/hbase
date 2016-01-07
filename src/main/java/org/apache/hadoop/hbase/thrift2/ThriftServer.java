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
package org.apache.hadoop.hbase.thrift2;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.thrift.CallQueue;
import org.apache.hadoop.hbase.thrift.CallQueue.Call;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * ThriftServer - this class starts up a Thrift server which implements the HBase API specified in the
 * HbaseClient.thrift IDL file.
 */
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ThriftServer {
  private static final Log log = LogFactory.getLog(ThriftServer.class);

  public static final String DEFAULT_LISTEN_PORT = "9090";
  //The max length of a message frame in MB
  static final String MAX_FRAME_SIZE_CONF_KEY = "hbase.regionserver.thrift.framed.max_frame_size_in_mb";

  public ThriftServer() {
  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, getOptions(),
        "To start the Thrift server run 'bin/hbase-daemon.sh start thrift2'\n" +
            "To shutdown the thrift server run 'bin/hbase-daemon.sh stop thrift2' or" +
            " send a kill signal to the thrift server pid",
        true);
  }

  private static Options getOptions() {
    Options options = new Options();
    options.addOption("b", "bind", true,
        "Address to bind the Thrift server to. Not supported by the Nonblocking and HsHa server [default: 0.0.0.0]");
    options.addOption("p", "port", true, "Port to bind to [default: " + DEFAULT_LISTEN_PORT + "]");
    options.addOption("f", "framed", false, "Use framed transport");
    options.addOption("c", "compact", false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");
    options.addOption(null, "infoport", true, "Port for web UI");

    OptionGroup servers = new OptionGroup();
    servers.addOption(
        new Option("nonblocking", false, "Use the TNonblockingServer. This implies the framed transport."));
    servers.addOption(new Option("hsha", false, "Use the THsHaServer. This implies the framed transport."));
    servers.addOption(new Option("threadpool", false, "Use the TThreadPoolServer. This is the default."));
    options.addOptionGroup(servers);
    return options;
  }

  private static CommandLine parseArguments(Configuration conf, Options options, String[] args)
      throws ParseException, IOException {
    GenericOptionsParser genParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = genParser.getRemainingArgs();
    CommandLineParser parser = new PosixParser();
    return parser.parse(options, remainingArgs);
  }

  private static TProtocolFactory getTProtocolFactory(boolean isCompact) {
    if (isCompact) {
      log.debug("Using compact protocol");
      return new TCompactProtocol.Factory();
    } else {
      log.debug("Using binary protocol");
      return new TBinaryProtocol.Factory();
    }
  }

  private static TTransportFactory getTTransportFactory(boolean framed, int maxFrameSize) {
    if (framed) {
      log.debug("Using framed transport");
      return new TFramedTransport.Factory(maxFrameSize);
    } else {
      return new TTransportFactory();
    }
  }

  /*
   * If bindValue is null, we don't bind. 
   */
  private static InetSocketAddress bindToPort(String bindValue, int listenPort)
      throws UnknownHostException {
    try {
      if (bindValue == null) {
        return new InetSocketAddress(listenPort);
      } else {
        return new InetSocketAddress(InetAddress.getByName(bindValue), listenPort);
      }
    } catch (UnknownHostException e) {
      throw new RuntimeException("Could not bind to provided ip address", e);
    }
  }

  private static TServer getTNonBlockingServer(TProtocolFactory protocolFactory, THBaseService.Processor processor,
      TTransportFactory transportFactory, InetSocketAddress inetSocketAddress) throws TTransportException {
    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
    log.info("starting HBase Nonblocking Thrift server on " + inetSocketAddress.toString());
    TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new TNonblockingServer(serverArgs);
  }

  private static TServer getTHsHaServer(TProtocolFactory protocolFactory,
      THBaseService.Processor processor, TTransportFactory transportFactory,
      InetSocketAddress inetSocketAddress, ThriftMetrics metrics)
      throws TTransportException {
    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
    log.info("starting HBase HsHA Thrift server on " + inetSocketAddress.toString());
    THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
    ExecutorService executorService = createExecutor(
        serverArgs.getWorkerThreads(), metrics);
    serverArgs.executorService(executorService);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new THsHaServer(serverArgs);
  }

  private static ExecutorService createExecutor(
      int workerThreads, ThriftMetrics metrics) {
    CallQueue callQueue = new CallQueue(
        new LinkedBlockingQueue<Call>(), metrics);
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift2-worker-%d");
    return new ThreadPoolExecutor(workerThreads, workerThreads,
            Long.MAX_VALUE, TimeUnit.SECONDS, callQueue, tfb.build());
  }

  private static TServer getTThreadPoolServer(TProtocolFactory protocolFactory, THBaseService.Processor processor,
      TTransportFactory transportFactory, InetSocketAddress inetSocketAddress) throws TTransportException {
    TServerTransport serverTransport = new TServerSocket(inetSocketAddress);
    log.info("starting HBase ThreadPool Thrift server on " + inetSocketAddress.toString());
    TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new TThreadPoolServer(serverArgs);
  }

  /**
   * Adds the option to pre-load filters at startup.
   *
   * @param conf  The current configuration instance.
   */
  protected static void registerFilters(Configuration conf) {
    String[] filters = conf.getStrings("hbase.thrift.filters");
    if(filters != null) {
      for(String filterClass: filters) {
        String[] filterPart = filterClass.split(":");
        if(filterPart.length != 2) {
          log.warn("Invalid filter specification " + filterClass + " - skipping");
        } else {
          ParseFilter.registerFilter(filterPart[0], filterPart[1]);
        }
      }
    }
  }

  /**
   * Start up the Thrift2 server.
   * 
   * @param args
   */
  public static void main(String[] args) throws Exception {
    TServer server = null;
    Options options = getOptions();
    try {
      Configuration conf = HBaseConfiguration.create();
      CommandLine cmd = parseArguments(conf, options, args);

      /**
       * This is to please both bin/hbase and bin/hbase-daemon. hbase-daemon provides "start" and "stop" arguments hbase
       * should print the help if no argument is provided
       */
      List<?> argList = cmd.getArgList();
      if (cmd.hasOption("help") || !argList.contains("start") || argList.contains("stop")) {
        printUsage();
        System.exit(1);
      }

      // Get port to bind to
      int listenPort = 0;
      try {
        listenPort = Integer.parseInt(cmd.getOptionValue("port", DEFAULT_LISTEN_PORT));
      } catch (NumberFormatException e) {
        throw new RuntimeException("Could not parse the value provided for the port option", e);
      }

      boolean nonblocking = cmd.hasOption("nonblocking");
      boolean hsha = cmd.hasOption("hsha");

      ThriftMetrics metrics = new ThriftMetrics(
          listenPort, conf, THBaseService.Iface.class);

      String implType = "threadpool";
      if (nonblocking) {
        implType = "nonblocking";
      } else if (hsha) {
        implType = "hsha";
      }

      conf.set("hbase.regionserver.thrift.server.type", implType);
      conf.setInt("hbase.regionserver.thrift.port", listenPort);
      registerFilters(conf);

      // Construct correct ProtocolFactory
      boolean compact = cmd.hasOption("compact") ||
        conf.getBoolean("hbase.regionserver.thrift.compact", false);
      TProtocolFactory protocolFactory = getTProtocolFactory(compact);
      THBaseService.Iface handler =
          ThriftHBaseServiceHandler.newInstance(conf, metrics);
      THBaseService.Processor processor = new THBaseService.Processor(handler);
      conf.setBoolean("hbase.regionserver.thrift.compact", compact);

      boolean framed = cmd.hasOption("framed") ||
        conf.getBoolean("hbase.regionserver.thrift.framed", false) || nonblocking || hsha;
      TTransportFactory transportFactory = getTTransportFactory(framed,
        conf.getInt(MAX_FRAME_SIZE_CONF_KEY, 2)  * 1024 * 1024);
      conf.setBoolean("hbase.regionserver.thrift.framed", framed);

      // TODO: Remove once HBASE-2155 is resolved
      if (cmd.hasOption("bind") && (nonblocking || hsha)) {
        log.error("The Nonblocking and HsHaServer servers don't support IP address binding at the moment." +
            " See https://issues.apache.org/jira/browse/HBASE-2155 for details.");
        printUsage();
        System.exit(1);
      }

      // check for user-defined info server port setting, if so override the conf
      try {
        if (cmd.hasOption("infoport")) {
          String val = cmd.getOptionValue("infoport");
          conf.setInt("hbase.thrift.info.port", Integer.valueOf(val));
          log.debug("Web UI port set to " + val);
        }
      } catch (NumberFormatException e) {
        log.error("Could not parse the value provided for the infoport option", e);
        printUsage();
        System.exit(1);
      }

      // Put up info server.
      int port = conf.getInt("hbase.thrift.info.port", 9095);
      if (port >= 0) {
        conf.setLong("startcode", System.currentTimeMillis());
        String a = conf.get("hbase.thrift.info.bindAddress", "0.0.0.0");
        InfoServer infoServer = new InfoServer("thrift", a, port, false, conf);
        infoServer.setAttribute("hbase.conf", conf);
        infoServer.start();
      }

      InetSocketAddress inetSocketAddress = bindToPort(cmd.getOptionValue("bind"), listenPort);

      if (nonblocking) {
        server = getTNonBlockingServer(protocolFactory, processor, transportFactory, inetSocketAddress);
      } else if (hsha) {
        server = getTHsHaServer(protocolFactory, processor, transportFactory, inetSocketAddress, metrics);
      } else {
        server = getTThreadPoolServer(protocolFactory, processor, transportFactory, inetSocketAddress);
      }
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      printUsage();
      System.exit(1);
    }
    server.serve();
  }
}
