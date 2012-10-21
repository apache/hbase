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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.thrift.CallQueue;
import org.apache.hadoop.hbase.thrift.CallQueue.Call;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
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
@InterfaceAudience.Private
public class ThriftServer {
  private static final Log log = LogFactory.getLog(ThriftServer.class);

  public static final String DEFAULT_LISTEN_PORT = "9090";

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
        "Address to bind the Thrift server to. [default: 0.0.0.0]");
    options.addOption("p", "port", true, "Port to bind to [default: " + DEFAULT_LISTEN_PORT + "]");
    options.addOption("f", "framed", false, "Use framed transport");
    options.addOption("c", "compact", false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");

    OptionGroup servers = new OptionGroup();
    servers.addOption(
        new Option("nonblocking", false, "Use the TNonblockingServer. This implies the framed transport."));
    servers.addOption(new Option("hsha", false, "Use the THsHaServer. This implies the framed transport."));
    servers.addOption(new Option("threadpool", false, "Use the TThreadPoolServer. This is the default."));
    options.addOptionGroup(servers);
    return options;
  }

  private static CommandLine parseArguments(Options options, String[] args) throws ParseException {
    CommandLineParser parser = new PosixParser();
    return parser.parse(options, args);
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

  private static TTransportFactory getTTransportFactory(boolean framed) {
    if (framed) {
      log.debug("Using framed transport");
      return new TFramedTransport.Factory();
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
   * Start up the Thrift2 server.
   * 
   * @param args
   */
  public static void main(String[] args) throws Exception {
    TServer server = null;
    Options options = getOptions();
    try {
      CommandLine cmd = parseArguments(options, args);

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

      Configuration conf = HBaseConfiguration.create();
      ThriftMetrics metrics = new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.TWO);

      // Construct correct ProtocolFactory
      TProtocolFactory protocolFactory = getTProtocolFactory(cmd.hasOption("compact"));
      THBaseService.Iface handler =
          ThriftHBaseServiceHandler.newInstance(conf, metrics);
      THBaseService.Processor processor = new THBaseService.Processor(handler);

      boolean framed = cmd.hasOption("framed") || nonblocking || hsha;
      TTransportFactory transportFactory = getTTransportFactory(framed);
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
