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
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityUtil;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.thrift.CallQueue;
import org.apache.hadoop.hbase.thrift.THBaseThreadPoolExecutor;
import org.apache.hadoop.hbase.thrift.ThriftMetrics;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.security.SaslRpcServer.SaslGssCallbackHandler;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Option;
import org.apache.hbase.thirdparty.org.apache.commons.cli.OptionGroup;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;

/**
 * ThriftServer - this class starts up a Thrift server which implements the HBase API specified in
 * the HbaseClient.thrift IDL file.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@SuppressWarnings({ "rawtypes", "unchecked" })
public class ThriftServer extends Configured implements Tool {
  private static final Logger log = LoggerFactory.getLogger(ThriftServer.class);

  /**
   * Thrift quality of protection configuration key. Valid values can be:
   * privacy: authentication, integrity and confidentiality checking
   * integrity: authentication and integrity checking
   * authentication: authentication only
   *
   * This is used to authenticate the callers and support impersonation.
   * The thrift server and the HBase cluster must run in secure mode.
   */
  static final String THRIFT_QOP_KEY = "hbase.thrift.security.qop";

  static final String BACKLOG_CONF_KEY = "hbase.regionserver.thrift.backlog";

  public static final int DEFAULT_LISTEN_PORT = 9090;

  private static final String READ_TIMEOUT_OPTION = "readTimeout";

  /**
   * Amount of time in milliseconds before a server thread will timeout
   * waiting for client to send data on a connected socket. Currently,
   * applies only to TBoundedThreadPoolServer
   */
  public static final String THRIFT_SERVER_SOCKET_READ_TIMEOUT_KEY =
    "hbase.thrift.server.socket.read.timeout";
  public static final int THRIFT_SERVER_SOCKET_READ_TIMEOUT_DEFAULT = 60000;

  public ThriftServer() {
  }

  private static void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, getOptions(),
        "To start the Thrift server run 'hbase-daemon.sh start thrift2' or " +
        "'hbase thrift2'\n" +
        "To shutdown the thrift server run 'hbase-daemon.sh stop thrift2' or" +
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
    options.addOption("w", "workers", true, "How many worker threads to use.");
    options.addOption("s", "selectors", true, "How many selector threads to use.");
    options.addOption("q", "callQueueSize", true,
      "Max size of request queue (unbounded by default)");
    options.addOption("h", "help", false, "Print help information");
    options.addOption(null, "infoport", true, "Port for web UI");
    options.addOption("t", READ_TIMEOUT_OPTION, true,
      "Amount of time in milliseconds before a server thread will timeout " +
      "waiting for client to send data on a connected socket. Currently, " +
      "only applies to TBoundedThreadPoolServer");
    options.addOption("ro", "readonly", false,
      "Respond only to read method requests [default: false]");
    OptionGroup servers = new OptionGroup();
    servers.addOption(new Option("nonblocking", false,
            "Use the TNonblockingServer. This implies the framed transport."));
    servers.addOption(new Option("hsha", false,
            "Use the THsHaServer. This implies the framed transport."));
    servers.addOption(new Option("selector", false,
            "Use the TThreadedSelectorServer. This implies the framed transport."));
    servers.addOption(new Option("threadpool", false,
            "Use the TThreadPoolServer. This is the default."));
    options.addOptionGroup(servers);
    return options;
  }

  private static CommandLine parseArguments(Configuration conf, Options options, String[] args)
      throws ParseException, IOException {
    CommandLineParser parser = new DefaultParser();
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

  private static TTransportFactory getTTransportFactory(
      SaslUtil.QualityOfProtection qop, String name, String host,
      boolean framed, int frameSize) {
    if (framed) {
      if (qop != null) {
        throw new RuntimeException("Thrift server authentication"
          + " doesn't work with framed transport yet");
      }
      log.debug("Using framed transport");
      return new TFramedTransport.Factory(frameSize);
    } else if (qop == null) {
      return new TTransportFactory();
    } else {
      Map<String, String> saslProperties = SaslUtil.initSaslProperties(qop.name());
      TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
      saslFactory.addServerDefinition("GSSAPI", name, host, saslProperties,
        new SaslGssCallbackHandler() {
          @Override
          public void handle(Callback[] callbacks)
              throws UnsupportedCallbackException {
            AuthorizeCallback ac = null;
            for (Callback callback : callbacks) {
              if (callback instanceof AuthorizeCallback) {
                ac = (AuthorizeCallback) callback;
              } else {
                throw new UnsupportedCallbackException(callback,
                    "Unrecognized SASL GSSAPI Callback");
              }
            }
            if (ac != null) {
              String authid = ac.getAuthenticationID();
              String authzid = ac.getAuthorizationID();
              if (!authid.equals(authzid)) {
                ac.setAuthorized(false);
              } else {
                ac.setAuthorized(true);
                String userName = SecurityUtil.getUserFromPrincipal(authzid);
                log.info("Effective user: " + userName);
                ac.setAuthorizedID(userName);
              }
            }
          }
        });
      return saslFactory;
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

  private static TServer getTNonBlockingServer(TProtocolFactory protocolFactory,
      TProcessor processor, TTransportFactory transportFactory, InetSocketAddress inetSocketAddress)
          throws TTransportException {
    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
    log.info("starting HBase Nonblocking Thrift server on " + inetSocketAddress.toString());
    TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new TNonblockingServer(serverArgs);
  }

  private static TServer getTHsHaServer(TProtocolFactory protocolFactory,
      TProcessor processor, TTransportFactory transportFactory,
      int workerThreads, int maxCallQueueSize,
      InetSocketAddress inetSocketAddress, ThriftMetrics metrics)
      throws TTransportException {
    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
    log.info("starting HBase HsHA Thrift server on " + inetSocketAddress.toString());
    THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
    if (workerThreads > 0) {
      // Could support the min & max threads, avoiding to preserve existing functionality.
      serverArgs.minWorkerThreads(workerThreads).maxWorkerThreads(workerThreads);
    }
    ExecutorService executorService = createExecutor(
        workerThreads, maxCallQueueSize, metrics);
    serverArgs.executorService(executorService);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new THsHaServer(serverArgs);
  }

  private static TServer getTThreadedSelectorServer(TProtocolFactory protocolFactory,
      TProcessor processor, TTransportFactory transportFactory,
      int workerThreads, int selectorThreads, int maxCallQueueSize,
      InetSocketAddress inetSocketAddress, ThriftMetrics metrics)
      throws TTransportException {
    TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
    log.info("starting HBase ThreadedSelector Thrift server on " + inetSocketAddress.toString());
    TThreadedSelectorServer.Args serverArgs = new TThreadedSelectorServer.Args(serverTransport);
    if (workerThreads > 0) {
      serverArgs.workerThreads(workerThreads);
    }
    if (selectorThreads > 0) {
      serverArgs.selectorThreads(selectorThreads);
    }

    ExecutorService executorService = createExecutor(
        workerThreads, maxCallQueueSize, metrics);
    serverArgs.executorService(executorService);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new TThreadedSelectorServer(serverArgs);
  }

  private static ExecutorService createExecutor(
      int workerThreads, int maxCallQueueSize, ThriftMetrics metrics) {
    CallQueue callQueue;
    if (maxCallQueueSize > 0) {
      callQueue = new CallQueue(new LinkedBlockingQueue<>(maxCallQueueSize), metrics);
    } else {
      callQueue = new CallQueue(new LinkedBlockingQueue<>(), metrics);
    }

    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift2-worker-%d");
    ThreadPoolExecutor pool = new THBaseThreadPoolExecutor(workerThreads, workerThreads,
            Long.MAX_VALUE, TimeUnit.SECONDS, callQueue, tfb.build(), metrics);
    pool.prestartAllCoreThreads();
    return pool;
  }

  private static TServer getTThreadPoolServer(TProtocolFactory protocolFactory,
                                              TProcessor processor,
                                              TTransportFactory transportFactory,
                                              int workerThreads,
                                              InetSocketAddress inetSocketAddress,
                                              int backlog,
                                              int clientTimeout,
                                              ThriftMetrics metrics)
      throws TTransportException {
    TServerTransport serverTransport = new TServerSocket(
                                           new TServerSocket.ServerSocketTransportArgs().
                                               bindAddr(inetSocketAddress).backlog(backlog).
                                               clientTimeout(clientTimeout));
    log.info("starting HBase ThreadPool Thrift server on " + inetSocketAddress.toString());
    TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    if (workerThreads > 0) {
      serverArgs.maxWorkerThreads(workerThreads);
    }
    ThreadPoolExecutor executor = new THBaseThreadPoolExecutor(serverArgs.minWorkerThreads,
        serverArgs.maxWorkerThreads, serverArgs.stopTimeoutVal, TimeUnit.SECONDS,
        new SynchronousQueue<>(), metrics);
    serverArgs.executorService(executor);

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
   */
  public static void main(String[] args) throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    // for now, only time we return is on an argument error.
    final int status = ToolRunner.run(conf, new ThriftServer(), args);
    System.exit(status);
  }

  @Override
  public int run(String[] args) throws Exception {
    final Configuration conf = getConf();
    Options options = getOptions();
    CommandLine cmd = parseArguments(conf, options, args);
    int workerThreads = 0;
    int selectorThreads = 0;
    int maxCallQueueSize = -1; // use unbounded queue by default

    if (cmd.hasOption("help")) {
      printUsage();
      return 1;
    }

    // Get address to bind
    String bindAddress = getBindAddress(conf, cmd);

    // check if server should only process read requests, if so override the conf
    if (cmd.hasOption("readonly")) {
      conf.setBoolean("hbase.thrift.readonly", true);
      if (log.isDebugEnabled()) {
        log.debug("readonly set to true");
      }
    }

    // Get read timeout
    int readTimeout = getReadTimeout(conf, cmd);
    // Get port to bind to
    int listenPort = getListenPort(conf, cmd);
    // Thrift's implementation uses '0' as a placeholder for 'use the default.'
    int backlog = conf.getInt(BACKLOG_CONF_KEY, 0);

    // Local hostname and user name, used only if QOP is configured.
    String host = null;
    String name = null;

    UserProvider userProvider = UserProvider.instantiate(conf);
    // login the server principal (if using secure Hadoop)
    boolean securityEnabled = userProvider.isHadoopSecurityEnabled()
      && userProvider.isHBaseSecurityEnabled();
    if (securityEnabled) {
      host = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
        conf.get("hbase.thrift.dns.interface", "default"),
        conf.get("hbase.thrift.dns.nameserver", "default")));
      userProvider.login("hbase.thrift.keytab.file", "hbase.thrift.kerberos.principal", host);
    }

    UserGroupInformation realUser = userProvider.getCurrent().getUGI();
    String stringQop = conf.get(THRIFT_QOP_KEY);
    SaslUtil.QualityOfProtection qop = null;
    if (stringQop != null) {
      qop = SaslUtil.getQop(stringQop);
      if (!securityEnabled) {
        throw new IOException("Thrift server must run in secure mode to support authentication");
      }
      // Extract the name from the principal
      name = SecurityUtil.getUserFromPrincipal(conf.get("hbase.thrift.kerberos.principal"));
    }

    boolean nonblocking = cmd.hasOption("nonblocking");
    boolean hsha = cmd.hasOption("hsha");
    boolean selector = cmd.hasOption("selector");

    ThriftMetrics metrics = new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.TWO);
    final JvmPauseMonitor pauseMonitor = new JvmPauseMonitor(conf, metrics.getSource());

    String implType = getImplType(nonblocking, hsha, selector);

    conf.set("hbase.regionserver.thrift.server.type", implType);
    conf.setInt("hbase.regionserver.thrift.port", listenPort);
    registerFilters(conf);

    // Construct correct ProtocolFactory
    boolean compact = cmd.hasOption("compact") ||
        conf.getBoolean("hbase.regionserver.thrift.compact", false);
    TProtocolFactory protocolFactory = getTProtocolFactory(compact);
    final ThriftHBaseServiceHandler hbaseHandler =
      new ThriftHBaseServiceHandler(conf, userProvider);
    THBaseService.Iface handler =
      ThriftHBaseServiceHandler.newInstance(hbaseHandler, metrics);
    final THBaseService.Processor p = new THBaseService.Processor(handler);
    conf.setBoolean("hbase.regionserver.thrift.compact", compact);
    TProcessor processor = p;

    boolean framed = cmd.hasOption("framed") ||
        conf.getBoolean("hbase.regionserver.thrift.framed", false) || nonblocking || hsha;
    TTransportFactory transportFactory = getTTransportFactory(qop, name, host, framed,
        conf.getInt("hbase.regionserver.thrift.framed.max_frame_size_in_mb", 2) * 1024 * 1024);
    InetSocketAddress inetSocketAddress = bindToPort(bindAddress, listenPort);
    conf.setBoolean("hbase.regionserver.thrift.framed", framed);
    if (qop != null) {
      // Create a processor wrapper, to get the caller
      processor = new TProcessor() {
        @Override
        public boolean process(TProtocol inProt,
            TProtocol outProt) throws TException {
          TSaslServerTransport saslServerTransport =
            (TSaslServerTransport)inProt.getTransport();
          SaslServer saslServer = saslServerTransport.getSaslServer();
          String principal = saslServer.getAuthorizationID();
          hbaseHandler.setEffectiveUser(principal);
          return p.process(inProt, outProt);
        }
      };
    }

    if (cmd.hasOption("w")) {
      workerThreads = Integer.parseInt(cmd.getOptionValue("w"));
    }
    if (cmd.hasOption("s")) {
      selectorThreads = Integer.parseInt(cmd.getOptionValue("s"));
    }
    if (cmd.hasOption("q")) {
      maxCallQueueSize = Integer.parseInt(cmd.getOptionValue("q"));
    }

    // check for user-defined info server port setting, if so override the conf
    try {
      if (cmd.hasOption("infoport")) {
        String val = cmd.getOptionValue("infoport");
        conf.setInt("hbase.thrift.info.port", Integer.parseInt(val));
        log.debug("Web UI port set to " + val);
      }
    } catch (NumberFormatException e) {
      log.error("Could not parse the value provided for the infoport option", e);
      printUsage();
      System.exit(1);
    }

    // Put up info server.
    startInfoServer(conf);

    final TServer tserver = getServer(workerThreads, selectorThreads, maxCallQueueSize, readTimeout,
            backlog, nonblocking, hsha, selector, metrics, protocolFactory, processor,
            transportFactory, inetSocketAddress);

    realUser.doAs(
      new PrivilegedAction<Object>() {
        @Override
        public Object run() {
          pauseMonitor.start();
          try {
            tserver.serve();
            return null;
          } finally {
            pauseMonitor.stop();
          }
        }
      });
    // when tserver.stop eventually happens we'll get here.
    return 0;
  }

  private String getImplType(boolean nonblocking, boolean hsha, boolean selector) {
    String implType = "threadpool";

    if (nonblocking) {
      implType = "nonblocking";
    } else if (hsha) {
      implType = "hsha";
    } else if (selector) {
      implType = "selector";
    }

    return implType;
  }

  private String getBindAddress(Configuration conf, CommandLine cmd) {
    String bindAddress;
    if (cmd.hasOption("bind")) {
      bindAddress = cmd.getOptionValue("bind");
      conf.set("hbase.thrift.info.bindAddress", bindAddress);
    } else {
      bindAddress = conf.get("hbase.thrift.info.bindAddress");
    }
    return bindAddress;
  }

  private int getListenPort(Configuration conf, CommandLine cmd) {
    int listenPort;
    try {
      if (cmd.hasOption("port")) {
        listenPort = Integer.parseInt(cmd.getOptionValue("port"));
      } else {
        listenPort = conf.getInt("hbase.regionserver.thrift.port", DEFAULT_LISTEN_PORT);
      }
    } catch (NumberFormatException e) {
      throw new RuntimeException("Could not parse the value provided for the port option", e);
    }
    return listenPort;
  }

  private int getReadTimeout(Configuration conf, CommandLine cmd) {
    int readTimeout;
    if (cmd.hasOption(READ_TIMEOUT_OPTION)) {
      try {
        readTimeout = Integer.parseInt(cmd.getOptionValue(READ_TIMEOUT_OPTION));
      } catch (NumberFormatException e) {
        throw new RuntimeException("Could not parse the value provided for the timeout option", e);
      }
    } else {
      readTimeout = conf.getInt(THRIFT_SERVER_SOCKET_READ_TIMEOUT_KEY,
        THRIFT_SERVER_SOCKET_READ_TIMEOUT_DEFAULT);
    }
    return readTimeout;
  }

  private void startInfoServer(Configuration conf) throws IOException {
    int port = conf.getInt("hbase.thrift.info.port", 9095);

    if (port >= 0) {
      conf.setLong("startcode", System.currentTimeMillis());
      String a = conf.get("hbase.thrift.info.bindAddress", "0.0.0.0");
      InfoServer infoServer = new InfoServer("thrift", a, port, false, conf);
      infoServer.setAttribute("hbase.conf", conf);
      infoServer.start();
    }
  }

  private TServer getServer(int workerThreads, int selectorThreads, int maxCallQueueSize,
        int readTimeout, int backlog, boolean nonblocking, boolean hsha, boolean selector,
        ThriftMetrics metrics, TProtocolFactory protocolFactory, TProcessor processor,
        TTransportFactory transportFactory, InetSocketAddress inetSocketAddress)
          throws TTransportException {
    TServer server;

    if (nonblocking) {
      server = getTNonBlockingServer(protocolFactory, processor, transportFactory,
              inetSocketAddress);
    } else if (hsha) {
      server = getTHsHaServer(protocolFactory, processor, transportFactory, workerThreads,
              maxCallQueueSize, inetSocketAddress, metrics);
    } else if (selector) {
      server = getTThreadedSelectorServer(protocolFactory, processor, transportFactory,
              workerThreads, selectorThreads, maxCallQueueSize, inetSocketAddress, metrics);
    } else {
      server = getTThreadPoolServer(protocolFactory, processor, transportFactory, workerThreads,
              inetSocketAddress, backlog, readTimeout, metrics);
    }
    return server;
  }
}
