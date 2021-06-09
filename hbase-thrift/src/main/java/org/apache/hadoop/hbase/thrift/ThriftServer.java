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

package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.Constants.BACKLOG_CONF_DEAFULT;
import static org.apache.hadoop.hbase.thrift.Constants.BACKLOG_CONF_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.BIND_CONF_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.BIND_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.COMPACT_CONF_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.COMPACT_CONF_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.COMPACT_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.DEFAULT_BIND_ADDR;
import static org.apache.hadoop.hbase.thrift.Constants.DEFAULT_HTTP_MAX_HEADER_SIZE;
import static org.apache.hadoop.hbase.thrift.Constants.DEFAULT_LISTEN_PORT;
import static org.apache.hadoop.hbase.thrift.Constants.FRAMED_CONF_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.FRAMED_CONF_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.FRAMED_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.HTTP_MAX_THREADS_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.HTTP_MAX_THREADS_KEY_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.HTTP_MIN_THREADS_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.HTTP_MIN_THREADS_KEY_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.INFOPORT_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.KEEP_ALIVE_SEC_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.MAX_FRAME_SIZE_CONF_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.MAX_FRAME_SIZE_CONF_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.MAX_QUEUE_SIZE_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.MAX_WORKERS_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.MIN_WORKERS_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.PORT_CONF_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.PORT_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.READ_TIMEOUT_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.SELECTOR_NUM_OPTION;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_DNS_INTERFACE_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_DNS_NAMESERVER_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_FILTERS;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_HTTP_ALLOW_OPTIONS_METHOD;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_HTTP_ALLOW_OPTIONS_METHOD_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_INFO_SERVER_BINDING_ADDRESS;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_INFO_SERVER_BINDING_ADDRESS_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_INFO_SERVER_PORT;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_INFO_SERVER_PORT_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_QOP_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SELECTOR_NUM;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SERVER_SOCKET_READ_TIMEOUT_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SERVER_SOCKET_READ_TIMEOUT_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SPNEGO_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SPNEGO_PRINCIPAL_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_ENABLED_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_EXCLUDE_CIPHER_SUITES_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_EXCLUDE_PROTOCOLS_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_INCLUDE_CIPHER_SUITES_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_INCLUDE_PROTOCOLS_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_KEYSTORE_KEYPASSWORD_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_KEYSTORE_PASSWORD_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_KEYSTORE_STORE_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_KEYSTORE_TYPE_DEFAULT;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SSL_KEYSTORE_TYPE_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SUPPORT_PROXYUSER_KEY;
import static org.apache.hadoop.hbase.thrift.Constants.USE_HTTP_CONF_KEY;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslServer;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.filter.ParseFilter;
import org.apache.hadoop.hbase.http.HttpServerUtil;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityUtil;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServlet;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Joiner;
import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLineParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.DefaultParser;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.eclipse.jetty.http.HttpVersion;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.HttpConfiguration;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.HttpConnectionFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.SecureRequestCustomizer;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.SslConnectionFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletContextHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.ssl.SslContextFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * ThriftServer- this class starts up a Thrift server which implements the
 * Hbase API specified in the Hbase.thrift IDL file. The server runs in an
 * independent process.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class ThriftServer  extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class);



  protected Configuration conf;

  protected InfoServer infoServer;

  protected TProcessor processor;

  protected ThriftMetrics metrics;
  protected HBaseServiceHandler hbaseServiceHandler;
  protected UserGroupInformation serviceUGI;
  protected UserGroupInformation httpUGI;
  protected boolean httpEnabled;

  protected SaslUtil.QualityOfProtection qop;
  protected String host;
  protected int listenPort;


  protected boolean securityEnabled;
  protected boolean doAsEnabled;

  protected JvmPauseMonitor pauseMonitor;

  protected volatile TServer tserver;
  protected volatile Server httpServer;


  //
  // Main program and support routines
  //

  public ThriftServer(Configuration conf) {
    this.conf = HBaseConfiguration.create(conf);
  }

  protected ThriftMetrics createThriftMetrics(Configuration conf) {
    return new ThriftMetrics(conf, ThriftMetrics.ThriftServerType.ONE);
  }

  protected void setupParamters() throws IOException {
    // login the server principal (if using secure Hadoop)
    UserProvider userProvider = UserProvider.instantiate(conf);
    securityEnabled = userProvider.isHadoopSecurityEnabled()
        && userProvider.isHBaseSecurityEnabled();
    if (securityEnabled) {
      host = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
          conf.get(THRIFT_DNS_INTERFACE_KEY, "default"),
          conf.get(THRIFT_DNS_NAMESERVER_KEY, "default")));
      userProvider.login(THRIFT_KEYTAB_FILE_KEY, THRIFT_KERBEROS_PRINCIPAL_KEY, host);

      // Setup the SPNEGO user for HTTP if configured
      String spnegoPrincipal = getSpengoPrincipal(conf, host);
      String spnegoKeytab = getSpnegoKeytab(conf);
      UserGroupInformation.setConfiguration(conf);
      // login the SPNEGO principal using UGI to avoid polluting the login user
      this.httpUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(spnegoPrincipal,
        spnegoKeytab);
    }
    this.serviceUGI = userProvider.getCurrent().getUGI();
    if (httpUGI == null) {
      this.httpUGI = serviceUGI;
    }

    this.listenPort = conf.getInt(PORT_CONF_KEY, DEFAULT_LISTEN_PORT);
    this.metrics = createThriftMetrics(conf);
    this.pauseMonitor = new JvmPauseMonitor(conf, this.metrics.getSource());
    this.hbaseServiceHandler = createHandler(conf, userProvider);
    this.hbaseServiceHandler.initMetrics(metrics);
    this.processor = createProcessor();

    httpEnabled = conf.getBoolean(USE_HTTP_CONF_KEY, false);
    doAsEnabled = conf.getBoolean(THRIFT_SUPPORT_PROXYUSER_KEY, false);
    if (doAsEnabled && !httpEnabled) {
      LOG.warn("Fail to enable the doAs feature. " + USE_HTTP_CONF_KEY + " is not configured");
    }

    String strQop = conf.get(THRIFT_QOP_KEY);
    if (strQop != null) {
      this.qop = SaslUtil.getQop(strQop);
    }
    if (qop != null) {
      if (qop != SaslUtil.QualityOfProtection.AUTHENTICATION &&
          qop != SaslUtil.QualityOfProtection.INTEGRITY &&
          qop != SaslUtil.QualityOfProtection.PRIVACY) {
        throw new IOException(String.format("Invalid %s: It must be one of %s, %s, or %s.",
            THRIFT_QOP_KEY,
            SaslUtil.QualityOfProtection.AUTHENTICATION.name(),
            SaslUtil.QualityOfProtection.INTEGRITY.name(),
            SaslUtil.QualityOfProtection.PRIVACY.name()));
      }
      checkHttpSecurity(qop, conf);
      if (!securityEnabled) {
        throw new IOException("Thrift server must run in secure mode to support authentication");
      }
    }
    registerFilters(conf);
    pauseMonitor.start();
  }

  private String getSpengoPrincipal(Configuration conf, String host) throws IOException {
    String principal = conf.get(THRIFT_SPNEGO_PRINCIPAL_KEY);
    if (principal == null) {
      // We cannot use the Hadoop configuration deprecation handling here since
      // the THRIFT_KERBEROS_PRINCIPAL_KEY config is still valid for regular Kerberos
      // communication. The preference should be to use the THRIFT_SPNEGO_PRINCIPAL_KEY
      // config so that THRIFT_KERBEROS_PRINCIPAL_KEY doesn't control both backend
      // Kerberos principal and SPNEGO principal.
      LOG.info("Using deprecated {} config for SPNEGO principal. Use {} instead.",
        THRIFT_KERBEROS_PRINCIPAL_KEY, THRIFT_SPNEGO_PRINCIPAL_KEY);
      principal = conf.get(THRIFT_KERBEROS_PRINCIPAL_KEY);
    }
    // Handle _HOST in principal value
    return org.apache.hadoop.security.SecurityUtil.getServerPrincipal(principal, host);
  }

  private String getSpnegoKeytab(Configuration conf) {
    String keytab = conf.get(THRIFT_SPNEGO_KEYTAB_FILE_KEY);
    if (keytab == null) {
      // We cannot use the Hadoop configuration deprecation handling here since
      // the THRIFT_KEYTAB_FILE_KEY config is still valid for regular Kerberos
      // communication. The preference should be to use the THRIFT_SPNEGO_KEYTAB_FILE_KEY
      // config so that THRIFT_KEYTAB_FILE_KEY doesn't control both backend
      // Kerberos keytab and SPNEGO keytab.
      LOG.info("Using deprecated {} config for SPNEGO keytab. Use {} instead.",
        THRIFT_KEYTAB_FILE_KEY, THRIFT_SPNEGO_KEYTAB_FILE_KEY);
      keytab = conf.get(THRIFT_KEYTAB_FILE_KEY);
    }
    return keytab;
  }

  protected void startInfoServer() throws IOException {
    // Put up info server.
    int port = conf.getInt(THRIFT_INFO_SERVER_PORT , THRIFT_INFO_SERVER_PORT_DEFAULT);

    if (port >= 0) {
      conf.setLong("startcode", System.currentTimeMillis());
      String a = conf
          .get(THRIFT_INFO_SERVER_BINDING_ADDRESS, THRIFT_INFO_SERVER_BINDING_ADDRESS_DEFAULT);
      infoServer = new InfoServer("thrift", a, port, false, conf);
      infoServer.setAttribute("hbase.conf", conf);
      infoServer.setAttribute("hbase.thrift.server.type", metrics.getThriftServerType().name());
      infoServer.start();
    }
  }

  protected void checkHttpSecurity(SaslUtil.QualityOfProtection qop, Configuration conf) {
    if (qop == SaslUtil.QualityOfProtection.PRIVACY &&
        conf.getBoolean(USE_HTTP_CONF_KEY, false) &&
        !conf.getBoolean(THRIFT_SSL_ENABLED_KEY, false)) {
      throw new IllegalArgumentException("Thrift HTTP Server's QoP is privacy, but " +
          THRIFT_SSL_ENABLED_KEY + " is false");
    }
  }

  protected HBaseServiceHandler createHandler(Configuration conf, UserProvider userProvider)
      throws IOException {
    return new ThriftHBaseServiceHandler(conf, userProvider);
  }

  protected TProcessor createProcessor() {
    return new Hbase.Processor<>(
        HbaseHandlerMetricsProxy.newInstance((Hbase.Iface) hbaseServiceHandler, metrics, conf));
  }

  /**
   * the thrift server, not null means the server is started, for test only
   * @return the tServer
   */
  @InterfaceAudience.Private
  public TServer getTserver() {
    return tserver;
  }

  /**
   * the Jetty server, not null means the HTTP server is started, for test only
   * @return the http server
   */
  @InterfaceAudience.Private
  public Server getHttpServer() {
    return httpServer;
  }

  protected void printUsageAndExit(Options options, int exitCode)
      throws ExitCodeException {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("Thrift", null, options,
        "To start the Thrift server run 'hbase-daemon.sh start thrift' or " +
            "'hbase thrift'\n" +
            "To shutdown the thrift server run 'hbase-daemon.sh stop " +
            "thrift' or send a kill signal to the thrift server pid",
        true);
    throw new ExitCodeException(exitCode, "");
  }

  /**
   * Create a Servlet for the http server
   * @param protocolFactory protocolFactory
   * @return the servlet
   */
  protected TServlet createTServlet(TProtocolFactory protocolFactory) {
    return new ThriftHttpServlet(processor, protocolFactory, serviceUGI, httpUGI,
        hbaseServiceHandler, securityEnabled, doAsEnabled);
  }

  /**
   * Setup an HTTP Server using Jetty to serve calls from THttpClient
   *
   * @throws IOException IOException
   */
  protected void setupHTTPServer() throws IOException {
    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    TServlet thriftHttpServlet = createTServlet(protocolFactory);

    // Set the default max thread number to 100 to limit
    // the number of concurrent requests so that Thrfit HTTP server doesn't OOM easily.
    // Jetty set the default max thread number to 250, if we don't set it.
    //
    // Our default min thread number 2 is the same as that used by Jetty.
    int minThreads = conf.getInt(HTTP_MIN_THREADS_KEY,
        conf.getInt(TBoundedThreadPoolServer.MIN_WORKER_THREADS_CONF_KEY,
            HTTP_MIN_THREADS_KEY_DEFAULT));
    int maxThreads = conf.getInt(HTTP_MAX_THREADS_KEY,
        conf.getInt(TBoundedThreadPoolServer.MAX_WORKER_THREADS_CONF_KEY,
            HTTP_MAX_THREADS_KEY_DEFAULT));
    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads);
    threadPool.setMinThreads(minThreads);
    httpServer = new Server(threadPool);

    // Context handler
    ServletContextHandler ctxHandler = new ServletContextHandler(httpServer, "/",
        ServletContextHandler.SESSIONS);
    ctxHandler.addServlet(new ServletHolder(thriftHttpServlet), "/*");
    HttpServerUtil.constrainHttpMethods(ctxHandler,
        conf.getBoolean(THRIFT_HTTP_ALLOW_OPTIONS_METHOD,
            THRIFT_HTTP_ALLOW_OPTIONS_METHOD_DEFAULT));

    // set up Jetty and run the embedded server
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSecureScheme("https");
    httpConfig.setSecurePort(listenPort);
    httpConfig.setHeaderCacheSize(DEFAULT_HTTP_MAX_HEADER_SIZE);
    httpConfig.setRequestHeaderSize(DEFAULT_HTTP_MAX_HEADER_SIZE);
    httpConfig.setResponseHeaderSize(DEFAULT_HTTP_MAX_HEADER_SIZE);
    httpConfig.setSendServerVersion(false);
    httpConfig.setSendDateHeader(false);

    ServerConnector serverConnector;
    if(conf.getBoolean(THRIFT_SSL_ENABLED_KEY, false)) {
      HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
      httpsConfig.addCustomizer(new SecureRequestCustomizer());

      SslContextFactory sslCtxFactory = new SslContextFactory();
      String keystore = conf.get(THRIFT_SSL_KEYSTORE_STORE_KEY);
      String password = HBaseConfiguration.getPassword(conf,
          THRIFT_SSL_KEYSTORE_PASSWORD_KEY, null);
      String keyPassword = HBaseConfiguration.getPassword(conf,
          THRIFT_SSL_KEYSTORE_KEYPASSWORD_KEY, password);
      sslCtxFactory.setKeyStorePath(keystore);
      sslCtxFactory.setKeyStorePassword(password);
      sslCtxFactory.setKeyManagerPassword(keyPassword);
      sslCtxFactory.setKeyStoreType(conf.get(
        THRIFT_SSL_KEYSTORE_TYPE_KEY, THRIFT_SSL_KEYSTORE_TYPE_DEFAULT));

      String[] excludeCiphers = conf.getStrings(
          THRIFT_SSL_EXCLUDE_CIPHER_SUITES_KEY, ArrayUtils.EMPTY_STRING_ARRAY);
      if (excludeCiphers.length != 0) {
        sslCtxFactory.setExcludeCipherSuites(excludeCiphers);
      }
      String[] includeCiphers = conf.getStrings(
          THRIFT_SSL_INCLUDE_CIPHER_SUITES_KEY, ArrayUtils.EMPTY_STRING_ARRAY);
      if (includeCiphers.length != 0) {
        sslCtxFactory.setIncludeCipherSuites(includeCiphers);
      }

      // Disable SSLv3 by default due to "Poodle" Vulnerability - CVE-2014-3566
      String[] excludeProtocols = conf.getStrings(
          THRIFT_SSL_EXCLUDE_PROTOCOLS_KEY, "SSLv3");
      if (excludeProtocols.length != 0) {
        sslCtxFactory.setExcludeProtocols(excludeProtocols);
      }
      String[] includeProtocols = conf.getStrings(
          THRIFT_SSL_INCLUDE_PROTOCOLS_KEY, ArrayUtils.EMPTY_STRING_ARRAY);
      if (includeProtocols.length != 0) {
        sslCtxFactory.setIncludeProtocols(includeProtocols);
      }

      serverConnector = new ServerConnector(httpServer,
          new SslConnectionFactory(sslCtxFactory, HttpVersion.HTTP_1_1.toString()),
          new HttpConnectionFactory(httpsConfig));
    } else {
      serverConnector = new ServerConnector(httpServer, new HttpConnectionFactory(httpConfig));
    }
    serverConnector.setPort(listenPort);
    serverConnector.setHost(getBindAddress(conf).getHostAddress());
    httpServer.addConnector(serverConnector);
    httpServer.setStopAtShutdown(true);

    if (doAsEnabled) {
      ProxyUsers.refreshSuperUserGroupsConfiguration(conf);
    }
    LOG.info("Starting Thrift HTTP Server on {}", Integer.toString(listenPort));
  }

  /**
   * Setting up the thrift TServer
   */
  protected void setupServer() throws Exception {
    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory = getProtocolFactory();

    ImplType implType = ImplType.getServerImpl(conf);
    TProcessor processorToUse = processor;

    // Construct correct TransportFactory
    TTransportFactory transportFactory;
    if (conf.getBoolean(FRAMED_CONF_KEY, FRAMED_CONF_DEFAULT) || implType.isAlwaysFramed) {
      if (qop != null) {
        throw new RuntimeException("Thrift server authentication"
            + " doesn't work with framed transport yet");
      }
      transportFactory = new TFramedTransport.Factory(
        conf.getInt(MAX_FRAME_SIZE_CONF_KEY, MAX_FRAME_SIZE_CONF_DEFAULT) * 1024 * 1024);
      LOG.debug("Using framed transport");
    } else if (qop == null) {
      transportFactory = new TTransportFactory();
    } else {
      // Extract the name from the principal
      String thriftKerberosPrincipal = conf.get(THRIFT_KERBEROS_PRINCIPAL_KEY);
      if (thriftKerberosPrincipal == null) {
        throw new IllegalArgumentException(THRIFT_KERBEROS_PRINCIPAL_KEY + " cannot be null");
      }
      String name = SecurityUtil.getUserFromPrincipal(thriftKerberosPrincipal);
      Map<String, String> saslProperties = SaslUtil.initSaslProperties(qop.name());
      TSaslServerTransport.Factory saslFactory = new TSaslServerTransport.Factory();
      saslFactory.addServerDefinition("GSSAPI", name, host, saslProperties,
          new SaslRpcServer.SaslGssCallbackHandler() {
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
                  LOG.info("Effective user: {}", userName);
                  ac.setAuthorizedID(userName);
                }
              }
            }
          });
      transportFactory = saslFactory;

      // Create a processor wrapper, to get the caller
      processorToUse = (inProt, outProt) -> {
        TSaslServerTransport saslServerTransport =
            (TSaslServerTransport)inProt.getTransport();
        SaslServer saslServer = saslServerTransport.getSaslServer();
        String principal = saslServer.getAuthorizationID();
        hbaseServiceHandler.setEffectiveUser(principal);
        processor.process(inProt, outProt);
      };
    }

    if (conf.get(BIND_CONF_KEY) != null && !implType.canSpecifyBindIP) {
      LOG.error("Server types {} don't support IP address binding at the moment. See " +
              "https://issues.apache.org/jira/browse/HBASE-2155 for details.",
          Joiner.on(", ").join(ImplType.serversThatCannotSpecifyBindIP()));
      throw new RuntimeException("-" + BIND_CONF_KEY + " not supported with " + implType);
    }

    InetSocketAddress inetSocketAddress = new InetSocketAddress(getBindAddress(conf), listenPort);
    if (implType == ImplType.HS_HA || implType == ImplType.NONBLOCKING ||
        implType == ImplType.THREADED_SELECTOR) {
      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(inetSocketAddress);
      if (implType == ImplType.NONBLOCKING) {
        tserver = getTNonBlockingServer(serverTransport, protocolFactory, processorToUse,
            transportFactory, inetSocketAddress);
      } else if (implType == ImplType.HS_HA) {
        tserver = getTHsHaServer(serverTransport, protocolFactory, processorToUse, transportFactory,
            inetSocketAddress);
      } else { // THREADED_SELECTOR
        tserver = getTThreadedSelectorServer(serverTransport, protocolFactory, processorToUse,
            transportFactory, inetSocketAddress);
      }
      LOG.info("starting HBase {} server on {}", implType.simpleClassName(),
          Integer.toString(listenPort));
    } else if (implType == ImplType.THREAD_POOL) {
      this.tserver = getTThreadPoolServer(protocolFactory, processorToUse, transportFactory,
          inetSocketAddress);
    } else {
      throw new AssertionError("Unsupported Thrift server implementation: " +
          implType.simpleClassName());
    }

    // A sanity check that we instantiated the right type of server.
    if (tserver.getClass() != implType.serverClass) {
      throw new AssertionError("Expected to create Thrift server class " +
          implType.serverClass.getName() + " but got " +
          tserver.getClass().getName());
    }
  }

  protected TServer getTNonBlockingServer(TNonblockingServerTransport serverTransport,
      TProtocolFactory protocolFactory, TProcessor processor, TTransportFactory transportFactory,
      InetSocketAddress inetSocketAddress) {
    LOG.info("starting HBase Nonblocking Thrift server on " + inetSocketAddress.toString());
    TNonblockingServer.Args serverArgs = new TNonblockingServer.Args(serverTransport);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new TNonblockingServer(serverArgs);
  }

  protected TServer getTHsHaServer(TNonblockingServerTransport serverTransport,
      TProtocolFactory protocolFactory, TProcessor processor, TTransportFactory transportFactory,
      InetSocketAddress inetSocketAddress) {
    LOG.info("starting HBase HsHA Thrift server on " + inetSocketAddress.toString());
    THsHaServer.Args serverArgs = new THsHaServer.Args(serverTransport);
    int queueSize = conf.getInt(TBoundedThreadPoolServer.MAX_QUEUED_REQUESTS_CONF_KEY,
        TBoundedThreadPoolServer.DEFAULT_MAX_QUEUED_REQUESTS);
    CallQueue callQueue = new CallQueue(new LinkedBlockingQueue<>(queueSize), metrics);
    int workerThread = conf.getInt(TBoundedThreadPoolServer.MAX_WORKER_THREADS_CONF_KEY,
        serverArgs.getMaxWorkerThreads());
    ExecutorService executorService = createExecutor(
        callQueue, workerThread, workerThread);
    serverArgs.executorService(executorService).processor(processor)
        .transportFactory(transportFactory).protocolFactory(protocolFactory);
    return new THsHaServer(serverArgs);
  }

  protected TServer getTThreadedSelectorServer(TNonblockingServerTransport serverTransport,
      TProtocolFactory protocolFactory, TProcessor processor, TTransportFactory transportFactory,
      InetSocketAddress inetSocketAddress) {
    LOG.info("starting HBase ThreadedSelector Thrift server on " + inetSocketAddress.toString());
    TThreadedSelectorServer.Args serverArgs =
        new HThreadedSelectorServerArgs(serverTransport, conf);
    int queueSize = conf.getInt(TBoundedThreadPoolServer.MAX_QUEUED_REQUESTS_CONF_KEY,
        TBoundedThreadPoolServer.DEFAULT_MAX_QUEUED_REQUESTS);
    CallQueue callQueue = new CallQueue(new LinkedBlockingQueue<>(queueSize), metrics);
    int workerThreads = conf.getInt(TBoundedThreadPoolServer.MAX_WORKER_THREADS_CONF_KEY,
        serverArgs.getWorkerThreads());
    int selectorThreads = conf.getInt(THRIFT_SELECTOR_NUM, serverArgs.getSelectorThreads());
    serverArgs.selectorThreads(selectorThreads);
    ExecutorService executorService = createExecutor(
        callQueue, workerThreads, workerThreads);
    serverArgs.executorService(executorService).processor(processor)
        .transportFactory(transportFactory).protocolFactory(protocolFactory);
    return new TThreadedSelectorServer(serverArgs);
  }

  protected TServer getTThreadPoolServer(TProtocolFactory protocolFactory, TProcessor processor,
      TTransportFactory transportFactory, InetSocketAddress inetSocketAddress) throws Exception {
    LOG.info("starting HBase ThreadPool Thrift server on " + inetSocketAddress.toString());
    // Thrift's implementation uses '0' as a placeholder for 'use the default.'
    int backlog = conf.getInt(BACKLOG_CONF_KEY, BACKLOG_CONF_DEAFULT);
    int readTimeout = conf.getInt(THRIFT_SERVER_SOCKET_READ_TIMEOUT_KEY,
        THRIFT_SERVER_SOCKET_READ_TIMEOUT_DEFAULT);
    TServerTransport serverTransport = new TServerSocket(
        new TServerSocket.ServerSocketTransportArgs().
            bindAddr(inetSocketAddress).backlog(backlog).
            clientTimeout(readTimeout));

    TBoundedThreadPoolServer.Args serverArgs =
        new TBoundedThreadPoolServer.Args(serverTransport, conf);
    serverArgs.processor(processor).transportFactory(transportFactory)
        .protocolFactory(protocolFactory);
    return new TBoundedThreadPoolServer(serverArgs, metrics);
  }

  protected TProtocolFactory getProtocolFactory() {
    TProtocolFactory protocolFactory;

    if (conf.getBoolean(COMPACT_CONF_KEY, COMPACT_CONF_DEFAULT)) {
      LOG.debug("Using compact protocol");
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      protocolFactory = new TBinaryProtocol.Factory();
    }

    return protocolFactory;
  }

  protected ExecutorService createExecutor(BlockingQueue<Runnable> callQueue,
      int minWorkers, int maxWorkers) {
    ThreadFactoryBuilder tfb = new ThreadFactoryBuilder();
    tfb.setDaemon(true);
    tfb.setNameFormat("thrift-worker-%d");
    ThreadPoolExecutor threadPool = new THBaseThreadPoolExecutor(minWorkers, maxWorkers,
        Long.MAX_VALUE, TimeUnit.SECONDS, callQueue, tfb.build(), metrics);
    threadPool.allowCoreThreadTimeOut(true);
    return threadPool;
  }

  protected InetAddress getBindAddress(Configuration conf)
      throws UnknownHostException {
    String bindAddressStr = conf.get(BIND_CONF_KEY, DEFAULT_BIND_ADDR);
    return InetAddress.getByName(bindAddressStr);
  }


  public static void registerFilters(Configuration conf) {
    String[] filters = conf.getStrings(THRIFT_FILTERS);
    Splitter splitter = Splitter.on(':');
    if(filters != null) {
      for(String filterClass: filters) {
        List<String> filterPart = splitter.splitToList(filterClass);
        if(filterPart.size() != 2) {
          LOG.warn("Invalid filter specification " + filterClass + " - skipping");
        } else {
          ParseFilter.registerFilter(filterPart.get(0), filterPart.get(1));
        }
      }
    }
  }

  /**
   * Add options to command lines
   * @param options options
   */
  protected void addOptions(Options options) {
    options.addOption("b", BIND_OPTION, true, "Address to bind " +
        "the Thrift server to. [default: " + DEFAULT_BIND_ADDR + "]");
    options.addOption("p", PORT_OPTION, true, "Port to bind to [default: " +
        DEFAULT_LISTEN_PORT + "]");
    options.addOption("f", FRAMED_OPTION, false, "Use framed transport");
    options.addOption("c", COMPACT_OPTION, false, "Use the compact protocol");
    options.addOption("h", "help", false, "Print help information");
    options.addOption("s", SELECTOR_NUM_OPTION, true, "How many selector threads to use.");
    options.addOption(null, INFOPORT_OPTION, true, "Port for web UI");

    options.addOption("m", MIN_WORKERS_OPTION, true,
        "The minimum number of worker threads for " +
            ImplType.THREAD_POOL.simpleClassName());

    options.addOption("w", MAX_WORKERS_OPTION, true,
        "The maximum number of worker threads for " +
            ImplType.THREAD_POOL.simpleClassName());

    options.addOption("q", MAX_QUEUE_SIZE_OPTION, true,
        "The maximum number of queued requests in " +
            ImplType.THREAD_POOL.simpleClassName());

    options.addOption("k", KEEP_ALIVE_SEC_OPTION, true,
        "The amount of time in secods to keep a thread alive when idle in " +
            ImplType.THREAD_POOL.simpleClassName());

    options.addOption("t", READ_TIMEOUT_OPTION, true,
        "Amount of time in milliseconds before a server thread will timeout " +
            "waiting for client to send data on a connected socket. Currently, " +
            "only applies to TBoundedThreadPoolServer");

    options.addOptionGroup(ImplType.createOptionGroup());
  }

  protected void parseCommandLine(CommandLine cmd, Options options) throws ExitCodeException {
    // Get port to bind to
    try {
      if (cmd.hasOption(PORT_OPTION)) {
        int listenPort = Integer.parseInt(cmd.getOptionValue(PORT_OPTION));
        conf.setInt(PORT_CONF_KEY, listenPort);
      }
    } catch (NumberFormatException e) {
      LOG.error("Could not parse the value provided for the port option", e);
      printUsageAndExit(options, -1);
    }
    // check for user-defined info server port setting, if so override the conf
    try {
      if (cmd.hasOption(INFOPORT_OPTION)) {
        String val = cmd.getOptionValue(INFOPORT_OPTION);
        conf.setInt(THRIFT_INFO_SERVER_PORT, Integer.parseInt(val));
        LOG.debug("Web UI port set to " + val);
      }
    } catch (NumberFormatException e) {
      LOG.error("Could not parse the value provided for the " + INFOPORT_OPTION +
          " option", e);
      printUsageAndExit(options, -1);
    }
    // Make optional changes to the configuration based on command-line options
    optionToConf(cmd, MIN_WORKERS_OPTION,
        conf, TBoundedThreadPoolServer.MIN_WORKER_THREADS_CONF_KEY);
    optionToConf(cmd, MAX_WORKERS_OPTION,
        conf, TBoundedThreadPoolServer.MAX_WORKER_THREADS_CONF_KEY);
    optionToConf(cmd, MAX_QUEUE_SIZE_OPTION,
        conf, TBoundedThreadPoolServer.MAX_QUEUED_REQUESTS_CONF_KEY);
    optionToConf(cmd, KEEP_ALIVE_SEC_OPTION,
        conf, TBoundedThreadPoolServer.THREAD_KEEP_ALIVE_TIME_SEC_CONF_KEY);
    optionToConf(cmd, READ_TIMEOUT_OPTION, conf, THRIFT_SERVER_SOCKET_READ_TIMEOUT_KEY);
    optionToConf(cmd, SELECTOR_NUM_OPTION, conf, THRIFT_SELECTOR_NUM);

    // Set general thrift server options
    boolean compact = cmd.hasOption(COMPACT_OPTION) ||
        conf.getBoolean(COMPACT_CONF_KEY, false);
    conf.setBoolean(COMPACT_CONF_KEY, compact);
    boolean framed = cmd.hasOption(FRAMED_OPTION) ||
        conf.getBoolean(FRAMED_CONF_KEY, false);
    conf.setBoolean(FRAMED_CONF_KEY, framed);

    optionToConf(cmd, BIND_OPTION, conf, BIND_CONF_KEY);


    ImplType.setServerImpl(cmd, conf);
  }

  /**
   * Parse the command line options to set parameters the conf.
   */
  protected void processOptions(final String[] args) throws Exception {
    if (args == null || args.length == 0) {
      return;
    }
    Options options = new Options();
    addOptions(options);

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    if (cmd.hasOption("help")) {
      printUsageAndExit(options, 1);
    }
    parseCommandLine(cmd, options);
  }

  public void stop() {
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception ex) {
        LOG.error("Failed to stop infoServer", ex);
      }
    }
    if (pauseMonitor != null) {
      pauseMonitor.stop();
    }
    if (tserver != null) {
      tserver.stop();
      tserver = null;
    }
    if (httpServer != null) {
      try {
        httpServer.stop();
        httpServer = null;
      } catch (Exception e) {
        LOG.error("Problem encountered in shutting down HTTP server", e);
      }
      httpServer = null;
    }
  }

  protected static void optionToConf(CommandLine cmd, String option,
      Configuration conf, String destConfKey) {
    if (cmd.hasOption(option)) {
      String value = cmd.getOptionValue(option);
      LOG.info("Set configuration key:" + destConfKey + " value:" + value);
      conf.set(destConfKey, value);
    }
  }

  /**
   * Run without any command line arguments
   * @return exit code
   * @throws Exception exception
   */
  public int run() throws Exception {
    return run(null);
  }

  @Override
  public int run(String[] strings) throws Exception {
    processOptions(strings);
    setupParamters();
    if (httpEnabled) {
      setupHTTPServer();
    } else {
      setupServer();
    }
    serviceUGI.doAs(new PrivilegedAction<Object>() {
      @Override
      public Object run() {
        try {
          startInfoServer();
          if (httpEnabled) {
            httpServer.start();
            httpServer.join();
          } else {
            tserver.serve();
          }
        } catch (Exception e) {
          LOG.error(HBaseMarkers.FATAL, "Cannot run ThriftServer", e);

          System.exit(-1);
        }
        return null;
      }
    });
    return 0;
  }

  public static void main(String [] args) throws Exception {
    LOG.info("***** STARTING service '" + ThriftServer.class.getSimpleName() + "' *****");
    VersionInfo.logVersion();
    final Configuration conf = HBaseConfiguration.create();
    // for now, only time we return is on an argument error.
    final int status = ToolRunner.run(conf, new ThriftServer(conf), args);
    LOG.info("***** STOPPING service '" + ThriftServer.class.getSimpleName() + "' *****");
    System.exit(status);
  }
}
