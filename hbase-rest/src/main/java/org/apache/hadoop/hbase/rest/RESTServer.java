/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.rest;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.EnumSet;
import java.util.concurrent.ArrayBlockingQueue;

import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.rest.filter.AuthFilter;
import org.apache.hadoop.hbase.rest.filter.GzipFilter;
import org.apache.hadoop.hbase.rest.filter.RestCsrfPreventionFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.http.HttpServerUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.VersionInfo;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;
import org.apache.hbase.thirdparty.org.apache.commons.cli.HelpFormatter;
import org.apache.hbase.thirdparty.org.apache.commons.cli.Options;
import org.apache.hbase.thirdparty.org.apache.commons.cli.ParseException;
import org.apache.hbase.thirdparty.org.apache.commons.cli.PosixParser;

import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.jmx.MBeanContainer;
import org.eclipse.jetty.servlet.FilterHolder;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.DispatcherType;

/**
 * Main class for launching REST gateway as a servlet hosted by Jetty.
 * <p>
 * The following options are supported:
 * <ul>
 * <li>-p --port : service port</li>
 * <li>-ro --readonly : server mode</li>
 * </ul>
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class RESTServer implements Constants {
  static Logger LOG = LoggerFactory.getLogger("RESTServer");

  static final String REST_CSRF_ENABLED_KEY = "hbase.rest.csrf.enabled";
  static final boolean REST_CSRF_ENABLED_DEFAULT = false;
  boolean restCSRFEnabled = false;
  static final String REST_CSRF_CUSTOM_HEADER_KEY ="hbase.rest.csrf.custom.header";
  static final String REST_CSRF_CUSTOM_HEADER_DEFAULT = "X-XSRF-HEADER";
  static final String REST_CSRF_METHODS_TO_IGNORE_KEY = "hbase.rest.csrf.methods.to.ignore";
  static final String REST_CSRF_METHODS_TO_IGNORE_DEFAULT = "GET,OPTIONS,HEAD,TRACE";
  public static final String SKIP_LOGIN_KEY = "hbase.rest.skip.login";

  private static final String PATH_SPEC_ANY = "/*";

  // HTTP OPTIONS method is commonly used in REST APIs for negotiation. It is disabled by default to
  // maintain backward incompatibility
  static final String REST_HTTP_ALLOW_OPTIONS_METHOD = "hbase.rest.http.allow.options.method";
  private static boolean REST_HTTP_ALLOW_OPTIONS_METHOD_DEFAULT = false;
  static final String REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY =
    "hbase.rest-csrf.browser-useragents-regex";

  // HACK, making this static for AuthFilter to get at our configuration. Necessary for unit tests.
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value={"ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD", "MS_CANNOT_BE_FINAL"},
    justification="For testing")
  public static Configuration conf = null;
  private final UserProvider userProvider;
  private Server server;
  private InfoServer infoServer;

  public RESTServer(Configuration conf) {
    RESTServer.conf = conf;
    this.userProvider = UserProvider.instantiate(conf);
  }

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("hbase rest start", "", options,
      "\nTo run the REST server as a daemon, execute " +
      "hbase-daemon.sh start|stop rest [--infoport <port>] [-p <port>] [-ro]\n", true);
    System.exit(exitCode);
  }

  void addCSRFFilter(ServletContextHandler ctxHandler, Configuration conf) {
    restCSRFEnabled = conf.getBoolean(REST_CSRF_ENABLED_KEY, REST_CSRF_ENABLED_DEFAULT);
    if (restCSRFEnabled) {
      Map<String, String> restCsrfParams = RestCsrfPreventionFilter
          .getFilterParams(conf, "hbase.rest-csrf.");
      FilterHolder holder = new FilterHolder();
      holder.setName("csrf");
      holder.setClassName(RestCsrfPreventionFilter.class.getName());
      holder.setInitParameters(restCsrfParams);
      ctxHandler.addFilter(holder, PATH_SPEC_ANY, EnumSet.allOf(DispatcherType.class));
    }
  }

  // login the server principal (if using secure Hadoop)
  private static Pair<FilterHolder, Class<? extends ServletContainer>> loginServerPrincipal(
    UserProvider userProvider, Configuration conf) throws Exception {
    Class<? extends ServletContainer> containerClass = ServletContainer.class;
    if (userProvider.isHadoopSecurityEnabled() && userProvider.isHBaseSecurityEnabled()) {
      String machineName = Strings.domainNamePointerToHostName(
        DNS.getDefaultHost(conf.get(REST_DNS_INTERFACE, "default"),
          conf.get(REST_DNS_NAMESERVER, "default")));
      String keytabFilename = conf.get(REST_KEYTAB_FILE);
      Preconditions.checkArgument(keytabFilename != null && !keytabFilename.isEmpty(),
        REST_KEYTAB_FILE + " should be set if security is enabled");
      String principalConfig = conf.get(REST_KERBEROS_PRINCIPAL);
      Preconditions.checkArgument(principalConfig != null && !principalConfig.isEmpty(),
        REST_KERBEROS_PRINCIPAL + " should be set if security is enabled");
      // Hook for unit tests, this will log out any other user and mess up tests.
      if (!conf.getBoolean(SKIP_LOGIN_KEY, false)) {
        userProvider.login(REST_KEYTAB_FILE, REST_KERBEROS_PRINCIPAL, machineName);
      }
      if (conf.get(REST_AUTHENTICATION_TYPE) != null) {
        containerClass = RESTServletContainer.class;
        FilterHolder authFilter = new FilterHolder();
        authFilter.setClassName(AuthFilter.class.getName());
        authFilter.setName("AuthenticationFilter");
        return new Pair<>(authFilter,containerClass);
      }
    }
    return new Pair<>(null, containerClass);
  }

  private static void parseCommandLine(String[] args, Configuration conf) {
    Options options = new Options();
    options.addOption("p", "port", true, "Port to bind to [default: " + DEFAULT_LISTEN_PORT + "]");
    options.addOption("ro", "readonly", false, "Respond only to GET HTTP " +
      "method requests [default: false]");
    options.addOption(null, "infoport", true, "Port for web UI");

    CommandLine commandLine = null;
    try {
      commandLine = new PosixParser().parse(options, args);
    } catch (ParseException e) {
      LOG.error("Could not parse: ", e);
      printUsageAndExit(options, -1);
    }

    // check for user-defined port setting, if so override the conf
    if (commandLine != null && commandLine.hasOption("port")) {
      String val = commandLine.getOptionValue("port");
      conf.setInt("hbase.rest.port", Integer.parseInt(val));
      if (LOG.isDebugEnabled()) {
        LOG.debug("port set to " + val);
      }
    }

    // check if server should only process GET requests, if so override the conf
    if (commandLine != null && commandLine.hasOption("readonly")) {
      conf.setBoolean("hbase.rest.readonly", true);
      if (LOG.isDebugEnabled()) {
        LOG.debug("readonly set to true");
      }
    }

    // check for user-defined info server port setting, if so override the conf
    if (commandLine != null && commandLine.hasOption("infoport")) {
      String val = commandLine.getOptionValue("infoport");
      conf.setInt("hbase.rest.info.port", Integer.parseInt(val));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Web UI port set to " + val);
      }
    }

    if (commandLine != null && commandLine.hasOption("skipLogin")) {
      conf.setBoolean(SKIP_LOGIN_KEY, true);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Skipping Kerberos login for REST server");
      }
    }

    List<String> remainingArgs = commandLine != null ? commandLine.getArgList() : new ArrayList<>();
    if (remainingArgs.size() != 1) {
      printUsageAndExit(options, 1);
    }

    String command = remainingArgs.get(0);
    if ("start".equals(command)) {
      // continue and start container
    } else if ("stop".equals(command)) {
      System.exit(1);
    } else {
      printUsageAndExit(options, 1);
    }
  }


  /**
   * Runs the REST server.
   */
  public synchronized void run() throws Exception {
    Pair<FilterHolder, Class<? extends ServletContainer>> pair = loginServerPrincipal(
      userProvider, conf);
    FilterHolder authFilter = pair.getFirst();
    Class<? extends ServletContainer> containerClass = pair.getSecond();
    RESTServlet servlet = RESTServlet.getInstance(conf, userProvider);


    // Set up the Jersey servlet container for Jetty
    // The Jackson1Feature is a signal to Jersey that it should use jackson doing json.
    // See here: https://stackoverflow.com/questions/39458230/how-register-jacksonfeature-on-clientconfig
    ResourceConfig application = new ResourceConfig().
        packages("org.apache.hadoop.hbase.rest").register(JacksonJaxbJsonProvider.class);
    // Using our custom ServletContainer is tremendously important. This is what makes sure the
    // UGI.doAs() is done for the remoteUser, and calls are not made as the REST server itself.
    ServletContainer servletContainer = ReflectionUtils.newInstance(containerClass, application);
    ServletHolder sh = new ServletHolder(servletContainer);

    // Set the default max thread number to 100 to limit
    // the number of concurrent requests so that REST server doesn't OOM easily.
    // Jetty set the default max thread number to 250, if we don't set it.
    //
    // Our default min thread number 2 is the same as that used by Jetty.
    int maxThreads = servlet.getConfiguration().getInt(REST_THREAD_POOL_THREADS_MAX, 100);
    int minThreads = servlet.getConfiguration().getInt(REST_THREAD_POOL_THREADS_MIN, 2);
    // Use the default queue (unbounded with Jetty 9.3) if the queue size is negative, otherwise use
    // bounded {@link ArrayBlockingQueue} with the given size
    int queueSize = servlet.getConfiguration().getInt(REST_THREAD_POOL_TASK_QUEUE_SIZE, -1);
    int idleTimeout = servlet.getConfiguration().getInt(REST_THREAD_POOL_THREAD_IDLE_TIMEOUT, 60000);
    QueuedThreadPool threadPool = queueSize > 0 ?
        new QueuedThreadPool(maxThreads, minThreads, idleTimeout, new ArrayBlockingQueue<>(queueSize)) :
        new QueuedThreadPool(maxThreads, minThreads, idleTimeout);

    this.server = new Server(threadPool);

    // Setup JMX
    MBeanContainer mbContainer=new MBeanContainer(ManagementFactory.getPlatformMBeanServer());
    server.addEventListener(mbContainer);
    server.addBean(mbContainer);


    String host = servlet.getConfiguration().get("hbase.rest.host", "0.0.0.0");
    int servicePort = servlet.getConfiguration().getInt("hbase.rest.port", 8080);
    HttpConfiguration httpConfig = new HttpConfiguration();
    httpConfig.setSecureScheme("https");
    httpConfig.setSecurePort(servicePort);
    httpConfig.setSendServerVersion(false);
    httpConfig.setSendDateHeader(false);

    ServerConnector serverConnector;
    if (conf.getBoolean(REST_SSL_ENABLED, false)) {
      HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
      httpsConfig.addCustomizer(new SecureRequestCustomizer());

      SslContextFactory sslCtxFactory = new SslContextFactory();
      String keystore = conf.get(REST_SSL_KEYSTORE_STORE);
      String password = HBaseConfiguration.getPassword(conf,
          REST_SSL_KEYSTORE_PASSWORD, null);
      String keyPassword = HBaseConfiguration.getPassword(conf,
          REST_SSL_KEYSTORE_KEYPASSWORD, password);
      sslCtxFactory.setKeyStorePath(keystore);
      sslCtxFactory.setKeyStorePassword(password);
      sslCtxFactory.setKeyManagerPassword(keyPassword);

      String[] excludeCiphers = servlet.getConfiguration().getStrings(
          REST_SSL_EXCLUDE_CIPHER_SUITES, ArrayUtils.EMPTY_STRING_ARRAY);
      if (excludeCiphers.length != 0) {
        sslCtxFactory.setExcludeCipherSuites(excludeCiphers);
      }
      String[] includeCiphers = servlet.getConfiguration().getStrings(
          REST_SSL_INCLUDE_CIPHER_SUITES, ArrayUtils.EMPTY_STRING_ARRAY);
      if (includeCiphers.length != 0) {
        sslCtxFactory.setIncludeCipherSuites(includeCiphers);
      }

      String[] excludeProtocols = servlet.getConfiguration().getStrings(
          REST_SSL_EXCLUDE_PROTOCOLS, ArrayUtils.EMPTY_STRING_ARRAY);
      if (excludeProtocols.length != 0) {
        sslCtxFactory.setExcludeProtocols(excludeProtocols);
      }
      String[] includeProtocols = servlet.getConfiguration().getStrings(
          REST_SSL_INCLUDE_PROTOCOLS, ArrayUtils.EMPTY_STRING_ARRAY);
      if (includeProtocols.length != 0) {
        sslCtxFactory.setIncludeProtocols(includeProtocols);
      }

      serverConnector = new ServerConnector(server,
          new SslConnectionFactory(sslCtxFactory, HttpVersion.HTTP_1_1.toString()),
          new HttpConnectionFactory(httpsConfig));
    } else {
      serverConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
    }

    int acceptQueueSize = servlet.getConfiguration().getInt(REST_CONNECTOR_ACCEPT_QUEUE_SIZE, -1);
    if (acceptQueueSize >= 0) {
      serverConnector.setAcceptQueueSize(acceptQueueSize);
    }

    serverConnector.setPort(servicePort);
    serverConnector.setHost(host);

    server.addConnector(serverConnector);
    server.setStopAtShutdown(true);

    // set up context
    ServletContextHandler ctxHandler = new ServletContextHandler(server, "/", ServletContextHandler.SESSIONS);
    ctxHandler.addServlet(sh, PATH_SPEC_ANY);
    if (authFilter != null) {
      ctxHandler.addFilter(authFilter, PATH_SPEC_ANY, EnumSet.of(DispatcherType.REQUEST));
    }

    // Load filters from configuration.
    String[] filterClasses = servlet.getConfiguration().getStrings(FILTER_CLASSES,
        GzipFilter.class.getName());
    for (String filter : filterClasses) {
      filter = filter.trim();
      ctxHandler.addFilter(filter, PATH_SPEC_ANY, EnumSet.of(DispatcherType.REQUEST));
    }
    addCSRFFilter(ctxHandler, conf);
    HttpServerUtil.constrainHttpMethods(ctxHandler, servlet.getConfiguration()
        .getBoolean(REST_HTTP_ALLOW_OPTIONS_METHOD, REST_HTTP_ALLOW_OPTIONS_METHOD_DEFAULT));

    // Put up info server.
    int port = conf.getInt("hbase.rest.info.port", 8085);
    if (port >= 0) {
      conf.setLong("startcode", System.currentTimeMillis());
      String a = conf.get("hbase.rest.info.bindAddress", "0.0.0.0");
      this.infoServer = new InfoServer("rest", a, port, false, conf);
      this.infoServer.setAttribute("hbase.conf", conf);
      this.infoServer.start();
    }
    try {
      // start server
      server.start();
    } catch (Exception e) {
      LOG.error(HBaseMarkers.FATAL, "Failed to start server", e);
      throw e;
    }
  }

  public synchronized void join() throws Exception {
    if (server == null) {
      throw new IllegalStateException("Server is not running");
    }
    server.join();
  }

  public synchronized void stop() throws Exception {
    if (server == null) {
      throw new IllegalStateException("Server is not running");
    }
    server.stop();
    server = null;
    RESTServlet.stop();
  }

  public synchronized int getPort() {
    if (server == null) {
      throw new IllegalStateException("Server is not running");
    }
    return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
  }

  @SuppressWarnings("deprecation")
  public synchronized int getInfoPort() {
    if (infoServer == null) {
      throw new IllegalStateException("InfoServer is not running");
    }
    return infoServer.getPort();
  }

  public Configuration getConf() {
    return conf;
  }

  /**
   * The main method for the HBase rest server.
   * @param args command-line arguments
   * @throws Exception exception
   */
  public static void main(String[] args) throws Exception {
    LOG.info("***** STARTING service '" + RESTServer.class.getSimpleName() + "' *****");
    VersionInfo.logVersion();
    final Configuration conf = HBaseConfiguration.create();
    parseCommandLine(args, conf);
    RESTServer server = new RESTServer(conf);

    try {
      server.run();
      server.join();
    } catch (Exception e) {
      System.exit(1);
    }

    LOG.info("***** STOPPING service '" + RESTServer.class.getSimpleName() + "' *****");
  }
}
