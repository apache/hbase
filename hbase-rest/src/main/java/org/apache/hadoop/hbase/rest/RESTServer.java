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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.rest.filter.AuthFilter;
import org.apache.hadoop.hbase.rest.filter.RestCsrfPreventionFilter;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.HttpServerUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.util.StringUtils;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.FilterHolder;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.spi.container.servlet.ServletContainer;

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
  static Log LOG = LogFactory.getLog("RESTServer");

  static String REST_CSRF_ENABLED_KEY = "hbase.rest.csrf.enabled";
  static boolean REST_CSRF_ENABLED_DEFAULT = false;
  static boolean restCSRFEnabled = false;
  static String REST_CSRF_CUSTOM_HEADER_KEY ="hbase.rest.csrf.custom.header";
  static String REST_CSRF_CUSTOM_HEADER_DEFAULT = "X-XSRF-HEADER";
  static String REST_CSRF_METHODS_TO_IGNORE_KEY = "hbase.rest.csrf.methods.to.ignore";
  static String REST_CSRF_METHODS_TO_IGNORE_DEFAULT = "GET,OPTIONS,HEAD,TRACE";

  private static void printUsageAndExit(Options options, int exitCode) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("bin/hbase rest start", "", options,
      "\nTo run the REST server as a daemon, execute " +
      "bin/hbase-daemon.sh start|stop rest [--infoport <port>] [-p <port>] [-ro]\n", true);
    System.exit(exitCode);
  }

  /**
   * Returns a list of strings from a comma-delimited configuration value.
   *
   * @param conf configuration to check
   * @param name configuration property name
   * @param defaultValue default value if no value found for name
   * @return list of strings from comma-delimited configuration value, or an
   *     empty list if not found
   */
  private static List<String> getTrimmedStringList(Configuration conf,
    String name, String defaultValue) {
    String valueString = conf.get(name, defaultValue);
    if (valueString == null) {
      return new ArrayList<>();
    }
    return new ArrayList<>(StringUtils.getTrimmedStringCollection(valueString));
  }

  static String REST_CSRF_BROWSER_USERAGENTS_REGEX_KEY = "hbase.rest-csrf.browser-useragents-regex";
  static void addCSRFFilter(Context context, Configuration conf) {
    restCSRFEnabled = conf.getBoolean(REST_CSRF_ENABLED_KEY, REST_CSRF_ENABLED_DEFAULT);
    if (restCSRFEnabled) {
      String[] urls = { "/*" };
      Set<String> restCsrfMethodsToIgnore = new HashSet<>();
      restCsrfMethodsToIgnore.addAll(getTrimmedStringList(conf,
        REST_CSRF_METHODS_TO_IGNORE_KEY, REST_CSRF_METHODS_TO_IGNORE_DEFAULT));
      Map<String, String> restCsrfParams = RestCsrfPreventionFilter
          .getFilterParams(conf, "hbase.rest-csrf.");
      HttpServer.defineFilter(context, "csrf", RestCsrfPreventionFilter.class.getName(),
        restCsrfParams, urls);
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
      userProvider.login(REST_KEYTAB_FILE, REST_KERBEROS_PRINCIPAL, machineName);
      if (conf.get(REST_AUTHENTICATION_TYPE) != null) {
        containerClass = RESTServletContainer.class;
        FilterHolder authFilter = new FilterHolder();
        authFilter.setClassName(AuthFilter.class.getName());
        authFilter.setName("AuthenticationFilter");
        return new Pair<FilterHolder, Class<? extends ServletContainer>>(authFilter,containerClass);
      }
    }
    return new Pair<FilterHolder, Class<? extends ServletContainer>>(null, containerClass);
  }

  private static void parseCommandLine(String[] args, RESTServlet servlet) {
    Options options = new Options();
    options.addOption("p", "port", true, "Port to bind to [default: 8080]");
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
      servlet.getConfiguration()
          .setInt("hbase.rest.port", Integer.valueOf(val));
      LOG.debug("port set to " + val);
    }

    // check if server should only process GET requests, if so override the conf
    if (commandLine != null && commandLine.hasOption("readonly")) {
      servlet.getConfiguration().setBoolean("hbase.rest.readonly", true);
      LOG.debug("readonly set to true");
    }

    // check for user-defined info server port setting, if so override the conf
    if (commandLine != null && commandLine.hasOption("infoport")) {
      String val = commandLine.getOptionValue("infoport");
      servlet.getConfiguration()
          .setInt("hbase.rest.info.port", Integer.valueOf(val));
      LOG.debug("Web UI port set to " + val);
    }

    @SuppressWarnings("unchecked")
    List<String> remainingArgs = commandLine != null ?
        commandLine.getArgList() : new ArrayList<String>();
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
   * The main method for the HBase rest server.
   * @param args command-line arguments
   * @throws Exception exception
   */
  public static void main(String[] args) throws Exception {
    VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    UserProvider userProvider = UserProvider.instantiate(conf);
    Pair<FilterHolder, Class<? extends ServletContainer>> pair = loginServerPrincipal(
      userProvider, conf);
    FilterHolder authFilter = pair.getFirst();
    Class<? extends ServletContainer> containerClass = pair.getSecond();
    RESTServlet servlet = RESTServlet.getInstance(conf, userProvider);

    parseCommandLine(args, servlet);

    // set up the Jersey servlet container for Jetty
    ServletHolder sh = new ServletHolder(containerClass);
    sh.setInitParameter(
      "com.sun.jersey.config.property.resourceConfigClass",
      ResourceConfig.class.getCanonicalName());
    sh.setInitParameter("com.sun.jersey.config.property.packages",
      "jetty");
    // The servlet holder below is instantiated to only handle the case
    // of the /status/cluster returning arrays of nodes (live/dead). Without
    // this servlet holder, the problem is that the node arrays in the response
    // are collapsed to single nodes. We want to be able to treat the
    // node lists as POJO in the response to /status/cluster servlet call,
    // but not change the behavior for any of the other servlets
    // Hence we don't use the servlet holder for all servlets / paths
    ServletHolder shPojoMap = new ServletHolder(containerClass);
    @SuppressWarnings("unchecked")
    Map<String, String> shInitMap = sh.getInitParameters();
    for (Entry<String, String> e : shInitMap.entrySet()) {
      shPojoMap.setInitParameter(e.getKey(), e.getValue());
    }
    shPojoMap.setInitParameter(JSONConfiguration.FEATURE_POJO_MAPPING, "true");

    // set up Jetty and run the embedded server

    Server server = new Server();

    Connector connector = new SelectChannelConnector();
    if(conf.getBoolean(REST_SSL_ENABLED, false)) {
      SslSelectChannelConnector sslConnector = new SslSelectChannelConnector();
      String keystore = conf.get(REST_SSL_KEYSTORE_STORE);
      String password = HBaseConfiguration.getPassword(conf,
        REST_SSL_KEYSTORE_PASSWORD, null);
      String keyPassword = HBaseConfiguration.getPassword(conf,
        REST_SSL_KEYSTORE_KEYPASSWORD, password);
      sslConnector.setKeystore(keystore);
      sslConnector.setPassword(password);
      sslConnector.setKeyPassword(keyPassword);
      connector = sslConnector;
    }
    connector.setPort(servlet.getConfiguration().getInt("hbase.rest.port", 8080));
    connector.setHost(servlet.getConfiguration().get("hbase.rest.host", "0.0.0.0"));
    connector.setHeaderBufferSize(65536);

    server.addConnector(connector);

    // Set the default max thread number to 100 to limit
    // the number of concurrent requests so that REST server doesn't OOM easily.
    // Jetty set the default max thread number to 250, if we don't set it.
    //
    // Our default min thread number 2 is the same as that used by Jetty.
    int maxThreads = servlet.getConfiguration().getInt("hbase.rest.threads.max", 100);
    int minThreads = servlet.getConfiguration().getInt("hbase.rest.threads.min", 2);
    QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads);
    threadPool.setMinThreads(minThreads);
    server.setThreadPool(threadPool);

    server.setSendServerVersion(false);
    server.setSendDateHeader(false);
    server.setStopAtShutdown(true);
      // set up context
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addServlet(shPojoMap, "/status/cluster");
    context.addServlet(sh, "/*");
    if (authFilter != null) {
      context.addFilter(authFilter, "/*", 1);
    }

    // Load filters from configuration.
    String[] filterClasses = servlet.getConfiguration().getStrings(FILTER_CLASSES,
      ArrayUtils.EMPTY_STRING_ARRAY);
    for (String filter : filterClasses) {
      filter = filter.trim();
      context.addFilter(Class.forName(filter), "/*", 0);
    }
    addCSRFFilter(context, conf);
    HttpServerUtil.constrainHttpMethods(context);

    // Put up info server.
    int port = conf.getInt("hbase.rest.info.port", 8085);
    if (port >= 0) {
      conf.setLong("startcode", System.currentTimeMillis());
      String a = conf.get("hbase.rest.info.bindAddress", "0.0.0.0");
      InfoServer infoServer = new InfoServer("rest", a, port, false, conf);
      infoServer.setAttribute("hbase.conf", conf);
      infoServer.start();
    }
    // start server
    server.start();
    server.join();
  }
}
