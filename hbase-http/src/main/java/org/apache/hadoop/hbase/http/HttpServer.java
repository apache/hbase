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
package org.apache.hadoop.hbase.http;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintStream;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.http.conf.ConfServlet;
import org.apache.hadoop.hbase.http.jmx.JMXJsonServlet;
import org.apache.hadoop.hbase.http.log.LogLevel;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.org.eclipse.jetty.http.HttpVersion;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Handler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.HttpConfiguration;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.HttpConnectionFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.RequestLog;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.SecureRequestCustomizer;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.Server;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.ServerConnector;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.SslConnectionFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.handler.HandlerCollection;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.handler.RequestLogHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.DefaultServlet;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.FilterHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.FilterMapping;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletContextHandler;
import org.apache.hbase.thirdparty.org.eclipse.jetty.servlet.ServletHolder;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.MultiException;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.ssl.SslContextFactory;
import org.apache.hbase.thirdparty.org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.apache.hbase.thirdparty.org.eclipse.jetty.webapp.WebAppContext;
import org.apache.hbase.thirdparty.org.glassfish.jersey.server.ResourceConfig;
import org.apache.hbase.thirdparty.org.glassfish.jersey.servlet.ServletContainer;

/**
 * Create a Jetty embedded server to answer http requests. The primary goal
 * is to serve up status information for the server.
 * There are three contexts:
 *   "/logs/" -&gt; points to the log directory
 *   "/static/" -&gt; points to common static files (src/webapps/static)
 *   "/" -&gt; the jsp server code from (src/webapps/&lt;name&gt;)
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HttpServer implements FilterContainer {
  private static final Logger LOG = LoggerFactory.getLogger(HttpServer.class);
  private static final String EMPTY_STRING = "";

  private static final int DEFAULT_MAX_HEADER_SIZE = 64 * 1024; // 64K

  static final String FILTER_INITIALIZERS_PROPERTY
      = "hbase.http.filter.initializers";
  static final String HTTP_MAX_THREADS = "hbase.http.max.threads";

  public static final String HTTP_UI_AUTHENTICATION = "hbase.security.authentication.ui";
  static final String HTTP_AUTHENTICATION_PREFIX = "hbase.security.authentication.";
  static final String HTTP_SPNEGO_AUTHENTICATION_PREFIX = HTTP_AUTHENTICATION_PREFIX
      + "spnego.";
  static final String HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_SUFFIX = "kerberos.principal";
  public static final String HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_KEY =
      HTTP_SPNEGO_AUTHENTICATION_PREFIX + HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_SUFFIX;
  static final String HTTP_SPNEGO_AUTHENTICATION_KEYTAB_SUFFIX = "kerberos.keytab";
  public static final String HTTP_SPNEGO_AUTHENTICATION_KEYTAB_KEY =
      HTTP_SPNEGO_AUTHENTICATION_PREFIX + HTTP_SPNEGO_AUTHENTICATION_KEYTAB_SUFFIX;
  static final String HTTP_SPNEGO_AUTHENTICATION_KRB_NAME_SUFFIX = "kerberos.name.rules";
  public static final String HTTP_SPNEGO_AUTHENTICATION_KRB_NAME_KEY =
      HTTP_SPNEGO_AUTHENTICATION_PREFIX + HTTP_SPNEGO_AUTHENTICATION_KRB_NAME_SUFFIX;
  static final String HTTP_SPNEGO_AUTHENTICATION_PROXYUSER_ENABLE_SUFFIX = "kerberos.proxyuser.enable";
  public static final String HTTP_SPNEGO_AUTHENTICATION_PROXYUSER_ENABLE_KEY =
      HTTP_SPNEGO_AUTHENTICATION_PREFIX + HTTP_SPNEGO_AUTHENTICATION_PROXYUSER_ENABLE_SUFFIX;
  public static final boolean  HTTP_SPNEGO_AUTHENTICATION_PROXYUSER_ENABLE_DEFAULT = false;
  static final String HTTP_AUTHENTICATION_SIGNATURE_SECRET_FILE_SUFFIX =
      "signature.secret.file";
  public static final String HTTP_AUTHENTICATION_SIGNATURE_SECRET_FILE_KEY =
      HTTP_AUTHENTICATION_PREFIX + HTTP_AUTHENTICATION_SIGNATURE_SECRET_FILE_SUFFIX;
  public static final String HTTP_SPNEGO_AUTHENTICATION_ADMIN_USERS_KEY =
      HTTP_SPNEGO_AUTHENTICATION_PREFIX + "admin.users";
  public static final String HTTP_SPNEGO_AUTHENTICATION_ADMIN_GROUPS_KEY =
      HTTP_SPNEGO_AUTHENTICATION_PREFIX + "admin.groups";
  public static final String HTTP_PRIVILEGED_CONF_KEY =
      "hbase.security.authentication.ui.config.protected";
  public static final boolean HTTP_PRIVILEGED_CONF_DEFAULT = false;

  // The ServletContext attribute where the daemon Configuration
  // gets stored.
  public static final String CONF_CONTEXT_ATTRIBUTE = "hbase.conf";
  public static final String ADMINS_ACL = "admins.acl";
  public static final String BIND_ADDRESS = "bind.address";
  public static final String SPNEGO_FILTER = "SpnegoFilter";
  public static final String SPNEGO_PROXYUSER_FILTER = "SpnegoProxyUserFilter";
  public static final String NO_CACHE_FILTER = "NoCacheFilter";
  public static final String APP_DIR = "webapps";

  private final AccessControlList adminsAcl;

  protected final Server webServer;
  protected String appDir;
  protected String logDir;

  private static final class ListenerInfo {
    /**
     * Boolean flag to determine whether the HTTP server should clean up the
     * listener in stop().
     */
    private final boolean isManaged;
    private final ServerConnector listener;
    private ListenerInfo(boolean isManaged, ServerConnector listener) {
      this.isManaged = isManaged;
      this.listener = listener;
    }
  }

  private final List<ListenerInfo> listeners = Lists.newArrayList();

  public List<ServerConnector> getServerConnectors() {
    return listeners.stream().map(info -> info.listener).collect(Collectors.toList());
  }

  protected final WebAppContext webAppContext;
  protected final boolean findPort;
  protected final Map<ServletContextHandler, Boolean> defaultContexts = new HashMap<>();
  protected final List<String> filterNames = new ArrayList<>();
  protected final boolean authenticationEnabled;
  static final String STATE_DESCRIPTION_ALIVE = " - alive";
  static final String STATE_DESCRIPTION_NOT_LIVE = " - not live";

  /**
   * Class to construct instances of HTTP server with specific options.
   */
  public static class Builder {
    private ArrayList<URI> endpoints = Lists.newArrayList();
    private Configuration conf;
    private String[] pathSpecs;
    private AccessControlList adminsAcl;
    private boolean securityEnabled = false;
    private String usernameConfKey;
    private String keytabConfKey;
    private boolean needsClientAuth;
    private String excludeCiphers;

    private String hostName;
    private String appDir = APP_DIR;
    private String logDir;
    private boolean findPort;

    private String trustStore;
    private String trustStorePassword;
    private String trustStoreType;

    private String keyStore;
    private String keyStorePassword;
    private String keyStoreType;

    // The -keypass option in keytool
    private String keyPassword;

    private String kerberosNameRulesKey;
    private String signatureSecretFileKey;

    /**
     * @see #setAppDir(String)
     * @deprecated Since 0.99.0. Use builder pattern via {@link #setAppDir(String)} instead.
     */
    @Deprecated
    private String name;
    /**
     * @see #addEndpoint(URI)
     * @deprecated Since 0.99.0. Use builder pattern via {@link #addEndpoint(URI)} instead.
     */
    @Deprecated
    private String bindAddress;
    /**
     * @see #addEndpoint(URI)
     * @deprecated Since 0.99.0. Use builder pattern via {@link #addEndpoint(URI)} instead.
     */
    @Deprecated
    private int port = -1;

    /**
     * Add an endpoint that the HTTP server should listen to.
     *
     * @param endpoint
     *          the endpoint of that the HTTP server should listen to. The
     *          scheme specifies the protocol (i.e. HTTP / HTTPS), the host
     *          specifies the binding address, and the port specifies the
     *          listening port. Unspecified or zero port means that the server
     *          can listen to any port.
     */
    public Builder addEndpoint(URI endpoint) {
      endpoints.add(endpoint);
      return this;
    }

    /**
     * Set the hostname of the http server. The host name is used to resolve the
     * _HOST field in Kerberos principals. The hostname of the first listener
     * will be used if the name is unspecified.
     */
    public Builder hostName(String hostName) {
      this.hostName = hostName;
      return this;
    }

    public Builder trustStore(String location, String password, String type) {
      this.trustStore = location;
      this.trustStorePassword = password;
      this.trustStoreType = type;
      return this;
    }

    public Builder keyStore(String location, String password, String type) {
      this.keyStore = location;
      this.keyStorePassword = password;
      this.keyStoreType = type;
      return this;
    }

    public Builder keyPassword(String password) {
      this.keyPassword = password;
      return this;
    }

    /**
     * Specify whether the server should authorize the client in SSL
     * connections.
     */
    public Builder needsClientAuth(boolean value) {
      this.needsClientAuth = value;
      return this;
    }

    /**
     * @see #setAppDir(String)
     * @deprecated Since 0.99.0. Use {@link #setAppDir(String)} instead.
     */
    @Deprecated
    public Builder setName(String name){
      this.name = name;
      return this;
    }

    /**
     * @see #addEndpoint(URI)
     * @deprecated Since 0.99.0. Use {@link #addEndpoint(URI)} instead.
     */
    @Deprecated
    public Builder setBindAddress(String bindAddress){
      this.bindAddress = bindAddress;
      return this;
    }

    /**
     * @see #addEndpoint(URI)
     * @deprecated Since 0.99.0. Use {@link #addEndpoint(URI)} instead.
     */
    @Deprecated
    public Builder setPort(int port) {
      this.port = port;
      return this;
    }

    public Builder setFindPort(boolean findPort) {
      this.findPort = findPort;
      return this;
    }

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public Builder setPathSpec(String[] pathSpec) {
      this.pathSpecs = pathSpec;
      return this;
    }

    public Builder setACL(AccessControlList acl) {
      this.adminsAcl = acl;
      return this;
    }

    public Builder setSecurityEnabled(boolean securityEnabled) {
      this.securityEnabled = securityEnabled;
      return this;
    }

    public Builder setUsernameConfKey(String usernameConfKey) {
      this.usernameConfKey = usernameConfKey;
      return this;
    }

    public Builder setKeytabConfKey(String keytabConfKey) {
      this.keytabConfKey = keytabConfKey;
      return this;
    }

    public Builder setKerberosNameRulesKey(String kerberosNameRulesKey) {
      this.kerberosNameRulesKey = kerberosNameRulesKey;
      return this;
    }

    public Builder setSignatureSecretFileKey(String signatureSecretFileKey) {
      this.signatureSecretFileKey = signatureSecretFileKey;
      return this;
    }

    public Builder setAppDir(String appDir) {
      this.appDir = appDir;
      return this;
    }

    public Builder setLogDir(String logDir) {
      this.logDir = logDir;
      return this;
    }

    public void excludeCiphers(String excludeCiphers) {
      this.excludeCiphers = excludeCiphers;
    }

    public HttpServer build() throws IOException {

      // Do we still need to assert this non null name if it is deprecated?
      if (this.name == null) {
        throw new HadoopIllegalArgumentException("name is not set");
      }

      // Make the behavior compatible with deprecated interfaces
      if (bindAddress != null && port != -1) {
        try {
          endpoints.add(0, new URI("http", "", bindAddress, port, "", "", ""));
        } catch (URISyntaxException e) {
          throw new HadoopIllegalArgumentException("Invalid endpoint: "+ e);
        }
      }

      if (endpoints.isEmpty()) {
        throw new HadoopIllegalArgumentException("No endpoints specified");
      }

      if (hostName == null) {
        hostName = endpoints.get(0).getHost();
      }

      if (this.conf == null) {
        conf = new Configuration();
      }

      HttpServer server = new HttpServer(this);

      for (URI ep : endpoints) {
        ServerConnector listener = null;
        String scheme = ep.getScheme();
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setHeaderCacheSize(DEFAULT_MAX_HEADER_SIZE);
        httpConfig.setResponseHeaderSize(DEFAULT_MAX_HEADER_SIZE);
        httpConfig.setRequestHeaderSize(DEFAULT_MAX_HEADER_SIZE);
        httpConfig.setSendServerVersion(false);

        if ("http".equals(scheme)) {
          listener = new ServerConnector(server.webServer, new HttpConnectionFactory(httpConfig));
        } else if ("https".equals(scheme)) {
          HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
          httpsConfig.addCustomizer(new SecureRequestCustomizer());
          SslContextFactory sslCtxFactory = new SslContextFactory();
          sslCtxFactory.setNeedClientAuth(needsClientAuth);
          sslCtxFactory.setKeyManagerPassword(keyPassword);

          if (keyStore != null) {
            sslCtxFactory.setKeyStorePath(keyStore);
            sslCtxFactory.setKeyStoreType(keyStoreType);
            sslCtxFactory.setKeyStorePassword(keyStorePassword);
          }

          if (trustStore != null) {
            sslCtxFactory.setTrustStorePath(trustStore);
            sslCtxFactory.setTrustStoreType(trustStoreType);
            sslCtxFactory.setTrustStorePassword(trustStorePassword);
          }

          if (excludeCiphers != null && !excludeCiphers.trim().isEmpty()) {
            sslCtxFactory.setExcludeCipherSuites(StringUtils.getTrimmedStrings(excludeCiphers));
            LOG.debug("Excluded SSL Cipher List:" + excludeCiphers);
          }

          listener = new ServerConnector(server.webServer, new SslConnectionFactory(sslCtxFactory,
              HttpVersion.HTTP_1_1.toString()), new HttpConnectionFactory(httpsConfig));
        } else {
          throw new HadoopIllegalArgumentException(
              "unknown scheme for endpoint:" + ep);
        }

        // default settings for connector
        listener.setAcceptQueueSize(128);
        if (Shell.WINDOWS) {
          // result of setting the SO_REUSEADDR flag is different on Windows
          // http://msdn.microsoft.com/en-us/library/ms740621(v=vs.85).aspx
          // without this 2 NN's can start on the same machine and listen on
          // the same port with indeterminate routing of incoming requests to them
          listener.setReuseAddress(false);
        }

        listener.setHost(ep.getHost());
        listener.setPort(ep.getPort() == -1 ? 0 : ep.getPort());
        server.addManagedListener(listener);
      }

      server.loadListeners();
      return server;

    }

  }

  /**
   * @see #HttpServer(String, String, int, boolean, Configuration)
   * @deprecated Since 0.99.0
   */
  @Deprecated
  public HttpServer(String name, String bindAddress, int port, boolean findPort)
          throws IOException {
    this(name, bindAddress, port, findPort, new Configuration());
  }

  /**
   * Create a status server on the given port. Allows you to specify the
   * path specifications that this server will be serving so that they will be
   * added to the filters properly.
   *
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and
   *        increment by 1 until it finds a free port.
   * @param conf Configuration
   * @param pathSpecs Path specifications that this httpserver will be serving.
   *        These will be added to any filters.
   * @deprecated Since 0.99.0
   */
  @Deprecated
  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, Configuration conf, String[] pathSpecs) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, pathSpecs);
  }

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/&lt;name&gt;.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and
   *        increment by 1 until it finds a free port.
   * @param conf Configuration
   * @deprecated Since 0.99.0
   */
  @Deprecated
  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, Configuration conf) throws IOException {
    this(name, bindAddress, port, findPort, conf, null, null);
  }

  /**
   * Creates a status server on the given port. The JSP scripts are taken
   * from src/webapp&lt;name&gt;.
   *
   * @param name the name of the server
   * @param bindAddress the address for this server
   * @param port the port to use on the server
   * @param findPort whether the server should start at the given port and increment by 1 until it
   *                 finds a free port
   * @param conf the configuration to use
   * @param adminsAcl {@link AccessControlList} of the admins
   * @throws IOException when creating the server fails
   * @deprecated Since 0.99.0
   */
  @Deprecated
  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, Configuration conf, AccessControlList adminsAcl)
      throws IOException {
    this(name, bindAddress, port, findPort, conf, adminsAcl, null);
  }

  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/&lt;name&gt;.
   * @param name The name of the server
   * @param bindAddress The address for this server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and
   *        increment by 1 until it finds a free port.
   * @param conf Configuration
   * @param adminsAcl {@link AccessControlList} of the admins
   * @param pathSpecs Path specifications that this httpserver will be serving.
   *        These will be added to any filters.
   * @deprecated Since 0.99.0
   */
  @Deprecated
  public HttpServer(String name, String bindAddress, int port,
      boolean findPort, Configuration conf, AccessControlList adminsAcl,
      String[] pathSpecs) throws IOException {
    this(new Builder().setName(name)
        .addEndpoint(URI.create("http://" + bindAddress + ":" + port))
        .setFindPort(findPort).setConf(conf).setACL(adminsAcl)
        .setPathSpec(pathSpecs));
  }

  private HttpServer(final Builder b) throws IOException {
    this.appDir = b.appDir;
    this.logDir = b.logDir;
    final String appDir = getWebAppsPath(b.name);


    int maxThreads = b.conf.getInt(HTTP_MAX_THREADS, 16);
    // If HTTP_MAX_THREADS is less than or equal to 0, QueueThreadPool() will use the
    // default value (currently 200).
    QueuedThreadPool threadPool = maxThreads <= 0 ? new QueuedThreadPool()
        : new QueuedThreadPool(maxThreads);
    threadPool.setDaemon(true);
    this.webServer = new Server(threadPool);

    this.adminsAcl = b.adminsAcl;
    this.webAppContext = createWebAppContext(b.name, b.conf, adminsAcl, appDir);
    this.findPort = b.findPort;
    this.authenticationEnabled = b.securityEnabled;
    initializeWebServer(b.name, b.hostName, b.conf, b.pathSpecs, b);
    this.webServer.setHandler(buildGzipHandler(this.webServer.getHandler()));
  }

  private void initializeWebServer(String name, String hostName,
      Configuration conf, String[] pathSpecs, HttpServer.Builder b)
      throws FileNotFoundException, IOException {

    Preconditions.checkNotNull(webAppContext);

    HandlerCollection handlerCollection = new HandlerCollection();

    ContextHandlerCollection contexts = new ContextHandlerCollection();
    RequestLog requestLog = HttpRequestLog.getRequestLog(name);

    if (requestLog != null) {
      RequestLogHandler requestLogHandler = new RequestLogHandler();
      requestLogHandler.setRequestLog(requestLog);
      handlerCollection.addHandler(requestLogHandler);
    }

    final String appDir = getWebAppsPath(name);

    handlerCollection.addHandler(contexts);
    handlerCollection.addHandler(webAppContext);

    webServer.setHandler(handlerCollection);

    webAppContext.setAttribute(ADMINS_ACL, adminsAcl);

    // Default apps need to be set first, so that all filters are applied to them.
    // Because they're added to defaultContexts, we need them there before we start
    // adding filters
    addDefaultApps(contexts, appDir, conf);

    addGlobalFilter("safety", QuotingInputFilter.class.getName(), null);

    addGlobalFilter("clickjackingprevention",
        ClickjackingPreventionFilter.class.getName(),
        ClickjackingPreventionFilter.getDefaultParameters(conf));

    HttpConfig httpConfig = new HttpConfig(conf);

    addGlobalFilter("securityheaders",
        SecurityHeadersFilter.class.getName(),
        SecurityHeadersFilter.getDefaultParameters(conf, httpConfig.isSecure()));

    // But security needs to be enabled prior to adding the other servlets
    if (authenticationEnabled) {
      initSpnego(conf, hostName, b.usernameConfKey, b.keytabConfKey, b.kerberosNameRulesKey,
          b.signatureSecretFileKey);
    }

    final FilterInitializer[] initializers = getFilterInitializers(conf);
    if (initializers != null) {
      conf = new Configuration(conf);
      conf.set(BIND_ADDRESS, hostName);
      for (FilterInitializer c : initializers) {
        c.initFilter(this, conf);
      }
    }

    addDefaultServlets(contexts, conf);

    if (pathSpecs != null) {
      for (String path : pathSpecs) {
        LOG.info("adding path spec: " + path);
        addFilterPathMapping(path, webAppContext);
      }
    }
  }

  private void addManagedListener(ServerConnector connector) {
    listeners.add(new ListenerInfo(true, connector));
  }

  private static WebAppContext createWebAppContext(String name,
      Configuration conf, AccessControlList adminsAcl, final String appDir) {
    WebAppContext ctx = new WebAppContext();
    ctx.setDisplayName(name);
    ctx.setContextPath("/");
    ctx.setWar(appDir + "/" + name);
    ctx.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    // for org.apache.hadoop.metrics.MetricsServlet
    ctx.getServletContext().setAttribute(
      org.apache.hadoop.http.HttpServer2.CONF_CONTEXT_ATTRIBUTE, conf);
    ctx.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
    addNoCacheFilter(ctx);
    return ctx;
  }

  /**
   * Construct and configure an instance of {@link GzipHandler}. With complex
   * multi-{@link WebAppContext} configurations, it's easiest to apply this handler directly to the
   * instance of {@link Server} near the end of its configuration, something like
   * <pre>
   *    Server server = new Server();
   *    //...
   *    server.setHandler(buildGzipHandler(server.getHandler()));
   *    server.start();
   * </pre>
   */
  public static GzipHandler buildGzipHandler(final Handler wrapped) {
    final GzipHandler gzipHandler = new GzipHandler();
    gzipHandler.setHandler(wrapped);
    return gzipHandler;
  }

  private static void addNoCacheFilter(WebAppContext ctxt) {
    defineFilter(ctxt, NO_CACHE_FILTER, NoCacheFilter.class.getName(),
        Collections.<String, String> emptyMap(), new String[] { "/*" });
  }

  /** Get an array of FilterConfiguration specified in the conf */
  private static FilterInitializer[] getFilterInitializers(Configuration conf) {
    if (conf == null) {
      return null;
    }

    Class<?>[] classes = conf.getClasses(FILTER_INITIALIZERS_PROPERTY);
    if (classes == null) {
      return null;
    }

    FilterInitializer[] initializers = new FilterInitializer[classes.length];
    for(int i = 0; i < classes.length; i++) {
      initializers[i] = (FilterInitializer)ReflectionUtils.newInstance(classes[i]);
    }
    return initializers;
  }

  /**
   * Add default apps.
   * @param appDir The application directory
   */
  protected void addDefaultApps(ContextHandlerCollection parent,
      final String appDir, Configuration conf) {
    // set up the context for "/logs/" if "hadoop.log.dir" property is defined.
    String logDir = this.logDir;
    if (logDir == null) {
      logDir = System.getProperty("hadoop.log.dir");
    }
    if (logDir != null) {
      ServletContextHandler logContext = new ServletContextHandler(parent, "/logs");
      logContext.addServlet(AdminAuthorizedServlet.class, "/*");
      logContext.setResourceBase(logDir);

      if (conf.getBoolean(
          ServerConfigurationKeys.HBASE_JETTY_LOGS_SERVE_ALIASES,
          ServerConfigurationKeys.DEFAULT_HBASE_JETTY_LOGS_SERVE_ALIASES)) {
        Map<String, String> params = logContext.getInitParams();
        params.put(
            "org.mortbay.jetty.servlet.Default.aliases", "true");
      }
      logContext.setDisplayName("logs");
      setContextAttributes(logContext, conf);
      defaultContexts.put(logContext, true);
    }
    // set up the context for "/static/*"
    ServletContextHandler staticContext = new ServletContextHandler(parent, "/static");
    staticContext.setResourceBase(appDir + "/static");
    staticContext.addServlet(DefaultServlet.class, "/*");
    staticContext.setDisplayName("static");
    setContextAttributes(staticContext, conf);
    defaultContexts.put(staticContext, true);
  }

  private void setContextAttributes(ServletContextHandler context, Configuration conf) {
    context.getServletContext().setAttribute(CONF_CONTEXT_ATTRIBUTE, conf);
    context.getServletContext().setAttribute(ADMINS_ACL, adminsAcl);
  }

  /**
   * Add default servlets.
   */
  protected void addDefaultServlets(
      ContextHandlerCollection contexts, Configuration conf) throws IOException {
    // set up default servlets
    addPrivilegedServlet("stacks", "/stacks", StackServlet.class);
    addPrivilegedServlet("logLevel", "/logLevel", LogLevel.Servlet.class);
    // Hadoop3 has moved completely to metrics2, and  dropped support for Metrics v1's
    // MetricsServlet (see HADOOP-12504).  We'll using reflection to load if against hadoop2.
    // Remove when we drop support for hbase on hadoop2.x.
    try {
      Class<?> clz = Class.forName("org.apache.hadoop.metrics.MetricsServlet");
      addPrivilegedServlet("metrics", "/metrics", clz.asSubclass(HttpServlet.class));
    } catch (Exception e) {
      // do nothing
    }
    addPrivilegedServlet("jmx", "/jmx", JMXJsonServlet.class);
    // While we don't expect users to have sensitive information in their configuration, they
    // might. Give them an option to not expose the service configuration to all users.
    if (conf.getBoolean(HTTP_PRIVILEGED_CONF_KEY, HTTP_PRIVILEGED_CONF_DEFAULT)) {
      addPrivilegedServlet("conf", "/conf", ConfServlet.class);
    } else {
      addUnprivilegedServlet("conf", "/conf", ConfServlet.class);
    }
    final String asyncProfilerHome = ProfileServlet.getAsyncProfilerHome();
    if (asyncProfilerHome != null && !asyncProfilerHome.trim().isEmpty()) {
      addPrivilegedServlet("prof", "/prof", ProfileServlet.class);
      Path tmpDir = Paths.get(ProfileServlet.OUTPUT_DIR);
      if (Files.notExists(tmpDir)) {
        Files.createDirectories(tmpDir);
      }
      ServletContextHandler genCtx = new ServletContextHandler(contexts, "/prof-output-hbase");
      genCtx.addServlet(ProfileOutputServlet.class, "/*");
      genCtx.setResourceBase(tmpDir.toAbsolutePath().toString());
      genCtx.setDisplayName("prof-output-hbase");
    } else {
      addUnprivilegedServlet("prof", "/prof", ProfileServlet.DisabledServlet.class);
      LOG.info("ASYNC_PROFILER_HOME environment variable and async.profiler.home system property " +
        "not specified. Disabling /prof endpoint.");
    }
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  /**
   * Add a Jersey resource package.
   * @param packageName The Java package name containing the Jersey resource.
   * @param pathSpec The path spec for the servlet
   */
  public void addJerseyResourcePackage(final String packageName,
      final String pathSpec) {
    LOG.info("addJerseyResourcePackage: packageName=" + packageName
        + ", pathSpec=" + pathSpec);

    ResourceConfig application = new ResourceConfig().packages(packageName);
    final ServletHolder sh = new ServletHolder(new ServletContainer(application));
    webAppContext.addServlet(sh, pathSpec);
  }

  /**
   * Adds a servlet in the server that any user can access. This method differs from
   * {@link #addPrivilegedServlet(String, String, Class)} in that any authenticated user
   * can interact with the servlet added by this method.
   * @param name The name of the servlet (can be passed as null)
   * @param pathSpec The path spec for the servlet
   * @param clazz The servlet class
   */
  public void addUnprivilegedServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addServletWithAuth(name, pathSpec, clazz, false);
  }

  /**
   * Adds a servlet in the server that any user can access. This method differs from
   * {@link #addPrivilegedServlet(String, ServletHolder)} in that any authenticated user
   * can interact with the servlet added by this method.
   * @param pathSpec The path spec for the servlet
   * @param holder The servlet holder
   */
  public void addUnprivilegedServlet(String pathSpec, ServletHolder holder) {
    addServletWithAuth(pathSpec, holder, false);
  }

  /**
   * Adds a servlet in the server that only administrators can access. This method differs from
   * {@link #addUnprivilegedServlet(String, String, Class)} in that only those authenticated user
   * who are identified as administrators can interact with the servlet added by this method.
   */
  public void addPrivilegedServlet(String name, String pathSpec,
      Class<? extends HttpServlet> clazz) {
    addServletWithAuth(name, pathSpec, clazz, true);
  }

  /**
   * Adds a servlet in the server that only administrators can access. This method differs from
   * {@link #addUnprivilegedServlet(String, ServletHolder)} in that only those
   * authenticated user who are identified as administrators can interact with the servlet added by
   * this method.
   */
  public void addPrivilegedServlet(String pathSpec, ServletHolder holder) {
    addServletWithAuth(pathSpec, holder, true);
  }

  /**
   * Internal method to add a servlet to the HTTP server. Developers should not call this method
   * directly, but invoke it via {@link #addUnprivilegedServlet(String, String, Class)} or
   * {@link #addPrivilegedServlet(String, String, Class)}.
   */
  void addServletWithAuth(String name, String pathSpec,
      Class<? extends HttpServlet> clazz, boolean requireAuthz) {
    addInternalServlet(name, pathSpec, clazz, requireAuthz);
    addFilterPathMapping(pathSpec, webAppContext);
  }

  /**
   * Internal method to add a servlet to the HTTP server. Developers should not call this method
   * directly, but invoke it via {@link #addUnprivilegedServlet(String, ServletHolder)} or
   * {@link #addPrivilegedServlet(String, ServletHolder)}.
   */
  void addServletWithAuth(String pathSpec, ServletHolder holder, boolean requireAuthz) {
    addInternalServlet(pathSpec, holder, requireAuthz);
    addFilterPathMapping(pathSpec, webAppContext);
  }

  /**
   * Add an internal servlet in the server, specifying whether or not to
   * protect with Kerberos authentication.
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters (except internal Kerberos
   * filters) are not enabled.
   *
   * @param name The name of the {@link Servlet} (can be passed as null)
   * @param pathSpec The path spec for the {@link Servlet}
   * @param clazz The {@link Servlet} class
   * @param requireAuthz Require Kerberos authenticate to access servlet
   */
  void addInternalServlet(String name, String pathSpec,
    Class<? extends HttpServlet> clazz, boolean requireAuthz) {
    ServletHolder holder = new ServletHolder(clazz);
    if (name != null) {
      holder.setName(name);
    }
    addInternalServlet(pathSpec, holder, requireAuthz);
  }

  /**
   * Add an internal servlet in the server, specifying whether or not to
   * protect with Kerberos authentication.
   * Note: This method is to be used for adding servlets that facilitate
   * internal communication and not for user facing functionality. For
   * servlets added using this method, filters (except internal Kerberos
   * filters) are not enabled.
   *
   * @param pathSpec The path spec for the {@link Servlet}
   * @param holder The object providing the {@link Servlet} instance
   * @param requireAuthz Require Kerberos authenticate to access servlet
   */
  void addInternalServlet(String pathSpec, ServletHolder holder, boolean requireAuthz) {
    if (authenticationEnabled && requireAuthz) {
      FilterHolder filter = new FilterHolder(AdminAuthorizedFilter.class);
      filter.setName(AdminAuthorizedFilter.class.getSimpleName());
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setDispatches(FilterMapping.ALL);
      fmap.setFilterName(AdminAuthorizedFilter.class.getSimpleName());
      webAppContext.getServletHandler().addFilter(filter, fmap);
    }
    webAppContext.getSessionHandler().getSessionCookieConfig().setHttpOnly(true);
    webAppContext.getSessionHandler().getSessionCookieConfig().setSecure(true);
    webAppContext.addServlet(holder, pathSpec);
  }

  @Override
  public void addFilter(String name, String classname, Map<String, String> parameters) {
    final String[] USER_FACING_URLS = { "*.html", "*.jsp" };
    defineFilter(webAppContext, name, classname, parameters, USER_FACING_URLS);
    LOG.info("Added filter " + name + " (class=" + classname
        + ") to context " + webAppContext.getDisplayName());
    final String[] ALL_URLS = { "/*" };
    for (Map.Entry<ServletContextHandler, Boolean> e : defaultContexts.entrySet()) {
      if (e.getValue()) {
        ServletContextHandler handler = e.getKey();
        defineFilter(handler, name, classname, parameters, ALL_URLS);
        LOG.info("Added filter " + name + " (class=" + classname
            + ") to context " + handler.getDisplayName());
      }
    }
    filterNames.add(name);
  }

  @Override
  public void addGlobalFilter(String name, String classname, Map<String, String> parameters) {
    final String[] ALL_URLS = { "/*" };
    defineFilter(webAppContext, name, classname, parameters, ALL_URLS);
    for (ServletContextHandler ctx : defaultContexts.keySet()) {
      defineFilter(ctx, name, classname, parameters, ALL_URLS);
    }
    LOG.info("Added global filter '" + name + "' (class=" + classname + ")");
  }

  /**
   * Define a filter for a context and set up default url mappings.
   */
  public static void defineFilter(ServletContextHandler handler, String name,
      String classname, Map<String,String> parameters, String[] urls) {
    FilterHolder holder = new FilterHolder();
    holder.setName(name);
    holder.setClassName(classname);
    if (parameters != null) {
      holder.setInitParameters(parameters);
    }
    FilterMapping fmap = new FilterMapping();
    fmap.setPathSpecs(urls);
    fmap.setDispatches(FilterMapping.ALL);
    fmap.setFilterName(name);
    handler.getServletHandler().addFilter(holder, fmap);
  }

  /**
   * Add the path spec to the filter path mapping.
   * @param pathSpec The path spec
   * @param webAppCtx The WebApplicationContext to add to
   */
  protected void addFilterPathMapping(String pathSpec,
      WebAppContext webAppCtx) {
    for(String name : filterNames) {
      FilterMapping fmap = new FilterMapping();
      fmap.setPathSpec(pathSpec);
      fmap.setFilterName(name);
      fmap.setDispatches(FilterMapping.ALL);
      webAppCtx.getServletHandler().addFilterMapping(fmap);
    }
  }

  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  public WebAppContext getWebAppContext(){
    return this.webAppContext;
  }

  public String getWebAppsPath(String appName) throws FileNotFoundException {
    return getWebAppsPath(this.appDir, appName);
  }

  /**
   * Get the pathname to the webapps files.
   * @param appName eg "secondary" or "datanode"
   * @return the pathname as a URL
   * @throws FileNotFoundException if 'webapps' directory cannot be found on CLASSPATH.
   */
  protected String getWebAppsPath(String webapps, String appName) throws FileNotFoundException {
    URL url = getClass().getClassLoader().getResource(webapps + "/" + appName);

    if (url == null) {
      throw new FileNotFoundException(webapps + "/" + appName
              + " not found in CLASSPATH");
    }

    String urlString = url.toString();
    return urlString.substring(0, urlString.lastIndexOf('/'));
  }

  /**
   * Get the port that the server is on
   * @return the port
   * @deprecated Since 0.99.0
   */
  @Deprecated
  public int getPort() {
    return ((ServerConnector)webServer.getConnectors()[0]).getLocalPort();
  }

  /**
   * Get the address that corresponds to a particular connector.
   *
   * @return the corresponding address for the connector, or null if there's no
   *         such connector or the connector is not bounded.
   */
  public InetSocketAddress getConnectorAddress(int index) {
    Preconditions.checkArgument(index >= 0);

    if (index > webServer.getConnectors().length) {
      return null;
    }

    ServerConnector c = (ServerConnector)webServer.getConnectors()[index];
    if (c.getLocalPort() == -1 || c.getLocalPort() == -2) {
      // -1 if the connector has not been opened
      // -2 if it has been closed
      return null;
    }

    return new InetSocketAddress(c.getHost(), c.getLocalPort());
  }

  /**
   * Set the min, max number of worker threads (simultaneous connections).
   */
  public void setThreads(int min, int max) {
    QueuedThreadPool pool = (QueuedThreadPool) webServer.getThreadPool();
    pool.setMinThreads(min);
    pool.setMaxThreads(max);
  }

  private void initSpnego(Configuration conf, String hostName,
      String usernameConfKey, String keytabConfKey, String kerberosNameRuleKey,
      String signatureSecretKeyFileKey) throws IOException {
    Map<String, String> params = new HashMap<>();
    String principalInConf = getOrEmptyString(conf, usernameConfKey);
    if (!principalInConf.isEmpty()) {
      params.put(HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_SUFFIX, SecurityUtil.getServerPrincipal(
          principalInConf, hostName));
    }
    String httpKeytab = getOrEmptyString(conf, keytabConfKey);
    if (!httpKeytab.isEmpty()) {
      params.put(HTTP_SPNEGO_AUTHENTICATION_KEYTAB_SUFFIX, httpKeytab);
    }
    String kerberosNameRule = getOrEmptyString(conf, kerberosNameRuleKey);
    if (!kerberosNameRule.isEmpty()) {
      params.put(HTTP_SPNEGO_AUTHENTICATION_KRB_NAME_SUFFIX, kerberosNameRule);
    }
    String signatureSecretKeyFile = getOrEmptyString(conf, signatureSecretKeyFileKey);
    if (!signatureSecretKeyFile.isEmpty()) {
      params.put(HTTP_AUTHENTICATION_SIGNATURE_SECRET_FILE_SUFFIX,
          signatureSecretKeyFile);
    }
    params.put(AuthenticationFilter.AUTH_TYPE, "kerberos");

    // Verify that the required options were provided
    if (isMissing(params.get(HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_SUFFIX)) ||
            isMissing(params.get(HTTP_SPNEGO_AUTHENTICATION_KEYTAB_SUFFIX))) {
      throw new IllegalArgumentException(usernameConfKey + " and "
          + keytabConfKey + " are both required in the configuration "
          + "to enable SPNEGO/Kerberos authentication for the Web UI");
    }

    if (conf.getBoolean(HTTP_SPNEGO_AUTHENTICATION_PROXYUSER_ENABLE_KEY,
        HTTP_SPNEGO_AUTHENTICATION_PROXYUSER_ENABLE_DEFAULT)) {
        //Copy/rename standard hadoop proxyuser settings to filter
        for(Map.Entry<String, String> proxyEntry :
            conf.getPropsWithPrefix(ProxyUsers.CONF_HADOOP_PROXYUSER).entrySet()) {
            params.put(ProxyUserAuthenticationFilter.PROXYUSER_PREFIX + proxyEntry.getKey(),
                proxyEntry.getValue());
        }
        addGlobalFilter(SPNEGO_PROXYUSER_FILTER, ProxyUserAuthenticationFilter.class.getName(), params);
    } else {
        addGlobalFilter(SPNEGO_FILTER, AuthenticationFilter.class.getName(), params);
    }
  }

  /**
   * Returns true if the argument is non-null and not whitespace
   */
  private boolean isMissing(String value) {
    if (null == value) {
      return true;
    }
    return value.trim().isEmpty();
  }

  /**
   * Extracts the value for the given key from the configuration of returns a string of
   * zero length.
   */
  private String getOrEmptyString(Configuration conf, String key) {
    if (null == key) {
      return EMPTY_STRING;
    }
    final String value = conf.get(key.trim());
    return null == value ? EMPTY_STRING : value;
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    try {
      try {
        openListeners();
        webServer.start();
      } catch (IOException ex) {
        LOG.info("HttpServer.start() threw a non Bind IOException", ex);
        throw ex;
      } catch (MultiException ex) {
        LOG.info("HttpServer.start() threw a MultiException", ex);
        throw ex;
      }
      // Make sure there is no handler failures.
      Handler[] handlers = webServer.getHandlers();
      for (int i = 0; i < handlers.length; i++) {
        if (handlers[i].isFailed()) {
          throw new IOException(
              "Problem in starting http server. Server handlers failed");
        }
      }
      // Make sure there are no errors initializing the context.
      Throwable unavailableException = webAppContext.getUnavailableException();
      if (unavailableException != null) {
        // Have to stop the webserver, or else its non-daemon threads
        // will hang forever.
        webServer.stop();
        throw new IOException("Unable to initialize WebAppContext",
            unavailableException);
      }
    } catch (IOException e) {
      throw e;
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException(
          "Interrupted while starting HTTP server").initCause(e);
    } catch (Exception e) {
      throw new IOException("Problem starting http server", e);
    }
  }

  private void loadListeners() {
    for (ListenerInfo li : listeners) {
      webServer.addConnector(li.listener);
    }
  }

  /**
   * Open the main listener for the server
   * @throws Exception if the listener cannot be opened or the appropriate port is already in use
   */
  void openListeners() throws Exception {
    for (ListenerInfo li : listeners) {
      ServerConnector listener = li.listener;
      if (!li.isManaged || (li.listener.getLocalPort() != -1 && li.listener.getLocalPort() != -2)) {
        // This listener is either started externally, or has not been opened, or has been closed
        continue;
      }
      int port = listener.getPort();
      while (true) {
        // jetty has a bug where you can't reopen a listener that previously
        // failed to open w/o issuing a close first, even if the port is changed
        try {
          listener.close();
          listener.open();
          LOG.info("Jetty bound to port " + listener.getLocalPort());
          break;
        } catch (IOException ex) {
          if(!(ex instanceof BindException) && !(ex.getCause() instanceof BindException)) {
            throw ex;
          }
          if (port == 0 || !findPort) {
            BindException be = new BindException("Port in use: "
                + listener.getHost() + ":" + listener.getPort());
            be.initCause(ex);
            throw be;
          }
        }
        // try the next port number
        listener.setPort(++port);
        Thread.sleep(100);
      }
    }
  }

  /**
   * stop the server
   */
  public void stop() throws Exception {
    MultiException exception = null;
    for (ListenerInfo li : listeners) {
      if (!li.isManaged) {
        continue;
      }

      try {
        li.listener.close();
      } catch (Exception e) {
        LOG.error(
            "Error while stopping listener for webapp"
                + webAppContext.getDisplayName(), e);
        exception = addMultiException(exception, e);
      }
    }

    try {
      // clear & stop webAppContext attributes to avoid memory leaks.
      webAppContext.clearAttributes();
      webAppContext.stop();
    } catch (Exception e) {
      LOG.error("Error while stopping web app context for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    try {
      webServer.stop();
    } catch (Exception e) {
      LOG.error("Error while stopping web server for webapp "
          + webAppContext.getDisplayName(), e);
      exception = addMultiException(exception, e);
    }

    if (exception != null) {
      exception.ifExceptionThrow();
    }

  }

  private MultiException addMultiException(MultiException exception, Exception e) {
    if(exception == null){
      exception = new MultiException();
    }
    exception.add(e);
    return exception;
  }

  public void join() throws InterruptedException {
    webServer.join();
  }

  /**
   * Test for the availability of the web server
   * @return true if the web server is started, false otherwise
   */
  public boolean isAlive() {
    return webServer != null && webServer.isStarted();
  }

  /**
   * Return the host and port of the HttpServer, if live
   * @return the classname and any HTTP URL
   */
  @Override
  public String toString() {
    if (listeners.isEmpty()) {
      return "Inactive HttpServer";
    } else {
      StringBuilder sb = new StringBuilder("HttpServer (")
        .append(isAlive() ? STATE_DESCRIPTION_ALIVE :
                STATE_DESCRIPTION_NOT_LIVE).append("), listening at:");
      for (ListenerInfo li : listeners) {
        ServerConnector l = li.listener;
        sb.append(l.getHost()).append(":").append(l.getPort()).append("/,");
      }
      return sb.toString();
    }
  }

  /**
   * Checks the user has privileges to access to instrumentation servlets.
   * <p>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to FALSE
   * (default value) it always returns TRUE.
   * </p><p>
   * If <code>hadoop.security.instrumentation.requires.admin</code> is set to TRUE
   * it will check that if the current user is in the admin ACLS. If the user is
   * in the admin ACLs it returns TRUE, otherwise it returns FALSE.
   * </p>
   *
   * @param servletContext the servlet context.
   * @param request the servlet request.
   * @param response the servlet response.
   * @return TRUE/FALSE based on the logic decribed above.
   */
  public static boolean isInstrumentationAccessAllowed(
    ServletContext servletContext, HttpServletRequest request,
    HttpServletResponse response) throws IOException {
    Configuration conf =
      (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);

    boolean access = true;
    boolean adminAccess = conf.getBoolean(
      CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN,
      false);
    if (adminAccess) {
      access = hasAdministratorAccess(servletContext, request, response);
    }
    return access;
  }

  /**
   * Does the user sending the HttpServletRequest has the administrator ACLs? If
   * it isn't the case, response will be modified to send an error to the user.
   *
   * @param servletContext the {@link ServletContext} to use
   * @param request the {@link HttpServletRequest} to check
   * @param response used to send the error response if user does not have admin access.
   * @return true if admin-authorized, false otherwise
   * @throws IOException if an unauthenticated or unauthorized user tries to access the page
   */
  public static boolean hasAdministratorAccess(
      ServletContext servletContext, HttpServletRequest request,
      HttpServletResponse response) throws IOException {
    Configuration conf =
        (Configuration) servletContext.getAttribute(CONF_CONTEXT_ATTRIBUTE);
    AccessControlList acl = (AccessControlList) servletContext.getAttribute(ADMINS_ACL);

    return hasAdministratorAccess(conf, acl, request, response);
  }

  public static boolean hasAdministratorAccess(Configuration conf, AccessControlList acl,
      HttpServletRequest request, HttpServletResponse response) throws IOException {
    // If there is no authorization, anybody has administrator access.
    if (!conf.getBoolean(
        CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false)) {
      return true;
    }

    String remoteUser = request.getRemoteUser();
    if (remoteUser == null) {
      response.sendError(HttpServletResponse.SC_UNAUTHORIZED,
                         "Unauthenticated users are not " +
                         "authorized to access this page.");
      return false;
    }

    if (acl != null && !userHasAdministratorAccess(acl, remoteUser)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, "User "
          + remoteUser + " is unauthorized to access this page.");
      return false;
    }

    return true;
  }

  /**
   * Get the admin ACLs from the given ServletContext and check if the given
   * user is in the ACL.
   *
   * @param servletContext the context containing the admin ACL.
   * @param remoteUser the remote user to check for.
   * @return true if the user is present in the ACL, false if no ACL is set or
   *         the user is not present
   */
  public static boolean userHasAdministratorAccess(ServletContext servletContext,
      String remoteUser) {
    AccessControlList adminsAcl = (AccessControlList) servletContext
        .getAttribute(ADMINS_ACL);
    return userHasAdministratorAccess(adminsAcl, remoteUser);
  }

  public static boolean userHasAdministratorAccess(AccessControlList acl, String remoteUser) {
    UserGroupInformation remoteUserUGI =
        UserGroupInformation.createRemoteUser(remoteUser);
    return acl != null && acl.isUserAllowed(remoteUserUGI);
  }

  /**
   * A very simple servlet to serve up a text representation of the current
   * stack traces. It both returns the stacks to the caller and logs them.
   * Currently the stack traces are done sequentially rather than exactly the
   * same data.
   */
  public static class StackServlet extends HttpServlet {
    private static final long serialVersionUID = -6284183679759467039L;

    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
      if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(),
                                                     request, response)) {
        return;
      }
      response.setContentType("text/plain; charset=UTF-8");
      try (PrintStream out = new PrintStream(
        response.getOutputStream(), false, "UTF-8")) {
        Threads.printThreadInfo(out, "");
        out.flush();
      }
      ReflectionUtils.logThreadInfo(LOG, "jsp requested", 1);
    }
  }

  /**
   * A Servlet input filter that quotes all HTML active characters in the
   * parameter names and values. The goal is to quote the characters to make
   * all of the servlets resistant to cross-site scripting attacks.
   */
  @InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
  public static class QuotingInputFilter implements Filter {
    private FilterConfig config;

    public static class RequestQuoter extends HttpServletRequestWrapper {
      private final HttpServletRequest rawRequest;
      public RequestQuoter(HttpServletRequest rawRequest) {
        super(rawRequest);
        this.rawRequest = rawRequest;
      }

      /**
       * Return the set of parameter names, quoting each name.
       */
      @Override
      public Enumeration<String> getParameterNames() {
        return new Enumeration<String>() {
          private Enumeration<String> rawIterator =
            rawRequest.getParameterNames();
          @Override
          public boolean hasMoreElements() {
            return rawIterator.hasMoreElements();
          }

          @Override
          public String nextElement() {
            return HtmlQuoting.quoteHtmlChars(rawIterator.nextElement());
          }
        };
      }

      /**
       * Unquote the name and quote the value.
       */
      @Override
      public String getParameter(String name) {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getParameter(
                HtmlQuoting.unquoteHtmlChars(name)));
      }

      @Override
      public String[] getParameterValues(String name) {
        String unquoteName = HtmlQuoting.unquoteHtmlChars(name);
        String[] unquoteValue = rawRequest.getParameterValues(unquoteName);
        if (unquoteValue == null) {
          return null;
        }
        String[] result = new String[unquoteValue.length];
        for(int i=0; i < result.length; ++i) {
          result[i] = HtmlQuoting.quoteHtmlChars(unquoteValue[i]);
        }
        return result;
      }

      @Override
      public Map<String, String[]> getParameterMap() {
        Map<String, String[]> result = new HashMap<>();
        Map<String, String[]> raw = rawRequest.getParameterMap();
        for (Map.Entry<String,String[]> item: raw.entrySet()) {
          String[] rawValue = item.getValue();
          String[] cookedValue = new String[rawValue.length];
          for(int i=0; i< rawValue.length; ++i) {
            cookedValue[i] = HtmlQuoting.quoteHtmlChars(rawValue[i]);
          }
          result.put(HtmlQuoting.quoteHtmlChars(item.getKey()), cookedValue);
        }
        return result;
      }

      /**
       * Quote the url so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public StringBuffer getRequestURL(){
        String url = rawRequest.getRequestURL().toString();
        return new StringBuffer(HtmlQuoting.quoteHtmlChars(url));
      }

      /**
       * Quote the server name so that users specifying the HOST HTTP header
       * can't inject attacks.
       */
      @Override
      public String getServerName() {
        return HtmlQuoting.quoteHtmlChars(rawRequest.getServerName());
      }
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
      this.config = config;
    }

    @Override
    public void destroy() {
    }

    @Override
    public void doFilter(ServletRequest request,
                         ServletResponse response,
                         FilterChain chain
                         ) throws IOException, ServletException {
      HttpServletRequestWrapper quoted =
        new RequestQuoter((HttpServletRequest) request);
      HttpServletResponse httpResponse = (HttpServletResponse) response;

      String mime = inferMimeType(request);
      if (mime == null) {
        httpResponse.setContentType("text/plain; charset=utf-8");
      } else if (mime.startsWith("text/html")) {
        // HTML with unspecified encoding, we want to
        // force HTML with utf-8 encoding
        // This is to avoid the following security issue:
        // http://openmya.hacker.jp/hasegawa/security/utf7cs.html
        httpResponse.setContentType("text/html; charset=utf-8");
      } else if (mime.startsWith("application/xml")) {
        httpResponse.setContentType("text/xml; charset=utf-8");
      }
      chain.doFilter(quoted, httpResponse);
    }

    /**
     * Infer the mime type for the response based on the extension of the request
     * URI. Returns null if unknown.
     */
    private String inferMimeType(ServletRequest request) {
      String path = ((HttpServletRequest)request).getRequestURI();
      ServletContext context = config.getServletContext();
      return context.getMimeType(path);
    }
  }
}
