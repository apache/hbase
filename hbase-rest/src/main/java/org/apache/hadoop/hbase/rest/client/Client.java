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
package org.apache.hadoop.hbase.rest.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import javax.net.ssl.SSLContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.security.ssl.SSLFactory.Mode;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * A wrapper around HttpClient which provides some useful function and semantics for interacting
 * with the REST gateway.
 */
@InterfaceAudience.Public
public class Client {
  public static final Header[] EMPTY_HEADER_ARRAY = new Header[0];

  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private CloseableHttpClient httpClient;
  private Cluster cluster;
  private Integer lastNodeId;
  private boolean sticky = false;
  private Configuration conf;
  private boolean sslEnabled;
  private CloseableHttpResponse resp;
  private HttpGet httpGet = null;
  private HttpClientContext stickyContext = null;
  private BasicCredentialsProvider provider;
  private Optional<KeyStore> trustStore;
  private Map<String, String> extraHeaders;
  private KerberosAuthenticator authenticator;

  private static final String AUTH_COOKIE = "hadoop.auth";
  private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
  private static final String COOKIE = "Cookie";

  public static final Header ACCEPT_PROTOBUF_HEADER =
    new BasicHeader("Accept", Constants.MIMETYPE_PROTOBUF);
  public static final Header ACCEPT_XML_HEADER = new BasicHeader("Accept", Constants.MIMETYPE_XML);
  public static final Header ACCEPT_JSON_HEADER =
    new BasicHeader("Accept", Constants.MIMETYPE_JSON);

  public static final Header[] ACCEPT_PROTOBUF_HEADER_ARR = new Header[] { ACCEPT_PROTOBUF_HEADER };
  public static final Header[] ACCEPT_XML_HEADER_ARR = new Header[] { ACCEPT_XML_HEADER };
  public static final Header[] ACCEPT_JSON_HEADER_ARR = new Header[] { ACCEPT_JSON_HEADER };

  /**
   * Default Constructor
   */
  public Client() {
    this(null);
  }

  private void initialize(Cluster cluster, Configuration conf, boolean sslEnabled, boolean sticky,
    Optional<KeyStore> trustStore, Optional<String> userName, Optional<String> password,
    Optional<String> bearerToken, Optional<HttpClientConnectionManager> connManager) {
    this.cluster = cluster;
    this.conf = conf;
    this.sslEnabled = sslEnabled;
    this.trustStore = trustStore;
    extraHeaders = new ConcurrentHashMap<>();
    String clspath = System.getProperty("java.class.path");
    LOG.debug("classpath " + clspath);
    HttpClientBuilder httpClientBuilder = HttpClients.custom();

    int connTimeout = this.conf.getInt(Constants.REST_CLIENT_CONN_TIMEOUT,
      Constants.DEFAULT_REST_CLIENT_CONN_TIMEOUT);
    int socketTimeout = this.conf.getInt(Constants.REST_CLIENT_SOCKET_TIMEOUT,
      Constants.DEFAULT_REST_CLIENT_SOCKET_TIMEOUT);
    RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(connTimeout)
      .setSocketTimeout(socketTimeout).setNormalizeUri(false) // URIs should not be normalized, see
                                                              // HBASE-26903
      .build();
    httpClientBuilder.setDefaultRequestConfig(requestConfig);

    // Since HBASE-25267 we don't use the deprecated DefaultHttpClient anymore.
    // The new http client would decompress the gzip content automatically.
    // In order to keep the original behaviour of this public class, we disable
    // automatic content compression.
    httpClientBuilder.disableContentCompression();

    if (sslEnabled && trustStore.isPresent()) {
      try {
        SSLContext sslcontext =
          SSLContexts.custom().loadTrustMaterial(trustStore.get(), null).build();
        httpClientBuilder.setSSLContext(sslcontext);
      } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
        throw new ClientTrustStoreInitializationException("Error while processing truststore", e);
      }
    }

    if (userName.isPresent() && password.isPresent()) {
      // We want to stick to the old very limited authentication and session handling when sticky is
      // not set
      // to preserve backwards compatibility
      if (!sticky) {
        throw new IllegalArgumentException("BASIC auth is only implemented when sticky is set");
      }
      provider = new BasicCredentialsProvider();
      // AuthScope.ANY is required for pre-emptive auth. We only ever use a single auth method
      // anyway.
      AuthScope anyAuthScope = AuthScope.ANY;
      this.provider.setCredentials(anyAuthScope,
        new UsernamePasswordCredentials(userName.get(), password.get()));
    }

    if (bearerToken.isPresent()) {
      // We want to stick to the old very limited authentication and session handling when sticky is
      // not set
      // to preserve backwards compatibility
      if (!sticky) {
        throw new IllegalArgumentException("BEARER auth is only implemented when sticky is set");
      }
      // We could also put the header into the context or connection, but that would have the same
      // effect.
      extraHeaders.put(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken.get());
    }

    connManager.ifPresent(httpClientBuilder::setConnectionManager);

    this.httpClient = httpClientBuilder.build();
    setSticky(sticky);
  }

  /**
   * Constructor This constructor will create an object using the old faulty load balancing logic.
   * When specifying multiple servers in the cluster object, it is highly recommended to call
   * setSticky() on the created client, or use the preferred constructor instead.
   * @param cluster the cluster definition
   */
  public Client(Cluster cluster) {
    this(cluster, false);
  }

  /**
   * Constructor This constructor will create an object using the old faulty load balancing logic.
   * When specifying multiple servers in the cluster object, it is highly recommended to call
   * setSticky() on the created client, or use the preferred constructor instead.
   * @param cluster    the cluster definition
   * @param sslEnabled enable SSL or not
   */
  public Client(Cluster cluster, boolean sslEnabled) {
    initialize(cluster, HBaseConfiguration.create(), sslEnabled, false, Optional.empty(),
      Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Constructor This constructor will create an object using the old faulty load balancing logic.
   * When specifying multiple servers in the cluster object, it is highly recommended to call
   * setSticky() on the created client, or use the preferred constructor instead.
   * @param cluster    the cluster definition
   * @param conf       Configuration
   * @param sslEnabled enable SSL or not
   */
  public Client(Cluster cluster, Configuration conf, boolean sslEnabled) {
    initialize(cluster, conf, sslEnabled, false, Optional.empty(), Optional.empty(),
      Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Constructor, allowing to define custom trust store (only for SSL connections) This constructor
   * will create an object using the old faulty load balancing logic. When specifying multiple
   * servers in the cluster object, it is highly recommended to call setSticky() on the created
   * client, or use the preferred constructor instead.
   * @param cluster            the cluster definition
   * @param trustStorePath     custom trust store to use for SSL connections
   * @param trustStorePassword password to use for custom trust store
   * @param trustStoreType     type of custom trust store
   * @throws ClientTrustStoreInitializationException if the trust store file can not be loaded
   */
  public Client(Cluster cluster, String trustStorePath, Optional<String> trustStorePassword,
    Optional<String> trustStoreType) {
    this(cluster, HBaseConfiguration.create(), trustStorePath, trustStorePassword, trustStoreType);
  }

  /**
   * Constructor that accepts an optional trustStore and authentication information for either BASIC
   * or BEARER authentication in sticky mode, which does not use the old faulty load balancing
   * logic, and enables correct session handling. If neither userName/password, nor the bearer token
   * is specified, the client falls back to SPNEGO auth. The loadTrustsore static method can be used
   * to load a local TrustStore file. If connManager is specified, it must be fully configured. Even
   * then, the TrustStore related parameters must be specified because they are also used for SPNEGO
   * authentication which uses a separate HTTP client implementation. Specifying the
   * HttpClientConnectionManager is an experimental feature. It exposes the internal HTTP library
   * details, and may be changed/removed when the library is updated or replaced.
   * @param cluster     the cluster definition
   * @param conf        HBase/Hadoop configuration
   * @param sslEnabled  use HTTPS
   * @param trustStore  the optional trustStore object
   * @param userName    for BASIC auth
   * @param password    for BASIC auth
   * @param bearerToken for BEAERER auth
   */
  @InterfaceAudience.Private
  public Client(Cluster cluster, Configuration conf, boolean sslEnabled,
    Optional<KeyStore> trustStore, Optional<String> userName, Optional<String> password,
    Optional<String> bearerToken, Optional<HttpClientConnectionManager> connManager) {
    initialize(cluster, conf, sslEnabled, true, trustStore, userName, password, bearerToken,
      connManager);
  }

  public Client(Cluster cluster, Configuration conf, boolean sslEnabled,
    Optional<KeyStore> trustStore, Optional<String> userName, Optional<String> password,
    Optional<String> bearerToken) {
    initialize(cluster, conf, sslEnabled, true, trustStore, userName, password, bearerToken,
      Optional.empty());
  }

  /**
   * Constructor, allowing to define custom trust store (only for SSL connections). This constructor
   * will create an object using the old faulty load balancing logic. When specifying multiple
   * servers in the cluster object, it is highly recommended to call setSticky() on the created
   * client, or use the preferred constructor instead.
   * @param cluster            the cluster definition
   * @param conf               HBase/Hadoop Configuration
   * @param trustStorePath     custom trust store to use for SSL connections
   * @param trustStorePassword password to use for custom trust store
   * @param trustStoreType     type of custom trust store
   * @throws ClientTrustStoreInitializationException if the trust store file can not be loaded
   */
  public Client(Cluster cluster, Configuration conf, String trustStorePath,
    Optional<String> trustStorePassword, Optional<String> trustStoreType) {
    KeyStore trustStore = loadTruststore(trustStorePath, trustStorePassword, trustStoreType);
    initialize(cluster, conf, true, false, Optional.of(trustStore), Optional.empty(),
      Optional.empty(), Optional.empty(), Optional.empty());
  }

  /**
   * Loads a trustStore from the local fileSystem. Can be used to load the trustStore for the
   * preferred constructor.
   */
  public static KeyStore loadTruststore(String trustStorePath, Optional<String> trustStorePassword,
    Optional<String> trustStoreType) {

    char[] truststorePassword = trustStorePassword.map(String::toCharArray).orElse(null);
    String type = trustStoreType.orElse(KeyStore.getDefaultType());

    KeyStore trustStore;
    try {
      trustStore = KeyStore.getInstance(type);
    } catch (KeyStoreException e) {
      throw new ClientTrustStoreInitializationException("Invalid trust store type: " + type, e);
    }
    try (InputStream inputStream =
      new BufferedInputStream(Files.newInputStream(new File(trustStorePath).toPath()))) {
      trustStore.load(inputStream, truststorePassword);
    } catch (CertificateException | NoSuchAlgorithmException | IOException e) {
      throw new ClientTrustStoreInitializationException("Trust store load error: " + trustStorePath,
        e);
    }
    return trustStore;
  }

  /**
   * Shut down the client. Close any open persistent connections.
   */
  public void shutdown() {
    close();
  }

  /** Returns the wrapped HttpClient */
  public HttpClient getHttpClient() {
    return httpClient;
  }

  /**
   * Add extra headers. These extra headers will be applied to all http methods before they are
   * removed. If any header is not used any more, client needs to remove it explicitly.
   */
  public void addExtraHeader(final String name, final String value) {
    extraHeaders.put(name, value);
  }

  /**
   * Get an extra header value.
   */
  public String getExtraHeader(final String name) {
    return extraHeaders.get(name);
  }

  /**
   * Get all extra headers (read-only).
   */
  public Map<String, String> getExtraHeaders() {
    return Collections.unmodifiableMap(extraHeaders);
  }

  /**
   * Remove an extra header.
   */
  public void removeExtraHeader(final String name) {
    extraHeaders.remove(name);
  }

  /**
   * Execute a transaction method given only the path. If sticky is false: Will select at random one
   * of the members of the supplied cluster definition and iterate through the list until a
   * transaction can be successfully completed. The definition of success here is a complete HTTP
   * transaction, irrespective of result code. If sticky is true: For the first request it will
   * select a random one of the members of the supplied cluster definition. For subsequent requests
   * it will use the same member, and it will not automatically re-try if the call fails.
   * @param cluster the cluster definition
   * @param method  the transaction method
   * @param headers HTTP header values to send
   * @param path    the properly urlencoded path
   * @return the HTTP response code
   */
  public CloseableHttpResponse executePathOnly(Cluster cluster, HttpUriRequest method,
    Header[] headers, String path) throws IOException {
    IOException lastException;
    if (cluster.nodes.size() < 1) {
      throw new IOException("Cluster is empty");
    }
    if (lastNodeId == null || !sticky) {
      lastNodeId = ThreadLocalRandom.current().nextInt(cluster.nodes.size());
    }
    int start = lastNodeId;
    do {
      cluster.lastHost = cluster.nodes.get(lastNodeId);
      try {
        StringBuilder sb = new StringBuilder();
        if (sslEnabled) {
          sb.append("https://");
        } else {
          sb.append("http://");
        }
        sb.append(cluster.lastHost);
        sb.append(path);
        URI uri = new URI(sb.toString());
        if (method instanceof HttpPut) {
          HttpPut put = new HttpPut(uri);
          put.setEntity(((HttpPut) method).getEntity());
          put.setHeaders(method.getAllHeaders());
          method = put;
        } else if (method instanceof HttpGet) {
          method = new HttpGet(uri);
        } else if (method instanceof HttpHead) {
          method = new HttpHead(uri);
        } else if (method instanceof HttpDelete) {
          method = new HttpDelete(uri);
        } else if (method instanceof HttpPost) {
          HttpPost post = new HttpPost(uri);
          post.setEntity(((HttpPost) method).getEntity());
          post.setHeaders(method.getAllHeaders());
          method = post;
        }
        return executeURI(method, headers, uri.toString());
      } catch (IOException e) {
        lastException = e;
      } catch (URISyntaxException use) {
        lastException = new IOException(use);
      }
      if (!sticky) {
        lastNodeId = (++lastNodeId) % cluster.nodes.size();
      }
      // Do not retry if sticky. Let the caller handle the error.
    } while (!sticky && lastNodeId != start);
    throw lastException;
  }

  /**
   * Execute a transaction method given a complete URI.
   * @param method  the transaction method
   * @param headers HTTP header values to send
   * @param uri     a properly urlencoded URI
   * @return the HTTP response code
   */
  public CloseableHttpResponse executeURI(HttpUriRequest method, Header[] headers, String uri)
    throws IOException {
    // method.setURI(new URI(uri, true));
    for (Map.Entry<String, String> e : extraHeaders.entrySet()) {
      method.addHeader(e.getKey(), e.getValue());
    }
    if (headers != null) {
      for (Header header : headers) {
        method.addHeader(header);
      }
    }
    long startTime = EnvironmentEdgeManager.currentTime();
    if (resp != null) EntityUtils.consumeQuietly(resp.getEntity());
    if (stickyContext != null) {
      resp = httpClient.execute(method, stickyContext);
    } else {
      resp = httpClient.execute(method);
    }
    if (resp.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
      // Authentication error
      LOG.debug("Performing negotiation with the server.");
      try {
        negotiate(method, uri);
      } catch (GeneralSecurityException e) {
        throw new IOException(e);
      }
      if (stickyContext != null) {
        resp = httpClient.execute(method, stickyContext);
      } else {
        resp = httpClient.execute(method);
      }
    }

    long endTime = EnvironmentEdgeManager.currentTime();
    if (LOG.isTraceEnabled()) {
      LOG.trace(method.getMethod() + " " + uri + " " + resp.getStatusLine().getStatusCode() + " "
        + resp.getStatusLine().getReasonPhrase() + " in " + (endTime - startTime) + " ms");
    }
    return resp;
  }

  /**
   * Execute a transaction method. Will call either <tt>executePathOnly</tt> or <tt>executeURI</tt>
   * depending on whether a path only is supplied in 'path', or if a complete URI is passed instead,
   * respectively.
   * @param method  the HTTP method
   * @param headers HTTP header values to send
   * @param path    the properly urlencoded path or URI
   * @return the HTTP response code
   */
  public CloseableHttpResponse execute(Cluster cluster, HttpUriRequest method, Header[] headers,
    String path) throws IOException {
    if (path.startsWith("/")) {
      return executePathOnly(cluster, method, headers, path);
    }
    return executeURI(method, headers, path);
  }

  /**
   * Execute a transaction method. Will call either <tt>executePathOnly</tt> or <tt>executeURI</tt>
   * depending on whether a path only is supplied in 'path', or if a complete URI is passed instead,
   * respectively.
   * @param cluster the cluster definition
   * @param method  the HTTP method
   * @param headers HTTP header values to send
   * @param path    the properly urlencoded path or URI
   * @return the HTTP response code
   */
  public CloseableHttpResponse execute(HttpUriRequest method, Header[] headers) throws IOException {
    String path = method.getURI().toASCIIString();
    if (path.startsWith("/")) {
      return executePathOnly(cluster, method, headers, path);
    }
    return executeURI(method, headers, path);
  }

  /**
   * Initiate client side Kerberos negotiation with the server.
   * @param method method to inject the authentication token into.
   * @param uri    the String to parse as a URL.
   * @throws IOException if unknown protocol is found.
   */
  private void negotiate(HttpUriRequest method, String uri)
    throws IOException, GeneralSecurityException {
    try {
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      if (authenticator == null) {
        authenticator = new KerberosAuthenticator();
        if (trustStore.isPresent()) {
          // The authenticator does not use Apache HttpClient, so we need to
          // configure it separately to use the specified trustStore
          Configuration sslConf = setupTrustStoreForHadoop(trustStore.get());
          SSLFactory sslFactory = new SSLFactory(Mode.CLIENT, sslConf);
          sslFactory.init();
          authenticator.setConnectionConfigurator(sslFactory);
        }
      }
      URL url = new URL(uri);
      authenticator.authenticate(url, token);
      if (sticky) {
        BasicClientCookie authCookie = new BasicClientCookie("hadoop.auth", token.toString());
        // Hadoop eats the domain even if set by server
        authCookie.setDomain(url.getHost());
        stickyContext.getCookieStore().addCookie(authCookie);
      } else {
        // session cookie is NOT set for backwards compatibility for non-sticky mode
        // Inject the obtained negotiated token in the method cookie
        // This is only done for this single request, the next one will trigger a new SPENGO
        // handshake
        injectToken(method, token);
      }
    } catch (AuthenticationException e) {
      LOG.error("Failed to negotiate with the server.", e);
      throw new IOException(e);
    }
  }

  private Configuration setupTrustStoreForHadoop(KeyStore trustStore)
    throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
    Path tmpDirPath = Files.createTempDirectory("hbase_rest_client_truststore");
    File trustStoreFile = tmpDirPath.resolve("truststore.jks").toFile();
    // Shouldn't be needed with the secure temp dir, but let's generate a password anyway
    String password = Double.toString(Math.random());
    try (FileOutputStream fos = new FileOutputStream(trustStoreFile)) {
      trustStore.store(fos, password.toCharArray());
    }

    Configuration sslConf = new Configuration();
    // Type is the Java default, we use the same JVM to read this back
    sslConf.set("ssl.client.truststore.location", trustStoreFile.getAbsolutePath());
    sslConf.set("ssl.client.truststore.password", password);
    return sslConf;
  }

  /**
   * Helper method that injects an authentication token to send with the method.
   * @param method method to inject the authentication token into.
   * @param token  authentication token to inject.
   */
  private void injectToken(HttpUriRequest method, AuthenticatedURL.Token token) {
    String t = token.toString();
    if (t != null) {
      if (!t.startsWith("\"")) {
        t = "\"" + t + "\"";
      }
      method.addHeader(COOKIE, AUTH_COOKIE_EQ + t);
    }
  }

  /** Returns the cluster definition */
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * @param cluster the cluster definition
   */
  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  /**
   * The default behaviour is load balancing by sending each request to a random host. This DOES NOT
   * work with scans, which have state on the REST servers. Make sure sticky is set to true before
   * attempting Scan related operations if more than one host is defined in the cluster.
   * @return whether subsequent requests will use the same host
   */
  public boolean isSticky() {
    return sticky;
  }

  /**
   * The default behaviour is load balancing by sending each request to a random host. This DOES NOT
   * work with scans, which have state on the REST servers. Set sticky to true before attempting
   * Scan related operations if more than one host is defined in the cluster. Nodes must not be
   * added or removed from the Cluster object while sticky is true. Setting the sticky flag also
   * enables session handling, which eliminates the need to re-authenticate each request, and lets
   * the client handle any other cookies (like the sticky cookie set by load balancers) correctly.
   * @param sticky whether subsequent requests will use the same host
   */
  public void setSticky(boolean sticky) {
    lastNodeId = null;
    if (sticky) {
      stickyContext = new HttpClientContext();
      if (provider != null) {
        stickyContext.setCredentialsProvider(provider);
      }
    } else {
      stickyContext = null;
    }
    this.sticky = sticky;
  }

  /**
   * Send a HEAD request
   * @param path the path or URI
   * @return a Response object with response detail
   */
  public Response head(String path) throws IOException {
    return head(cluster, path, null);
  }

  /**
   * Send a HEAD request
   * @param cluster the cluster definition
   * @param path    the path or URI
   * @param headers the HTTP headers to include in the request
   * @return a Response object with response detail
   */
  public Response head(Cluster cluster, String path, Header[] headers) throws IOException {
    HttpHead method = new HttpHead(path);
    try {
      CloseableHttpResponse resp = execute(cluster, method, null, path);
      return new Response(resp.getStatusLine().getStatusCode(), resp.getAllHeaders(), null);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a GET request
   * @param path the path or URI
   * @return a Response object with response detail
   */
  public Response get(String path) throws IOException {
    return get(cluster, path);
  }

  /**
   * Send a GET request
   * @param cluster the cluster definition
   * @param path    the path or URI
   * @return a Response object with response detail
   */
  public Response get(Cluster cluster, String path) throws IOException {
    return get(cluster, path, EMPTY_HEADER_ARRAY);
  }

  /**
   * Send a GET request
   * @param path   the path or URI
   * @param accept Accept header value
   * @return a Response object with response detail
   */
  public Response get(String path, String accept) throws IOException {
    return get(cluster, path, accept);
  }

  /**
   * Send a GET request
   * @param cluster the cluster definition
   * @param path    the path or URI
   * @param accept  Accept header value
   * @return a Response object with response detail
   */
  public Response get(Cluster cluster, String path, String accept) throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new BasicHeader("Accept", accept);
    return get(cluster, path, headers);
  }

  /**
   * Send a GET request
   * @param path    the path or URI
   * @param headers the HTTP headers to include in the request, <tt>Accept</tt> must be supplied
   * @return a Response object with response detail
   */
  public Response get(String path, Header[] headers) throws IOException {
    return get(cluster, path, headers);
  }

  /**
   * Returns the response body of the HTTPResponse, if any, as an array of bytes. If response body
   * is not available or cannot be read, returns <tt>null</tt> Note: This will cause the entire
   * response body to be buffered in memory. A malicious server may easily exhaust all the VM
   * memory. It is strongly recommended, to use getResponseAsStream if the content length of the
   * response is unknown or reasonably large.
   * @param resp HttpResponse
   * @return The response body, null if body is empty
   * @throws IOException If an I/O (transport) problem occurs while obtaining the response body.
   */
  public static byte[] getResponseBody(HttpResponse resp) throws IOException {
    if (resp.getEntity() == null) {
      return null;
    }
    InputStream instream = resp.getEntity().getContent();
    if (instream == null) {
      return null;
    }
    try {
      long contentLength = resp.getEntity().getContentLength();
      if (contentLength > Integer.MAX_VALUE) {
        // guard integer cast from overflow
        throw new IOException("Content too large to be buffered: " + contentLength + " bytes");
      }
      if (contentLength > 0) {
        byte[] content = new byte[(int) contentLength];
        ByteStreams.readFully(instream, content);
        return content;
      } else {
        return ByteStreams.toByteArray(instream);
      }
    } finally {
      Closeables.closeQuietly(instream);
    }
  }

  /**
   * Send a GET request
   * @param c       the cluster definition
   * @param path    the path or URI
   * @param headers the HTTP headers to include in the request
   * @return a Response object with response detail
   */
  public Response get(Cluster c, String path, Header[] headers) throws IOException {
    if (httpGet != null) {
      httpGet.releaseConnection();
    }
    httpGet = new HttpGet(path);
    CloseableHttpResponse resp = execute(c, httpGet, headers, path);
    return new Response(resp.getStatusLine().getStatusCode(), resp.getAllHeaders(), resp,
      resp.getEntity() == null ? null : resp.getEntity().getContent());
  }

  /**
   * Send a PUT request
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @return a Response object with response detail
   */
  public Response put(String path, String contentType, byte[] content) throws IOException {
    return put(cluster, path, contentType, content);
  }

  /**
   * Send a PUT request
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @param extraHdr    extra Header to send
   * @return a Response object with response detail
   */
  public Response put(String path, String contentType, byte[] content, Header extraHdr)
    throws IOException {
    return put(cluster, path, contentType, content, extraHdr);
  }

  /**
   * Send a PUT request
   * @param cluster     the cluster definition
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response put(Cluster cluster, String path, String contentType, byte[] content)
    throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new BasicHeader("Content-Type", contentType);
    return put(cluster, path, headers, content);
  }

  /**
   * Send a PUT request
   * @param cluster     the cluster definition
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @param extraHdr    additional Header to send
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response put(Cluster cluster, String path, String contentType, byte[] content,
    Header extraHdr) throws IOException {
    int cnt = extraHdr == null ? 1 : 2;
    Header[] headers = new Header[cnt];
    headers[0] = new BasicHeader("Content-Type", contentType);
    if (extraHdr != null) {
      headers[1] = extraHdr;
    }
    return put(cluster, path, headers, content);
  }

  /**
   * Send a PUT request
   * @param path    the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be supplied
   * @param content the content bytes
   * @return a Response object with response detail
   */
  public Response put(String path, Header[] headers, byte[] content) throws IOException {
    return put(cluster, path, headers, content);
  }

  /**
   * Send a PUT request
   * @param cluster the cluster definition
   * @param path    the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be supplied
   * @param content the content bytes
   * @return a Response object with response detail
   */
  public Response put(Cluster cluster, String path, Header[] headers, byte[] content)
    throws IOException {
    HttpPut method = new HttpPut(path);
    try {
      method.setEntity(new ByteArrayEntity(content));
      CloseableHttpResponse resp = execute(cluster, method, headers, path);
      headers = resp.getAllHeaders();
      content = getResponseBody(resp);
      return new Response(resp.getStatusLine().getStatusCode(), headers, content);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a POST request
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @return a Response object with response detail
   */
  public Response post(String path, String contentType, byte[] content) throws IOException {
    return post(cluster, path, contentType, content);
  }

  /**
   * Send a POST request
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @param extraHdr    additional Header to send
   * @return a Response object with response detail
   */
  public Response post(String path, String contentType, byte[] content, Header extraHdr)
    throws IOException {
    return post(cluster, path, contentType, content, extraHdr);
  }

  /**
   * Send a POST request
   * @param cluster     the cluster definition
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response post(Cluster cluster, String path, String contentType, byte[] content)
    throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new BasicHeader("Content-Type", contentType);
    return post(cluster, path, headers, content);
  }

  /**
   * Send a POST request
   * @param cluster     the cluster definition
   * @param path        the path or URI
   * @param contentType the content MIME type
   * @param content     the content bytes
   * @param extraHdr    additional Header to send
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response post(Cluster cluster, String path, String contentType, byte[] content,
    Header extraHdr) throws IOException {
    int cnt = extraHdr == null ? 1 : 2;
    Header[] headers = new Header[cnt];
    headers[0] = new BasicHeader("Content-Type", contentType);
    if (extraHdr != null) {
      headers[1] = extraHdr;
    }
    return post(cluster, path, headers, content);
  }

  /**
   * Send a POST request
   * @param path    the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be supplied
   * @param content the content bytes
   * @return a Response object with response detail
   */
  public Response post(String path, Header[] headers, byte[] content) throws IOException {
    return post(cluster, path, headers, content);
  }

  /**
   * Send a POST request
   * @param cluster the cluster definition
   * @param path    the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be supplied
   * @param content the content bytes
   * @return a Response object with response detail
   */
  public Response post(Cluster cluster, String path, Header[] headers, byte[] content)
    throws IOException {
    HttpPost method = new HttpPost(path);
    try {
      method.setEntity(new ByteArrayEntity(content));
      CloseableHttpResponse resp = execute(cluster, method, headers, path);
      headers = resp.getAllHeaders();
      content = getResponseBody(resp);
      return new Response(resp.getStatusLine().getStatusCode(), headers, content);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a DELETE request
   * @param path the path or URI
   * @return a Response object with response detail
   */
  public Response delete(String path) throws IOException {
    return delete(cluster, path);
  }

  /**
   * Send a DELETE request
   * @param path     the path or URI
   * @param extraHdr additional Header to send
   * @return a Response object with response detail
   */
  public Response delete(String path, Header extraHdr) throws IOException {
    return delete(cluster, path, extraHdr);
  }

  /**
   * Send a DELETE request
   * @param cluster the cluster definition
   * @param path    the path or URI
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response delete(Cluster cluster, String path) throws IOException {
    HttpDelete method = new HttpDelete(path);
    try {
      CloseableHttpResponse resp = execute(cluster, method, null, path);
      Header[] headers = resp.getAllHeaders();
      byte[] content = getResponseBody(resp);
      return new Response(resp.getStatusLine().getStatusCode(), headers, content);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a DELETE request
   * @param cluster the cluster definition
   * @param path    the path or URI
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response delete(Cluster cluster, String path, Header extraHdr) throws IOException {
    HttpDelete method = new HttpDelete(path);
    try {
      Header[] headers = { extraHdr };
      CloseableHttpResponse resp = execute(cluster, method, headers, path);
      headers = resp.getAllHeaders();
      byte[] content = getResponseBody(resp);
      return new Response(resp.getStatusLine().getStatusCode(), headers, content);
    } finally {
      method.releaseConnection();
    }
  }

  public static class ClientTrustStoreInitializationException extends RuntimeException {

    public ClientTrustStoreInitializationException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  public void close() {
    try {
      httpClient.close();
    } catch (Exception e) {
      LOG.info("Exception while shutting down connection manager", e);
    }
  }

}
