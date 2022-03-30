/*
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

package org.apache.hadoop.hbase.rest.client;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.rest.Constants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around HttpClient which provides some useful function and
 * semantics for interacting with the REST gateway.
 */
@InterfaceAudience.Public
public class Client {
  public static final Header[] EMPTY_HEADER_ARRAY = new Header[0];

  private static final Logger LOG = LoggerFactory.getLogger(Client.class);

  private HttpClient httpClient;
  private Cluster cluster;
  private Configuration conf;
  private boolean sslEnabled;
  private HttpResponse resp;
  private HttpGet httpGet = null;

  private Map<String, String> extraHeaders;

  private static final String AUTH_COOKIE = "hadoop.auth";
  private static final String AUTH_COOKIE_EQ = AUTH_COOKIE + "=";
  private static final String COOKIE = "Cookie";

  /**
   * Default Constructor
   */
  public Client() {
    this(null);
  }

  private void initialize(Cluster cluster, Configuration conf, boolean sslEnabled,
      Optional<KeyStore> trustStore) {
    this.cluster = cluster;
    this.conf = conf;
    this.sslEnabled = sslEnabled;
    extraHeaders = new ConcurrentHashMap<>();
    String clspath = System.getProperty("java.class.path");
    LOG.debug("classpath " + clspath);
    HttpClientBuilder httpClientBuilder = HttpClients.custom();

    int connTimeout = this.conf.getInt(Constants.REST_CLIENT_CONN_TIMEOUT,
      Constants.DEFAULT_REST_CLIENT_CONN_TIMEOUT);
    int socketTimeout = this.conf.getInt(Constants.REST_CLIENT_SOCKET_TIMEOUT,
      Constants.DEFAULT_REST_CLIENT_SOCKET_TIMEOUT);
    RequestConfig requestConfig = RequestConfig.custom()
      .setConnectTimeout(connTimeout)
      .setSocketTimeout(socketTimeout)
      .setNormalizeUri(false) // URIs should not be normalized, see HBASE-26903
      .build();
    httpClientBuilder.setDefaultRequestConfig(requestConfig);

    // Since HBASE-25267 we don't use the deprecated DefaultHttpClient anymore.
    // The new http client would decompress the gzip content automatically.
    // In order to keep the original behaviour of this public class, we disable
    // automatic content compression.
    httpClientBuilder.disableContentCompression();

    if(sslEnabled && trustStore.isPresent()) {
      try {
        SSLContext sslcontext =
          SSLContexts.custom().loadTrustMaterial(trustStore.get(), null).build();
        httpClientBuilder.setSSLContext(sslcontext);
      } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
        throw new ClientTrustStoreInitializationException("Error while processing truststore", e);
      }
    }

    this.httpClient = httpClientBuilder.build();
  }

  /**
   * Constructor
   * @param cluster the cluster definition
   */
  public Client(Cluster cluster) {
    this(cluster, false);
  }

  /**
   * Constructor
   * @param cluster the cluster definition
   * @param sslEnabled enable SSL or not
   */
  public Client(Cluster cluster, boolean sslEnabled) {
    initialize(cluster, HBaseConfiguration.create(), sslEnabled, Optional.empty());
  }

  /**
   * Constructor
   * @param cluster the cluster definition
   * @param conf Configuration
   * @param sslEnabled enable SSL or not
   */
  public Client(Cluster cluster, Configuration conf, boolean sslEnabled) {
    initialize(cluster, conf, sslEnabled, Optional.empty());
  }

  /**
   * Constructor, allowing to define custom trust store (only for SSL connections)
   *
   * @param cluster the cluster definition
   * @param trustStorePath custom trust store to use for SSL connections
   * @param trustStorePassword password to use for custom trust store
   * @param trustStoreType type of custom trust store
   *
   * @throws ClientTrustStoreInitializationException if the trust store file can not be loaded
   */
  public Client(Cluster cluster, String trustStorePath, Optional<String> trustStorePassword,
      Optional<String> trustStoreType) {
    this(cluster, HBaseConfiguration.create(), trustStorePath, trustStorePassword, trustStoreType);
  }

  /**
   * Constructor, allowing to define custom trust store (only for SSL connections)
   *
   * @param cluster the cluster definition
   * @param conf Configuration
   * @param trustStorePath custom trust store to use for SSL connections
   * @param trustStorePassword password to use for custom trust store
   * @param trustStoreType type of custom trust store
   * @throws ClientTrustStoreInitializationException if the trust store file can not be loaded
   */
  public Client(Cluster cluster, Configuration conf, String trustStorePath,
      Optional<String> trustStorePassword, Optional<String> trustStoreType) {

    char[] password = trustStorePassword.map(String::toCharArray).orElse(null);
    String type = trustStoreType.orElse(KeyStore.getDefaultType());

    KeyStore trustStore;
    try {
      trustStore = KeyStore.getInstance(type);
    } catch (KeyStoreException e) {
      throw new ClientTrustStoreInitializationException("Invalid trust store type: " + type, e);
    }
    try (InputStream inputStream = new BufferedInputStream(
      Files.newInputStream(new File(trustStorePath).toPath()))) {
      trustStore.load(inputStream, password);
    } catch (CertificateException | NoSuchAlgorithmException | IOException e) {
      throw new ClientTrustStoreInitializationException("Trust store load error: " + trustStorePath,
        e);
    }

    initialize(cluster, conf, true, Optional.of(trustStore));
  }

  /**
   * Shut down the client. Close any open persistent connections.
   */
  public void shutdown() {
  }

  /**
   * @return the wrapped HttpClient
   */
  public HttpClient getHttpClient() {
    return httpClient;
  }

  /**
   * Add extra headers.  These extra headers will be applied to all http
   * methods before they are removed. If any header is not used any more,
   * client needs to remove it explicitly.
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
   * Execute a transaction method given only the path. Will select at random
   * one of the members of the supplied cluster definition and iterate through
   * the list until a transaction can be successfully completed. The
   * definition of success here is a complete HTTP transaction, irrespective
   * of result code.
   * @param cluster the cluster definition
   * @param method the transaction method
   * @param headers HTTP header values to send
   * @param path the properly urlencoded path
   * @return the HTTP response code
   * @throws IOException
   */
  public HttpResponse executePathOnly(Cluster cluster, HttpUriRequest method,
      Header[] headers, String path) throws IOException {
    IOException lastException;
    if (cluster.nodes.size() < 1) {
      throw new IOException("Cluster is empty");
    }
    int start = (int)Math.round((cluster.nodes.size() - 1) * Math.random());
    int i = start;
    do {
      cluster.lastHost = cluster.nodes.get(i);
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
    } while (++i != start && i < cluster.nodes.size());
    throw lastException;
  }

  /**
   * Execute a transaction method given a complete URI.
   * @param method the transaction method
   * @param headers HTTP header values to send
   * @param uri a properly urlencoded URI
   * @return the HTTP response code
   * @throws IOException
   */
  public HttpResponse executeURI(HttpUriRequest method, Header[] headers, String uri)
      throws IOException {
    // method.setURI(new URI(uri, true));
    for (Map.Entry<String, String> e: extraHeaders.entrySet()) {
      method.addHeader(e.getKey(), e.getValue());
    }
    if (headers != null) {
      for (Header header: headers) {
        method.addHeader(header);
      }
    }
    long startTime = EnvironmentEdgeManager.currentTime();
    if (resp != null) EntityUtils.consumeQuietly(resp.getEntity());
    resp = httpClient.execute(method);
    if (resp.getStatusLine().getStatusCode() == HttpStatus.SC_UNAUTHORIZED) {
      // Authentication error
      LOG.debug("Performing negotiation with the server.");
      negotiate(method, uri);
      resp = httpClient.execute(method);
    }

    long endTime = EnvironmentEdgeManager.currentTime();
    if (LOG.isTraceEnabled()) {
      LOG.trace(method.getMethod() + " " + uri + " " + resp.getStatusLine().getStatusCode() + " " +
          resp.getStatusLine().getReasonPhrase() + " in " + (endTime - startTime) + " ms");
    }
    return resp;
  }

  /**
   * Execute a transaction method. Will call either <tt>executePathOnly</tt>
   * or <tt>executeURI</tt> depending on whether a path only is supplied in
   * 'path', or if a complete URI is passed instead, respectively.
   * @param cluster the cluster definition
   * @param method the HTTP method
   * @param headers HTTP header values to send
   * @param path the properly urlencoded path or URI
   * @return the HTTP response code
   * @throws IOException
   */
  public HttpResponse execute(Cluster cluster, HttpUriRequest method, Header[] headers,
      String path) throws IOException {
    if (path.startsWith("/")) {
      return executePathOnly(cluster, method, headers, path);
    }
    return executeURI(method, headers, path);
  }

  /**
   * Initiate client side Kerberos negotiation with the server.
   * @param method method to inject the authentication token into.
   * @param uri the String to parse as a URL.
   * @throws IOException if unknown protocol is found.
   */
  private void negotiate(HttpUriRequest method, String uri) throws IOException {
    try {
      AuthenticatedURL.Token token = new AuthenticatedURL.Token();
      KerberosAuthenticator authenticator = new KerberosAuthenticator();
      authenticator.authenticate(new URL(uri), token);
      // Inject the obtained negotiated token in the method cookie
      injectToken(method, token);
    } catch (AuthenticationException e) {
      LOG.error("Failed to negotiate with the server.", e);
      throw new IOException(e);
    }
  }

  /**
   * Helper method that injects an authentication token to send with the method.
   * @param method method to inject the authentication token into.
   * @param token authentication token to inject.
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

  /**
   * @return the cluster definition
   */
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
   * Send a HEAD request
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response head(String path) throws IOException {
    return head(cluster, path, null);
  }

  /**
   * Send a HEAD request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include in the request
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response head(Cluster cluster, String path, Header[] headers)
      throws IOException {
    HttpHead method = new HttpHead(path);
    try {
      HttpResponse resp = execute(cluster, method, null, path);
      return new Response(resp.getStatusLine().getStatusCode(), resp.getAllHeaders(), null);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a GET request
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(String path) throws IOException {
    return get(cluster, path);
  }

  /**
   * Send a GET request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(Cluster cluster, String path) throws IOException {
    return get(cluster, path, EMPTY_HEADER_ARRAY);
  }

  /**
   * Send a GET request
   * @param path the path or URI
   * @param accept Accept header value
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(String path, String accept) throws IOException {
    return get(cluster, path, accept);
  }

  /**
   * Send a GET request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param accept Accept header value
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(Cluster cluster, String path, String accept)
      throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new BasicHeader("Accept", accept);
    return get(cluster, path, headers);
  }

  /**
   * Send a GET request
   * @param path the path or URI
   * @param headers the HTTP headers to include in the request,
   * <tt>Accept</tt> must be supplied
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(String path, Header[] headers) throws IOException {
    return get(cluster, path, headers);
  }

  /**
   * Returns the response body of the HTTPResponse, if any, as an array of bytes.
   * If response body is not available or cannot be read, returns <tt>null</tt>
   *
   * Note: This will cause the entire response body to be buffered in memory. A
   * malicious server may easily exhaust all the VM memory. It is strongly
   * recommended, to use getResponseAsStream if the content length of the response
   * is unknown or reasonably large.
   *
   * @param resp HttpResponse
   * @return The response body, null if body is empty
   * @throws IOException If an I/O (transport) problem occurs while obtaining the
   * response body.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value =
      "NP_LOAD_OF_KNOWN_NULL_VALUE", justification = "null is possible return value")
  public static byte[] getResponseBody(HttpResponse resp) throws IOException {
    if (resp.getEntity() == null) return null;
    try (InputStream instream = resp.getEntity().getContent()) {
      if (instream != null) {
        long contentLength = resp.getEntity().getContentLength();
        if (contentLength > Integer.MAX_VALUE) {
          //guard integer cast from overflow
          throw new IOException("Content too large to be buffered: " + contentLength +" bytes");
        }
        ByteArrayOutputStream outstream = new ByteArrayOutputStream(
            contentLength > 0 ? (int) contentLength : 4*1024);
        byte[] buffer = new byte[4096];
        int len;
        while ((len = instream.read(buffer)) > 0) {
          outstream.write(buffer, 0, len);
        }
        outstream.close();
        return outstream.toByteArray();
      }
      return null;
    }
  }

  /**
   * Send a GET request
   * @param c the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include in the request
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response get(Cluster c, String path, Header[] headers)
      throws IOException {
    if (httpGet != null) {
      httpGet.releaseConnection();
    }
    httpGet = new HttpGet(path);
    HttpResponse resp = execute(c, httpGet, headers, path);
    return new Response(resp.getStatusLine().getStatusCode(), resp.getAllHeaders(),
        resp, resp.getEntity() == null ? null : resp.getEntity().getContent());
  }

  /**
   * Send a PUT request
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(String path, String contentType, byte[] content)
      throws IOException {
    return put(cluster, path, contentType, content);
  }

  /**
   * Send a PUT request
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @param extraHdr extra Header to send
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(String path, String contentType, byte[] content, Header extraHdr)
      throws IOException {
    return put(cluster, path, contentType, content, extraHdr);
  }

  /**
   * Send a PUT request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response put(Cluster cluster, String path, String contentType,
      byte[] content) throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new BasicHeader("Content-Type", contentType);
    return put(cluster, path, headers, content);
  }

  /**
   * Send a PUT request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @param extraHdr additional Header to send
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response put(Cluster cluster, String path, String contentType,
      byte[] content, Header extraHdr) throws IOException {
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
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(String path, Header[] headers, byte[] content)
      throws IOException {
    return put(cluster, path, headers, content);
  }

  /**
   * Send a PUT request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response put(Cluster cluster, String path, Header[] headers,
      byte[] content) throws IOException {
    HttpPut method = new HttpPut(path);
    try {
      method.setEntity(new InputStreamEntity(new ByteArrayInputStream(content), content.length));
      HttpResponse resp = execute(cluster, method, headers, path);
      headers = resp.getAllHeaders();
      content = getResponseBody(resp);
      return new Response(resp.getStatusLine().getStatusCode(), headers, content);
    } finally {
      method.releaseConnection();
    }
  }

  /**
   * Send a POST request
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(String path, String contentType, byte[] content)
      throws IOException {
    return post(cluster, path, contentType, content);
  }

  /**
   * Send a POST request
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @param extraHdr additional Header to send
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(String path, String contentType, byte[] content, Header extraHdr)
      throws IOException {
    return post(cluster, path, contentType, content, extraHdr);
  }

  /**
   * Send a POST request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response post(Cluster cluster, String path, String contentType,
      byte[] content) throws IOException {
    Header[] headers = new Header[1];
    headers[0] = new BasicHeader("Content-Type", contentType);
    return post(cluster, path, headers, content);
  }

  /**
   * Send a POST request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param contentType the content MIME type
   * @param content the content bytes
   * @param extraHdr additional Header to send
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response post(Cluster cluster, String path, String contentType,
      byte[] content, Header extraHdr) throws IOException {
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
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(String path, Header[] headers, byte[] content)
      throws IOException {
    return post(cluster, path, headers, content);
  }

  /**
   * Send a POST request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @param headers the HTTP headers to include, <tt>Content-Type</tt> must be
   * supplied
   * @param content the content bytes
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response post(Cluster cluster, String path, Header[] headers,
      byte[] content) throws IOException {
    HttpPost method = new HttpPost(path);
    try {
      method.setEntity(new InputStreamEntity(new ByteArrayInputStream(content), content.length));
      HttpResponse resp = execute(cluster, method, headers, path);
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
   * @throws IOException
   */
  public Response delete(String path) throws IOException {
    return delete(cluster, path);
  }

  /**
   * Send a DELETE request
   * @param path the path or URI
   * @param extraHdr additional Header to send
   * @return a Response object with response detail
   * @throws IOException
   */
  public Response delete(String path, Header extraHdr) throws IOException {
    return delete(cluster, path, extraHdr);
  }

  /**
   * Send a DELETE request
   * @param cluster the cluster definition
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response delete(Cluster cluster, String path) throws IOException {
    HttpDelete method = new HttpDelete(path);
    try {
      HttpResponse resp = execute(cluster, method, null, path);
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
   * @param path the path or URI
   * @return a Response object with response detail
   * @throws IOException for error
   */
  public Response delete(Cluster cluster, String path, Header extraHdr) throws IOException {
    HttpDelete method = new HttpDelete(path);
    try {
      Header[] headers = { extraHdr };
      HttpResponse resp = execute(cluster, method, headers, path);
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
}
