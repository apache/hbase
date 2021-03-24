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
package org.apache.hadoop.hbase.thrift2.client;

import static org.apache.hadoop.hbase.ipc.RpcClient.DEFAULT_SOCKET_TIMEOUT_CONNECT;
import static org.apache.hadoop.hbase.ipc.RpcClient.SOCKET_TIMEOUT_CONNECT;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Constructor;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import javax.net.ssl.SSLException;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.thrift.Constants;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.http.HttpRequest;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.utils.HttpClientUtils;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.protocol.HttpContext;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

@InterfaceAudience.Private
public class ThriftConnection implements Connection {
  private Configuration conf;
  private User user;
  // For HTTP protocol
  private HttpClient httpClient;
  private boolean httpClientCreated = false;
  private boolean isClosed = false;

  private String host;
  private int port;
  private boolean isFramed = false;
  private boolean isCompact = false;

  private ThriftClientBuilder clientBuilder;

  private int operationTimeout;
  private int connectTimeout;

  public ThriftConnection(Configuration conf, ExecutorService pool, final User user)
      throws IOException {
    this.conf = conf;
    this.user = user;
    this.host = conf.get(Constants.HBASE_THRIFT_SERVER_NAME);
    this.port = conf.getInt(Constants.HBASE_THRIFT_SERVER_PORT, -1);
    Preconditions.checkArgument(port > 0);
    Preconditions.checkArgument(host != null);
    this.isFramed = conf.getBoolean(Constants.FRAMED_CONF_KEY, Constants.FRAMED_CONF_DEFAULT);
    this.isCompact = conf.getBoolean(Constants.COMPACT_CONF_KEY, Constants.COMPACT_CONF_DEFAULT);
    this.operationTimeout = conf.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
    this.connectTimeout = conf.getInt(SOCKET_TIMEOUT_CONNECT, DEFAULT_SOCKET_TIMEOUT_CONNECT);

    String className = conf.get(Constants.HBASE_THRIFT_CLIENT_BUIDLER_CLASS,
        DefaultThriftClientBuilder.class.getName());
    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> constructor = clazz
          .getDeclaredConstructor(ThriftConnection.class);
      constructor.setAccessible(true);
      clientBuilder = (ThriftClientBuilder) constructor.newInstance(this);
    }catch (Exception e) {
      throw new IOException(e);
    }
  }

  public synchronized void setHttpClient(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public boolean isFramed() {
    return isFramed;
  }

  public boolean isCompact() {
    return isCompact;
  }

  public int getOperationTimeout() {
    return operationTimeout;
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public ThriftClientBuilder getClientBuilder() {
    return clientBuilder;
  }

  /**
   * the default thrift client builder.
   * One can extend the ThriftClientBuilder to builder custom client, implement
   * features like authentication(hbase-examples/thrift/DemoClient)
   *
   */
  public static class DefaultThriftClientBuilder extends ThriftClientBuilder  {

    @Override
    public Pair<THBaseService.Client, TTransport> getClient() throws IOException {
      TTransport tTransport = null;
      try {
        TSocket sock = new TSocket(connection.getHost(), connection.getPort());
        sock.setSocketTimeout(connection.getOperationTimeout());
        sock.setConnectTimeout(connection.getConnectTimeout());
        tTransport = sock;
        if (connection.isFramed()) {
          tTransport = new TFramedTransport(tTransport);
        }

        sock.open();
      } catch (TTransportException e) {
        throw new IOException(e);
      }
      TProtocol prot;
      if (connection.isCompact()) {
        prot = new TCompactProtocol(tTransport);
      } else {
        prot = new TBinaryProtocol(tTransport);
      }
      THBaseService.Client client = new THBaseService.Client(prot);
      return new Pair<>(client, tTransport);
    }

    public DefaultThriftClientBuilder(ThriftConnection connection) {
      super(connection);
    }
  }

  /**
   * the default thrift http client builder.
   * One can extend the ThriftClientBuilder to builder custom http client, implement
   * features like authentication or 'DoAs'(hbase-examples/thrift/HttpDoAsClient)
   *
   */
  public static class HTTPThriftClientBuilder extends ThriftClientBuilder {
    Map<String,String> customHeader = new HashMap<>();

    public HTTPThriftClientBuilder(ThriftConnection connection) {
      super(connection);
    }

    public void addCostumHeader(String key, String value) {
      customHeader.put(key, value);
    }

    @Override
    public Pair<THBaseService.Client, TTransport> getClient() throws IOException {
      Preconditions.checkArgument(connection.getHost().startsWith("http"),
          "http client host must start with http or https");
      String url = connection.getHost() + ":" + connection.getPort();
      try {
        THttpClient httpClient = new THttpClient(url, connection.getHttpClient());
        for (Map.Entry<String, String> header : customHeader.entrySet()) {
          httpClient.setCustomHeader(header.getKey(), header.getValue());
        }
        httpClient.open();
        TProtocol prot = new TBinaryProtocol(httpClient);
        THBaseService.Client client = new THBaseService.Client(prot);
        return new Pair<>(client, httpClient);
      } catch (TTransportException e) {
        throw new IOException(e);
      }

    }
  }

  /**
   * Get a ThriftAdmin, ThriftAdmin is NOT thread safe
   * @return a ThriftAdmin
   * @throws IOException IOException
   */
  @Override
  public Admin getAdmin() throws IOException {
    Pair<THBaseService.Client, TTransport> client = clientBuilder.getClient();
    return new ThriftAdmin(client.getFirst(), client.getSecond(), conf);
  }

  public static class DelayRetryHandler extends DefaultHttpRequestRetryHandler {
    private long pause;

    public DelayRetryHandler(int retryCount, long pause) {
      super(retryCount, true, Arrays.asList(
          InterruptedIOException.class,
          UnknownHostException.class,
          SSLException.class));
      this.pause = pause;
    }

    @Override
    public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
      // Don't sleep for retrying the first time
      if (executionCount > 1 && pause > 0) {
        try {
          long sleepTime = ConnectionUtils.getPauseTime(pause, executionCount - 1);
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          //reset interrupt marker
          Thread.currentThread().interrupt();
        }
      }
      return super.retryRequest(exception, executionCount, context);
    }

    @Override
    protected boolean handleAsIdempotent(HttpRequest request) {
      return true;
    }
  }

  public synchronized HttpClient getHttpClient() {
    if (httpClient != null) {
      return httpClient;
    }
    int retry = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    long pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE, 5);
    HttpClientBuilder builder = HttpClientBuilder.create();
    RequestConfig.Builder requestBuilder = RequestConfig.custom();
    requestBuilder = requestBuilder.setConnectTimeout(getConnectTimeout());
    requestBuilder = requestBuilder.setSocketTimeout(getOperationTimeout());
    builder.setRetryHandler(new DelayRetryHandler(retry, pause));
    builder.setDefaultRequestConfig(requestBuilder.build());
    httpClient = builder.build();
    httpClientCreated = true;
    return httpClient;
  }

  @Override
  public synchronized void close() throws IOException {
    if (httpClient != null && httpClientCreated) {
      HttpClientUtils.closeQuietly(httpClient);
    }
    isClosed = true;
  }

  @Override
  public boolean isClosed() {
    return isClosed;
  }

  /**
   * Get a TableBuider to build ThriftTable, ThriftTable is NOT thread safe
   * @return a TableBuilder
   * @throws IOException IOException
   */
  @Override
  public TableBuilder getTableBuilder(TableName tableName, ExecutorService pool) {
    return new TableBuilder() {
      @Override
      public TableBuilder setOperationTimeout(int timeout) {
        return this;
      }

      @Override
      public TableBuilder setRpcTimeout(int timeout) {
        return this;
      }

      @Override
      public TableBuilder setReadRpcTimeout(int timeout) {
        return this;
      }

      @Override
      public TableBuilder setWriteRpcTimeout(int timeout) {
        return this;
      }

      @Override
      public Table build() {
        try {
          Pair<THBaseService.Client, TTransport> client = clientBuilder.getClient();
          return new ThriftTable(tableName, client.getFirst(), client.getSecond(), conf);
        } catch (IOException ioE) {
          throw new RuntimeException(ioE);
        }

      }
    };
  }

  @Override
  public void abort(String why, Throwable e) {

  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public BufferedMutator getBufferedMutator(TableName tableName) throws IOException {
    throw new NotImplementedException("batchCoprocessorService not supported in ThriftTable");
  }

  @Override
  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) throws IOException {
    throw new NotImplementedException("batchCoprocessorService not supported in ThriftTable");
  }

  @Override
  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    throw new NotImplementedException("batchCoprocessorService not supported in ThriftTable");
  }

  @Override
  public void clearRegionLocationCache() {
    throw new NotImplementedException("clearRegionLocationCache not supported in ThriftTable");
  }
}
