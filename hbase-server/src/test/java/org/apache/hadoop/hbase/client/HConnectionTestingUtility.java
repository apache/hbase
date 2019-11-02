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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.client.ConnectionManager.HConnectionImplementation;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * {@link ClusterConnection} testing utility.
 */
public class HConnectionTestingUtility {
  /*
   * Not part of {@link HBaseTestingUtility} because this class is not
   * in same package as {@link HConnection}.  Would have to reveal ugly
   * {@link HConnectionManager} innards to HBaseTestingUtility to give it access.
   */
  /**
   * Get a Mocked {@link HConnection} that goes with the passed <code>conf</code>
   * configuration instance.  Minimally the mock will return
   * <code>conf</conf> when {@link ClusterConnection#getConfiguration()} is invoked.
   * Be sure to shutdown the connection when done by calling
   * {@link HConnectionManager#deleteConnection(Configuration)} else it
   * will stick around; this is probably not what you want.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static ClusterConnection getMockedConnection(final Configuration conf)
  throws ZooKeeperConnectionException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (ConnectionManager.CONNECTION_INSTANCES) {
      HConnectionImplementation connection =
          ConnectionManager.CONNECTION_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = Mockito.mock(HConnectionImplementation.class);
        Mockito.when(connection.getConfiguration()).thenReturn(conf);
        Mockito.when(connection.getRpcControllerFactory()).thenReturn(
        Mockito.mock(RpcControllerFactory.class));
        // we need a real retrying caller
        RpcRetryingCallerFactory callerFactory = new RpcRetryingCallerFactory(conf);
        Mockito.when(connection.getRpcRetryingCallerFactory()).thenReturn(callerFactory);
        ConnectionManager.CONNECTION_INSTANCES.put(connectionKey, connection);
      }
      return connection;
    }
  }

  /**
   * @param connection
   */
  private static void mockRegionLocator(final HConnectionImplementation connection) {
    try {
      Mockito.when(connection.getRegionLocator(Mockito.any(TableName.class))).thenAnswer(
          new Answer<RegionLocator>() {
            @Override
            public RegionLocator answer(InvocationOnMock invocation) throws Throwable {
              TableName tableName = (TableName) invocation.getArguments()[0];
              return new HRegionLocator(tableName, connection);
            }
          });
    } catch (IOException e) {
    }
  }

  /**
   * Calls {@link #getMockedConnection(Configuration)} and then mocks a few
   * more of the popular {@link ClusterConnection} methods so they do 'normal'
   * operation (see return doc below for list). Be sure to shutdown the
   * connection when done by calling
   * {@link HConnectionManager#deleteConnection(Configuration)} else it
   * will stick around; this is probably not what you want.
   *
   * @param conf Configuration to use
   * @param admin An AdminProtocol; can be null but is usually
   * itself a mock.
   * @param client A ClientProtocol; can be null but is usually
   * itself a mock.
   * @param sn ServerName to include in the region location returned by this
   * <code>connection</code>
   * @param hri HRegionInfo to include in the location returned when
   * getRegionLocator is called on the mocked connection
   * @return Mock up a connection that returns a {@link Configuration} when
   * {@link ClusterConnection#getConfiguration()} is called, a 'location' when
   * {@link ClusterConnection#getRegionLocation(org.apache.hadoop.hbase.TableName, byte[], boolean)}
   * is called,
   * and that returns the passed {@link AdminProtos.AdminService.BlockingInterface} instance when
   * {@link ClusterConnection#getAdmin(ServerName)} is called, returns the passed
   * {@link ClientProtos.ClientService.BlockingInterface} instance when
   * {@link ClusterConnection#getClient(ServerName)} is called (Be sure to call
   * {@link HConnectionManager#deleteConnection(Configuration)}
   * when done with this mocked Connection.
   * @throws IOException
   */
  public static ClusterConnection getMockedConnectionAndDecorate(final Configuration conf,
      final AdminProtos.AdminService.BlockingInterface admin,
      final ClientProtos.ClientService.BlockingInterface client,
      final ServerName sn, final HRegionInfo hri)
  throws IOException {
    HConnectionImplementation c = Mockito.mock(HConnectionImplementation.class);
    Mockito.when(c.getConfiguration()).thenReturn(conf);
    ConnectionManager.CONNECTION_INSTANCES.put(new HConnectionKey(conf), c);
    Mockito.doNothing().when(c).close();
    // Make it so we return a particular location when asked.
    final HRegionLocation loc = new HRegionLocation(hri, sn);
    mockRegionLocator(c);
    Mockito.when(c.getRegionLocation((TableName) Mockito.any(),
        (byte[]) Mockito.any(), Mockito.anyBoolean())).
      thenReturn(loc);
    Mockito.when(c.locateRegion((TableName) Mockito.any(), (byte[]) Mockito.any())).
      thenReturn(loc);
    Mockito.when(c.locateRegion((TableName) Mockito.any(), (byte[]) Mockito.any(),
        Mockito.anyBoolean(), Mockito.anyBoolean(),  Mockito.anyInt()))
        .thenReturn(new RegionLocations(loc));
    if (admin != null) {
      // If a call to getAdmin, return this implementation.
      Mockito.when(c.getAdmin(Mockito.any(ServerName.class))).
        thenReturn(admin);
    }
    if (client != null) {
      // If a call to getClient, return this client.
      Mockito.when(c.getClient(Mockito.any(ServerName.class))).
        thenReturn(client);
    }
    NonceGenerator ng = Mockito.mock(NonceGenerator.class);
    Mockito.when(c.getNonceGenerator()).thenReturn(ng);
    AsyncProcess asyncProcess =
        new AsyncProcess(c, conf, null, RpcRetryingCallerFactory.instantiate(conf), false,
          RpcControllerFactory.instantiate(conf), conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
            HConstants.DEFAULT_HBASE_RPC_TIMEOUT));
    Mockito.when(c.getAsyncProcess()).thenReturn(asyncProcess);
    Mockito.doNothing().when(c).incCount();
    Mockito.doNothing().when(c).decCount();
    Mockito.when(c.getNewRpcRetryingCallerFactory(conf)).thenReturn(
        RpcRetryingCallerFactory.instantiate(conf,
            RetryingCallerInterceptorFactory.NO_OP_INTERCEPTOR, null));
    Mockito.when(c.getRpcControllerFactory()).thenReturn(Mockito.mock(RpcControllerFactory.class));
    HTableInterface t = Mockito.mock(HTableInterface.class);
    Mockito.when(c.getTable((TableName)Mockito.any())).thenReturn(t);
    ResultScanner rs = Mockito.mock(ResultScanner.class);
    Mockito.when(t.getScanner((Scan)Mockito.any())).thenReturn(rs);
    return c;
  }

  /**
   * Get a Mockito spied-upon {@link ClusterConnection} that goes with the passed
   * <code>conf</code> configuration instance.
   * Be sure to shutdown the connection when done by calling
   * {@link HConnectionManager#deleteConnection(Configuration)} else it
   * will stick around; this is probably not what you want.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   * @see @link
   * {http://mockito.googlecode.com/svn/branches/1.6/javadoc/org/mockito/Mockito.html#spy(T)}
   */
  public static ClusterConnection getSpiedConnection(final Configuration conf)
  throws IOException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (ConnectionManager.CONNECTION_INSTANCES) {
      HConnectionImplementation connection =
          ConnectionManager.CONNECTION_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = Mockito.spy(new HConnectionImplementation(conf, true));
        ConnectionManager.CONNECTION_INSTANCES.put(connectionKey, connection);
      }
      return connection;
    }
  }

  public static ClusterConnection getSpiedClusterConnection(final Configuration conf)
  throws IOException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (ConnectionManager.CONNECTION_INSTANCES) {
      HConnectionImplementation connection =
          ConnectionManager.CONNECTION_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = Mockito.spy(new HConnectionImplementation(conf, true));
        ConnectionManager.CONNECTION_INSTANCES.put(connectionKey, connection);
      }
      return connection;
    }
  }

  /**
   * @return Count of extant connection instances
   */
  public static int getConnectionCount() {
    synchronized (ConnectionManager.CONNECTION_INSTANCES) {
      return ConnectionManager.CONNECTION_INSTANCES.size();
    }
  }

  /**
   * This coproceesor sleep 2s at first increment/append rpc call.
   */
  public static class SleepAtFirstRpcCall extends BaseRegionObserver {
    static final AtomicLong ct = new AtomicLong(0);
    static final String SLEEP_TIME_CONF_KEY =
        "hbase.coprocessor.SleepAtFirstRpcCall.sleepTime";
    static final long DEFAULT_SLEEP_TIME = 2000;
    static final AtomicLong sleepTime = new AtomicLong(DEFAULT_SLEEP_TIME);

    public SleepAtFirstRpcCall() {
    }

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
      RegionCoprocessorEnvironment env = e.getEnvironment();
      Configuration conf = env.getConfiguration();
      sleepTime.set(conf.getLong(SLEEP_TIME_CONF_KEY, DEFAULT_SLEEP_TIME));
    }

    @Override
    public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Increment increment, final Result result) throws IOException {
      if (ct.incrementAndGet() == 1) {
        Threads.sleep(sleepTime.get());
      }
      return result;
    }

    @Override
    public Result postAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
        final Append append, final Result result) throws IOException {
      if (ct.incrementAndGet() == 1) {
        Threads.sleep(sleepTime.get());
      }
      return result;
    }
  }

}
