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
package org.apache.hadoop.hbase.rsgroup;

import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.ConnectException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;

@Category({ MiscTests.class, MediumTests.class })
public class TestUtility {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestUtility.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestUtility.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final ServerName SN =
    ServerName.valueOf("example.org", 1234, System.currentTimeMillis());

  private ZKWatcher watcher;

  private Abortable abortable;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Set this down so tests run quicker
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void afterClass() throws IOException {
    UTIL.getZkCluster().shutdown();
  }

  @Before
  public void before() throws IOException {
    this.abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.info(why, e);
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    };
    this.watcher =
      new ZKWatcher(UTIL.getConfiguration(), this.getClass().getSimpleName(), this.abortable, true);
  }

  @After
  public void after() {
    try {
      // Clean out meta location or later tests will be confused... they presume
      // start fresh in zk.
      MetaTableLocator.deleteMetaLocation(this.watcher);
    } catch (KeeperException e) {
      LOG.warn("Unable to delete hbase:meta location", e);
    }

    this.watcher.close();
  }

  /**
   * @param admin An {@link AdminProtos.AdminService.BlockingInterface} instance; you'll likely want
   *          to pass a mocked HRS; can be null.
   * @param client A mocked ClientProtocol instance, can be null
   * @return Mock up a connection that returns a {@link Configuration} when
   *         {@link org.apache.hadoop.hbase.client.ClusterConnection#getConfiguration()} is called,
   *         a 'location' when
   *         {@link org.apache.hadoop.hbase.client.RegionLocator#getRegionLocation(byte[], boolean)}
   *         is called, and that returns the passed
   *         {@link AdminProtos.AdminService.BlockingInterface} instance when
   *         {@link org.apache.hadoop.hbase.client.ClusterConnection#getAdmin(ServerName)} is
   *         called, returns the passed {@link ClientProtos.ClientService.BlockingInterface}
   *         instance when
   *         {@link org.apache.hadoop.hbase.client.ClusterConnection#getClient(ServerName)} is
   *         called.
   */
  private ClusterConnection mockConnection(final AdminProtos.AdminService.BlockingInterface admin,
      final ClientProtos.ClientService.BlockingInterface client) throws IOException {
    ClusterConnection connection =
      HConnectionTestingUtility.getMockedConnection(UTIL.getConfiguration());
    Mockito.doNothing().when(connection).close();
    // Make it so we return any old location when asked.
    final HRegionLocation anyLocation =
      new HRegionLocation(RegionInfoBuilder.FIRST_META_REGIONINFO, SN);
    Mockito.when(connection.getRegionLocation((TableName) Mockito.any(), (byte[]) Mockito.any(),
      Mockito.anyBoolean())).thenReturn(anyLocation);
    Mockito.when(connection.locateRegion((TableName) Mockito.any(), (byte[]) Mockito.any()))
      .thenReturn(anyLocation);
    if (admin != null) {
      // If a call to getHRegionConnection, return this implementation.
      Mockito.when(connection.getAdmin(Mockito.any())).thenReturn(admin);
    }
    if (client != null) {
      // If a call to getClient, return this implementation.
      Mockito.when(connection.getClient(Mockito.any())).thenReturn(client);
    }
    return connection;
  }

  private void testVerifyMetaRegionLocationWithException(Exception ex)
      throws IOException, InterruptedException, KeeperException, ServiceException {
    // Mock an ClientProtocol.
    final ClientProtos.ClientService.BlockingInterface implementation =
      Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);

    ClusterConnection connection = mockConnection(null, implementation);

    // If a 'get' is called on mocked interface, throw connection refused.
    Mockito.when(implementation.get((RpcController) Mockito.any(), (GetRequest) Mockito.any()))
      .thenThrow(new ServiceException(ex));

    long timeout = UTIL.getConfiguration().getLong("hbase.catalog.verification.timeout", 1000);
    MetaTableLocator.setMetaLocation(this.watcher, SN, RegionState.State.OPENING);
    assertFalse(Utility.verifyMetaRegionLocation(connection, watcher, timeout));

    MetaTableLocator.setMetaLocation(this.watcher, SN, RegionState.State.OPEN);
    assertFalse(Utility.verifyMetaRegionLocation(connection, watcher, timeout));
  }

  /**
   * Test get of meta region fails properly if nothing to connect to.
   */
  @Test
  public void testVerifyMetaRegionLocationFails()
      throws IOException, InterruptedException, KeeperException, ServiceException {
    ClusterConnection connection = Mockito.mock(ClusterConnection.class);
    ServiceException connectException =
      new ServiceException(new ConnectException("Connection refused"));
    final AdminProtos.AdminService.BlockingInterface implementation =
      Mockito.mock(AdminProtos.AdminService.BlockingInterface.class);
    Mockito.when(implementation.getRegionInfo((RpcController) Mockito.any(),
      (GetRegionInfoRequest) Mockito.any())).thenThrow(connectException);
    Mockito.when(connection.getAdmin(Mockito.any())).thenReturn(implementation);
    RpcControllerFactory controllerFactory = Mockito.mock(RpcControllerFactory.class);
    Mockito.when(controllerFactory.newController())
      .thenReturn(Mockito.mock(HBaseRpcController.class));
    Mockito.when(connection.getRpcControllerFactory()).thenReturn(controllerFactory);

    ServerName sn = ServerName.valueOf("example.com", 1234, System.currentTimeMillis());
    MetaTableLocator.setMetaLocation(this.watcher, sn, RegionState.State.OPENING);
    assertFalse(Utility.verifyMetaRegionLocation(connection, watcher, 100));
    MetaTableLocator.setMetaLocation(this.watcher, sn, RegionState.State.OPEN);
    assertFalse(Utility.verifyMetaRegionLocation(connection, watcher, 100));
  }

  /**
   * Test we survive a connection refused {@link ConnectException}
   */
  @Test
  public void testGetMetaServerConnectionFails()
      throws IOException, InterruptedException, KeeperException, ServiceException {
    testVerifyMetaRegionLocationWithException(new ConnectException("Connection refused"));
  }

  /**
   * Test that verifyMetaRegionLocation properly handles getting a ServerNotRunningException. See
   * HBASE-4470. Note this doesn't check the exact exception thrown in the HBASE-4470 as there it is
   * thrown from getHConnection() and here it is thrown from get() -- but those are both called from
   * the same function anyway, and this way is less invasive than throwing from getHConnection would
   * be.
   */
  @Test
  public void testVerifyMetaRegionServerNotRunning()
      throws IOException, InterruptedException, KeeperException, ServiceException {
    testVerifyMetaRegionLocationWithException(new ServerNotRunningYetException("mock"));
  }
}
