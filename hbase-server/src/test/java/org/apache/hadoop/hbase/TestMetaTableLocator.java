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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.net.ConnectException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test {@link org.apache.hadoop.hbase.zookeeper.MetaTableLocator}
 */
@Category(MediumTests.class)
public class TestMetaTableLocator {
  private static final Log LOG = LogFactory.getLog(TestMetaTableLocator.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final ServerName SN =
      ServerName.valueOf("example.org", 1234, System.currentTimeMillis());
  private ZooKeeperWatcher watcher;
  private Abortable abortable;

  @BeforeClass public static void beforeClass() throws Exception {
    // Set this down so tests run quicker
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    UTIL.startMiniZKCluster();
  }

  @AfterClass public static void afterClass() throws IOException {
    UTIL.getZkCluster().shutdown();
  }

  @Before public void before() throws IOException {
    this.abortable = new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.info(why, e);
      }

      @Override
      public boolean isAborted()  {
        return false;
      }
    };
    this.watcher = new ZooKeeperWatcher(UTIL.getConfiguration(),
      this.getClass().getSimpleName(), this.abortable, true);
  }

  @After public void after() {
    try {
      // Clean out meta location or later tests will be confused... they presume
      // start fresh in zk.
      new MetaTableLocator().deleteMetaLocation(this.watcher);
    } catch (KeeperException e) {
      LOG.warn("Unable to delete hbase:meta location", e);
    }

    this.watcher.close();
  }

  /**
   * Test normal operations
   */
  @Test public void testMetaLookup()
          throws IOException, InterruptedException, ServiceException, KeeperException {
    final ClientProtos.ClientService.BlockingInterface client =
            Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);

    Mockito.when(client.get((RpcController)Mockito.any(), (GetRequest)Mockito.any())).
            thenReturn(GetResponse.newBuilder().build());

    final MetaTableLocator mtl = new MetaTableLocator();
    assertNull(mtl.getMetaRegionLocation(this.watcher));
    for (RegionState.State state : RegionState.State.values()) {
      if (state.equals(RegionState.State.OPEN))
        continue;
      MetaTableLocator.setMetaLocation(this.watcher, SN, state);
      assertNull(mtl.getMetaRegionLocation(this.watcher));
      assertEquals(state, MetaTableLocator.getMetaRegionState(this.watcher).getState());
    }
    MetaTableLocator.setMetaLocation(this.watcher, SN, RegionState.State.OPEN);
    assertEquals(mtl.getMetaRegionLocation(this.watcher), SN);
    assertEquals(RegionState.State.OPEN,
      MetaTableLocator.getMetaRegionState(this.watcher).getState());

    mtl.deleteMetaLocation(this.watcher);
    assertNull(MetaTableLocator.getMetaRegionState(this.watcher).getServerName());
    assertEquals(MetaTableLocator.getMetaRegionState(this.watcher).getState(),
      RegionState.State.OFFLINE);
    assertNull(mtl.getMetaRegionLocation(this.watcher));
  }


  /**
   * Test interruptable while blocking wait on meta.
   * @throws IOException
   * @throws ServiceException
   * @throws InterruptedException
   */
  @Test public void testInterruptWaitOnMeta()
  throws IOException, InterruptedException, ServiceException {
    final ClientProtos.ClientService.BlockingInterface client =
      Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);

    Mockito.when(client.get((RpcController)Mockito.any(), (GetRequest)Mockito.any())).
    thenReturn(GetResponse.newBuilder().build());

    final MetaTableLocator mtl = new MetaTableLocator();
    ServerName meta = new MetaTableLocator().getMetaRegionLocation(this.watcher);
    assertNull(meta);
    Thread t = new Thread() {
      @Override
      public void run() {
        try {
          mtl.waitMetaRegionLocation(watcher);
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted", e);
        }
      }
    };
    t.start();
    while (!t.isAlive())
      Threads.sleep(1);
    Threads.sleep(1);
    assertTrue(t.isAlive());
    mtl.stop();
    // Join the thread... should exit shortly.
    t.join();
  }

  private void testVerifyMetaRegionLocationWithException(Exception ex)
  throws IOException, InterruptedException, KeeperException, ServiceException {
    // Mock an ClientProtocol.
    final ClientProtos.ClientService.BlockingInterface implementation =
      Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
 
    ClusterConnection connection = mockConnection(null, implementation);

    // If a 'get' is called on mocked interface, throw connection refused.
    Mockito.when(implementation.get((RpcController) Mockito.any(), (GetRequest) Mockito.any())).
      thenThrow(new ServiceException(ex));

    long timeout = UTIL.getConfiguration().
            getLong("hbase.catalog.verification.timeout", 1000);
    MetaTableLocator.setMetaLocation(this.watcher, SN, RegionState.State.OPENING);
    assertFalse(new MetaTableLocator().verifyMetaRegionLocation(
      connection, watcher, timeout));

    MetaTableLocator.setMetaLocation(this.watcher, SN, RegionState.State.OPEN);
    assertFalse(new MetaTableLocator().verifyMetaRegionLocation(
            connection, watcher, timeout));
  }

  /**
   * Test we survive a connection refused {@link ConnectException}
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ServiceException
   */
  @Test
  public void testGetMetaServerConnectionFails()
  throws IOException, InterruptedException, KeeperException, ServiceException {
    testVerifyMetaRegionLocationWithException(new ConnectException("Connection refused"));
  }

  /**
   * Test that verifyMetaRegionLocation properly handles getting a
   * ServerNotRunningException. See HBASE-4470.
   * Note this doesn't check the exact exception thrown in the
   * HBASE-4470 as there it is thrown from getHConnection() and
   * here it is thrown from get() -- but those are both called
   * from the same function anyway, and this way is less invasive than
   * throwing from getHConnection would be.
   *
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ServiceException
   */
  @Test
  public void testVerifyMetaRegionServerNotRunning()
  throws IOException, InterruptedException, KeeperException, ServiceException {
    testVerifyMetaRegionLocationWithException(new ServerNotRunningYetException("mock"));
  }

  /**
   * Test get of meta region fails properly if nothing to connect to.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   * @throws ServiceException
   */
  @Test
  public void testVerifyMetaRegionLocationFails()
  throws IOException, InterruptedException, KeeperException, ServiceException {
    ClusterConnection connection = Mockito.mock(ClusterConnection.class);
    ServiceException connectException =
      new ServiceException(new ConnectException("Connection refused"));
    final AdminProtos.AdminService.BlockingInterface implementation =
      Mockito.mock(AdminProtos.AdminService.BlockingInterface.class);
    Mockito.when(implementation.getRegionInfo((RpcController)Mockito.any(),
      (GetRegionInfoRequest)Mockito.any())).thenThrow(connectException);
    Mockito.when(connection.getAdmin(Mockito.any(ServerName.class))).
      thenReturn(implementation);

    ServerName sn = ServerName.valueOf("example.com", 1234, System.currentTimeMillis());
    MetaTableLocator.setMetaLocation(this.watcher,
            sn,
            RegionState.State.OPENING);
    assertFalse(new MetaTableLocator().verifyMetaRegionLocation(connection, watcher, 100));
    MetaTableLocator.setMetaLocation(this.watcher, sn, RegionState.State.OPEN);
    assertFalse(new MetaTableLocator().verifyMetaRegionLocation(connection, watcher, 100));
  }

  @Test (expected = NotAllMetaRegionsOnlineException.class)
  public void testTimeoutWaitForMeta()
  throws IOException, InterruptedException {
    new MetaTableLocator().waitMetaRegionLocation(watcher, 100);
  }

  /**
   * Test waiting on meat w/ no timeout specified.
   * @throws IOException
   * @throws InterruptedException
   * @throws KeeperException
   */
  @Test public void testNoTimeoutWaitForMeta()
  throws IOException, InterruptedException, KeeperException {
    final MetaTableLocator mtl = new MetaTableLocator();
    ServerName hsa = mtl.getMetaRegionLocation(watcher);
    assertNull(hsa);

    // Now test waiting on meta location getting set.
    Thread t = new WaitOnMetaThread();
    startWaitAliveThenWaitItLives(t, 1);
    // Set a meta location.
    MetaTableLocator.setMetaLocation(this.watcher, SN, RegionState.State.OPEN);
    hsa = SN;
    // Join the thread... should exit shortly.
    t.join();
    // Now meta is available.
    assertTrue(mtl.getMetaRegionLocation(watcher).equals(hsa));
  }

  /**
   * @param admin An {@link AdminProtos.AdminService.BlockingInterface} instance; you'll likely
   * want to pass a mocked HRS; can be null.
   * @param client A mocked ClientProtocol instance, can be null
   * @return Mock up a connection that returns a {@link Configuration} when
   * {@link HConnection#getConfiguration()} is called, a 'location' when
   * {@link HConnection#getRegionLocation(byte[], byte[], boolean)} is called,
   * and that returns the passed {@link AdminProtos.AdminService.BlockingInterface} instance when
   * {@link HConnection#getAdmin(ServerName)} is called, returns the passed
   * {@link ClientProtos.ClientService.BlockingInterface} instance when
   * {@link HConnection#getClient(ServerName)} is called.
   * @throws IOException
   */
  private ClusterConnection mockConnection(final AdminProtos.AdminService.BlockingInterface admin,
      final ClientProtos.ClientService.BlockingInterface client)
  throws IOException {
    ClusterConnection connection =
      HConnectionTestingUtility.getMockedConnection(UTIL.getConfiguration());
    Mockito.doNothing().when(connection).close();
    // Make it so we return any old location when asked.
    final HRegionLocation anyLocation = new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO, SN);
    Mockito.when(connection.getRegionLocation((TableName) Mockito.any(),
        (byte[]) Mockito.any(), Mockito.anyBoolean())).
      thenReturn(anyLocation);
    Mockito.when(connection.locateRegion((TableName) Mockito.any(),
        (byte[]) Mockito.any())).
      thenReturn(anyLocation);
    if (admin != null) {
      // If a call to getHRegionConnection, return this implementation.
      Mockito.when(connection.getAdmin(Mockito.any(ServerName.class))).
        thenReturn(admin);
    }
    if (client != null) {
      // If a call to getClient, return this implementation.
      Mockito.when(connection.getClient(Mockito.any(ServerName.class))).
        thenReturn(client);
    }
    return connection;
  }

  private void startWaitAliveThenWaitItLives(final Thread t, final int ms) {
    t.start();
    while(!t.isAlive()) {
      // Wait
    }
    // Wait one second.
    Threads.sleep(ms);
    assertTrue("Assert " + t.getName() + " still waiting", t.isAlive());
  }

  /**
   * Wait on META.
   */
  class WaitOnMetaThread extends Thread {

    WaitOnMetaThread() {
      super("WaitOnMeta");
    }

    @Override
    public void run() {
      try {
        doWaiting();
      } catch (InterruptedException e) {
        throw new RuntimeException("Failed wait", e);
      }
      LOG.info("Exiting " + getName());
    }

    void doWaiting() throws InterruptedException {
      try {
        while (new MetaTableLocator().waitMetaRegionLocation(watcher, 10000) == null);
      } catch (NotAllMetaRegionsOnlineException e) {
        //Ignore
      }
    }
  }
}
