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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Threads;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;

/**
 * Test {@link org.apache.hadoop.hbase.zookeeper.MetaTableLocator}
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestMetaTableLocator {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaTableLocator.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaTableLocator.class);
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
   * Test normal operations
   */
  @Test
  public void testMetaLookup()
      throws IOException, InterruptedException, ServiceException, KeeperException {
    final ClientProtos.ClientService.BlockingInterface client =
      Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);

    Mockito.when(client.get((RpcController) Mockito.any(), (GetRequest) Mockito.any()))
      .thenReturn(GetResponse.newBuilder().build());

    assertNull(MetaTableLocator.getMetaRegionLocation(this.watcher));
    for (RegionState.State state : RegionState.State.values()) {
      if (state.equals(RegionState.State.OPEN)) {
        continue;
      }
      MetaTableLocator.setMetaLocation(this.watcher, SN, state);
      assertNull(MetaTableLocator.getMetaRegionLocation(this.watcher));
      assertEquals(state, MetaTableLocator.getMetaRegionState(this.watcher).getState());
    }
    MetaTableLocator.setMetaLocation(this.watcher, SN, RegionState.State.OPEN);
    assertEquals(SN, MetaTableLocator.getMetaRegionLocation(this.watcher));
    assertEquals(RegionState.State.OPEN,
      MetaTableLocator.getMetaRegionState(this.watcher).getState());

    MetaTableLocator.deleteMetaLocation(this.watcher);
    assertNull(MetaTableLocator.getMetaRegionState(this.watcher).getServerName());
    assertEquals(RegionState.State.OFFLINE,
      MetaTableLocator.getMetaRegionState(this.watcher).getState());
    assertNull(MetaTableLocator.getMetaRegionLocation(this.watcher));
  }

  @Test(expected = NotAllMetaRegionsOnlineException.class)
  public void testTimeoutWaitForMeta() throws IOException, InterruptedException {
    MetaTableLocator.waitMetaRegionLocation(watcher, 100);
  }

  /**
   * Test waiting on meat w/ no timeout specified.
   */
  @Test
  public void testNoTimeoutWaitForMeta() throws IOException, InterruptedException, KeeperException {
    ServerName hsa = MetaTableLocator.getMetaRegionLocation(watcher);
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
    assertTrue(MetaTableLocator.getMetaRegionLocation(watcher).equals(hsa));
  }

  private void startWaitAliveThenWaitItLives(final Thread t, final int ms) {
    t.start();
    UTIL.waitFor(2000, t::isAlive);
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
        for (;;) {
          if (MetaTableLocator.waitMetaRegionLocation(watcher, 10000) != null) {
            break;
          }
        }
      } catch (NotAllMetaRegionsOnlineException e) {
        // Ignore
      }
    }
  }
}
