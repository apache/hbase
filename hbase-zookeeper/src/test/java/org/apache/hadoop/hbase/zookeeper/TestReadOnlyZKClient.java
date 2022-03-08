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
package org.apache.hadoop.hbase.zookeeper;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ZKTests.class, MediumTests.class })
public class TestReadOnlyZKClient {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReadOnlyZKClient.class);

  private static HBaseZKTestingUtility UTIL = new HBaseZKTestingUtility();

  private static String PATH = "/test";

  private static byte[] DATA;

  private static int CHILDREN = 5;

  private static ReadOnlyZKClient RO_ZK;

  @BeforeClass
  public static void setUp() throws Exception {
    final int port = UTIL.startMiniZKCluster().getClientPort();
    String hostPort = UTIL.getZkCluster().getAddress().toString();

    ZooKeeper zk = ZooKeeperHelper.getConnectedZooKeeper(hostPort, 10000);
    DATA = new byte[10];
    Bytes.random(DATA);
    zk.create(PATH, DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    for (int i = 0; i < CHILDREN; i++) {
      zk.create(PATH + "/c" + i, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    zk.close();
    Configuration conf = UTIL.getConfiguration();
    conf.set(HConstants.ZOOKEEPER_QUORUM, hostPort);
    conf.setInt(ReadOnlyZKClient.RECOVERY_RETRY, 3);
    conf.setInt(ReadOnlyZKClient.RECOVERY_RETRY_INTERVAL_MILLIS, 100);
    conf.setInt(ReadOnlyZKClient.KEEPALIVE_MILLIS, 3000);
    RO_ZK = new ReadOnlyZKClient(conf);
    // only connect when necessary
    assertNull(RO_ZK.zookeeper);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    RO_ZK.close();
    UTIL.shutdownMiniZKCluster();
    UTIL.cleanupTestDir();
  }

  private void waitForIdleConnectionClosed() throws Exception {
    // The zookeeper client should be closed finally after the keep alive time elapsed
    UTIL.waitFor(10000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() {
        return RO_ZK.zookeeper == null;
      }

      @Override
      public String explainFailure() {
        return "Connection to zookeeper is still alive";
      }
    });
  }

  @Test
  public void testRead() throws Exception {
    assertArrayEquals(DATA, RO_ZK.get(PATH).get());
    assertEquals(CHILDREN, RO_ZK.exists(PATH).get().getNumChildren());
    List<String> children = RO_ZK.list(PATH).get();
    assertEquals(CHILDREN, children.size());
    Collections.sort(children);
    for (int i = 0; i < CHILDREN; i++) {
      assertEquals("c" + i, children.get(i));
    }
    assertNotNull(RO_ZK.zookeeper);
    waitForIdleConnectionClosed();
  }

  @Test
  public void testNoNode() throws InterruptedException, ExecutionException {
    String pathNotExists = PATH + "_whatever";
    try {
      RO_ZK.get(pathNotExists).get();
      fail("should fail because of " + pathNotExists + " does not exist");
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(KeeperException.class));
      KeeperException ke = (KeeperException) e.getCause();
      assertEquals(Code.NONODE, ke.code());
      assertEquals(pathNotExists, ke.getPath());
    }
    try {
      RO_ZK.list(pathNotExists).get();
      fail("should fail because of " + pathNotExists + " does not exist");
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(KeeperException.class));
      KeeperException ke = (KeeperException) e.getCause();
      assertEquals(Code.NONODE, ke.code());
      assertEquals(pathNotExists, ke.getPath());
    }
    // exists will not throw exception.
    assertNull(RO_ZK.exists(pathNotExists).get());
  }

  @Test
  public void testSessionExpire() throws Exception {
    assertArrayEquals(DATA, RO_ZK.get(PATH).get());
    ZooKeeper zk = RO_ZK.zookeeper;
    long sessionId = zk.getSessionId();
    UTIL.getZkCluster().getZooKeeperServers().get(0).closeSession(sessionId);
    // should not reach keep alive so still the same instance
    assertSame(zk, RO_ZK.zookeeper);
    byte[] got = RO_ZK.get(PATH).get();
    assertArrayEquals(DATA, got);
    assertNotNull(RO_ZK.zookeeper);
    assertNotSame(zk, RO_ZK.zookeeper);
    assertNotEquals(sessionId, RO_ZK.zookeeper.getSessionId());
  }

  @Test
  public void testNotCloseZkWhenPending() throws Exception {
    ZooKeeper mockedZK = mock(ZooKeeper.class);
    Exchanger<AsyncCallback.DataCallback> exchanger = new Exchanger<>();
    doAnswer(i -> {
      exchanger.exchange(i.getArgument(2));
      return null;
    }).when(mockedZK).getData(anyString(), anyBoolean(),
      any(AsyncCallback.DataCallback.class), any());
    doAnswer(i -> null).when(mockedZK).close();
    when(mockedZK.getState()).thenReturn(ZooKeeper.States.CONNECTED);
    RO_ZK.zookeeper = mockedZK;
    CompletableFuture<byte[]> future = RO_ZK.get(PATH);
    AsyncCallback.DataCallback callback = exchanger.exchange(null);
    // 2 * keep alive time to ensure that we will not close the zk when there are pending requests
    Thread.sleep(6000);
    assertNotNull(RO_ZK.zookeeper);
    verify(mockedZK, never()).close();
    callback.processResult(Code.OK.intValue(), PATH, null, DATA, null);
    assertArrayEquals(DATA, future.get());
    // now we will close the idle connection.
    waitForIdleConnectionClosed();
    verify(mockedZK, times(1)).close();
  }
}
