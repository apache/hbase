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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.AdditionalAnswers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ZKTests.class, MediumTests.class })
public class TestZKUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestZKUtil.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestZKUtil.class);

  private static HBaseZKTestingUtil UTIL = new HBaseZKTestingUtil();

  private static ZKWatcher ZKW;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniZKCluster().getClientPort();
    ZKW = new ZKWatcher(new Configuration(UTIL.getConfiguration()), TestZKUtil.class.getName(),
      new WarnOnlyAbortable());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    Closeables.close(ZKW, true);
    UTIL.shutdownMiniZKCluster();
    UTIL.cleanupTestDir();
  }

  /**
   * Create a znode with data
   */
  @Test
  public void testCreateWithParents() throws KeeperException, InterruptedException {
    byte[] expectedData = new byte[] { 1, 2, 3 };
    ZKUtil.createWithParents(ZKW, "/l1/l2/l3/l4/testCreateWithParents", expectedData);
    byte[] data = ZKUtil.getData(ZKW, "/l1/l2/l3/l4/testCreateWithParents");
    assertTrue(Bytes.equals(expectedData, data));
    ZKUtil.deleteNodeRecursively(ZKW, "/l1");

    ZKUtil.createWithParents(ZKW, "/testCreateWithParents", expectedData);
    data = ZKUtil.getData(ZKW, "/testCreateWithParents");
    assertTrue(Bytes.equals(expectedData, data));
    ZKUtil.deleteNodeRecursively(ZKW, "/testCreateWithParents");
  }

  /**
   * Create a bunch of znodes in a hierarchy, try deleting one that has childs (it will fail), then
   * delete it recursively, then delete the last znode
   */
  @Test
  public void testZNodeDeletes() throws Exception {
    ZKUtil.createWithParents(ZKW, "/l1/l2/l3/l4");
    try {
      ZKUtil.deleteNode(ZKW, "/l1/l2");
      fail("We should not be able to delete if znode has childs");
    } catch (KeeperException ex) {
      assertNotNull(ZKUtil.getDataNoWatch(ZKW, "/l1/l2/l3/l4", null));
    }
    ZKUtil.deleteNodeRecursively(ZKW, "/l1/l2");
    // make sure it really is deleted
    assertNull(ZKUtil.getDataNoWatch(ZKW, "/l1/l2/l3/l4", null));

    // do the same delete again and make sure it doesn't crash
    ZKUtil.deleteNodeRecursively(ZKW, "/l1/l2");

    ZKUtil.deleteNode(ZKW, "/l1");
    assertNull(ZKUtil.getDataNoWatch(ZKW, "/l1/l2", null));
  }

  private int getZNodeDataVersion(String znode) throws KeeperException {
    Stat stat = new Stat();
    ZKUtil.getDataNoWatch(ZKW, znode, stat);
    return stat.getVersion();
  }

  @Test
  public void testSetDataWithVersion() throws Exception {
    ZKUtil.createWithParents(ZKW, "/s1/s2/s3");
    int v0 = getZNodeDataVersion("/s1/s2/s3");
    assertEquals(0, v0);

    ZKUtil.setData(ZKW, "/s1/s2/s3", Bytes.toBytes(12L));
    int v1 = getZNodeDataVersion("/s1/s2/s3");
    assertEquals(1, v1);

    ZKUtil.multiOrSequential(ZKW,
      ImmutableList.of(ZKUtilOp.setData("/s1/s2/s3", Bytes.toBytes(13L), v1)), false);
    int v2 = getZNodeDataVersion("/s1/s2/s3");
    assertEquals(2, v2);
  }

  private <V> V callAndIgnoreTransientError(Callable<V> action) throws Exception {
    for (;;) {
      try {
        return action.call();
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case SESSIONEXPIRED:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception", e);
            Threads.sleep(100);
            break;
          default:
            throw e;
        }
      }
    }
  }

  /**
   * A test for HBASE-3238
   */
  @Test
  public void testCreateSilentIsReallySilent() throws Exception {
    Configuration c = UTIL.getConfiguration();

    String aclZnode = "/aclRoot";
    String quorumServers = ZKConfig.getZKQuorumServersString(c);
    int sessionTimeout = 5 * 1000; // 5 seconds
    try (ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout, EmptyWatcher.instance)) {
      zk.addAuthInfo("digest", Bytes.toBytes("hbase:rox"));

      // Save the previous ACL
      List<ACL> oldACL = callAndIgnoreTransientError(() -> zk.getACL("/", new Stat()));

      // I set this acl after the attempted creation of the cluster home node.
      // Add retries in case of retryable zk exceptions.
      callAndIgnoreTransientError(() -> zk.setACL("/", ZooDefs.Ids.CREATOR_ALL_ACL, -1));

      ZKWatcher watcher = spy(ZKW);
      RecoverableZooKeeper rzk = mock(RecoverableZooKeeper.class,
        AdditionalAnswers.delegatesTo(ZKW.getRecoverableZooKeeper()));
      when(watcher.getRecoverableZooKeeper()).thenReturn(rzk);
      AtomicBoolean firstExists = new AtomicBoolean(true);
      doAnswer(inv -> {
        String path = inv.getArgument(0);
        boolean watch = inv.getArgument(1);
        Stat stat = ZKW.getRecoverableZooKeeper().exists(path, watch);
        // create the znode after first exists check, this is to simulate that we enter the create
        // branch but we have no permission for creation, but the znode has been created by others
        if (firstExists.compareAndSet(true, false)) {
          callAndIgnoreTransientError(() -> zk.create(aclZnode, null,
            Arrays.asList(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE),
              new ACL(ZooDefs.Perms.ALL, ZooDefs.Ids.AUTH_IDS)),
            CreateMode.PERSISTENT));
        }
        return stat;
      }).when(rzk).exists(any(), anyBoolean());
      ZKUtil.createAndFailSilent(watcher, aclZnode);
      // make sure we call the exists method twice and create once
      verify(rzk, times(2)).exists(any(), anyBoolean());
      verify(rzk).create(anyString(), any(), anyList(), any());
      // Restore the ACL
      zk.addAuthInfo("digest", Bytes.toBytes("hbase:rox"));
      zk.setACL("/", oldACL, -1);
    }
  }

  /**
   * Test should not fail with NPE when getChildDataAndWatchForNewChildren invoked with wrongNode
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testGetChildDataAndWatchForNewChildrenShouldNotThrowNPE() throws Exception {
    ZKUtil.getChildDataAndWatchForNewChildren(ZKW, "/wrongNode");
  }

  private static class WarnOnlyAbortable implements Abortable {

    @Override
    public void abort(String why, Throwable e) {
      LOG.warn("ZKWatcher received abort, ignoring.  Reason: " + why);
      if (LOG.isDebugEnabled()) {
        LOG.debug(e.toString(), e);
      }
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }
}
