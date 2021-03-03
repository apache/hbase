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
package org.apache.hadoop.hbase.replication.replicationserver;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestReplicationQueueListener {

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationQueueListener.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationQueueListener.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration CONF;

  private static ZKReplicationQueueStorage queueStorage;

  private static final String PEER_ID = "1";

  private static Path walRootDir;

  private AtomicInteger fileCounter = new AtomicInteger();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(1);
    CONF = UTIL.getConfiguration();
    queueStorage = new ZKReplicationQueueStorage(UTIL.getZooKeeperWatcher(), CONF);
    walRootDir = CommonFSUtils.getWALRootDir(UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testEnqueueLog() throws Exception {
    Optional<HRegionServer> rs =
      UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().stream().findFirst()
        .map(JVMClusterUtil.RegionServerThread::getRegionServer);
    Assert.assertTrue(rs.isPresent());
    ServerName serverName = rs.get().getServerName();

    ReplicationSourceInterface src = mock(ReplicationSourceInterface.class);
    when(src.getQueueOwner()).thenReturn(serverName);
    when(src.getQueueId()).thenReturn(PEER_ID);

    ReplicationQueueListener listener =
      new ReplicationQueueListener(src, queueStorage, walRootDir, new DummyAbortable());
    // put some initial WALs in queue before listener starting watching
    List<String> files = addSomeWalsIntoQueue(serverName);
    listener.start(queueStorage.getWALsInQueue(serverName, PEER_ID));
    Threads.sleep(1000);
    for (String file : files) {
      verify(src, never()).enqueueLog(buildWalPath(serverName, file));
    }

    // region server generates multi WALs, and listener can always watch that.
    files = addSomeWalsIntoQueue(serverName);
    Threads.sleep(1000);
    for (String file : files) {
      verify(src, Mockito.times(1)).enqueueLog(buildWalPath(serverName, file));
    }
  }

  private Path buildWalPath(ServerName serverName, String fileName) {
    Path walDir = new Path(walRootDir,
      AbstractFSWALProvider.getWALDirectoryName(serverName.toString()));
    return new Path(walDir, fileName);
  }

  private List<String> addSomeWalsIntoQueue(ServerName rs) throws Exception {
    List<String> files = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      String file = "file." + fileCounter.getAndIncrement();
      files.add(file);
      queueStorage.addWAL(rs, PEER_ID, file);
    }
    return files;
  }

  public static class DummyAbortable implements Abortable {

    @Override
    public void abort(String why, Throwable e) {
      LOG.error(why, e);
      throw new RuntimeException(e);
    }

    @Override
    public boolean isAborted() {
      return false;
    }
  }

}