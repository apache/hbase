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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ReplicationTests.class, MediumTests.class})
public class TestReplicationSource {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationSource.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReplicationSource.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL_PEER =
      new HBaseTestingUtility();
  private static FileSystem FS;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf = TEST_UTIL.getConfiguration();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(1);
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
    Path rootDir = TEST_UTIL.createRootDir();
    oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (FS.exists(oldLogDir)) FS.delete(oldLogDir, true);
    logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    if (FS.exists(logDir)) FS.delete(logDir, true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL_PEER.shutdownMiniHBaseCluster();
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  /**
   * Sanity check that we can move logs around while we are reading
   * from them. Should this test fail, ReplicationSource would have a hard
   * time reading logs that are being archived.
   * @throws Exception
   */
  @Test
  public void testLogMoving() throws Exception{
    Path logPath = new Path(logDir, "log");
    if (!FS.exists(logDir)) FS.mkdirs(logDir);
    if (!FS.exists(oldLogDir)) FS.mkdirs(oldLogDir);
    WALProvider.Writer writer = WALFactory.createWALWriter(FS, logPath,
        TEST_UTIL.getConfiguration());
    for(int i = 0; i < 3; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      WALKeyImpl key = new WALKeyImpl(b, TableName.valueOf(b), 0, 0,
          HConstants.DEFAULT_CLUSTER_ID);
      writer.append(new WAL.Entry(key, edit));
      writer.sync();
    }
    writer.close();

    WAL.Reader reader = WALFactory.createReader(FS, logPath, TEST_UTIL.getConfiguration());
    WAL.Entry entry = reader.next();
    assertNotNull(entry);

    Path oldLogPath = new Path(oldLogDir, "log");
    FS.rename(logPath, oldLogPath);

    entry = reader.next();
    assertNotNull(entry);

    entry = reader.next();
    entry = reader.next();

    assertNull(entry);
    reader.close();
  }

  /**
   * Tests that {@link ReplicationSource#terminate(String)} will timeout properly
   */
  @Test
  public void testTerminateTimeout() throws Exception {
    ReplicationSource source = new ReplicationSource();
    ReplicationEndpoint replicationEndpoint = new HBaseInterClusterReplicationEndpoint() {
      @Override
      protected void doStart() {
        notifyStarted();
      }

      @Override
      protected void doStop() {
        // not calling notifyStopped() here causes the caller of stop() to get a Future that never
        // completes
      }
    };
    replicationEndpoint.start();
    ReplicationPeer mockPeer = Mockito.mock(ReplicationPeer.class);
    Mockito.when(mockPeer.getPeerBandwidth()).thenReturn(0L);
    Configuration testConf = HBaseConfiguration.create();
    testConf.setInt("replication.source.maxretriesmultiplier", 1);
    ReplicationSourceManager manager = Mockito.mock(ReplicationSourceManager.class);
    Mockito.when(manager.getTotalBufferUsed()).thenReturn(new AtomicLong());
    source.init(testConf, null, manager, null, mockPeer, null, "testPeer", null,
      p -> OptionalLong.empty(), null);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<?> future = executor.submit(new Runnable() {

      @Override
      public void run() {
        source.terminate("testing source termination");
      }
    });
    long sleepForRetries = testConf.getLong("replication.source.sleepforretries", 1000);
    Waiter.waitFor(testConf, sleepForRetries * 2, new Predicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return future.isDone();
      }

    });

  }

  /**
   * Tests that recovered queues are preserved on a regionserver shutdown.
   * See HBASE-18192
   * @throws Exception
   */
  @Test
  public void testServerShutdownRecoveredQueue() throws Exception {
    try {
      // Ensure single-threaded WAL
      conf.set("hbase.wal.provider", "defaultProvider");
      conf.setInt("replication.sleep.before.failover", 2000);
      // Introduces a delay in regionserver shutdown to give the race condition a chance to kick in.
      conf.set(HConstants.REGION_SERVER_IMPL, ShutdownDelayRegionServer.class.getName());
      MiniHBaseCluster cluster = TEST_UTIL.startMiniCluster(2);
      TEST_UTIL_PEER.startMiniCluster(1);

      HRegionServer serverA = cluster.getRegionServer(0);
      final ReplicationSourceManager managerA =
          ((Replication) serverA.getReplicationSourceService()).getReplicationManager();
      HRegionServer serverB = cluster.getRegionServer(1);
      final ReplicationSourceManager managerB =
          ((Replication) serverB.getReplicationSourceService()).getReplicationManager();
      final Admin admin = TEST_UTIL.getAdmin();

      final String peerId = "TestPeer";
      admin.addReplicationPeer(peerId,
          new ReplicationPeerConfig().setClusterKey(TEST_UTIL_PEER.getClusterKey()));
      // Wait for replication sources to come up
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return !(managerA.getSources().isEmpty() || managerB.getSources().isEmpty());
        }
      });
      // Disabling peer makes sure there is at least one log to claim when the server dies
      // The recovered queue will also stay there until the peer is disabled even if the
      // WALs it contains have no data.
      admin.disableReplicationPeer(peerId);

      // Stopping serverA
      // It's queues should be claimed by the only other alive server i.e. serverB
      cluster.stopRegionServer(serverA.getServerName());
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return managerB.getOldSources().size() == 1;
        }
      });

      final HRegionServer serverC = cluster.startRegionServer().getRegionServer();
      serverC.waitForServerOnline();
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return serverC.getReplicationSourceService() != null;
        }
      });
      final ReplicationSourceManager managerC =
          ((Replication) serverC.getReplicationSourceService()).getReplicationManager();
      // Sanity check
      assertEquals(0, managerC.getOldSources().size());

      // Stopping serverB
      // Now serverC should have two recovered queues:
      // 1. The serverB's normal queue
      // 2. serverA's recovered queue on serverB
      cluster.stopRegionServer(serverB.getServerName());
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return managerC.getOldSources().size() == 2;
        }
      });
      admin.enableReplicationPeer(peerId);
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return managerC.getOldSources().size() == 0;
        }
      });
    } finally {
      conf.set(HConstants.REGION_SERVER_IMPL, HRegionServer.class.getName());
    }
  }

  /**
   * Regionserver implementation that adds a delay on the graceful shutdown.
   */
  public static class ShutdownDelayRegionServer extends HRegionServer {
    public ShutdownDelayRegionServer(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected void stopServiceThreads() {
      // Add a delay before service threads are shutdown.
      // This will keep the zookeeper connection alive for the duration of the delay.
      LOG.info("Adding a delay to the regionserver shutdown");
      try {
        Thread.sleep(2000);
      } catch (InterruptedException ex) {
        LOG.error("Interrupted while sleeping");
      }
      super.stopServiceThreads();
    }
  }

}

