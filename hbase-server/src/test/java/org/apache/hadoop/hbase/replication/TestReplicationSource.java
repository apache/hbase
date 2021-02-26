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

package org.apache.hadoop.hbase.replication;

import static org.apache.hadoop.hbase.replication.TestReplicationEndpoint.ReplicationEndpointForTest;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationSourceFactory;
import org.apache.hadoop.hbase.replication.regionserver.MetricsReplicationSourceSource;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceInterface;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.HFileTestUtil;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestReplicationSource {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationSource.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private final static HBaseTestingUtility TEST_UTIL_PEER =
      new HBaseTestingUtility();
  private static FileSystem FS;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf = TEST_UTIL.getConfiguration();

  /**
   * @throws java.lang.Exception exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(1);
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
    Path rootDir = TEST_UTIL.createRootDir();
    oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (FS.exists(oldLogDir)) {
      FS.delete(oldLogDir, true);
    }
    logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    if (FS.exists(logDir)) {
      FS.delete(logDir, true);
    }
    conf.setBoolean("replication.source.eof.autorecovery", true);
  }

  @Before
  public void setup() throws IOException {
    if (!FS.exists(logDir)) {
      FS.mkdirs(logDir);
    }
    if (!FS.exists(oldLogDir)) {
      FS.mkdirs(oldLogDir);
    }

    ReplicationEndpointForTest.contructedCount.set(0);
    ReplicationEndpointForTest.startedCount.set(0);
    ReplicationEndpointForTest.replicateCount.set(0);
    ReplicationEndpointForTest.stoppedCount.set(0);
    ReplicationEndpointForTest.lastEntries = null;
  }

  @After
  public void tearDown() throws IOException {
    if (FS.exists(oldLogDir)) {
      FS.delete(oldLogDir, true);
    }
    if (FS.exists(logDir)) {
      FS.delete(logDir, true);
    }
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
   * @throws Exception exception
   */
  @Test
  public void testLogMoving() throws Exception{
    Path logPath = new Path(logDir, "log");
    WALProvider.Writer writer = WALFactory.createWALWriter(FS, logPath,
        TEST_UTIL.getConfiguration());
    for(int i = 0; i < 3; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      WALKey key = new WALKey(b, TableName.valueOf(b), 0, 0,
          HConstants.DEFAULT_CLUSTER_ID);
      writer.append(new WAL.Entry(key, edit));
      writer.sync(false);
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
    final ReplicationSource source = new ReplicationSource();
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
    ReplicationPeers mockPeers = Mockito.mock(ReplicationPeers.class);
    ReplicationPeer mockPeer = Mockito.mock(ReplicationPeer.class);
    Mockito.when(mockPeer.getPeerBandwidth()).thenReturn(0L);
    Configuration testConf = HBaseConfiguration.create();
    testConf.setInt("replication.source.maxretriesmultiplier", 1);
    ReplicationSourceManager manager = Mockito.mock(ReplicationSourceManager.class);
    source.init(testConf, null, manager, null, mockPeers, null, "testPeer",
        null, replicationEndpoint, null);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<?> future = executor.submit(new Runnable() {

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

  private void appendEntries(WALProvider.Writer writer, int numEntries) throws IOException {
    for (int i = 0; i < numEntries; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      WALKey key = new WALKey(b, TableName.valueOf(b), 0, 0,
              HConstants.DEFAULT_CLUSTER_ID);
      NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      scopes.put(b, HConstants.REPLICATION_SCOPE_GLOBAL);
      key.setScopes(scopes);
      writer.append(new WAL.Entry(key, edit));
      writer.sync(false);
    }
    writer.close();
  }

  private long getPosition(WALFactory wals, Path log2, int numEntries) throws IOException {
    WAL.Reader reader = wals.createReader(FS, log2);
    for (int i = 0; i < numEntries; i++) {
      reader.next();
    }
    return reader.getPosition();
  }

  private static final class Mocks {
    private ReplicationSourceManager manager = mock(ReplicationSourceManager.class);
    private final ReplicationQueues queues = mock(ReplicationQueues.class);
    private final ReplicationPeers peers = mock(ReplicationPeers.class);
    private final MetricsSource metrics = mock(MetricsSource.class);
    private final ReplicationPeer peer = mock(ReplicationPeer.class);
    private final ReplicationEndpoint.Context context = mock(ReplicationEndpoint.Context.class);
    private final AtomicLong totalBufferUsed = new AtomicLong();

    private Mocks() {
      when(peers.getStatusOfPeer(anyString())).thenReturn(true);
      when(context.getReplicationPeer()).thenReturn(peer);
      when(manager.getTotalBufferUsed()).thenReturn(totalBufferUsed);
    }

    ReplicationSource createReplicationSourceAndManagerWithMocks(ReplicationEndpoint endpoint)
        throws Exception {
      ReplicationTracker tracker = mock(ReplicationTracker.class);
      Server server = mock(Server.class);
      FileSystem fs = mock(FileSystem.class);
      UUID clusterId = UUID.randomUUID();
      String peerId = "testPeerClusterZnode";

      manager = Mockito.spy(new ReplicationSourceManager(
        queues, peers, tracker, conf, server, fs, logDir, oldLogDir, clusterId));

      doCallRealMethod().when(manager).removePeer(Mockito.anyString());
      // Mock the failure during cleaning log with node already deleted
      doThrow(new ReplicationSourceWithoutPeerException("Peer Removed")).when(queues)
        .removeLog(anyString(), anyString());
      doCallRealMethod().when(manager)
        .logPositionAndCleanOldLogs(Mockito.<Path>anyObject(), Mockito.anyString(),
          Mockito.anyLong(), Mockito.anyBoolean(), Mockito.anyBoolean());
      final ReplicationSource source = new ReplicationSource();
      endpoint.init(context);
      source.init(conf, FS, manager, queues, peers, mock(Stoppable.class),
        peerId, clusterId, endpoint, metrics);
      manager.getSources().add(source);
      SortedSet<String> walsWithPrefix = Sets.newTreeSet(Collections.singletonList("fake"));
      doReturn(walsWithPrefix).when(manager).getLogsWithPrefix(anyString(), anyString());
      return source;
    }

    ReplicationSource createReplicationSourceWithMocks(ReplicationEndpoint endpoint,
      boolean isRecovered) throws IOException {
      final ReplicationSource source = new ReplicationSource();
      endpoint.init(context);
      source.init(conf, FS, manager, queues, peers, mock(Stoppable.class),
        "testPeerClusterZnode", UUID.randomUUID(), endpoint, metrics);
      if (isRecovered) {
        when(manager.getOldSources())
          .thenReturn(Lists.<ReplicationSourceInterface>newArrayList(source));
      }
      return source;
    }

    ReplicationSource createReplicationSourceWithMocks(MetricsSource metrics,
            ReplicationEndpoint endpoint) throws IOException {
      final ReplicationSource source = new ReplicationSource();
      endpoint.init(context);
      source.init(conf, FS, manager, queues, peers, mock(Stoppable.class),
        "testPeerClusterZnode", UUID.randomUUID(), endpoint, metrics);
      return source;
    }

    public AtomicLong getTotalBufferUsed() {
      return totalBufferUsed;
    }
  }

  @Test
  public void testSetLogPositionForWALCurrentlyReadingWhenLogsRolled() throws Exception {
    final int numWALEntries = 5;
    int nbCapacity = conf.getInt("replication.source.nb.capacity", 25000);
    try {
      conf.setInt("replication.source.nb.capacity", numWALEntries);

      Mocks mocks = new Mocks();
      final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest() {
        @Override public WALEntryFilter getWALEntryfilter() {
          return null;
        }
      };
      WALFactory wals = new WALFactory(TEST_UTIL.getConfiguration(), null, "test");
      final Path log1 = new Path(logDir, "log.1");
      final Path log2 = new Path(logDir, "log.2");

      WALProvider.Writer writer1
        = WALFactory.createWALWriter(FS, log1, TEST_UTIL.getConfiguration());
      WALProvider.Writer writer2
        = WALFactory.createWALWriter(FS, log2, TEST_UTIL.getConfiguration());

      appendEntries(writer1, 3);
      appendEntries(writer2, 2);

      long pos = getPosition(wals, log2, 2);

      final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, false);
      source.run();

      source.enqueueLog(log1);
      // log rolled
      source.enqueueLog(log2);

      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() {
          return endpoint.replicateCount.get() > 0;
        }
      });

      ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
      ArgumentCaptor<Long> positionCaptor = ArgumentCaptor.forClass(Long.class);
      verify(mocks.manager, times(1))
        .logPositionAndCleanOldLogs(pathCaptor.capture(), anyString(), positionCaptor.capture(),
          anyBoolean(), anyBoolean());
      assertTrue(endpoint.lastEntries.size() == 5);
      assertThat(pathCaptor.getValue(), is(log2));
      assertThat(positionCaptor.getValue(), is(pos));
    } finally {
      conf.setInt("replication.source.nb.capacity", nbCapacity);
    }
  }

  @Test
  public void testUpdateQuotaWhenBulkLoad() throws Exception {
    byte[] cfBytes = Bytes.toBytes("cf");
    TableName tableName = TableName.valueOf("test_table");
    Path dir = TEST_UTIL.getDataTestDirOnTestFS(tableName.getNameAsString());
    Map<String, Long> storeFilesSize = new HashMap<>(1);
    Map<byte[], List<Path>> storeFiles = new HashMap<>(1);
    int numRows = 10;

    Path familyDir = new Path(dir, Bytes.toString(cfBytes));
    Path hfilePath = new Path(familyDir, "test_hfile");
    HFileTestUtil.createHFile(conf, FS, hfilePath, cfBytes, cfBytes,
      Bytes.toBytes("a"), Bytes.toBytes("z"), numRows);
    storeFilesSize.put(hfilePath.getName(), FS.getFileStatus(hfilePath).getLen());
    storeFiles.put(cfBytes, Collections.singletonList(hfilePath));

    HRegionInfo regionInfo = new HRegionInfo(tableName);
    WALProtos.BulkLoadDescriptor loadDescriptor = ProtobufUtil
      .toBulkLoadDescriptor(tableName, ByteStringer.wrap(Bytes.toBytes("test_region")),
        storeFiles, storeFilesSize, 1, null);
    WALEdit edit = WALEdit.createBulkLoadEvent(regionInfo, loadDescriptor);

    final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest() {
      @Override
      public WALEntryFilter getWALEntryfilter() {
        return null;
      }
    };
    final Path log = new Path(logDir, "log.1");

    WALProvider.Writer writer = WALFactory.createWALWriter(FS, log, TEST_UTIL.getConfiguration());
    WALKey key = new WALKey(regionInfo.getEncodedNameAsBytes(), tableName, 0, 0,
      HConstants.DEFAULT_CLUSTER_ID);
    WAL.Entry bulkLoadEventEntry = new WAL.Entry(key, edit);
    WAL.Entry entryWithoutCells = new WAL.Entry(key, new WALEdit());
    writer.append(bulkLoadEventEntry);
    writer.append(entryWithoutCells);
    writer.close();

    Mocks mocks = new Mocks();
    final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, false);
    source.run();

    source.enqueueLog(log);
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return endpoint.replicateCount.get() > 0;
      }
    });

    assertEquals(0L, mocks.getTotalBufferUsed().get());
  }

  @Test
  public void testSetLogPositionAndRemoveOldWALsEvenIfEmptyWALsRolled() throws Exception {
    Mocks mocks = new Mocks();

    final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest();
    final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, false);
    WALFactory wals = new WALFactory(TEST_UTIL.getConfiguration(), null, "test");

    final Path log1 = new Path(logDir, "log.1");
    final Path log2 = new Path(logDir, "log.2");

    WALFactory.createWALWriter(FS, log1, TEST_UTIL.getConfiguration()).close();
    WALFactory.createWALWriter(FS, log2, TEST_UTIL.getConfiguration()).close();
    final long startPos = getPosition(wals, log2, 0);

    source.run();
    source.enqueueLog(log1);
    source.enqueueLog(log2);

    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return log2.equals(source.getLastLoggedPath())
                && source.getLastLoggedPosition() >= startPos;
      }
    });

    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    ArgumentCaptor<Long> positionCaptor = ArgumentCaptor.forClass(Long.class);

    verify(mocks.manager, times(1))
            .logPositionAndCleanOldLogs(pathCaptor.capture(), anyString(), positionCaptor.capture(),
                    anyBoolean(), anyBoolean());
    assertThat(pathCaptor.getValue(), is(log2));
    assertThat(positionCaptor.getValue(), is(startPos));
  }

  @Test
  public void testSetLogPositionAndRemoveOldWALsEvenIfNoCfsReplicated() throws Exception {
    Mocks mocks = new Mocks();
    // set table cfs to filter all cells out
    final TableName replicatedTable = TableName.valueOf("replicated_table");
    final Map<TableName, List<String>> cfs =
            Collections.singletonMap(replicatedTable, Collections.<String>emptyList());
    when(mocks.peer.getTableCFs()).thenReturn(cfs);

    WALFactory wals = new WALFactory(TEST_UTIL.getConfiguration(), null, "test");
    final Path log1 = new Path(logDir, "log.1");
    final Path log2 = new Path(logDir, "log.2");

    WALProvider.Writer writer1 = WALFactory.createWALWriter(FS, log1, TEST_UTIL.getConfiguration());
    WALProvider.Writer writer2 = WALFactory.createWALWriter(FS, log2, TEST_UTIL.getConfiguration());

    appendEntries(writer1, 3);
    appendEntries(writer2, 2);
    final long pos = getPosition(wals, log2, 2);

    final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest();
    final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, false);
    source.enqueueLog(log1);
    source.enqueueLog(log2);
    source.run();
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        // wait until reader read all cells
        return log2.equals(source.getLastLoggedPath()) && source.getLastLoggedPosition() >= pos;
      }
    });

    ArgumentCaptor<Path> pathCaptor = ArgumentCaptor.forClass(Path.class);
    ArgumentCaptor<Long> positionCaptor = ArgumentCaptor.forClass(Long.class);

    // all old wals should be removed by updating wal position, even if all cells are filtered out.
    verify(mocks.manager, times(1))
        .logPositionAndCleanOldLogs(pathCaptor.capture(), anyString(), positionCaptor.capture(),
              anyBoolean(), anyBoolean());
    assertThat(pathCaptor.getValue(), is(log2));
    assertThat(positionCaptor.getValue(), is(pos));
  }

  /**
   * There can be a scenario of replication peer removed but the replication source
   * still running since termination of source depends upon zk listener and there
   * can a rare scenario where zk listener might not get invoked or get delayed.
   * In that case, replication source manager will throw since it won't be able
   * to remove the znode while removing the log. We should terminate the source
   * in that case. See HBASE-25583
   * @throws Exception any exception
   */
  @Test
  public void testReplicationSourceTerminationWhenNoZnodeForPeerAndQueues() throws Exception {
    final Mocks mocks = new Mocks();
    // set table cfs to filter all cells out
    final TableName replicatedTable = TableName.valueOf("replicated_table");
    final Map<TableName, List<String>> cfs =
      Collections.singletonMap(replicatedTable, Collections.<String>emptyList());
    when(mocks.peer.getTableCFs()).thenReturn(cfs);

    // Append 3 entries in a log
    final Path log1 = new Path(logDir, "log.1");
    WALProvider.Writer writer1 = WALFactory.createWALWriter(FS, log1, TEST_UTIL.getConfiguration());
    appendEntries(writer1, 3);

    // Replication end point with no filter
    final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest() {
      @Override
      public WALEntryFilter getWALEntryfilter() {
        return null;
      }
    };

    final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, false);
    source.run();
    source.enqueueLog(log1);

    // Wait for source to replicate
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return endpoint.replicateCount.get() == 1;
      }
    });

    // Wait for all the entries to get replicated
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return endpoint.lastEntries.size() == 3;
      }
    });

    // And the source should be terminated
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return !source.isSourceActive();
      }
    });
  }

  @Test
  public void testReplicationOnEmptyLogAtTheEndOfQueueWithMultipleLogs() throws Exception {
    final String logPrefix = "logPrefix";
    Mocks mocks = new Mocks();
    // set table cfs to filter all cells out
    final TableName replicatedTable = TableName.valueOf("replicated_table");
    final Map<TableName, List<String>> cfs =
      Collections.singletonMap(replicatedTable, Collections.<String>emptyList());
    when(mocks.peer.getTableCFs()).thenReturn(cfs);

    // Append 3 entries in a log
    final Path log1 = new Path(logDir, logPrefix + ".1");
    WALProvider.Writer writer1 = WALFactory.createWALWriter(FS, log1, TEST_UTIL.getConfiguration());
    appendEntries(writer1, 3);

    // Create a 0 length log.
    Path emptyLog = new Path(logDir, logPrefix + ".2");
    FSDataOutputStream fsdos = FS.create(emptyLog);
    fsdos.close();
    assertEquals(0, FS.getFileStatus(emptyLog).getLen());

    // Replication end point with no filter
    final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest() {
      @Override
      public WALEntryFilter getWALEntryfilter() {
        return null;
      }
    };

    final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, true);
    source.run();
    source.enqueueLog(log1);
    source.enqueueLog(emptyLog);

    // Wait for source to replicate
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return endpoint.replicateCount.get() == 1;
      }
    });

    // Wait and verify if all the entries get replicated for non empty logs
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return endpoint.lastEntries.size() == 3;
      }
    });

    // Wait and verify if log queue has been drained fully
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return source.getQueues().get(logPrefix).isEmpty();
      }
    });
  }

  @Test
  public void testReplicationOnEmptyLogAtTheEndOfQueueWithSingleLog() throws Exception {
    final String logPrefix = "logPrefix";
    Mocks mocks = new Mocks();
    // set table cfs to filter all cells out
    final TableName replicatedTable = TableName.valueOf("replicated_table");
    final Map<TableName, List<String>> cfs =
      Collections.singletonMap(replicatedTable, Collections.<String>emptyList());
    when(mocks.peer.getTableCFs()).thenReturn(cfs);

    // Create a 0 length log.
    Path emptyLog = new Path(logDir, logPrefix + ".1");
    FSDataOutputStream fsdos = FS.create(emptyLog);
    fsdos.close();
    assertEquals(0, FS.getFileStatus(emptyLog).getLen());

    // Replication end point with no filter
    final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest() {
      @Override
      public WALEntryFilter getWALEntryfilter() {
        return null;
      }
    };

    final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, true);
    source.run();
    source.enqueueLog(emptyLog);

    // Wait and verify if no entry got replicated
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return endpoint.lastEntries == null;
      }
    });

    // Wait and verify get is queue is empty
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return source.getQueues().get(logPrefix).isEmpty();
      }
    });
  }

  @Test
  public void testReplicationOnEmptyLogBetweenTheNonEmptyLogsInLogQueue() throws Exception {
    final String logPrefix = "logPrefix";
    Mocks mocks = new Mocks();
    // set table cfs to filter all cells out
    final TableName replicatedTable = TableName.valueOf("replicated_table");
    final Map<TableName, List<String>> cfs =
      Collections.singletonMap(replicatedTable, Collections.<String>emptyList());
    when(mocks.peer.getTableCFs()).thenReturn(cfs);

    // Append 3 entries in a log
    final Path log1 = new Path(logDir, logPrefix + ".11");
    WALProvider.Writer writer1 = WALFactory.createWALWriter(FS, log1, TEST_UTIL.getConfiguration());
    appendEntries(writer1, 3);

    // Create a 0 length log.
    Path emptyLog = new Path(logDir, logPrefix + ".12");
    FSDataOutputStream fsdos = FS.create(emptyLog);
    fsdos.close();
    assertEquals(0, FS.getFileStatus(emptyLog).getLen());

    // Append 5 entries in a log
    final Path log3 = new Path(logDir, logPrefix + ".13");
    WALProvider.Writer writer3 = WALFactory.createWALWriter(FS, log3, TEST_UTIL.getConfiguration());
    appendEntries(writer3, 5);

    // Append 10 entries in a log
    final Path log4 = new Path(logDir, logPrefix + ".14");
    WALProvider.Writer writer4 = WALFactory.createWALWriter(FS, log4, TEST_UTIL.getConfiguration());
    appendEntries(writer4, 10);

    // Replication end point with no filter
    final ReplicationEndpointForTest endpoint = new ReplicationEndpointForTest() {
      @Override
      public WALEntryFilter getWALEntryfilter() {
        return null;
      }
    };

    final ReplicationSource source = mocks.createReplicationSourceWithMocks(endpoint, true);
    source.run();
    source.enqueueLog(log1);
    source.enqueueLog(emptyLog);
    source.enqueueLog(log3);
    source.enqueueLog(log4);

    // Wait for source to replicate
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return endpoint.replicateCount.get() == 2;
      }
    });

    // Wait and verify the last replicated entries
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return endpoint.lastEntries.size() == 15;
      }
    });

    // Wait and verify only one log is there in queue
    Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() {
        return source.getQueues().get(logPrefix).size() == 1;
      }
    });
  }

  /**
   * Tests that recovered queues are preserved on a regionserver shutdown.
   * See HBASE-18192
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
      final ReplicationAdmin replicationAdmin = new ReplicationAdmin(TEST_UTIL.getConfiguration());

      final String peerId = "TestPeer";
      replicationAdmin.addPeer(peerId,
          new ReplicationPeerConfig().setClusterKey(TEST_UTIL_PEER.getClusterKey()), null);
      // Wait for replication sources to come up
      Waiter.waitFor(conf, 20000, new Waiter.Predicate<Exception>() {
        @Override public boolean evaluate() throws Exception {
          return !(managerA.getSources().isEmpty() || managerB.getSources().isEmpty());
        }
      });
      // Disabling peer makes sure there is at least one log to claim when the server dies
      // The recovered queue will also stay there until the peer is disabled even if the
      // WALs it contains have no data.
      replicationAdmin.disablePeer(peerId);

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
      replicationAdmin.enablePeer(peerId);
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

    public ShutdownDelayRegionServer(Configuration conf, CoordinatedStateManager csm)
        throws IOException, InterruptedException {
      super(conf, csm);
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

  /*
  Test age of oldest wal metric.
  */
  @Test
  public void testAgeOfOldestWal() throws Exception {
    try {
      ManualEnvironmentEdge manualEdge = new ManualEnvironmentEdge();
      EnvironmentEdgeManager.injectEdge(manualEdge);

      String id = "1";
      MetricsSource metrics = new MetricsSource(id);
      Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
      conf.setInt("replication.source.maxretriesmultiplier", 1);
      Mocks mocks = new Mocks();
      ReplicationEndpoint endpoint = mock(ReplicationEndpoint.class);
      ReplicationSource source = mocks.createReplicationSourceWithMocks(metrics, endpoint);

      final Path log1 = new Path(logDir, "log-walgroup-a.8");
      manualEdge.setValue(10);
      // Diff of current time (10) and  log-walgroup-a.8 timestamp will be 2.
      source.enqueueLog(log1);
      MetricsReplicationSourceSource metricsSource1 = getSourceMetrics(id);
      assertEquals(2, metricsSource1.getOldestWalAge());

      final Path log2 = new Path(logDir, "log-walgroup-b.4");
      // Diff of current time (10) and log-walgroup-b.4 will be 6 so oldestWalAge should be 6
      source.enqueueLog(log2);
      assertEquals(6, metricsSource1.getOldestWalAge());
      // Clear all metrics.
      metrics.clear();
    } finally {
      EnvironmentEdgeManager.reset();
    }
  }

  private MetricsReplicationSourceSource getSourceMetrics(String sourceId) {
    MetricsReplicationSourceFactory factory = CompatibilitySingletonFactory
      .getInstance(MetricsReplicationSourceFactory.class);
    return factory.getSource(sourceId);
  }
}
