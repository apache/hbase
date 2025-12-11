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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_CLUSTER_ID;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_CONF_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.ExtendedCellBuilder;
import org.apache.hadoop.hbase.ExtendedCellBuilderFactory;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for bulk load replication. Defines three clusters, with the following
 * replication topology: "1 <-> 2 <-> 3" (active-active between 1 and 2, and active-active between 2
 * and 3). For each of defined test clusters, it performs a bulk load, asserting values on bulk
 * loaded file gets replicated to other two peers. Since we are doing 3 bulk loads, with the given
 * replication topology all these bulk loads should get replicated only once on each peer. To assert
 * this, this test defines a preBulkLoad coprocessor and adds it to all test table regions, on each
 * of the clusters. This CP counts the amount of times bulk load actually gets invoked, certifying
 * we are not entering the infinite loop condition addressed by HBASE-22380.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestBulkLoadReplication extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadReplication.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestBulkLoadReplication.class);

  private static final String PEER1_CLUSTER_ID = "peer1";
  private static final String PEER2_CLUSTER_ID = "peer2";
  private static final String PEER3_CLUSTER_ID = "peer3";

  private static final String PEER_ID1 = "1";
  private static final String PEER_ID3 = "3";

  private static AtomicInteger BULK_LOADS_COUNT;
  private static CountDownLatch BULK_LOAD_LATCH;

  private static AtomicInteger REPLICATION_COUNT;

  protected static final HBaseTestingUtility UTIL3 = new HBaseTestingUtility();
  protected static final Configuration CONF3 = UTIL3.getConfiguration();

  private static final Path BULK_LOAD_BASE_DIR = new Path("/bulk_dir");

  @Rule
  public TestName name = new TestName();

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  private static final List<ReplicationQueueStorage> queueStorages = new ArrayList<>();

  private static boolean replicationPeersAdded = false;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupBulkLoadConfigsForCluster(CONF1, PEER1_CLUSTER_ID);
    setupBulkLoadConfigsForCluster(CONF2, PEER2_CLUSTER_ID);
    setupBulkLoadConfigsForCluster(CONF3, PEER3_CLUSTER_ID);
    setupConfig(UTIL3, "/3");
    TestReplicationBase.setUpBeforeClass();
    startThirdCluster();
    for (HBaseTestingUtility util : new HBaseTestingUtility[] { UTIL1, UTIL2, UTIL3 }) {
      ReplicationQueueStorage queueStorage = ReplicationStorageFactory
        .getReplicationQueueStorage(util.getZooKeeperWatcher(), util.getConfiguration());
      queueStorages.add(queueStorage);
    }
  }

  private static void startThirdCluster() throws Exception {
    LOG.info("Setup Zk to same one from UTIL1 and UTIL2");
    UTIL3.setZkCluster(UTIL1.getZkCluster());
    UTIL3.startMiniCluster(NUM_SLAVES1);

    TableDescriptor table = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName).setMobEnabled(true)
        .setMobThreshold(4000).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();

    Connection connection3 = ConnectionFactory.createConnection(CONF3);
    try (Admin admin3 = connection3.getAdmin()) {
      admin3.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    UTIL3.waitUntilAllRegionsAssigned(tableName);
  }

  @Before
  @Override
  public void setUpBase() throws Exception {
    // removing the peer and adding again causing the previously completed bulk load jobs getting
    // submitted again, adding a check to add the peers only once.
    if (!replicationPeersAdded) {
      // "super.setUpBase()" already sets replication from 1->2,
      // then on the subsequent lines, sets 2->1, 2->3 and 3->2.
      // So we have following topology: "1 <-> 2 <->3"
      super.setUpBase();
      ReplicationPeerConfig peer1Config = getPeerConfigForCluster(UTIL1);
      ReplicationPeerConfig peer2Config = getPeerConfigForCluster(UTIL2);
      ReplicationPeerConfig peer3Config = getPeerConfigForCluster(UTIL3);
      // adds cluster1 as a remote peer on cluster2
      UTIL2.getAdmin().addReplicationPeer(PEER_ID1, peer1Config);
      // adds cluster3 as a remote peer on cluster2
      UTIL2.getAdmin().addReplicationPeer(PEER_ID3, peer3Config);
      // adds cluster2 as a remote peer on cluster3
      UTIL3.getAdmin().addReplicationPeer(PEER_ID2, peer2Config);
      setupCoprocessor(UTIL1);
      setupCoprocessor(UTIL2);
      setupCoprocessor(UTIL3);
      replicationPeersAdded = true;
    }

    BULK_LOADS_COUNT = new AtomicInteger(0);
    REPLICATION_COUNT = new AtomicInteger(0);
  }

  private ReplicationPeerConfig getPeerConfigForCluster(HBaseTestingUtility util) {
    return ReplicationPeerConfig.newBuilder().setClusterKey(util.getClusterKey())
      .setSerial(isSerialPeer()).build();
  }

  private void setupCoprocessor(HBaseTestingUtility cluster) {
    Configuration conf = cluster.getConfiguration();
    String clusterKey = cluster.getClusterKey();
    Class<TestBulkLoadReplication.BulkReplicationTestObserver> cpClass =
      TestBulkLoadReplication.BulkReplicationTestObserver.class;

    cluster.getHBaseCluster().getRegions(tableName).forEach(r -> {
      try {
        RegionCoprocessorHost cpHost = r.getCoprocessorHost();
        TestBulkLoadReplication.BulkReplicationTestObserver cp = cpHost.findCoprocessor(cpClass);
        if (cp == null) {
          cpHost.load(cpClass, 0, conf);
          cp = cpHost.findCoprocessor(cpClass);
          cp.clusterName = clusterKey;
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    });

    try {
      RegionServerCoprocessorHost cpHost =
        cluster.getHBaseCluster().getRegionServer(0).getRegionServerCoprocessorHost();
      TestBulkLoadReplication.BulkReplicationTestObserver cp = cpHost.findCoprocessor(cpClass);
      if (cp == null) {
        cpHost.load(cpClass, 0, conf);
        cp = cpHost.findCoprocessor(cpClass);
        cp.clusterName = clusterKey;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  protected static void setupBulkLoadConfigsForCluster(Configuration config,
    String clusterReplicationId) throws Exception {
    config.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    config.set(REPLICATION_CLUSTER_ID, clusterReplicationId);
    File sourceConfigFolder = testFolder.newFolder(clusterReplicationId);
    File sourceConfigFile = new File(sourceConfigFolder.getAbsolutePath() + "/hbase-site.xml");
    config.writeXml(new FileOutputStream(sourceConfigFile));
    config.set(REPLICATION_CONF_DIR, testFolder.getRoot().getAbsolutePath());
  }

  @Test
  public void testBulkLoadReplicationActiveActive() throws Exception {
    Table peer1TestTable = UTIL1.getConnection().getTable(TestReplicationBase.tableName);
    Table peer2TestTable = UTIL2.getConnection().getTable(TestReplicationBase.tableName);
    Table peer3TestTable = UTIL3.getConnection().getTable(TestReplicationBase.tableName);
    byte[] row = Bytes.toBytes("001");
    byte[] value = Bytes.toBytes("v1");
    assertBulkLoadConditions(tableName, row, value, UTIL1, peer1TestTable, peer2TestTable,
      peer3TestTable);
    row = Bytes.toBytes("002");
    value = Bytes.toBytes("v2");
    assertBulkLoadConditions(tableName, row, value, UTIL2, peer1TestTable, peer2TestTable,
      peer3TestTable);
    row = Bytes.toBytes("003");
    value = Bytes.toBytes("v3");
    assertBulkLoadConditions(tableName, row, value, UTIL3, peer1TestTable, peer2TestTable,
      peer3TestTable);
    // Additional wait to make sure no extra bulk load happens
    Thread.sleep(400);
    // We have 3 bulk load events (1 initiated on each cluster).
    // Each event gets 3 counts (the originator cluster, plus the two peers),
    // so BULK_LOADS_COUNT expected value is 3 * 3 = 9.
    assertEquals(9, BULK_LOADS_COUNT.get());
    // Each bulk load event gets replicated twice (to two peers),
    // so REPLICATION_COUNT expected value is 3 * 2 = 6.
    assertEquals(6, REPLICATION_COUNT.get());
    waitForReplicationQueuesToEmpty();
    for (ReplicationQueueStorage queueStorage : queueStorages) {
      assertEquals(0, queueStorage.getAllHFileRefs().size());
    }
  }

  protected void assertBulkLoadConditions(TableName tableName, byte[] row, byte[] value,
    HBaseTestingUtility utility, Table... tables) throws Exception {
    BULK_LOAD_LATCH = new CountDownLatch(3);
    bulkLoadOnCluster(tableName, row, value, utility);
    assertTrue(BULK_LOAD_LATCH.await(1, TimeUnit.MINUTES));
    assertTableHasValue(tables[0], row, value);
    assertTableHasValue(tables[1], row, value);
    assertTableHasValue(tables[2], row, value);
  }

  protected void bulkLoadOnCluster(TableName tableName, byte[] row, byte[] value,
    HBaseTestingUtility cluster) throws Exception {
    String bulkLoadFilePath = createHFileForFamilies(row, value, cluster.getConfiguration());
    copyToHdfs(bulkLoadFilePath, cluster.getDFSCluster());
    BulkLoadHFilesTool bulkLoadHFilesTool = new BulkLoadHFilesTool(cluster.getConfiguration());
    bulkLoadHFilesTool.bulkLoad(tableName, BULK_LOAD_BASE_DIR);
  }

  private void copyToHdfs(String bulkLoadFilePath, MiniDFSCluster cluster) throws Exception {
    Path bulkLoadDir = new Path(BULK_LOAD_BASE_DIR, "f");
    cluster.getFileSystem().mkdirs(bulkLoadDir);
    cluster.getFileSystem().copyFromLocalFile(new Path(bulkLoadFilePath), bulkLoadDir);
  }

  protected void assertTableHasValue(Table table, byte[] row, byte[] value) throws Exception {
    Get get = new Get(row);
    Result result = table.get(get);
    assertTrue(result.advance());
    assertEquals(Bytes.toString(value), Bytes.toString(result.value()));
  }

  protected void assertTableNoValue(Table table, byte[] row, byte[] value) throws Exception {
    Get get = new Get(row);
    Result result = table.get(get);
    assertTrue(result.isEmpty());
  }

  private String createHFileForFamilies(byte[] row, byte[] value, Configuration clusterConfig)
    throws IOException {
    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
    cellBuilder.setRow(row).setFamily(TestReplicationBase.famName).setQualifier(Bytes.toBytes("1"))
      .setValue(value).setType(Cell.Type.Put);

    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(clusterConfig);
    // TODO We need a way to do this without creating files
    File hFileLocation = testFolder.newFile();
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(hFileLocation), null);
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContextBuilder().build());
      HFile.Writer writer = hFileFactory.create();
      try {
        writer.append(new KeyValue(cellBuilder.build()));
      } finally {
        writer.close();
      }
    } finally {
      out.close();
    }
    return hFileLocation.getAbsoluteFile().getAbsolutePath();
  }

  public static class BulkReplicationTestObserver
    implements RegionCoprocessor, RegionServerCoprocessor {

    String clusterName;
    AtomicInteger bulkLoadCounts = new AtomicInteger();
    AtomicInteger replicationCount = new AtomicInteger();

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(new RegionObserver() {

        @Override
        public void postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
          List<Pair<byte[], String>> stagingFamilyPaths, Map<byte[], List<Path>> finalPaths)
          throws IOException {
          BULK_LOAD_LATCH.countDown();
          BULK_LOADS_COUNT.incrementAndGet();
          LOG.debug("Another file bulk loaded. Total for {}: {}", clusterName,
            bulkLoadCounts.addAndGet(1));
        }
      });
    }

    @Override
    public Optional<RegionServerObserver> getRegionServerObserver() {
      return Optional.of(new RegionServerObserver() {

        // FIXME: this is deprecated, but we need it for validating bulk load replication
        @Override
        public void
        postReplicateLogEntries(final ObserverContext<RegionServerCoprocessorEnvironment> ctx) {
          REPLICATION_COUNT.incrementAndGet();
          LOG.info("Replication succeeded. Total for {}: {}", clusterName,
            replicationCount.addAndGet(1));
        }
      });
    }
  }

  @Test
  public void testBulkloadReplicationActiveActiveForNoRepFamily() throws Exception {
    Table peer1TestTable = UTIL1.getConnection().getTable(TestReplicationBase.tableName);
    Table peer2TestTable = UTIL2.getConnection().getTable(TestReplicationBase.tableName);
    Table peer3TestTable = UTIL3.getConnection().getTable(TestReplicationBase.tableName);
    byte[] row = Bytes.toBytes("004");
    byte[] value = Bytes.toBytes("v4");
    assertBulkLoadConditionsForNoRepFamily(row, value, UTIL1, peer1TestTable, peer2TestTable,
      peer3TestTable);
    // additional wait to make sure no extra bulk load happens
    Thread.sleep(400);
    assertEquals(1, BULK_LOADS_COUNT.get());
    // No replication should happen for no-replication family
    assertEquals(0, REPLICATION_COUNT.get());
    waitForReplicationQueuesToEmpty();
    for (ReplicationQueueStorage queueStorage : queueStorages) {
      assertEquals(0, queueStorage.getAllHFileRefs().size());
    }
  }

  private void assertBulkLoadConditionsForNoRepFamily(byte[] row, byte[] value,
    HBaseTestingUtility utility, Table... tables) throws Exception {
    BULK_LOAD_LATCH = new CountDownLatch(1);
    bulkLoadOnClusterForNoRepFamily(row, value, utility);
    assertTrue(BULK_LOAD_LATCH.await(1, TimeUnit.MINUTES));
    assertTableHasValue(tables[0], row, value);
    assertTableNotHasValue(tables[1], row, value);
    assertTableNotHasValue(tables[2], row, value);
  }

  private void bulkLoadOnClusterForNoRepFamily(byte[] row, byte[] value,
    HBaseTestingUtility cluster) throws Exception {
    String bulkloadFile = createHFileForNoRepFamilies(row, value, cluster.getConfiguration());
    Path bulkLoadFilePath = new Path(bulkloadFile);
    copyToHdfsForNoRepFamily(bulkloadFile, cluster.getDFSCluster());
    BulkLoadHFilesTool bulkLoadHFilesTool = new BulkLoadHFilesTool(cluster.getConfiguration());
    Map<byte[], List<Path>> family2Files = new HashMap<>();
    List<Path> files = new ArrayList<>();
    files.add(new Path(
      BULK_LOAD_BASE_DIR + "/" + Bytes.toString(noRepfamName) + "/" + bulkLoadFilePath.getName()));
    family2Files.put(noRepfamName, files);
    bulkLoadHFilesTool.bulkLoad(tableName, family2Files);
  }

  private String createHFileForNoRepFamilies(byte[] row, byte[] value, Configuration clusterConfig)
    throws IOException {
    ExtendedCellBuilder cellBuilder = ExtendedCellBuilderFactory.create(CellBuilderType.DEEP_COPY);
    cellBuilder.setRow(row).setFamily(TestReplicationBase.noRepfamName)
      .setQualifier(Bytes.toBytes("1")).setValue(value).setType(Cell.Type.Put);

    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(clusterConfig);
    // TODO We need a way to do this without creating files
    File hFileLocation = testFolder.newFile();
    FSDataOutputStream out = new FSDataOutputStream(new FileOutputStream(hFileLocation), null);
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContextBuilder().build());
      HFile.Writer writer = hFileFactory.create();
      try {
        writer.append(new KeyValue(cellBuilder.build()));
      } finally {
        writer.close();
      }
    } finally {
      out.close();
    }
    return hFileLocation.getAbsoluteFile().getAbsolutePath();
  }

  private void copyToHdfsForNoRepFamily(String bulkLoadFilePath, MiniDFSCluster cluster)
    throws Exception {
    Path bulkLoadDir = new Path(BULK_LOAD_BASE_DIR + "/" + Bytes.toString(noRepfamName) + "/");
    cluster.getFileSystem().mkdirs(bulkLoadDir);
    cluster.getFileSystem().copyFromLocalFile(new Path(bulkLoadFilePath), bulkLoadDir);
  }

  private void assertTableNotHasValue(Table table, byte[] row, byte[] value) throws IOException {
    Get get = new Get(row);
    Result result = table.get(get);
    assertNotEquals(Bytes.toString(value), Bytes.toString(result.value()));
  }

  private void waitForReplicationQueuesToEmpty() {
    Waiter.waitFor(CONF1, 1000, () -> {
      for (ReplicationQueueStorage queueStorage : queueStorages) {
        if (!queueStorage.getAllHFileRefs().isEmpty()) {
          return false;
        }
      }
      return true;
    });
  }
}
