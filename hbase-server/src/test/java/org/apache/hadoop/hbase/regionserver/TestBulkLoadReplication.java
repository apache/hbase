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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_CLUSTER_ID;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_CONF_DIR;
import static org.junit.Assert.assertEquals;
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
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
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
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
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
 * replication topology: "1 <-> 2 <-> 3" (active-active between 1 and 2, and active-active between
 * 2 and 3).
 *
 * For each of defined test clusters, it performs a bulk load, asserting values on bulk loaded file
 * gets replicated to other two peers. Since we are doing 3 bulk loads, with the given replication
 * topology all these bulk loads should get replicated only once on each peer. To assert this,
 * this test defines a preBulkLoad coprocessor and adds it to all test table regions, on each of the
 * clusters. This CP counts the amount of times bulk load actually gets invoked, certifying
 * we are not entering the infinite loop condition addressed by HBASE-22380.
 */
@Category({ ReplicationTests.class, MediumTests.class})
public class TestBulkLoadReplication extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadReplication.class);

  protected static final Logger LOG =
    LoggerFactory.getLogger(TestBulkLoadReplication.class);

  private static final String PEER1_CLUSTER_ID = "peer1";
  private static final String PEER4_CLUSTER_ID = "peer4";
  private static final String PEER3_CLUSTER_ID = "peer3";

  private static final String PEER_ID1 = "1";
  private static final String PEER_ID3 = "3";
  private static final String PEER_ID4 = "4";

  private static final AtomicInteger BULK_LOADS_COUNT = new AtomicInteger(0);
  private static CountDownLatch BULK_LOAD_LATCH;

  private static HBaseTestingUtility utility3;
  private static HBaseTestingUtility utility4;
  private static Configuration conf3;
  private static Configuration conf4;
  private static Table htable3;
  private static Table htable4;

  @Rule
  public TestName name = new TestName();

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupBulkLoadConfigsForCluster(conf1, PEER1_CLUSTER_ID);
    conf3 = HBaseConfiguration.create(conf1);
    conf3.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");
    utility3 = new HBaseTestingUtility(conf3);
    conf4 = HBaseConfiguration.create(conf1);
    conf4.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/4");
    utility3 = new HBaseTestingUtility(conf3);
    utility4 = new HBaseTestingUtility(conf4);
    TestReplicationBase.setUpBeforeClass();
    setupBulkLoadConfigsForCluster(conf3, PEER3_CLUSTER_ID);
    //utility4 is started within TestReplicationBase.setUpBeforeClass(), but we had not set
    //bulkload replication configs yet, so setting a 4th utility.
    setupBulkLoadConfigsForCluster(conf4, PEER4_CLUSTER_ID);
    startCluster(utility3, conf3);
    startCluster(utility4, conf4);
  }

  private static void startCluster(HBaseTestingUtility util, Configuration configuration)
      throws Exception {
    LOG.info("Setup Zk to same one from utility1 and utility4");
    util.setZkCluster(utility1.getZkCluster());
    util.startMiniCluster(2);

    TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(famName).setMaxVersions(100)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build())
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(noRepfamName)).build();

    Connection connection = ConnectionFactory.createConnection(configuration);
    try (Admin admin = connection.getAdmin()) {
      admin.createTable(tableDesc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    util.waitUntilAllRegionsAssigned(tableName);
  }

  @Before
  @Override
  public void setUpBase() throws Exception {
    super.setUpBase();
    ReplicationPeerConfig peer1Config = getPeerConfigForCluster(utility1);
    ReplicationPeerConfig peer4Config = getPeerConfigForCluster(utility4);
    ReplicationPeerConfig peer3Config = getPeerConfigForCluster(utility3);
    //adds cluster4 as a remote peer on cluster1
    utility1.getAdmin().addReplicationPeer(PEER_ID4, peer4Config);
    //adds cluster1 as a remote peer on cluster4
    utility4.getAdmin().addReplicationPeer(PEER_ID1, peer1Config);
    //adds cluster3 as a remote peer on cluster4
    utility4.getAdmin().addReplicationPeer(PEER_ID3, peer3Config);
    //adds cluster4 as a remote peer on cluster3
    utility3.getAdmin().addReplicationPeer(PEER_ID4, peer4Config);
    setupCoprocessor(utility1);
    setupCoprocessor(utility4);
    setupCoprocessor(utility3);
  }

  private ReplicationPeerConfig getPeerConfigForCluster(HBaseTestingUtility util) {
    return ReplicationPeerConfig.newBuilder()
      .setClusterKey(util.getClusterKey()).setSerial(isSerialPeer()).build();
  }

  private void setupCoprocessor(HBaseTestingUtility cluster){
    cluster.getHBaseCluster().getRegions(tableName).forEach(r -> {
      try {
        r.getCoprocessorHost()
          .load(TestBulkLoadReplication.BulkReplicationTestObserver.class, 0,
            cluster.getConfiguration());
      } catch (Exception e){
        LOG.error(e.getMessage(), e);
      }
    });
  }

  @After
  @Override
  public void tearDownBase() throws Exception {
    super.tearDownBase();
    utility4.getAdmin().removeReplicationPeer(PEER_ID1);
    utility4.getAdmin().removeReplicationPeer(PEER_ID3);
    utility3.getAdmin().removeReplicationPeer(PEER_ID4);
  }

  private static void setupBulkLoadConfigsForCluster(Configuration config,
    String clusterReplicationId) throws Exception {
    config.setBoolean(HConstants.REPLICATION_BULKLOAD_ENABLE_KEY, true);
    config.set(REPLICATION_CLUSTER_ID, clusterReplicationId);
    File sourceConfigFolder = testFolder.newFolder(clusterReplicationId);
    File sourceConfigFile = new File(sourceConfigFolder.getAbsolutePath()
      + "/hbase-site.xml");
    config.writeXml(new FileOutputStream(sourceConfigFile));
    config.set(REPLICATION_CONF_DIR, testFolder.getRoot().getAbsolutePath());
  }

  @Test
  public void testBulkLoadReplicationActiveActive() throws Exception {
    Table peer1TestTable = utility1.getConnection().getTable(TestReplicationBase.tableName);
    Table peer4TestTable = utility4.getConnection().getTable(TestReplicationBase.tableName);
    Table peer3TestTable = utility3.getConnection().getTable(TestReplicationBase.tableName);
    byte[] row = Bytes.toBytes("001");
    byte[] value = Bytes.toBytes("v1");
    assertBulkLoadConditions(row, value, utility1, peer1TestTable, peer4TestTable, peer3TestTable);
    row = Bytes.toBytes("002");
    value = Bytes.toBytes("v2");
    assertBulkLoadConditions(row, value, utility4, peer1TestTable, peer4TestTable, peer3TestTable);
    row = Bytes.toBytes("003");
    value = Bytes.toBytes("v3");
    assertBulkLoadConditions(row, value, utility3, peer1TestTable, peer4TestTable, peer3TestTable);
    //Additional wait to make sure no extra bulk load happens
    Thread.sleep(400);
    //We have 3 bulk load events (1 initiated on each cluster).
    //Each event gets 3 counts (the originator cluster, plus the two peers),
    //so BULK_LOADS_COUNT expected value is 3 * 3 = 9.
    assertEquals(9, BULK_LOADS_COUNT.get());
  }

  private void assertBulkLoadConditions(byte[] row, byte[] value,
      HBaseTestingUtility utility, Table...tables) throws Exception {
    BULK_LOAD_LATCH = new CountDownLatch(3);
    bulkLoadOnCluster(row, value, utility);
    assertTrue(BULK_LOAD_LATCH.await(1, TimeUnit.MINUTES));
    assertTableHasValue(tables[0], row, value);
    assertTableHasValue(tables[1], row, value);
    assertTableHasValue(tables[2], row, value);
  }

  private void bulkLoadOnCluster(byte[] row, byte[] value,
      HBaseTestingUtility cluster) throws Exception {
    String bulkLoadFile = createHFileForFamilies(row, value, cluster.getConfiguration());
    Path bulkLoadFilePath = new Path(bulkLoadFile);
    copyToHdfs(bulkLoadFile, cluster.getDFSCluster());
    LoadIncrementalHFiles bulkLoadHFilesTool =
      new LoadIncrementalHFiles(cluster.getConfiguration());
    Map<byte[], List<Path>> family2Files = new HashMap<>();
    List<Path> files = new ArrayList<>();
    files.add(new Path("/bulk_dir/f/" + bulkLoadFilePath.getName()));
    family2Files.put(Bytes.toBytes("f"), files);
    bulkLoadHFilesTool.run(family2Files, tableName);
  }

  private void copyToHdfs(String bulkLoadFilePath, MiniDFSCluster cluster) throws Exception {
    Path bulkLoadDir = new Path("/bulk_dir/f");
    cluster.getFileSystem().mkdirs(bulkLoadDir);
    cluster.getFileSystem().copyFromLocalFile(new Path(bulkLoadFilePath), bulkLoadDir);
  }

  private void assertTableHasValue(Table table, byte[] row, byte[] value) throws Exception {
    Get get = new Get(row);
    Result result = table.get(get);
    assertTrue(result.advance());
    assertEquals(Bytes.toString(value), Bytes.toString(result.value()));
  }

  private String createHFileForFamilies(byte[] row, byte[] value,
      Configuration clusterConfig) throws IOException {
    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
    cellBuilder.setRow(row)
      .setFamily(TestReplicationBase.famName)
      .setQualifier(Bytes.toBytes("1"))
      .setValue(value)
      .setType(Cell.Type.Put);

    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(clusterConfig);
    // TODO We need a way to do this without creating files
    File hFileLocation = testFolder.newFile();
    FSDataOutputStream out =
      new FSDataOutputStream(new FileOutputStream(hFileLocation), null);
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContext());
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

  public static class BulkReplicationTestObserver implements RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(new RegionObserver() {
        @Override
        public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
          List<Pair<byte[], String>> familyPaths) throws IOException {
            BULK_LOADS_COUNT.incrementAndGet();
        }

        @Override
        public void postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
          List<Pair<byte[], String>> stagingFamilyPaths, Map<byte[], List<Path>> finalPaths)
            throws IOException {
          BULK_LOAD_LATCH.countDown();
        }
      });
    }
  }
}
