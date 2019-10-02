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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
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
//  private static Table htable3;
//  private static Table htable4;

  @Rule
  public TestName name = new TestName();

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupBulkLoadConfigsForCluster(conf1, PEER1_CLUSTER_ID);
    conf3 = HBaseConfiguration.create(conf1);
    setupBulkLoadConfigsForCluster(conf3, PEER3_CLUSTER_ID);
    conf3.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");
    utility3 = new HBaseTestingUtility(conf3);
    conf4 = HBaseConfiguration.create(conf1);
    setupBulkLoadConfigsForCluster(conf4, PEER4_CLUSTER_ID);
    conf4.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/4");
    utility4 = new HBaseTestingUtility(conf4);
    TestReplicationBase.setUpBeforeClass();
    startCluster(utility3, conf3);
    startCluster(utility4, conf4);
  }

  private static void startCluster(HBaseTestingUtility util, Configuration configuration)
      throws Exception {
    LOG.info("Setup Zk to same one from utility1 and utility4");
    util.setZkCluster(utility1.getZkCluster());
    util.startMiniCluster(2);

    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    HColumnDescriptor columnDesc = new HColumnDescriptor(famName);
    columnDesc.setScope(1);
    tableDesc.addFamily(columnDesc);

    Connection connection = ConnectionFactory.createConnection(configuration);
    try (Admin admin = connection.getAdmin()) {
      admin.createTable(tableDesc, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    util.waitUntilAllRegionsAssigned(tableName);
  }

  @Before
  public void setUpBase() throws Exception {
    ReplicationPeerConfig peer1Config = getPeerConfigForCluster(utility1);
    ReplicationPeerConfig peer4Config = getPeerConfigForCluster(utility4);
    ReplicationPeerConfig peer3Config = getPeerConfigForCluster(utility3);
    //adds cluster4 as a remote peer on cluster1
    getReplicationAdmin(utility1.getConfiguration()).addPeer(PEER_ID4, peer4Config);
    //adds cluster1 as a remote peer on cluster4
    ReplicationAdmin admin4 = getReplicationAdmin(utility4.getConfiguration());
    admin4.addPeer(PEER_ID1, peer1Config);
    //adds cluster3 as a remote peer on cluster4
    admin4.addPeer(PEER_ID3, peer3Config);
    //adds cluster4 as a remote peer on cluster3
    getReplicationAdmin(utility3.getConfiguration()).addPeer(PEER_ID4, peer4Config);
    setupCoprocessor(utility1);
    setupCoprocessor(utility4);
    setupCoprocessor(utility3);
  }

  private ReplicationAdmin getReplicationAdmin(Configuration configuration) throws IOException {
    return new ReplicationAdmin(configuration);
  }

  private ReplicationPeerConfig getPeerConfigForCluster(HBaseTestingUtility util) {
    ReplicationPeerConfig config =  new ReplicationPeerConfig();
    config.setClusterKey(util.getClusterKey());
    return config;
  }

  private void setupCoprocessor(HBaseTestingUtility cluster) throws IOException {
    for(HRegion region : cluster.getHBaseCluster().getRegions(tableName)){
      region.getCoprocessorHost().load(TestBulkLoadReplication.BulkReplicationTestObserver.class,
        0, cluster.getConfiguration());
    }
  }

  @After
  public void tearDownBase() throws Exception {
    getReplicationAdmin(utility4.getConfiguration()).removePeer(PEER_ID1);
    getReplicationAdmin(utility4.getConfiguration()).removePeer(PEER_ID3);
    getReplicationAdmin(utility3.getConfiguration()).removePeer(PEER_ID4);
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
    assertBulkLoadConditions(row, value, utility4, peer4TestTable, peer1TestTable, peer3TestTable);
    row = Bytes.toBytes("003");
    value = Bytes.toBytes("v3");
    assertBulkLoadConditions(row, value, utility3, peer3TestTable, peer4TestTable, peer1TestTable);
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
    copyToHdfs(bulkLoadFile, cluster.getDFSCluster());
    LoadIncrementalHFiles bulkLoadHFilesTool =
      new LoadIncrementalHFiles(cluster.getConfiguration());
    bulkLoadHFilesTool.run(new String[]{"/bulk_dir/region1/", tableName.getNameAsString()});
  }

  private void copyToHdfs(String bulkLoadFilePath, MiniDFSCluster cluster) throws Exception {
    Path bulkLoadDir = new Path("/bulk_dir/region1/f");
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
    final KeyValue kv = new KeyValue(row, famName, Bytes.toBytes("1"), System.currentTimeMillis(),
      KeyValue.Type.Put, value);
    final HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(clusterConfig);
    // TODO We need a way to do this without creating files
    final File hFileLocation = testFolder.newFile();
    final FSDataOutputStream out =
      new FSDataOutputStream(new FileOutputStream(hFileLocation), null);
    try {
      hFileFactory.withOutputStream(out);
      hFileFactory.withFileContext(new HFileContext());
      HFile.Writer writer = hFileFactory.create();
      try {
        writer.append(kv);
      } finally {
        writer.close();
      }
    } finally {
      out.close();
    }
    return hFileLocation.getAbsoluteFile().getAbsolutePath();
  }

  public static class BulkReplicationTestObserver extends BaseRegionObserver {

    @Override
    public void preBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
      List<Pair<byte[], String>> familyPaths) throws IOException {
        BULK_LOADS_COUNT.incrementAndGet();
    }

    @Override
    public boolean postBulkLoadHFile(ObserverContext<RegionCoprocessorEnvironment> ctx,
        List<Pair<byte[], String>> familyPaths, boolean hasLoaded) throws IOException {
      if(hasLoaded) {
        BULK_LOAD_LATCH.countDown();
      }
      return true;
    }

  }
}
