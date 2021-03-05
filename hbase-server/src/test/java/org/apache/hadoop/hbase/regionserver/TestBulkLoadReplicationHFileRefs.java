/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_CLUSTER_ID;
import static org.apache.hadoop.hbase.HConstants.REPLICATION_CONF_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@Category({ ReplicationTests.class, SmallTests.class})
public class TestBulkLoadReplicationHFileRefs extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadReplicationHFileRefs.class);

  private static final String PEER1_CLUSTER_ID = "peer1";
  private static final String PEER2_CLUSTER_ID = "peer2";

  private static final String REPLICATE_NAMESPACE = "replicate_ns";
  private static final String NO_REPLICATE_NAMESPACE = "no_replicate_ns";
  private static final TableName REPLICATE_TABLE =
    TableName.valueOf(REPLICATE_NAMESPACE, "replicate_table");
  private static final TableName NO_REPLICATE_TABLE =
    TableName.valueOf(NO_REPLICATE_NAMESPACE, "no_replicate_table");
  private static final byte[] CF_A = Bytes.toBytes("cfa");
  private static final byte[] CF_B = Bytes.toBytes("cfb");

  private byte[] row = Bytes.toBytes("r1");
  private byte[] qualifier = Bytes.toBytes("q1");
  private byte[] value = Bytes.toBytes("v1");

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  private static final Path BULK_LOAD_BASE_DIR = new Path("/bulk_dir");

  private static Admin admin1;
  private static Admin admin2;

  private static ReplicationQueueStorage queueStorage;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupBulkLoadConfigsForCluster(CONF1, PEER1_CLUSTER_ID);
    setupBulkLoadConfigsForCluster(CONF2, PEER2_CLUSTER_ID);
    TestReplicationBase.setUpBeforeClass();
    admin1 = UTIL1.getConnection().getAdmin();
    admin2 = UTIL2.getConnection().getAdmin();

    queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(UTIL1.getZooKeeperWatcher(),
      UTIL1.getConfiguration());

    admin1.createNamespace(NamespaceDescriptor.create(REPLICATE_NAMESPACE).build());
    admin2.createNamespace(NamespaceDescriptor.create(REPLICATE_NAMESPACE).build());
    admin1.createNamespace(NamespaceDescriptor.create(NO_REPLICATE_NAMESPACE).build());
    admin2.createNamespace(NamespaceDescriptor.create(NO_REPLICATE_NAMESPACE).build());
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

  @Before
  public void setUp() throws Exception {
    for (ReplicationPeerDescription peer : admin1.listReplicationPeers()) {
      admin1.removeReplicationPeer(peer.getPeerId());
    }
  }

  @After
  public void teardown() throws Exception {
    for (ReplicationPeerDescription peer : admin1.listReplicationPeers()) {
      admin1.removeReplicationPeer(peer.getPeerId());
    }
    for (TableName tableName : admin1.listTableNames()) {
      UTIL1.deleteTable(tableName);
    }
    for (TableName tableName : admin2.listTableNames()) {
      UTIL2.deleteTable(tableName);
    }
  }

  @Test
  public void testWhenExcludeCF() throws Exception {
    // Create table in source and remote clusters.
    createTableOnClusters(REPLICATE_TABLE, CF_A, CF_B);
    // Add peer, setReplicateAllUserTables true, but exclude CF_B.
    Map<TableName, List<String>> excludeTableCFs = Maps.newHashMap();
    excludeTableCFs.put(REPLICATE_TABLE, Lists.newArrayList(Bytes.toString(CF_B)));
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL2.getClusterKey())
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(excludeTableCFs)
      .build();
    admin1.addReplicationPeer(PEER_ID2, peerConfig);
    Assert.assertTrue(peerConfig.needToReplicate(REPLICATE_TABLE));
    Assert.assertTrue(peerConfig.needToReplicate(REPLICATE_TABLE, CF_A));
    Assert.assertFalse(peerConfig.needToReplicate(REPLICATE_TABLE, CF_B));

    assertEquals(0, queueStorage.getAllHFileRefs().size());

    // Bulk load data into the CF that is not replicated.
    bulkLoadOnCluster(REPLICATE_TABLE, CF_B);
    Threads.sleep(1000);

    // Cannot get data from remote cluster
    Table table2 = UTIL2.getConnection().getTable(REPLICATE_TABLE);
    Result result = table2.get(new Get(row));
    assertTrue(Bytes.equals(null, result.getValue(CF_B, qualifier)));
    // The extra HFile is never added to the HFileRefs
    assertEquals(0, queueStorage.getAllHFileRefs().size());
  }

  @Test
  public void testWhenExcludeTable() throws Exception {
    // Create 2 tables in source and remote clusters.
    createTableOnClusters(REPLICATE_TABLE, CF_A);
    createTableOnClusters(NO_REPLICATE_TABLE, CF_A);

    // Add peer, setReplicateAllUserTables true, but exclude one table.
    Map<TableName, List<String>> excludeTableCFs = Maps.newHashMap();
    excludeTableCFs.put(NO_REPLICATE_TABLE, null);
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL2.getClusterKey())
      .setReplicateAllUserTables(true)
      .setExcludeTableCFsMap(excludeTableCFs)
      .build();
    admin1.addReplicationPeer(PEER_ID2, peerConfig);
    assertTrue(peerConfig.needToReplicate(REPLICATE_TABLE));
    assertFalse(peerConfig.needToReplicate(NO_REPLICATE_TABLE));
    assertTrue(peerConfig.needToReplicate(REPLICATE_TABLE, CF_A));
    assertFalse(peerConfig.needToReplicate(NO_REPLICATE_TABLE, CF_A));

    assertEquals(0, queueStorage.getAllHFileRefs().size());

    // Bulk load data into the table that is not replicated.
    bulkLoadOnCluster(NO_REPLICATE_TABLE, CF_A);
    Threads.sleep(1000);

    // Cannot get data from remote cluster
    Table table2 = UTIL2.getConnection().getTable(NO_REPLICATE_TABLE);
    Result result = table2.get(new Get(row));
    assertTrue(Bytes.equals(null, result.getValue(CF_A, qualifier)));

    // The extra HFile is never added to the HFileRefs
    assertEquals(0, queueStorage.getAllHFileRefs().size());
  }

  @Test
  public void testWhenExcludeNamespace() throws Exception {
    // Create 2 tables in source and remote clusters.
    createTableOnClusters(REPLICATE_TABLE, CF_A);
    createTableOnClusters(NO_REPLICATE_TABLE, CF_A);

    // Add peer, setReplicateAllUserTables true, but exclude one namespace.
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL2.getClusterKey())
      .setReplicateAllUserTables(true)
      .setExcludeNamespaces(Sets.newHashSet(NO_REPLICATE_NAMESPACE))
      .build();
    admin1.addReplicationPeer(PEER_ID2, peerConfig);
    assertTrue(peerConfig.needToReplicate(REPLICATE_TABLE));
    assertFalse(peerConfig.needToReplicate(NO_REPLICATE_TABLE));
    assertTrue(peerConfig.needToReplicate(REPLICATE_TABLE, CF_A));
    assertFalse(peerConfig.needToReplicate(NO_REPLICATE_TABLE, CF_A));

    assertEquals(0, queueStorage.getAllHFileRefs().size());

    // Bulk load data into the table of the namespace that is not replicated.
    byte[] row = Bytes.toBytes("001");
    byte[] value = Bytes.toBytes("v1");
    bulkLoadOnCluster(NO_REPLICATE_TABLE, CF_A);
    Threads.sleep(1000);

    // Cannot get data from remote cluster
    Table table2 = UTIL2.getConnection().getTable(NO_REPLICATE_TABLE);
    Result result = table2.get(new Get(row));
    assertTrue(Bytes.equals(null, result.getValue(CF_A, qualifier)));

    // The extra HFile is never added to the HFileRefs
    assertEquals(0, queueStorage.getAllHFileRefs().size());
  }

  protected void bulkLoadOnCluster(TableName tableName, byte[] family)
    throws Exception {
    String bulkLoadFilePath = createHFileForFamilies(family);
    copyToHdfs(family, bulkLoadFilePath, UTIL1.getDFSCluster());
    BulkLoadHFilesTool bulkLoadHFilesTool = new BulkLoadHFilesTool(UTIL1.getConfiguration());
    bulkLoadHFilesTool.bulkLoad(tableName, BULK_LOAD_BASE_DIR);
  }

  private String createHFileForFamilies(byte[] family) throws IOException {
    CellBuilder cellBuilder = CellBuilderFactory.create(CellBuilderType.DEEP_COPY);
    cellBuilder.setRow(row)
      .setFamily(family)
      .setQualifier(qualifier)
      .setValue(value)
      .setType(Cell.Type.Put);

    HFile.WriterFactory hFileFactory = HFile.getWriterFactoryNoCache(UTIL1.getConfiguration());
    File hFileLocation = testFolder.newFile();
    FSDataOutputStream out =
      new FSDataOutputStream(new FileOutputStream(hFileLocation), null);
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

  private void copyToHdfs(byte[] family, String bulkLoadFilePath, MiniDFSCluster cluster)
    throws Exception {
    Path bulkLoadDir = new Path(BULK_LOAD_BASE_DIR, Bytes.toString(family));
    cluster.getFileSystem().mkdirs(bulkLoadDir);
    cluster.getFileSystem().copyFromLocalFile(new Path(bulkLoadFilePath), bulkLoadDir);
  }

  private void createTableOnClusters(TableName tableName, byte[]... cfs) throws IOException {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    for (byte[] cf : cfs) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf)
        .setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build());
    }
    TableDescriptor td = builder.build();
    admin1.createTable(td);
    admin2.createTable(td);
  }
}
