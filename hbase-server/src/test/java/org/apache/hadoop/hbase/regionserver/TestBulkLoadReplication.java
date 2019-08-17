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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilder;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
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
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Integration test for bulk load replication. Defines two clusters, with two way replication.
 * Performs a bulk load on cluster defined by UTIL1 first, asserts the Cell on the bulk loaded file
 * gets into the related table in UTIL1, then also validates the same got replicated to cluster
 * UTIL2. Then, bulk loads another file into UTIL2, and checks if related values are present on
 * UTIL2, and also gets replicated to UTIL1.
 * It also defines a preBulkLoad coprocessor that is added to all test table regions on each of the
 * clusters, in order to count amount of times bulk load actually gets invoked. This is to certify
 * we are not entered in the infinite loop condition addressed by HBASE-22380.
 */
@Category({ ReplicationTests.class, MediumTests.class})
public class TestBulkLoadReplication extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBulkLoadReplication.class);

  protected static final Logger LOG =
    LoggerFactory.getLogger(TestBulkLoadReplication.class);

  private static final String SRC_REP_CLUSTER_ID = "src";
  private static final String DEST_REP_CLUSTER_ID = "dest";
  private static final String PEER_ID = "1";
  private static final AtomicInteger BULK_LOADS_COUNT = new AtomicInteger(0);

  @Rule
  public TestName name = new TestName();

  @ClassRule
  public static TemporaryFolder testFolder = new TemporaryFolder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setupBulkLoadConfigsForCluster(TestReplicationBase.CONF1, SRC_REP_CLUSTER_ID);
    setupBulkLoadConfigsForCluster(TestReplicationBase.CONF2, DEST_REP_CLUSTER_ID);
    TestReplicationBase.setUpBeforeClass();
  }

  @Before
  @Override
  public void setUpBase() throws Exception {
    super.setUpBase();
    ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
      .setClusterKey(UTIL1.getClusterKey()).setSerial(isSerialPeer()).build();
    UTIL2.getAdmin().addReplicationPeer(PEER_ID, peerConfig);
    setupCoprocessor(UTIL1);
    setupCoprocessor(UTIL2);
  }

  private void setupCoprocessor(HBaseTestingUtility cluster){
    cluster.getHBaseCluster().getRegions(tableName).forEach( r -> {
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
    UTIL2.getAdmin().removeReplicationPeer(PEER_ID);
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
    Table srcTestTable = UTIL1.getConnection().getTable(TestReplicationBase.tableName);
    Table destTestTable = UTIL2.getConnection().getTable(TestReplicationBase.tableName);
    byte[] row = Bytes.toBytes("001");
    byte[] value = Bytes.toBytes("v1");
    bulkLoadOnCluster(row, value, UTIL1);
    Thread.sleep(400);
    assertTableHasValue(srcTestTable, row, value);
    Thread.sleep(400);
    assertTableHasValue(destTestTable, row, value);
    Thread.sleep(400);
    assertEquals(2, BULK_LOADS_COUNT.get());
    BULK_LOADS_COUNT.set(0);
    row = Bytes.toBytes("002");
    value = Bytes.toBytes("v2");
    bulkLoadOnCluster(row, value, UTIL2);
    Thread.sleep(400);
    assertTableHasValue(destTestTable, row, value);
    Thread.sleep(400);
    assertTableHasValue(srcTestTable, row, value);
    Thread.sleep(400);
    assertEquals(2, BULK_LOADS_COUNT.get());
  }

  private void bulkLoadOnCluster(byte[] row, byte[] value,
      HBaseTestingUtility cluster) throws Exception {
    String bulkLoadFilePath = createHFileForFamilies(row, value, cluster.getConfiguration());
    copyToHdfs(bulkLoadFilePath, cluster.getDFSCluster());
    BulkLoadHFilesTool bulkLoadHFilesTool = new BulkLoadHFilesTool(cluster.getConfiguration());
    bulkLoadHFilesTool.bulkLoad(tableName, new Path("/bulk_dir"));
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
      });
    }
  }
}
