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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Test separate old wal deletion as logs are rolled.
 */
@Category(SmallTests.class)
public class TestSeparateOldWALCleaner  {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSeparateOldWALCleaner.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestSeparateOldWALCleaner.class);
  protected HRegionServer server;
  protected String tableName;
  protected FileSystem fs;
  protected MiniDFSCluster dfsCluster;
  protected Admin admin;
  protected MiniHBaseCluster cluster;
  protected final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  @Rule
  public final TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean("hbase.separate.oldlogdir.by.regionserver",true);
    // Use 2 DataNodes and default values for other StartMiniCluster options.
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numDataNodes(2).build());

    cluster = TEST_UTIL.getHBaseCluster();
    dfsCluster = TEST_UTIL.getDFSCluster();
    fs = TEST_UTIL.getTestFileSystem();
    admin = TEST_UTIL.getAdmin();

    // disable region rebalancing (interferes with log watching)
    cluster.getMaster().balanceSwitch(false);
  }

  @After
  public void tearDown() throws Exception  {
    TEST_UTIL.shutdownMiniCluster();
  }

  void startAndWriteData() throws IOException, InterruptedException {
    TEST_UTIL.getConnection().getTable(TableName.META_TABLE_NAME);
    this.server = cluster.getRegionServerThreads().get(0).getRegionServer();

    Table table = createTestTable();
    server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
    for (int i = 1; i <= 32; i++) {
      doPut(table, i);
    }
  }

  /**
   * Tests that oldWALs/rs are deleted
   */
  @Test
  public void testCleanedRSLogRolling() throws Exception {
    this.tableName = getName();
    startAndWriteData();
    RegionInfo region = server.getRegions(TableName.valueOf(tableName)).get(0).getRegionInfo();
    final WAL log = server.getWAL(region);
    // flush all regions
    for (HRegion r : server.getOnlineRegionsLocalContext()) {
      r.flush(true);
    }

    //start cleaner chore
    Configuration conf = TEST_UTIL.getConfiguration();
    Path rootdir = CommonFSUtils.getWALRootDir(conf);
    Path oldWalsDir =
        new Path(rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
    DirScanPool pool = new DirScanPool(TEST_UTIL.getConfiguration());
    LogCleaner cleaner = new LogCleaner(100, server, conf, fs, oldWalsDir, pool);

    Path oldWalRSDir = new Path(oldWalsDir, server.toString());

    assertEquals(true, fs.exists(oldWalRSDir));
    cleaner.chore();
    assertEquals(false, fs.exists(oldWalRSDir));
    LOG.info("oldWALs/rs path does not exist");
    // Now roll the log
    log.rollWriter();
  }

  protected String getName() {
    return "Test-" + name.getMethodName();
  }

  protected void doPut(Table table, int i) throws IOException {
    Put put = new Put(Bytes.toBytes("row" + String.format("%1$04d", i)));
    put.addColumn(HConstants.CATALOG_FAMILY, null, Bytes.toBytes("s"));
    table.put(put);
  }

  protected Table createTestTable() throws IOException {
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(getName()))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(HConstants.CATALOG_FAMILY)).build();
    admin.createTable(desc);
    return TEST_UTIL.getConnection().getTable(desc.getTableName());
  }
}