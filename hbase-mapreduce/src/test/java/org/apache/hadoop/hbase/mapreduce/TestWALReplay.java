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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_BULKLOAD_ENABLE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic test for the WALReplay M/R tool to test restore of bulkload operation
 */
@Category({ MapReduceTests.class, LargeTests.class })
public class TestWALReplay {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALReplay.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestWALReplay.class);
  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static SingleProcessHBaseCluster cluster;
  private static Path rootDir;
  private static Path walRootDir;
  private static FileSystem fs;
  private static FileSystem logFs;
  private static Configuration conf;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    rootDir = TEST_UTIL.createRootDir();
    walRootDir = TEST_UTIL.createWALRootDir();
    fs = CommonFSUtils.getRootDirFileSystem(conf);
    logFs = CommonFSUtils.getWALFileSystem(conf);
    cluster = TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    fs.delete(rootDir, true);
    logFs.delete(walRootDir, true);
  }

  /**
   * Test to validate restore of bulkload operation
   */
  @Test
  public void testWALReplay() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName() + "1");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN1 = Bytes.toBytes("c1");
    final byte[] ROW = Bytes.toBytes("row");
    Table t1 = TEST_UTIL.createTable(tableName, FAMILY);

    // Put a row into the table
    Put p = new Put(ROW);
    p.addColumn(FAMILY, COLUMN1, COLUMN1);
    t1.put(p);

    WAL log = cluster.getRegionServer(0).getWAL(null);
    log.rollWriter();
    Path walInputPath = new Path(cluster.getMaster().getMasterFileSystem().getWALRootDir(),
      HConstants.HREGION_LOGDIR_NAME);
    String walInputDir = walInputPath.toString();

    // Create hfile to outPath needed for bulkload operation later
    Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
    String backupDir = "/" + name.getMethodName() + "/backup/";
    String bulkloadDir = backupDir + "/bulk-load-files/";
    Path bulkloadPath = new Path(bulkloadDir);
    configuration.set(WALPlayer.BULK_OUTPUT_CONF_KEY, bulkloadDir);
    configuration.setBoolean(WALPlayer.MULTI_TABLES_SUPPORT, true);
    WALPlayer player = new WALPlayer(configuration);
    assertEquals(0, ToolRunner.run(configuration, player,
      new String[] { walInputDir, tableName.getNameAsString() }));

    LOG.info("ANKIT Hfile created");
    // Recreate walDir to get rid of Put WALEntry from WAL
    //fs.delete(walRootDir, true);
    //fs.mkdirs(walRootDir);

    // Delete table, later bulkload it
    TEST_UTIL.deleteTable(t1.getName());
    LOG.info("ANKIT deleted table first time");

    //configuration.setBoolean(BulkLoadHFiles.ALWAYS_COPY_FILES, true);
    configuration.setBoolean(REPLICATION_BULKLOAD_ENABLE_KEY, true);

    // Do bulkload operation, this will create a bulkload WAL entry
    BulkLoadHFiles.create(configuration).bulkLoad(tableName,
      new Path(bulkloadDir, tableName.getNamespaceAsString() + "/" + tableName.getNameAsString()));

    LOG.info("ANKIT Bulkload done");

    // Verify table
    Get g = new Get(ROW);
    Result r = t1.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN1));


    // Roll log and copy WAL to the backup directory
    log = cluster.getRegionServer(0).getWAL(null);
    log.rollWriter();
    String backupWalsDir = backupDir + "/WALs/";
    Path backupWalsPath = new Path(backupWalsDir);
    assertTrue(FileUtil.copy(walInputPath.getFileSystem(conf), walInputPath,
      backupWalsPath.getFileSystem(conf), backupWalsPath, false, false, conf));

    //FileUtil.copy(bulkloadPath.getFileSystem(conf), bulkloadPath,
    //  backupWalsPath.getFileSystem(conf), backupWalsPath, false, false, conf);

    // Delete table
    TEST_UTIL.deleteTable(t1.getName());

    LOG.info("ANKIT deleted table second time");

    // Use WALReplay to restore hfile
    configuration = new Configuration(TEST_UTIL.getConfiguration());
    WALReplay walReplay = new WALReplay(configuration);
    String optionName = "_test_.name";
    configuration.set(optionName, "1000");
    player.setupTime(configuration, optionName);
    assertEquals(1000, configuration.getLong(optionName, 0));
    assertEquals(0, ToolRunner.run(configuration, walReplay,
      new String[] { backupDir, tableName.getNameAsString() }));

    // Verify restored table
    g = new Get(ROW);
    r = t1.get(g);
    //assertEquals(1, r.size());
    //assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN1));


  }

}
