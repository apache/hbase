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

import static org.apache.hadoop.hbase.mapreduce.WALReplay.BULKLOAD_BACKUP_LOCATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
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

/**
 * Basic test for the WALReplay M/R tool to test restore of bulkload operation
 */
@Category({ MapReduceTests.class, LargeTests.class })
public class TestWALReplay {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWALReplay.class);
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
    Get gg = new Get(ROW);

    WAL log = cluster.getRegionServer(0).getWAL(null);
    log.rollWriter();
    Path walInputPath = new Path(cluster.getMaster().getMasterFileSystem().getWALRootDir(),
      HConstants.HREGION_LOGDIR_NAME);
    String walInputDir = walInputPath.toString();

    // Create hfile to outPath needed for bulkload operation later
    Configuration configuration = new Configuration(TEST_UTIL.getConfiguration());
    String outPath = "/tmp/" + name.getMethodName();
    configuration.set(WALPlayer.BULK_OUTPUT_CONF_KEY, outPath);
    configuration.setBoolean(WALPlayer.MULTI_TABLES_SUPPORT, true);
    WALPlayer player = new WALPlayer(configuration);
    assertEquals(0, ToolRunner.run(configuration, player,
      new String[] { walInputDir, tableName.getNameAsString() }));

    // Recreate walDir to get rid of Put WALEntry from WAL
    fs.delete(walRootDir, true);
    fs.mkdirs(walRootDir);

    // Do bulkload operation, this will create a bulkload WAL entry
    BulkLoadHFiles.create(configuration).bulkLoad(tableName,
      new Path(outPath, tableName.getNamespaceAsString() + "/" + tableName.getNameAsString()));

    // Roll log and copy WAL to the backup directory (/tmp/)
    log = cluster.getRegionServer(0).getWAL(null);
    log.rollWriter();
    Path outP = new Path(outPath);
    assertTrue(FileUtil.copy(walInputPath.getFileSystem(conf), walInputPath,
      outP.getFileSystem(conf), outP, true, true, conf));

    // Delete table
    TEST_UTIL.deleteTable(t1.getName());

    // Use WALReplay to restore hfile
    configuration = new Configuration(TEST_UTIL.getConfiguration());
    configuration.set(BULKLOAD_BACKUP_LOCATION, outPath);
    WALReplay walReplay = new WALReplay(configuration);
    String optionName = "_test_.name";
    configuration.set(optionName, "1000");
    player.setupTime(configuration, optionName);
    assertEquals(1000, configuration.getLong(optionName, 0));
    assertEquals(0, ToolRunner.run(configuration, walReplay,
      new String[] { outPath, tableName.getNameAsString() }));

    /*
     * TODO fix it // Verify restored table Get g = new Get(ROW); Result r = t1.get(g);
     * assertEquals(1, r.size()); assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN1));
     */
  }

}
