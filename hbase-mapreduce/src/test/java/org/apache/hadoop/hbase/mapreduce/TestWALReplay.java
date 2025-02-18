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
import static org.apache.hadoop.hbase.HConstants.REPLICATION_CLUSTER_ID;
import static org.apache.hadoop.hbase.mapreduce.WALReplay.BULKLOAD_FILES;
import static org.apache.hadoop.hbase.mapreduce.WALReplay.WAL_DIR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALStreamReader;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;

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
  private final TableName TABLENAME = TableName.valueOf("table");
  private final byte[] FAMILY = Bytes.toBytes("family");
  private final byte[] COLUMN = Bytes.toBytes("col");
  private final byte[] ROW = Bytes.toBytes("row");
  private static SingleProcessHBaseCluster cluster;
  private static Path rootDir;
  private static FileSystem fs;
  private static Configuration conf;
  private static Path baseNamespacePath;
  private static Path bulkLoadPath;
  private static Path backupPath;
  private static Path backupWALPath;
  private static Path backupBLFPath; // BLF = bulk load file

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster();
    conf = TEST_UTIL.getConfiguration();
    rootDir = CommonFSUtils.getRootDir(conf);
    fs = CommonFSUtils.getRootDirFileSystem(conf);
    baseNamespacePath = new Path(rootDir, new Path(HConstants.BASE_NAMESPACE_DIR));
    bulkLoadPath = new Path(rootDir, "bulkLoadDir");
    backupPath = new Path(rootDir, "backup");
    backupWALPath = new Path(backupPath, WAL_DIR);
    backupBLFPath = new Path(backupPath, BULKLOAD_FILES);
    conf.setBoolean(REPLICATION_BULKLOAD_ENABLE_KEY, true);
    conf.set(REPLICATION_CLUSTER_ID, "clusterId1");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Table createTableWithSomeRows() throws IOException {
    Table table = TEST_UTIL.createTable(TABLENAME, FAMILY);
    Put p = new Put(ROW);
    p.addColumn(FAMILY, COLUMN, COLUMN);
    table.put(p);
    return table;
  }

  private void createHFile() throws Exception {
    conf.set(WALPlayer.BULK_OUTPUT_CONF_KEY, bulkLoadPath.toString());
    conf.setBoolean(WALPlayer.MULTI_TABLES_SUPPORT, true);
    WALPlayer player = new WALPlayer(conf);
    assertEquals(0,
      ToolRunner.run(conf, player, new String[] { getWalDir(), TABLENAME.getNameAsString() }));
  }

  private void copyWALtoBackupDir(Path walPath) throws IOException {
    WAL log = cluster.getRegionServer(0).getWAL(null);
    log.rollWriter();
    assertTrue(FileUtil.copy(fs, walPath, fs, backupWALPath, false, false, conf));
  }

  private String getWalDir() throws IOException {
    WAL log = cluster.getRegionServer(0).getWAL(null);
    log.rollWriter();
    Path walPath = new Path(cluster.getMaster().getMasterFileSystem().getWALRootDir(),
      HConstants.HREGION_LOGDIR_NAME);
    return walPath.toString();
  }

  private void verifyTable(Table t1) throws IOException {
    Get g = new Get(ROW);
    Result r = t1.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN));
  }

  private static Set<String> listFiles(final FileSystem fs, final Path root, final Path dir)
    throws IOException {
    Set<String> files = new HashSet<>();
    FileStatus[] list = CommonFSUtils.listStatus(fs, dir);
    if (list != null) {
      for (FileStatus fstat : list) {
        LOG.debug(Objects.toString(fstat.getPath()));
        if (fstat.isDirectory()) {
          files.addAll(listFiles(fs, root, fstat.getPath()));
        } else {
          String file = fstat.getPath().makeQualified(fs).toString();
          files.add(file);
        }
      }
    }
    return files;
  }

  private Path getBulkloadStorePath(Path walInputPath) throws IOException {
    WALProtos.BulkLoadDescriptor bld = null;
    WALFactory wals = new WALFactory(TEST_UTIL.getConfiguration(), "wals");
    Set<String> listOfFiles = listFiles(fs, rootDir, walInputPath);
    for (String file : listOfFiles) {
      if (bld != null) {
        break;
      }
      WALStreamReader reader =
        wals.createStreamReader(CommonFSUtils.getWALFileSystem(conf), new Path(file));
      while (true) {
        WAL.Entry entry = null;
        try {
          entry = reader.next();
        } catch (EOFException e) {
          break;
        }
        if (entry == null) {
          break;
        }
        WALEdit edit = entry.getEdit();
        if (
          edit != null && !edit.getCells().isEmpty()
            && WALEdit.getBulkLoadDescriptor(edit.getCells().get(0)) != null
        ) {
          bld = WALEdit.getBulkLoadDescriptor(edit.getCells().get(0));
          break;
        }
      }
      reader.close();
    }

    String regionName = bld.getEncodedRegionName().toStringUtf8();
    String columnFamilyName = bld.getStoresList().get(0).getFamilyName().toStringUtf8();
    String storeFileName = bld.getStoresList().get(0).getStoreFileList().get(0);
    String pathFromNS = new StringBuilder().append(TABLENAME.getNamespaceAsString())
      .append(Path.SEPARATOR).append(Bytes.toString(TABLENAME.getName())).append(Path.SEPARATOR)
      .append(regionName).append(Path.SEPARATOR).append(columnFamilyName).append(Path.SEPARATOR)
      .append(storeFileName).toString();
    String backupBLFDir = backupBLFPath.toString() + Path.SEPARATOR + pathFromNS;
    backupBLFPath = new Path(backupBLFDir);
    Path storePath = new Path(baseNamespacePath.toString() + Path.SEPARATOR + pathFromNS);
    return storePath;
  }

  /**
   * Test to validate restore of bulkload operation
   */
  @Test
  public void testWALReplay() throws Exception {

    Table t1 = createTableWithSomeRows();
    String walDir = getWalDir();
    Path walPath = new Path(walDir);

    // Create hfile to bulkLoadPath using WALPlayer
    createHFile();
    LOG.info("Hfile created");

    // Truncate test table
    TEST_UTIL.truncateTable(t1.getName());

    // Do bulkload operation, this will create a bulkload WAL entry
    BulkLoadHFiles.create(conf).bulkLoad(TABLENAME, new Path(bulkLoadPath.toString(),
      TABLENAME.getNamespaceAsString() + "/" + TABLENAME.getNameAsString()));
    Thread.sleep(100);
    LOG.info("Bulkload done");
    verifyTable(t1);

    // copy bulkload files to backupBLFPath (This is to simulate backup operation of hfile)
    Path storeFile = getBulkloadStorePath(walPath);
    assertTrue(FileUtil.copy(fs, storeFile, fs, backupBLFPath, false, false, conf));

    // Roll log and copy WAL to the backup directory
    copyWALtoBackupDir(walPath);

    TEST_UTIL.truncateTable(t1.getName());

    // Use WALReplay to restore hfile
    WALReplay walReplay = new WALReplay(conf);
    assertEquals(0, ToolRunner.run(conf, walReplay,
      new String[] { backupPath.toString(), TABLENAME.getNameAsString() }));

    // Verify restored table
    verifyTable(t1);
  }

}
