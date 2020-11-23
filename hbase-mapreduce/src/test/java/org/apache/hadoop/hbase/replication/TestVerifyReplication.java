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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestVerifyReplication extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVerifyReplication.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestVerifyReplication.class);

  private static final String PEER_ID = "2";
  private static final TableName peerTableName = TableName.valueOf("peerTest");
  private static Table htable3;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    cleanUp();
    UTIL2.deleteTableData(peerTableName);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();

    TableDescriptor peerTable = TableDescriptorBuilder.newBuilder(peerTableName).setColumnFamily(
                    ColumnFamilyDescriptorBuilder.newBuilder(noRepfamName).setMaxVersions(100)
                            .build()).build();

    Connection connection2 = ConnectionFactory.createConnection(CONF2);
    try (Admin admin2 = connection2.getAdmin()) {
      admin2.createTable(peerTable, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    htable3 = connection2.getTable(peerTableName);
  }

  static void runVerifyReplication(String[] args, int expectedGoodRows, int expectedBadRows)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new VerifyReplication().createSubmittableJob(new Configuration(CONF1), args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(expectedGoodRows,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(expectedBadRows,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
  }

  /**
   * Do a small loading into a table, make sure the data is really the same, then run the
   * VerifyReplication job to check the results. Do a second comparison where all the cells are
   * different.
   */
  @Test
  public void testVerifyRepJob() throws Exception {
    // Populate the tables, at the same time it guarantees that the tables are
    // identical since it does the check
    runSmallBatchTest();

    String[] args = new String[] { PEER_ID, tableName.getNameAsString() };
    runVerifyReplication(args, NB_ROWS_IN_BATCH, 0);

    Scan scan = new Scan();
    ResultScanner rs = htable2.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      Cell firstVal = result.rawCells()[0];
      put.addColumn(CellUtil.cloneFamily(firstVal), CellUtil.cloneQualifier(firstVal),
        Bytes.toBytes("diff data"));
      htable2.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable2.delete(delete);
    runVerifyReplication(args, 0, NB_ROWS_IN_BATCH);
  }

  /**
   * Load a row into a table, make sure the data is really the same, delete the row, make sure the
   * delete marker is replicated, run verify replication with and without raw to check the results.
   */
  @Test
  public void testVerifyRepJobWithRawOptions() throws Exception {
    LOG.info(name.getMethodName());

    final TableName tableName = TableName.valueOf(name.getMethodName());
    byte[] familyname = Bytes.toBytes("fam_raw");
    byte[] row = Bytes.toBytes("row_raw");

    Table lHtable1 = null;
    Table lHtable2 = null;

    try {
      ColumnFamilyDescriptor fam = ColumnFamilyDescriptorBuilder.newBuilder(familyname)
          .setMaxVersions(100).setScope(HConstants.REPLICATION_SCOPE_GLOBAL).build();
      TableDescriptor table =
          TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(fam).build();

      Connection connection1 = ConnectionFactory.createConnection(CONF1);
      Connection connection2 = ConnectionFactory.createConnection(CONF2);
      try (Admin admin1 = connection1.getAdmin()) {
        admin1.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      }
      try (Admin admin2 = connection2.getAdmin()) {
        admin2.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      }
      UTIL1.waitUntilAllRegionsAssigned(tableName);
      UTIL2.waitUntilAllRegionsAssigned(tableName);

      lHtable1 = UTIL1.getConnection().getTable(tableName);
      lHtable2 = UTIL2.getConnection().getTable(tableName);

      Put put = new Put(row);
      put.addColumn(familyname, row, row);
      lHtable1.put(put);

      Get get = new Get(row);
      for (int i = 0; i < NB_RETRIES; i++) {
        if (i == NB_RETRIES - 1) {
          fail("Waited too much time for put replication");
        }
        Result res = lHtable2.get(get);
        if (res.isEmpty()) {
          LOG.info("Row not available");
          Thread.sleep(SLEEP_TIME);
        } else {
          assertArrayEquals(res.value(), row);
          break;
        }
      }

      Delete del = new Delete(row);
      lHtable1.delete(del);

      get = new Get(row);
      for (int i = 0; i < NB_RETRIES; i++) {
        if (i == NB_RETRIES - 1) {
          fail("Waited too much time for del replication");
        }
        Result res = lHtable2.get(get);
        if (res.size() >= 1) {
          LOG.info("Row not deleted");
          Thread.sleep(SLEEP_TIME);
        } else {
          break;
        }
      }

      // Checking verifyReplication for the default behavior.
      String[] argsWithoutRaw = new String[] { PEER_ID, tableName.getNameAsString() };
      runVerifyReplication(argsWithoutRaw, 0, 0);

      // Checking verifyReplication with raw
      String[] argsWithRawAsTrue = new String[] { "--raw", PEER_ID, tableName.getNameAsString() };
      runVerifyReplication(argsWithRawAsTrue, 1, 0);
    } finally {
      if (lHtable1 != null) {
        lHtable1.close();
      }
      if (lHtable2 != null) {
        lHtable2.close();
      }
    }
  }

  static void checkRestoreTmpDir(Configuration conf, String restoreTmpDir, int expectedCount)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] subDirectories = fs.listStatus(new Path(restoreTmpDir));
    assertNotNull(subDirectories);
    assertEquals(subDirectories.length, expectedCount);
    for (int i = 0; i < expectedCount; i++) {
      assertTrue(subDirectories[i].isDirectory());
    }
  }


  @Test
  public void testVerifyRepJobWithQuorumAddress() throws Exception {
    // Populate the tables, at the same time it guarantees that the tables are
    // identical since it does the check
    runSmallBatchTest();

    // with a quorum address (a cluster key)
    String[] args = new String[] { UTIL2.getClusterKey(), tableName.getNameAsString() };
    runVerifyReplication(args, NB_ROWS_IN_BATCH, 0);

    Scan scan = new Scan();
    ResultScanner rs = htable2.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      Cell firstVal = result.rawCells()[0];
      put.addColumn(CellUtil.cloneFamily(firstVal), CellUtil.cloneQualifier(firstVal),
        Bytes.toBytes("diff data"));
      htable2.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable2.delete(delete);
    runVerifyReplication(args, 0, NB_ROWS_IN_BATCH);
  }

  @Test
  public void testVerifyRepJobWithQuorumAddressAndSnapshotSupport() throws Exception {
    // Populate the tables, at the same time it guarantees that the tables are
    // identical since it does the check
    runSmallBatchTest();

    // Take source and target tables snapshot
    Path rootDir = CommonFSUtils.getRootDir(CONF1);
    FileSystem fs = rootDir.getFileSystem(CONF1);
    String sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL1.getAdmin(), tableName,
      Bytes.toString(famName), sourceSnapshotName, rootDir, fs, true);

    // Take target snapshot
    Path peerRootDir = CommonFSUtils.getRootDir(CONF2);
    FileSystem peerFs = peerRootDir.getFileSystem(CONF2);
    String peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL2.getAdmin(), tableName,
      Bytes.toString(famName), peerSnapshotName, peerRootDir, peerFs, true);

    String peerFSAddress = peerFs.getUri().toString();
    String tmpPath1 = UTIL1.getRandomDir().toString();
    String tmpPath2 = "/tmp" + System.currentTimeMillis();

    String[] args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
      "--sourceSnapshotTmpDir=" + tmpPath1, "--peerSnapshotName=" + peerSnapshotName,
      "--peerSnapshotTmpDir=" + tmpPath2, "--peerFSAddress=" + peerFSAddress,
      "--peerHBaseRootAddress=" + CommonFSUtils.getRootDir(CONF2), UTIL2.getClusterKey(),
      tableName.getNameAsString() };
    runVerifyReplication(args, NB_ROWS_IN_BATCH, 0);
    checkRestoreTmpDir(CONF1, tmpPath1, 1);
    checkRestoreTmpDir(CONF2, tmpPath2, 1);

    Scan scan = new Scan();
    ResultScanner rs = htable2.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      Cell firstVal = result.rawCells()[0];
      put.addColumn(CellUtil.cloneFamily(firstVal), CellUtil.cloneQualifier(firstVal),
        Bytes.toBytes("diff data"));
      htable2.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable2.delete(delete);

    sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL1.getAdmin(), tableName,
      Bytes.toString(famName), sourceSnapshotName, rootDir, fs, true);

    peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL2.getAdmin(), tableName,
      Bytes.toString(famName), peerSnapshotName, peerRootDir, peerFs, true);

    args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
      "--sourceSnapshotTmpDir=" + tmpPath1, "--peerSnapshotName=" + peerSnapshotName,
      "--peerSnapshotTmpDir=" + tmpPath2, "--peerFSAddress=" + peerFSAddress,
      "--peerHBaseRootAddress=" + CommonFSUtils.getRootDir(CONF2), UTIL2.getClusterKey(),
      tableName.getNameAsString() };
    runVerifyReplication(args, 0, NB_ROWS_IN_BATCH);
    checkRestoreTmpDir(CONF1, tmpPath1, 2);
    checkRestoreTmpDir(CONF2, tmpPath2, 2);
  }

  static void runBatchCopyTest() throws Exception {
    // normal Batch tests for htable1
    loadData("", row, noRepfamName);

    Scan scan1 = new Scan();
    List<Put> puts = new ArrayList<>(NB_ROWS_IN_BATCH);
    ResultScanner scanner1 = htable1.getScanner(scan1);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    for (Result result : res1) {
      Put put = new Put(result.getRow());
      for (Cell cell : result.rawCells()) {
        put.add(cell);
      }
      puts.add(put);
    }
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    // Copy the data to htable3
    htable3.put(puts);

    Scan scan2 = new Scan();
    ResultScanner scanner2 = htable3.getScanner(scan2);
    Result[] res2 = scanner2.next(NB_ROWS_IN_BATCH);
    scanner2.close();
    assertEquals(NB_ROWS_IN_BATCH, res2.length);
  }

  @Test
  public void testVerifyRepJobWithPeerTableName() throws Exception {
    // Populate the tables with same data
    runBatchCopyTest();

    // with a peerTableName along with quorum address (a cluster key)
    String[] args = new String[] { "--peerTableName=" + peerTableName.getNameAsString(),
        UTIL2.getClusterKey(), tableName.getNameAsString() };
    runVerifyReplication(args, NB_ROWS_IN_BATCH, 0);

    UTIL2.deleteTableData(peerTableName);
    runVerifyReplication(args, 0, NB_ROWS_IN_BATCH);
  }

  @Test
  public void testVerifyRepJobWithPeerTableNameAndSnapshotSupport() throws Exception {
    // Populate the tables with same data
    runBatchCopyTest();

    // Take source and target tables snapshot
    Path rootDir = CommonFSUtils.getRootDir(CONF1);
    FileSystem fs = rootDir.getFileSystem(CONF1);
    String sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL1.getAdmin(), tableName,
            Bytes.toString(noRepfamName), sourceSnapshotName, rootDir, fs, true);

    // Take target snapshot
    Path peerRootDir = CommonFSUtils.getRootDir(CONF2);
    FileSystem peerFs = peerRootDir.getFileSystem(CONF2);
    String peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL2.getAdmin(), peerTableName,
            Bytes.toString(noRepfamName), peerSnapshotName, peerRootDir, peerFs, true);

    String peerFSAddress = peerFs.getUri().toString();
    String tmpPath1 = UTIL1.getRandomDir().toString();
    String tmpPath2 = "/tmp" + System.currentTimeMillis();

    String[] args = new String[] { "--peerTableName=" + peerTableName.getNameAsString(),
      "--sourceSnapshotName=" + sourceSnapshotName,
      "--sourceSnapshotTmpDir=" + tmpPath1, "--peerSnapshotName=" + peerSnapshotName,
      "--peerSnapshotTmpDir=" + tmpPath2, "--peerFSAddress=" + peerFSAddress,
      "--peerHBaseRootAddress=" + CommonFSUtils.getRootDir(CONF2), UTIL2.getClusterKey(),
      tableName.getNameAsString() };
    runVerifyReplication(args, NB_ROWS_IN_BATCH, 0);
    checkRestoreTmpDir(CONF1, tmpPath1, 1);
    checkRestoreTmpDir(CONF2, tmpPath2, 1);

    Scan scan = new Scan();
    ResultScanner rs = htable3.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      Cell firstVal = result.rawCells()[0];
      put.addColumn(CellUtil.cloneFamily(firstVal), CellUtil.cloneQualifier(firstVal),
              Bytes.toBytes("diff data"));
      htable3.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable3.delete(delete);

    sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL1.getAdmin(), tableName,
            Bytes.toString(noRepfamName), sourceSnapshotName, rootDir, fs, true);

    peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(UTIL2.getAdmin(), peerTableName,
            Bytes.toString(noRepfamName), peerSnapshotName, peerRootDir, peerFs, true);

    args = new String[] { "--peerTableName=" + peerTableName.getNameAsString(),
      "--sourceSnapshotName=" + sourceSnapshotName,
      "--sourceSnapshotTmpDir=" + tmpPath1, "--peerSnapshotName=" + peerSnapshotName,
      "--peerSnapshotTmpDir=" + tmpPath2, "--peerFSAddress=" + peerFSAddress,
      "--peerHBaseRootAddress=" + CommonFSUtils.getRootDir(CONF2), UTIL2.getClusterKey(),
      tableName.getNameAsString() };
    runVerifyReplication(args, 0, NB_ROWS_IN_BATCH);
    checkRestoreTmpDir(CONF1, tmpPath1, 2);
    checkRestoreTmpDir(CONF2, tmpPath2, 2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    htable3.close();
    TestReplicationBase.tearDownAfterClass();
  }
}
