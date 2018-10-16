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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.TreeMap;
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
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestVerifyReplication extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVerifyReplication.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestVerifyReplication.class);

  private static final String PEER_ID = "2";

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    cleanUp();
  }

  private void runVerifyReplication(String[] args, int expectedGoodRows, int expectedBadRows)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = new VerifyReplication().createSubmittableJob(new Configuration(conf1), args);
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
      scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (ColumnFamilyDescriptor f : table.getColumnFamilies()) {
        scopes.put(f.getName(), f.getScope());
      }

      Connection connection1 = ConnectionFactory.createConnection(conf1);
      Connection connection2 = ConnectionFactory.createConnection(conf2);
      try (Admin admin1 = connection1.getAdmin()) {
        admin1.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      }
      try (Admin admin2 = connection2.getAdmin()) {
        admin2.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      }
      utility1.waitUntilAllRegionsAssigned(tableName);
      utility2.waitUntilAllRegionsAssigned(tableName);

      lHtable1 = utility1.getConnection().getTable(tableName);
      lHtable2 = utility2.getConnection().getTable(tableName);

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

  // VerifyReplication should honor versions option
  @Test
  public void testHBase14905() throws Exception {
    // normal Batch tests
    byte[] qualifierName = Bytes.toBytes("f1");
    Put put = new Put(Bytes.toBytes("r1"));
    put.addColumn(famName, qualifierName, Bytes.toBytes("v1002"));
    htable1.put(put);
    put.addColumn(famName, qualifierName, Bytes.toBytes("v1001"));
    htable1.put(put);
    put.addColumn(famName, qualifierName, Bytes.toBytes("v1112"));
    htable1.put(put);

    Scan scan = new Scan();
    scan.readVersions(100);
    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(1);
    scanner1.close();

    assertEquals(1, res1.length);
    assertEquals(3, res1[0].getColumnCells(famName, qualifierName).size());

    for (int i = 0; i < NB_RETRIES; i++) {
      scan = new Scan();
      scan.readVersions(100);
      scanner1 = htable2.getScanner(scan);
      res1 = scanner1.next(1);
      scanner1.close();
      if (res1.length != 1) {
        LOG.info("Only got " + res1.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        int cellNumber = res1[0].getColumnCells(famName, Bytes.toBytes("f1")).size();
        if (cellNumber != 3) {
          LOG.info("Only got " + cellNumber + " cells");
          Thread.sleep(SLEEP_TIME);
        } else {
          break;
        }
      }
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for normal batch replication");
      }
    }

    put.addColumn(famName, qualifierName, Bytes.toBytes("v1111"));
    htable2.put(put);
    put.addColumn(famName, qualifierName, Bytes.toBytes("v1112"));
    htable2.put(put);

    scan = new Scan();
    scan.readVersions(100);
    scanner1 = htable2.getScanner(scan);
    res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();

    assertEquals(1, res1.length);
    assertEquals(5, res1[0].getColumnCells(famName, qualifierName).size());

    String[] args = new String[] { "--versions=100", PEER_ID, tableName.getNameAsString() };
    runVerifyReplication(args, 0, 1);
  }

  // VerifyReplication should honor versions option
  @Test
  public void testVersionMismatchHBase14905() throws Exception {
    // normal Batch tests
    byte[] qualifierName = Bytes.toBytes("f1");
    Put put = new Put(Bytes.toBytes("r1"));
    long ts = System.currentTimeMillis();
    put.addColumn(famName, qualifierName, ts + 1, Bytes.toBytes("v1"));
    htable1.put(put);
    put.addColumn(famName, qualifierName, ts + 2, Bytes.toBytes("v2"));
    htable1.put(put);
    put.addColumn(famName, qualifierName, ts + 3, Bytes.toBytes("v3"));
    htable1.put(put);

    Scan scan = new Scan();
    scan.readVersions(100);
    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(1);
    scanner1.close();

    assertEquals(1, res1.length);
    assertEquals(3, res1[0].getColumnCells(famName, qualifierName).size());

    for (int i = 0; i < NB_RETRIES; i++) {
      scan = new Scan();
      scan.readVersions(100);
      scanner1 = htable2.getScanner(scan);
      res1 = scanner1.next(1);
      scanner1.close();
      if (res1.length != 1) {
        LOG.info("Only got " + res1.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        int cellNumber = res1[0].getColumnCells(famName, Bytes.toBytes("f1")).size();
        if (cellNumber != 3) {
          LOG.info("Only got " + cellNumber + " cells");
          Thread.sleep(SLEEP_TIME);
        } else {
          break;
        }
      }
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for normal batch replication");
      }
    }

    try {
      // Disabling replication and modifying the particular version of the cell to validate the
      // feature.
      hbaseAdmin.disableReplicationPeer(PEER_ID);
      Put put2 = new Put(Bytes.toBytes("r1"));
      put2.addColumn(famName, qualifierName, ts + 2, Bytes.toBytes("v99"));
      htable2.put(put2);

      scan = new Scan();
      scan.readVersions(100);
      scanner1 = htable2.getScanner(scan);
      res1 = scanner1.next(NB_ROWS_IN_BATCH);
      scanner1.close();
      assertEquals(1, res1.length);
      assertEquals(3, res1[0].getColumnCells(famName, qualifierName).size());

      String[] args = new String[] { "--versions=100", PEER_ID, tableName.getNameAsString() };
      runVerifyReplication(args, 0, 1);
    } finally {
      hbaseAdmin.enableReplicationPeer(PEER_ID);
    }
  }

  @Test
  public void testVerifyReplicationPrefixFiltering() throws Exception {
    final byte[] prefixRow = Bytes.toBytes("prefixrow");
    final byte[] prefixRow2 = Bytes.toBytes("secondrow");
    loadData("prefixrow", prefixRow);
    loadData("secondrow", prefixRow2);
    loadData("aaa", row);
    loadData("zzz", row);
    waitForReplication(NB_ROWS_IN_BATCH * 4, NB_RETRIES * 4);
    String[] args =
        new String[] { "--row-prefixes=prefixrow,secondrow", PEER_ID, tableName.getNameAsString() };
    runVerifyReplication(args, NB_ROWS_IN_BATCH * 2, 0);
  }

  @Test
  public void testVerifyReplicationSnapshotArguments() {
    String[] args =
        new String[] { "--sourceSnapshotName=snapshot1", "2", tableName.getNameAsString() };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--sourceSnapshotTmpDir=tmp", "2", tableName.getNameAsString() };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--sourceSnapshotName=snapshot1", "--sourceSnapshotTmpDir=tmp", "2",
        tableName.getNameAsString() };
    assertTrue(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--peerSnapshotName=snapshot1", "2", tableName.getNameAsString() };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--peerSnapshotTmpDir=/tmp/", "2", tableName.getNameAsString() };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--peerSnapshotName=snapshot1", "--peerSnapshotTmpDir=/tmp/",
        "--peerFSAddress=tempfs", "--peerHBaseRootAddress=hdfs://tempfs:50070/hbase/", "2",
        tableName.getNameAsString() };
    assertTrue(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--sourceSnapshotName=snapshot1", "--sourceSnapshotTmpDir=/tmp/",
        "--peerSnapshotName=snapshot2", "--peerSnapshotTmpDir=/tmp/", "--peerFSAddress=tempfs",
        "--peerHBaseRootAddress=hdfs://tempfs:50070/hbase/", "2", tableName.getNameAsString() };

    assertTrue(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));
  }

  private void checkRestoreTmpDir(Configuration conf, String restoreTmpDir, int expectedCount)
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
  public void testVerifyReplicationWithSnapshotSupport() throws Exception {
    // Populate the tables, at the same time it guarantees that the tables are
    // identical since it does the check
    runSmallBatchTest();

    // Take source and target tables snapshot
    Path rootDir = FSUtils.getRootDir(conf1);
    FileSystem fs = rootDir.getFileSystem(conf1);
    String sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getAdmin(), tableName,
      new String(famName), sourceSnapshotName, rootDir, fs, true);

    // Take target snapshot
    Path peerRootDir = FSUtils.getRootDir(conf2);
    FileSystem peerFs = peerRootDir.getFileSystem(conf2);
    String peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility2.getAdmin(), tableName,
      new String(famName), peerSnapshotName, peerRootDir, peerFs, true);

    String peerFSAddress = peerFs.getUri().toString();
    String temPath1 = utility1.getRandomDir().toString();
    String temPath2 = "/tmp2";

    String[] args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
        "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
        "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
        "--peerHBaseRootAddress=" + FSUtils.getRootDir(conf2), "2", tableName.getNameAsString() };

    Job job = new VerifyReplication().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());

    checkRestoreTmpDir(conf1, temPath1, 1);
    checkRestoreTmpDir(conf2, temPath2, 1);

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
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getAdmin(), tableName,
      new String(famName), sourceSnapshotName, rootDir, fs, true);

    peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility2.getAdmin(), tableName,
      new String(famName), peerSnapshotName, peerRootDir, peerFs, true);

    args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
        "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
        "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
        "--peerHBaseRootAddress=" + FSUtils.getRootDir(conf2), "2", tableName.getNameAsString() };

    job = new VerifyReplication().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(0,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(NB_ROWS_IN_BATCH,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());

    checkRestoreTmpDir(conf1, temPath1, 2);
    checkRestoreTmpDir(conf2, temPath2, 2);
  }
}
