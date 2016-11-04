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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;


@Category(LargeTests.class)
public class TestReplicationSmallTests extends TestReplicationBase {

  private static final Log LOG = LogFactory.getLog(TestReplicationSmallTests.class);
  private static final String PEER_ID = "2";

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    htable1.setAutoFlush(true, true);
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for ( JVMClusterUtil.RegionServerThread r :
        utility1.getHBaseCluster().getRegionServerThreads()) {
      r.getRegionServer().getWAL().rollWriter();
    }
    int rowCount = utility1.countRows(htable1);
    utility1.truncateTable(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call truncateTable on
    // utility2 since late writes could make it to the slave in some way.
    // Instead, we truncate the first table and wait for all the Deletes to
    // make it to the slave.
    Scan scan = new Scan();
    int lastCount = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for truncate");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(rowCount);
      scanner.close();
      if (res.length != 0) {
        if (res.length < lastCount) {
          i--; // Don't increment timeout if we make progress
        }
        lastCount = res.length;
        LOG.info("Still got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * Verify that version and column delete marker types are replicated
   * correctly.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testDeleteTypes() throws Exception {
    LOG.info("testDeleteTypes");
    final byte[] v1 = Bytes.toBytes("v1");
    final byte[] v2 = Bytes.toBytes("v2");
    final byte[] v3 = Bytes.toBytes("v3");
    htable1 = new HTable(conf1, tableName);

    long t = EnvironmentEdgeManager.currentTimeMillis();
    // create three versions for "row"
    Put put = new Put(row);
    put.add(famName, row, t, v1);
    htable1.put(put);

    put = new Put(row);
    put.add(famName, row, t+1, v2);
    htable1.put(put);

    put = new Put(row);
    put.add(famName, row, t+2, v3);
    htable1.put(put);

    Get get = new Get(row);
    get.setMaxVersions();
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() < 3) {
        LOG.info("Rows not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(CellUtil.cloneValue(res.rawCells()[0]), v3);
        assertArrayEquals(CellUtil.cloneValue(res.rawCells()[1]), v2);
        assertArrayEquals(CellUtil.cloneValue(res.rawCells()[2]), v1);
        break;
      }
    }
    // place a version delete marker (delete last version)
    Delete d = new Delete(row);
    d.deleteColumn(famName, row, t);
    htable1.delete(d);

    get = new Get(row);
    get.setMaxVersions();
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() > 2) {
        LOG.info("Version not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(CellUtil.cloneValue(res.rawCells()[0]), v3);
        assertArrayEquals(CellUtil.cloneValue(res.rawCells()[1]), v2);
        break;
      }
    }

    // place a column delete marker
    d = new Delete(row);
    d.deleteColumns(famName, row, t+2);
    htable1.delete(d);

    // now *both* of the remaining version should be deleted
    // at the replica
    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        LOG.info("Rows not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * Add a row, check it's replicated, delete it, check's gone
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testSimplePutDelete() throws Exception {
    LOG.info("testSimplePutDelete");
    Put put = new Put(row);
    put.add(famName, row, row);

    htable1 = new HTable(conf1, tableName);
    htable1.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }

    Delete del = new Delete(row);
    htable1.delete(del);

    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        LOG.info("Row not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * Try a small batch upload using the write buffer, check it's replicated
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testSmallBatch() throws Exception {
    LOG.info("testSmallBatch");
    Put put;
    // normal Batch tests
    htable1.setAutoFlush(false, true);
    loadData("", row);
    htable1.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);
  }

  private void waitForReplication(int expectedRows, int retries) throws IOException, InterruptedException {
    Scan scan;
    for (int i = 0; i < retries; i++) {
      scan = new Scan();
      if (i== retries -1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(expectedRows);
      scanner.close();
      if (res.length != expectedRows) {
        LOG.info("Only got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  private void loadData(String prefix, byte[] row) throws IOException {
    List<Put> puts = new ArrayList<Put>();
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(prefix + Integer.toString(i)));
      put.add(famName, row, row);
      puts.add(put);
    }
    htable1.put(puts);
  }

  /**
   * Test disable/enable replication, trying to insert, make sure nothing's
   * replicated, enable it, the insert should be replicated
   *
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testDisableEnable() throws Exception {

    // Test disabling replication
    admin.disablePeer(PEER_ID);

    byte[] rowkey = Bytes.toBytes("disable enable");
    Put put = new Put(rowkey);
    put.add(famName, row, row);
    htable1.put(put);

    Get get = new Get(rowkey);
    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        fail("Replication wasn't disabled");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    // Test enable replication
    admin.enablePeer(PEER_ID);

    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        return;
      }
    }
    fail("Waited too much time for put replication");
  }

  /**
   * Integration test for TestReplicationAdmin, removes and re-add a peer
   * cluster
   *
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testAddAndRemoveClusters() throws Exception {
    LOG.info("testAddAndRemoveClusters");
    admin.removePeer(PEER_ID);
    Thread.sleep(SLEEP_TIME);
    byte[] rowKey = Bytes.toBytes("Won't be replicated");
    Put put = new Put(rowKey);
    put.add(famName, row, row);
    htable1.put(put);

    Get get = new Get(rowKey);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES-1) {
        break;
      }
      Result res = htable2.get(get);
      if (res.size() >= 1) {
        fail("Not supposed to be replicated");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    admin.addPeer("2", utility2.getClusterKey());
    Thread.sleep(SLEEP_TIME);
    rowKey = Bytes.toBytes("do rep");
    put = new Put(rowKey);
    put.add(famName, row, row);
    LOG.info("Adding new row");
    htable1.put(put);

    get = new Get(rowKey);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME*i);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }
  }


  /**
   * Do a more intense version testSmallBatch, one  that will trigger
   * hlog rolling and other non-trivial code paths
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testLoading() throws Exception {
    LOG.info("Writing out rows to table1 in testLoading");
    htable1.setWriteBufferSize(1024);
    htable1.setAutoFlush(false, true);
    for (int i = 0; i < NB_ROWS_IN_BIG_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(famName, row, row);
      htable1.put(put);
    }
    htable1.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner = htable1.getScanner(scan);
    Result[] res = scanner.next(NB_ROWS_IN_BIG_BATCH);
    scanner.close();

    assertEquals(NB_ROWS_IN_BIG_BATCH, res.length);

    LOG.info("Looking in table2 for replicated rows in testLoading");
    long start = System.currentTimeMillis();
    // Retry more than NB_RETRIES.  As it was, retries were done in 5 seconds and we'd fail
    // sometimes.
    final long retries = NB_RETRIES * 10;
    for (int i = 0; i < retries; i++) {
      scan = new Scan();
      scanner = htable2.getScanner(scan);
      res = scanner.next(NB_ROWS_IN_BIG_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BIG_BATCH) {
        if (i == retries - 1) {
          int lastRow = -1;
          for (Result result : res) {
            int currentRow = Bytes.toInt(result.getRow());
            for (int row = lastRow+1; row < currentRow; row++) {
              LOG.error("Row missing: " + row);
            }
            lastRow = currentRow;
          }
          LOG.error("Last row: " + lastRow);
          fail("Waited too much time for normal batch replication, " +
            res.length + " instead of " + NB_ROWS_IN_BIG_BATCH + "; waited=" +
            (System.currentTimeMillis() - start) + "ms");
        } else {
          LOG.info("Only got " + res.length + " rows... retrying");
          Thread.sleep(SLEEP_TIME);
        }
      } else {
        break;
      }
    }
  }

  /**
   * Do a small loading into a table, make sure the data is really the same,
   * then run the VerifyReplication job to check the results. Do a second
   * comparison where all the cells are different.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testVerifyRepJob() throws Exception {
    // Populate the tables, at the same time it guarantees that the tables are
    // identical since it does the check
    testSmallBatch();

    String[] args = new String[] {PEER_ID, Bytes.toString(tableName)};
    runVerifyReplication(args, NB_ROWS_IN_BATCH, 0);

    Scan scan = new Scan();
    ResultScanner rs = htable2.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      Cell firstVal = result.rawCells()[0];
      put.add(CellUtil.cloneFamily(firstVal),
          CellUtil.cloneQualifier(firstVal), Bytes.toBytes("diff data"));
      htable2.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable2.delete(delete);
    runVerifyReplication(args, 0, NB_ROWS_IN_BATCH);
  }

  /**
   * Load a row into a table, make sure the data is really the same,
   * delete the row, make sure the delete marker is replicated,
   * run verify replication with and without raw to check the results.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testVerifyRepJobWithRawOptions() throws Exception {
    LOG.info("testVerifyRepJobWithRawOptions");

    TableName tablename = TableName.valueOf("test_raw");
    byte[] familyname = Bytes.toBytes("fam_raw");
    byte[] row = Bytes.toBytes("row_raw");

    HTableInterface lHtable1 = null;
    HTableInterface lHtable2 = null;

    try {
      HTableDescriptor table = new HTableDescriptor(tablename);
      HColumnDescriptor fam = new HColumnDescriptor(familyname);
      fam.setMaxVersions(100);
      fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
      table.addFamily(fam);

      HBaseAdmin admin1 = new HBaseAdmin(conf1);
      try {
        admin1.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      } finally {
        admin1.close();
      }
      utility1.waitUntilAllRegionsAssigned(tablename);

      HBaseAdmin admin2 = new HBaseAdmin(conf2);
      try {
        admin2.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      } finally {
        admin2.close();
      }
      utility2.waitUntilAllRegionsAssigned(tablename);

      lHtable1 = HConnectionManager.createConnection(conf1).getTable(tablename);
      lHtable2 = HConnectionManager.createConnection(conf2).getTable(tablename);

      Put put = new Put(row);
      put.add(familyname, row, row);
      lHtable1.put(put);

      Get get = new Get(row);
      for (int i = 0; i < NB_RETRIES; i++) {
        if (i==NB_RETRIES-1) {
          fail("Waited too much time for put replication");
        }
        Result res = lHtable2.get(get);
        if (res.size() == 0) {
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
        if (i==NB_RETRIES-1) {
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
      String[] argsWithoutRaw = new String[] {PEER_ID, tablename.getNameAsString()};
      runVerifyReplication(argsWithoutRaw, 0, 0);

      // Checking verifyReplication with raw
      String[] argsWithRawAsTrue = new String[] {"--raw", PEER_ID, tablename.getNameAsString()};
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

  private void runVerifyReplication(String[] args, int expectedGoodRows, int expectedBadRows)
      throws IOException, InterruptedException, ClassNotFoundException {
    Job job = VerifyReplication.createSubmittableJob(new Configuration(CONF_WITH_LOCALFS), args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(expectedGoodRows, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(expectedBadRows, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
  }

  @Test(timeout=300000)
  // VerifyReplication should honor versions option
  public void testHBase14905() throws Exception {
    // normal Batch tests
    byte[] qualifierName = Bytes.toBytes("f1");
    Put put = new Put(Bytes.toBytes("r1"));
    put.add(famName, qualifierName, Bytes.toBytes("v1002"));
    htable1.put(put);
    put.add(famName, qualifierName, Bytes.toBytes("v1001"));
    htable1.put(put);
    put.add(famName, qualifierName, Bytes.toBytes("v1112"));
    htable1.put(put);

    Scan scan = new Scan();
    scan.setMaxVersions(100);
    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(1);
    scanner1.close();

    assertEquals(1, res1.length);
    assertEquals(3, res1[0].getColumnCells(famName, qualifierName).size());

    for (int i = 0; i < NB_RETRIES; i++) {
      scan = new Scan();
      scan.setMaxVersions(100);
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
      if (i == NB_RETRIES-1) {
        fail("Waited too much time for normal batch replication");
      }
    }

    put.add(famName, qualifierName, Bytes.toBytes("v1111"));
    htable2.put(put);
    put.add(famName, qualifierName, Bytes.toBytes("v1112"));
    htable2.put(put);

    scan = new Scan();
    scan.setMaxVersions(100);
    scanner1 = htable2.getScanner(scan);
    res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();

    assertEquals(1, res1.length);
    assertEquals(5, res1[0].getColumnCells(famName, qualifierName).size());

    String[] args = new String[] {"--versions=100", PEER_ID, Bytes.toString(tableName)};
    runVerifyReplication(args, 0, 1);
  }

  @Test(timeout=300000)
  // VerifyReplication should honor versions option
  public void testVersionMismatchHBase14905() throws Exception {
    // normal Batch tests
    byte[] qualifierName = Bytes.toBytes("f1");
    Put put = new Put(Bytes.toBytes("r1"));
    long ts = System.currentTimeMillis();
    put.add(famName, qualifierName, ts + 1, Bytes.toBytes("v1"));
    htable1.put(put);
    put.add(famName, qualifierName, ts + 2, Bytes.toBytes("v2"));
    htable1.put(put);
    put.add(famName, qualifierName, ts + 3, Bytes.toBytes("v3"));
    htable1.put(put);
       
    Scan scan = new Scan();
    scan.setMaxVersions(100);
    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(1);
    scanner1.close();

    assertEquals(1, res1.length);
    assertEquals(3, res1[0].getColumnCells(famName, qualifierName).size());
    
    for (int i = 0; i < NB_RETRIES; i++) {
      scan = new Scan();
      scan.setMaxVersions(100);
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
      if (i == NB_RETRIES-1) {
        fail("Waited too much time for normal batch replication");
      }
    }
   
    try {
      // Disabling replication and modifying the particular version of the cell to validate the feature.  
      admin.disablePeer(PEER_ID);
      Put put2 = new Put(Bytes.toBytes("r1"));
      put2.add(famName, qualifierName, ts +2, Bytes.toBytes("v99"));
      htable2.put(put2);
      
      scan = new Scan();
      scan.setMaxVersions(100);
      scanner1 = htable2.getScanner(scan);
      res1 = scanner1.next(NB_ROWS_IN_BATCH);
      scanner1.close();
      assertEquals(1, res1.length);
      assertEquals(3, res1[0].getColumnCells(famName, qualifierName).size());

      String[] args = new String[] {"--versions=100", PEER_ID, Bytes.toString(tableName)};
      runVerifyReplication(args, 0, 1);
      }
    finally {
      admin.enablePeer(PEER_ID);
    }
  }

  /**
   * Test for HBASE-9038, Replication.scopeWALEdits would NPE if it wasn't filtering out
   * the compaction WALEdit
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testCompactionWALEdits() throws Exception {
    WALProtos.CompactionDescriptor compactionDescriptor =
        WALProtos.CompactionDescriptor.getDefaultInstance();
    HRegionInfo hri = new HRegionInfo(htable1.getName(),
      HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    WALEdit edit = WALEdit.createCompaction(hri, compactionDescriptor);
    Replication.scopeWALEdits(htable1.getTableDescriptor(), new HLogKey(), edit);
  }

  /**
   * Test for HBASE-8663
   * Create two new Tables with colfamilies enabled for replication then run
   * ReplicationAdmin.listReplicated(). Finally verify the table:colfamilies. Note:
   * TestReplicationAdmin is a better place for this testing but it would need mocks.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testVerifyListReplicatedTable() throws Exception {
	LOG.info("testVerifyListReplicatedTable");

    final String tName = "VerifyListReplicated_";
    final String colFam = "cf1";
    final int numOfTables = 3;

    HBaseAdmin hadmin = new HBaseAdmin(conf1);

    // Create Tables
    for (int i = 0; i < numOfTables; i++) {
      HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tName + i));
      HColumnDescriptor cfd = new HColumnDescriptor(colFam);
      cfd.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
      ht.addFamily(cfd);
      hadmin.createTable(ht);
    }

    // verify the result
    List<HashMap<String, String>> replicationColFams = admin.listReplicated();
    int[] match = new int[numOfTables]; // array of 3 with init value of zero

    for (int i = 0; i < replicationColFams.size(); i++) {
      HashMap<String, String> replicationEntry = replicationColFams.get(i);
      String tn = replicationEntry.get(ReplicationAdmin.TNAME);
      if ((tn.startsWith(tName)) && replicationEntry.get(ReplicationAdmin.CFNAME).equals(colFam)) {
        int m = Integer.parseInt(tn.substring(tn.length() - 1)); // get the last digit
        match[m]++; // should only increase once
      }
    }

    // check the matching result
    for (int i = 0; i < match.length; i++) {
      assertTrue("listReplicated() does not match table " + i, (match[i] == 1));
    }

    // drop tables
    for (int i = 0; i < numOfTables; i++) {
      String ht = tName + i;
      hadmin.disableTable(ht);
      hadmin.deleteTable(ht);
    }

    hadmin.close();
  }

  @Test(timeout=300000)
  public void testVerifyReplicationPrefixFiltering() throws Exception {
    final byte[] prefixRow = Bytes.toBytes("prefixrow");
    final byte[] prefixRow2 = Bytes.toBytes("secondrow");
    loadData("prefixrow", prefixRow);
    loadData("secondrow", prefixRow2);
    loadData("aaa", row);
    loadData("zzz", row);
    waitForReplication(NB_ROWS_IN_BATCH * 4, NB_RETRIES * 4);
    String[] args = new String[] {"--row-prefixes=prefixrow,secondrow", PEER_ID,
        Bytes.toString(tableName)};
    runVerifyReplication(args, NB_ROWS_IN_BATCH *2, 0);
  }
}
