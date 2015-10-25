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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.Replication;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ReplicationTests.class, LargeTests.class})
public class TestReplicationSmallTests extends TestReplicationBase {

  private static final Log LOG = LogFactory.getLog(TestReplicationSmallTests.class);

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for ( JVMClusterUtil.RegionServerThread r :
        utility1.getHBaseCluster().getRegionServerThreads()) {
      utility1.getHBaseAdmin().rollWALWriter(r.getRegionServer().getServerName());
    }
    utility1.deleteTableData(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call deleteTableData on
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
      Result[] res = scanner.next(NB_ROWS_IN_BIG_BATCH);
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
    htable1 = utility1.getConnection().getTable(tableName);

    long t = EnvironmentEdgeManager.currentTime();
    // create three versions for "row"
    Put put = new Put(row);
    put.addColumn(famName, row, t, v1);
    htable1.put(put);

    put = new Put(row);
    put.addColumn(famName, row, t + 1, v2);
    htable1.put(put);

    put = new Put(row);
    put.addColumn(famName, row, t + 2, v3);
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
    put.addColumn(famName, row, row);

    htable1 = utility1.getConnection().getTable(tableName);
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
    // normal Batch tests
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(famName, row, row);
      puts.add(put);
    }
    htable1.put(puts);

    Scan scan = new Scan();

    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    for (int i = 0; i < NB_RETRIES; i++) {
      scan = new Scan();
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(NB_ROWS_IN_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BATCH) {
        LOG.info("Only got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
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
    admin.disablePeer("2");

    byte[] rowkey = Bytes.toBytes("disable enable");
    Put put = new Put(rowkey);
    put.addColumn(famName, row, row);
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
    admin.enablePeer("2");

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
    admin.removePeer("2");
    Thread.sleep(SLEEP_TIME);
    byte[] rowKey = Bytes.toBytes("Won't be replicated");
    Put put = new Put(rowKey);
    put.addColumn(famName, row, row);
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
    put.addColumn(famName, row, row);
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
   * wal rolling and other non-trivial code paths
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testLoading() throws Exception {
    LOG.info("Writing out rows to table1 in testLoading");
    List<Put> puts = new ArrayList<Put>();
    for (int i = 0; i < NB_ROWS_IN_BIG_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.addColumn(famName, row, row);
      puts.add(put);
    }
    htable1.setWriteBufferSize(1024);
    // The puts will be iterated through and flushed only when the buffer
    // size is reached.
    htable1.put(puts);

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

    String[] args = new String[] {"2", tableName.getNameAsString()};
    Job job = VerifyReplication.createSubmittableJob(CONF_WITH_LOCALFS, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());

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
    job = VerifyReplication.createSubmittableJob(CONF_WITH_LOCALFS, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(0, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(NB_ROWS_IN_BATCH, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
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
    Replication.scopeWALEdits(htable1.getTableDescriptor(), new WALKey(), edit);
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

    HBaseAdmin hadmin = utility1.getHBaseAdmin();

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

  /**
   * Test for HBASE-9531
   * put a few rows into htable1, which should be replicated to htable2
   * create a ClusterStatus instance 'status' from HBaseAdmin
   * test : status.getLoad(server).getReplicationLoadSourceList()
   * test : status.getLoad(server).getReplicationLoadSink()
   * * @throws Exception
   */
  @Test(timeout = 300000)
  public void testReplicationStatus() throws Exception {
    LOG.info("testReplicationStatus");

    try (Admin admin = utility1.getConnection().getAdmin()) {

      final byte[] qualName = Bytes.toBytes("q");
      Put p;

      for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
        p = new Put(Bytes.toBytes("row" + i));
        p.addColumn(famName, qualName, Bytes.toBytes("val" + i));
        htable1.put(p);
      }

      ClusterStatus status = admin.getClusterStatus();

      for (ServerName server : status.getServers()) {
        ServerLoad sl = status.getLoad(server);
        List<ReplicationLoadSource> rLoadSourceList = sl.getReplicationLoadSourceList();
        ReplicationLoadSink rLoadSink = sl.getReplicationLoadSink();

        // check SourceList has at least one entry
        assertTrue("failed to get ReplicationLoadSourceList", (rLoadSourceList.size() > 0));

        // check Sink exist only as it is difficult to verify the value on the fly
        assertTrue("failed to get ReplicationLoadSink.AgeOfLastShippedOp ",
          (rLoadSink.getAgeOfLastAppliedOp() >= 0));
        assertTrue("failed to get ReplicationLoadSink.TimeStampsOfLastAppliedOp ",
          (rLoadSink.getTimeStampsOfLastAppliedOp() >= 0));
      }
    }
  }
}
