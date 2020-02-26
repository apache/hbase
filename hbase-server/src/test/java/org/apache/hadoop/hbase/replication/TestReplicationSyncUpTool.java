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

import static org.apache.hadoop.hbase.replication.TestReplicationBase.NB_RETRIES;
import static org.apache.hadoop.hbase.replication.TestReplicationBase.NB_ROWS_IN_BATCH;
import static org.apache.hadoop.hbase.replication.TestReplicationBase.SLEEP_TIME;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationSyncUpTool extends TestReplicationSyncUpToolBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestReplicationSyncUpTool.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestReplicationSyncUpTool.class);

  /**
   * Add a row to a table in each cluster, check it's replicated, delete it, check's gone Also check
   * the puts and deletes are not replicated back to the originating cluster.
   */
  @Test
  public void testSyncUpTool() throws Exception {

    /**
     * Set up Replication: on Master and one Slave Table: t1_syncup and t2_syncup columnfamily:
     * 'cf1' : replicated 'norep': not replicated
     */
    setupReplication();

    /**
     * at Master: t1_syncup: put 100 rows into cf1, and 1 rows into norep t2_syncup: put 200 rows
     * into cf1, and 1 rows into norep verify correctly replicated to slave
     */
    putAndReplicateRows();

    /**
     * Verify delete works step 1: stop hbase on Slave step 2: at Master: t1_syncup: delete 50 rows
     * from cf1 t2_syncup: delete 100 rows from cf1 no change on 'norep' step 3: stop hbase on
     * master, restart hbase on Slave step 4: verify Slave still have the rows before delete
     * t1_syncup: 100 rows from cf1 t2_syncup: 200 rows from cf1 step 5: run syncup tool on Master
     * step 6: verify that delete show up on Slave t1_syncup: 50 rows from cf1 t2_syncup: 100 rows
     * from cf1 verify correctly replicated to Slave
     */
    mimicSyncUpAfterDelete();

    /**
     * Verify put works step 1: stop hbase on Slave step 2: at Master: t1_syncup: put 100 rows from
     * cf1 t2_syncup: put 200 rows from cf1 and put another row on 'norep' ATTN: put to 'cf1' will
     * overwrite existing rows, so end count will be 100 and 200 respectively put to 'norep' will
     * add a new row. step 3: stop hbase on master, restart hbase on Slave step 4: verify Slave
     * still has the rows before put t1_syncup: 50 rows from cf1 t2_syncup: 100 rows from cf1 step
     * 5: run syncup tool on Master step 6: verify that put show up on Slave and 'norep' does not
     * t1_syncup: 100 rows from cf1 t2_syncup: 200 rows from cf1 verify correctly replicated to
     * Slave
     */
    mimicSyncUpAfterPut();
  }

  private void putAndReplicateRows() throws Exception {
    LOG.debug("putAndReplicateRows");
    // add rows to Master cluster,
    Put p;

    // 100 + 1 row to t1_syncup
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("val" + i));
      ht1Source.put(p);
    }
    p = new Put(Bytes.toBytes("row" + 9999));
    p.addColumn(NO_REP_FAMILY, QUALIFIER, Bytes.toBytes("val" + 9999));
    ht1Source.put(p);

    // 200 + 1 row to t2_syncup
    for (int i = 0; i < NB_ROWS_IN_BATCH * 2; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("val" + i));
      ht2Source.put(p);
    }
    p = new Put(Bytes.toBytes("row" + 9999));
    p.addColumn(NO_REP_FAMILY, QUALIFIER, Bytes.toBytes("val" + 9999));
    ht2Source.put(p);

    // ensure replication completed
    Thread.sleep(SLEEP_TIME);
    int rowCountHt1Source = UTIL1.countRows(ht1Source);
    for (int i = 0; i < NB_RETRIES; i++) {
      int rowCountHt1TargetAtPeer1 = UTIL2.countRows(ht1TargetAtPeer1);
      if (i == NB_RETRIES - 1) {
        assertEquals("t1_syncup has 101 rows on source, and 100 on slave1", rowCountHt1Source - 1,
          rowCountHt1TargetAtPeer1);
      }
      if (rowCountHt1Source - 1 == rowCountHt1TargetAtPeer1) {
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }

    int rowCountHt2Source = UTIL1.countRows(ht2Source);
    for (int i = 0; i < NB_RETRIES; i++) {
      int rowCountHt2TargetAtPeer1 = UTIL2.countRows(ht2TargetAtPeer1);
      if (i == NB_RETRIES - 1) {
        assertEquals("t2_syncup has 201 rows on source, and 200 on slave1", rowCountHt2Source - 1,
          rowCountHt2TargetAtPeer1);
      }
      if (rowCountHt2Source - 1 == rowCountHt2TargetAtPeer1) {
        break;
      }
      Thread.sleep(SLEEP_TIME);
    }
  }

  private void mimicSyncUpAfterDelete() throws Exception {
    LOG.debug("mimicSyncUpAfterDelete");
    shutDownTargetHBaseCluster();

    List<Delete> list = new ArrayList<>();
    // delete half of the rows
    for (int i = 0; i < NB_ROWS_IN_BATCH / 2; i++) {
      String rowKey = "row" + i;
      Delete del = new Delete(Bytes.toBytes(rowKey));
      list.add(del);
    }
    ht1Source.delete(list);

    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      String rowKey = "row" + i;
      Delete del = new Delete(Bytes.toBytes(rowKey));
      list.add(del);
    }
    ht2Source.delete(list);

    int rowCount_ht1Source = UTIL1.countRows(ht1Source);
    assertEquals("t1_syncup has 51 rows on source, after remove 50 of the replicated colfam", 51,
      rowCount_ht1Source);

    int rowCount_ht2Source = UTIL1.countRows(ht2Source);
    assertEquals("t2_syncup has 101 rows on source, after remove 100 of the replicated colfam", 101,
      rowCount_ht2Source);

    shutDownSourceHBaseCluster();
    restartTargetHBaseCluster(1);

    Thread.sleep(SLEEP_TIME);

    // before sync up
    int rowCountHt1TargetAtPeer1 = UTIL2.countRows(ht1TargetAtPeer1);
    int rowCountHt2TargetAtPeer1 = UTIL2.countRows(ht2TargetAtPeer1);
    assertEquals("@Peer1 t1_syncup should still have 100 rows", 100, rowCountHt1TargetAtPeer1);
    assertEquals("@Peer1 t2_syncup should still have 200 rows", 200, rowCountHt2TargetAtPeer1);

    // After sync up
    for (int i = 0; i < NB_RETRIES; i++) {
      syncUp(UTIL1);
      rowCountHt1TargetAtPeer1 = UTIL2.countRows(ht1TargetAtPeer1);
      rowCountHt2TargetAtPeer1 = UTIL2.countRows(ht2TargetAtPeer1);
      if (i == NB_RETRIES - 1) {
        if (rowCountHt1TargetAtPeer1 != 50 || rowCountHt2TargetAtPeer1 != 100) {
          // syncUP still failed. Let's look at the source in case anything wrong there
          restartSourceHBaseCluster(1);
          rowCount_ht1Source = UTIL1.countRows(ht1Source);
          LOG.debug("t1_syncup should have 51 rows at source, and it is " + rowCount_ht1Source);
          rowCount_ht2Source = UTIL1.countRows(ht2Source);
          LOG.debug("t2_syncup should have 101 rows at source, and it is " + rowCount_ht2Source);
        }
        assertEquals("@Peer1 t1_syncup should be sync up and have 50 rows", 50,
          rowCountHt1TargetAtPeer1);
        assertEquals("@Peer1 t2_syncup should be sync up and have 100 rows", 100,
          rowCountHt2TargetAtPeer1);
      }
      if (rowCountHt1TargetAtPeer1 == 50 && rowCountHt2TargetAtPeer1 == 100) {
        LOG.info("SyncUpAfterDelete succeeded at retry = " + i);
        break;
      } else {
        LOG.debug("SyncUpAfterDelete failed at retry = " + i + ", with rowCount_ht1TargetPeer1 =" +
          rowCountHt1TargetAtPeer1 + " and rowCount_ht2TargetAtPeer1 =" + rowCountHt2TargetAtPeer1);
      }
      Thread.sleep(SLEEP_TIME);
    }
  }

  private void mimicSyncUpAfterPut() throws Exception {
    LOG.debug("mimicSyncUpAfterPut");
    restartSourceHBaseCluster(1);
    shutDownTargetHBaseCluster();

    Put p;
    // another 100 + 1 row to t1_syncup
    // we should see 100 + 2 rows now
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("val" + i));
      ht1Source.put(p);
    }
    p = new Put(Bytes.toBytes("row" + 9998));
    p.addColumn(NO_REP_FAMILY, QUALIFIER, Bytes.toBytes("val" + 9998));
    ht1Source.put(p);

    // another 200 + 1 row to t1_syncup
    // we should see 200 + 2 rows now
    for (int i = 0; i < NB_ROWS_IN_BATCH * 2; i++) {
      p = new Put(Bytes.toBytes("row" + i));
      p.addColumn(FAMILY, QUALIFIER, Bytes.toBytes("val" + i));
      ht2Source.put(p);
    }
    p = new Put(Bytes.toBytes("row" + 9998));
    p.addColumn(NO_REP_FAMILY, QUALIFIER, Bytes.toBytes("val" + 9998));
    ht2Source.put(p);

    int rowCount_ht1Source = UTIL1.countRows(ht1Source);
    assertEquals("t1_syncup has 102 rows on source", 102, rowCount_ht1Source);
    int rowCount_ht2Source = UTIL1.countRows(ht2Source);
    assertEquals("t2_syncup has 202 rows on source", 202, rowCount_ht2Source);

    shutDownSourceHBaseCluster();
    restartTargetHBaseCluster(1);

    Thread.sleep(SLEEP_TIME);

    // before sync up
    int rowCountHt1TargetAtPeer1 = UTIL2.countRows(ht1TargetAtPeer1);
    int rowCountHt2TargetAtPeer1 = UTIL2.countRows(ht2TargetAtPeer1);
    assertEquals("@Peer1 t1_syncup should be NOT sync up and have 50 rows", 50,
      rowCountHt1TargetAtPeer1);
    assertEquals("@Peer1 t2_syncup should be NOT sync up and have 100 rows", 100,
      rowCountHt2TargetAtPeer1);

    // after syun up
    for (int i = 0; i < NB_RETRIES; i++) {
      syncUp(UTIL1);
      rowCountHt1TargetAtPeer1 = UTIL2.countRows(ht1TargetAtPeer1);
      rowCountHt2TargetAtPeer1 = UTIL2.countRows(ht2TargetAtPeer1);
      if (i == NB_RETRIES - 1) {
        if (rowCountHt1TargetAtPeer1 != 100 || rowCountHt2TargetAtPeer1 != 200) {
          // syncUP still failed. Let's look at the source in case anything wrong there
          restartSourceHBaseCluster(1);
          rowCount_ht1Source = UTIL1.countRows(ht1Source);
          LOG.debug("t1_syncup should have 102 rows at source, and it is " + rowCount_ht1Source);
          rowCount_ht2Source = UTIL1.countRows(ht2Source);
          LOG.debug("t2_syncup should have 202 rows at source, and it is " + rowCount_ht2Source);
        }
        assertEquals("@Peer1 t1_syncup should be sync up and have 100 rows", 100,
          rowCountHt1TargetAtPeer1);
        assertEquals("@Peer1 t2_syncup should be sync up and have 200 rows", 200,
          rowCountHt2TargetAtPeer1);
      }
      if (rowCountHt1TargetAtPeer1 == 100 && rowCountHt2TargetAtPeer1 == 200) {
        LOG.info("SyncUpAfterPut succeeded at retry = " + i);
        break;
      } else {
        LOG.debug("SyncUpAfterPut failed at retry = " + i + ", with rowCount_ht1TargetPeer1 =" +
          rowCountHt1TargetAtPeer1 + " and rowCount_ht2TargetAtPeer1 =" + rowCountHt2TargetAtPeer1);
      }
      Thread.sleep(SLEEP_TIME);
    }
  }
}
