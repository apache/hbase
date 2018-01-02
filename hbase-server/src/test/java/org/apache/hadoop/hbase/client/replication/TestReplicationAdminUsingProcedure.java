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
package org.apache.hadoop.hbase.client.replication;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;

@Category({ MediumTests.class, ClientTests.class })
public class TestReplicationAdminUsingProcedure extends TestReplicationBase {

  private static final String PEER_ID = "2";
  private static final Logger LOG = Logger.getLogger(TestReplicationAdminUsingProcedure.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.setInt("hbase.multihconnection.threads.max", 10);

    // Start the master & slave mini cluster.
    TestReplicationBase.setUpBeforeClass();

    // Remove the replication peer
    hbaseAdmin.removeReplicationPeer(PEER_ID);
  }

  private void loadData(int startRowKey, int endRowKey) throws IOException {
    for (int i = startRowKey; i < endRowKey; i++) {
      byte[] rowKey = Bytes.add(row, Bytes.toBytes(i));
      Put put = new Put(rowKey);
      put.addColumn(famName, null, Bytes.toBytes(i));
      htable1.put(put);
    }
  }

  private void waitForReplication(int expectedRows, int retries)
      throws IOException, InterruptedException {
    Scan scan;
    for (int i = 0; i < retries; i++) {
      scan = new Scan();
      if (i == retries - 1) {
        throw new IOException("Waited too much time for normal batch replication");
      }
      try (ResultScanner scanner = htable2.getScanner(scan)) {
        int count = 0;
        for (Result res : scanner) {
          count++;
        }
        if (count != expectedRows) {
          LOG.info("Only got " + count + " rows,  expected rows: " + expectedRows);
          Thread.sleep(SLEEP_TIME);
        } else {
          return;
        }
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    hbaseAdmin.addReplicationPeer(PEER_ID, rpc);

    utility1.waitUntilAllRegionsAssigned(tableName);
    utility2.waitUntilAllRegionsAssigned(tableName);
  }

  @After
  public void tearDown() throws IOException {
    hbaseAdmin.removeReplicationPeer(PEER_ID);
    truncateBoth();
  }

  private void truncateBoth() throws IOException {
    utility1.deleteTableData(tableName);
    utility2.deleteTableData(tableName);
  }

  @Test
  public void testAddPeer() throws Exception {
    // Load data
    loadData(0, NB_ROWS_IN_BATCH);

    // Wait the replication finished
    waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);
  }

  @Test
  public void testRemovePeer() throws Exception {
    // prev-check
    waitForReplication(0, NB_RETRIES);

    // Load data
    loadData(0, NB_ROWS_IN_BATCH);

    // Wait the replication finished
    waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);

    // Remove the peer id
    hbaseAdmin.removeReplicationPeer(PEER_ID);

    // Load data again
    loadData(NB_ROWS_IN_BATCH, 2 * NB_ROWS_IN_BATCH);

    // Wait the replication again
    boolean foundException = false;
    try {
      waitForReplication(NB_ROWS_IN_BATCH * 2, NB_RETRIES);
    } catch (IOException e) {
      foundException = true;
    }
    Assert.assertTrue(foundException);

    // Truncate the table in source cluster
    truncateBoth();

    // Add peer again
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    hbaseAdmin.addReplicationPeer(PEER_ID, rpc);

    // Load data again
    loadData(0, NB_ROWS_IN_BATCH);

    // Wait the replication finished
    waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);
  }

  @Test
  public void testDisableAndEnablePeer() throws Exception {
    // disable peer
    hbaseAdmin.disableReplicationPeer(PEER_ID);

    // Load data
    loadData(0, NB_ROWS_IN_BATCH);

    // Will failed to wait the replication.
    boolean foundException = false;
    try {
      waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);
    } catch (IOException e) {
      foundException = true;
    }
    Assert.assertTrue(foundException);

    // Enable the peer
    hbaseAdmin.enableReplicationPeer(PEER_ID);
    waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);

    // Load more data
    loadData(NB_ROWS_IN_BATCH, NB_ROWS_IN_BATCH * 2);

    // Wait replication again.
    waitForReplication(NB_ROWS_IN_BATCH * 2, NB_RETRIES);
  }

  @Test
  public void testUpdatePeerConfig() throws Exception {
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setExcludeTableCFsMap(
      ImmutableMap.of(tableName, ImmutableList.of(Bytes.toString(famName))));

    // Update the peer config to exclude the test table name.
    hbaseAdmin.updateReplicationPeerConfig(PEER_ID, rpc);

    // Load data
    loadData(0, NB_ROWS_IN_BATCH);

    // Will failed to wait the replication
    boolean foundException = false;
    try {
      waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);
    } catch (IOException e) {
      foundException = true;
    }
    Assert.assertTrue(foundException);

    // Truncate the table in source cluster
    truncateBoth();

    // Update the peer config to include the test table name.
    ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
    rpc2.setClusterKey(utility2.getClusterKey());
    hbaseAdmin.updateReplicationPeerConfig(PEER_ID, rpc2);

    // Load data again
    loadData(0, NB_ROWS_IN_BATCH);

    // Wait the replication finished
    waitForReplication(NB_ROWS_IN_BATCH, NB_RETRIES);
  }
}
