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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Confirm that the empty replication endpoint can work.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestVerifyCellsReplicationEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestVerifyCellsReplicationEndpoint.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestVerifyCellsReplicationEndpoint.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("empty");

  private static final byte[] CF = Bytes.toBytes("family");

  private static final byte[] CQ = Bytes.toBytes("qualifier");

  private static final String PEER_ID = "empty";

  private static final BlockingQueue<Cell> CELLS = new LinkedBlockingQueue<>();

  public static final class EndpointForTest extends VerifyWALEntriesReplicationEndpoint {

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
      LOG.info(replicateContext.getEntries().toString());
      replicateContext.entries.stream().map(WAL.Entry::getEdit).map(WALEdit::getCells)
        .forEachOrdered(CELLS::addAll);
      return super.replicate(replicateContext);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(3);
    // notice that we do not need to set replication scope here, EmptyReplicationEndpoint take all
    // edits no matter what the replications scope is.
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.getAdmin().addReplicationPeer(PEER_ID,
      ReplicationPeerConfig.newBuilder().setClusterKey("zk1:8888:/hbase")
        .setReplicationEndpointImpl(EndpointForTest.class.getName()).build());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    try (Table table = UTIL.getConnection().getTable(TABLE_NAME)) {
      for (int i = 0; i < 100; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      }
    }
    long lastNoCellTime = -1;
    for (int i = 0; i < 100;) {
      Cell cell = CELLS.poll();
      if (cell == null) {
        if (lastNoCellTime < 0) {
          lastNoCellTime = System.nanoTime();
        } else {
          if (System.nanoTime() - lastNoCellTime >= TimeUnit.SECONDS.toNanos(30)) {
            throw new TimeoutException("Timeout waiting for wal edit");
          }
        }
        Thread.sleep(1000);
        continue;
      }
      lastNoCellTime = -1;
      if (!Bytes.equals(CF, CellUtil.cloneFamily(cell))) {
        // meta edits, such as open/close/flush, etc. skip
        continue;
      }
      assertArrayEquals(Bytes.toBytes(i), CellUtil.cloneRow(cell));
      assertArrayEquals(CQ, CellUtil.cloneQualifier(cell));
      assertArrayEquals(Bytes.toBytes(i), CellUtil.cloneValue(cell));
      i++;
    }
  }
}
