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
import static org.junit.Assert.fail;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * Test handling of changes to the number of a peer's regionservers.
 */
@RunWith(Parameterized.class)
@Category({ ReplicationTests.class, LargeTests.class })
public class TestReplicationChangingPeerRegionservers extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationChangingPeerRegionservers.class);

  private static final Logger LOG =
      LoggerFactory.getLogger(TestReplicationChangingPeerRegionservers.class);

  @SuppressWarnings("checkstyle:VisibilityModifier") @Parameter
  public boolean serialPeer;

  @Override
  protected boolean isSerialPeer() {
    return serialPeer;
  }

  @Parameters(name = "{index}: serialPeer={0}")
  public static List<Boolean> parameters() {
    return ImmutableList.of(true, false);
  }

  @Before
  public void setUp() throws Exception {
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for (JVMClusterUtil.RegionServerThread r : UTIL1.getHBaseCluster()
        .getRegionServerThreads()) {
      UTIL1.getAdmin().rollWALWriter(r.getRegionServer().getServerName());
    }
    UTIL1.deleteTableData(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call deleteTableData on
    // utility2 since late writes could make it to the slave in some way.
    // Instead, we truncate the first table and wait for all the Deletes to
    // make it to the slave.
    Scan scan = new Scan();
    int lastCount = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
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

  @Test
  public void testChangingNumberOfPeerRegionServers() throws IOException, InterruptedException {
    LOG.info("testSimplePutDelete");
    MiniHBaseCluster peerCluster = UTIL2.getMiniHBaseCluster();
    // This test wants two RS's up. We only run one generally so add one.
    peerCluster.startRegionServer();
    Waiter.waitFor(peerCluster.getConfiguration(), 30000, new Waiter.Predicate<Exception>() {
      @Override public boolean evaluate() throws Exception {
        return peerCluster.getLiveRegionServerThreads().size() > 1;
      }
    });
    int numRS = peerCluster.getRegionServerThreads().size();

    doPutTest(Bytes.toBytes(1));

    int rsToStop = peerCluster.getServerWithMeta() == 0 ? 1 : 0;
    peerCluster.stopRegionServer(rsToStop);
    peerCluster.waitOnRegionServer(rsToStop);

    // Sanity check
    assertEquals(numRS - 1, peerCluster.getRegionServerThreads().size());

    doPutTest(Bytes.toBytes(2));

    peerCluster.startRegionServer();

    // Sanity check
    assertEquals(numRS, peerCluster.getRegionServerThreads().size());

    doPutTest(Bytes.toBytes(3));
  }

  private void doPutTest(byte[] row) throws IOException, InterruptedException {
    Put put = new Put(row);
    put.addColumn(famName, row, row);

    if (htable1 == null) {
      htable1 = UTIL1.getConnection().getTable(tableName);
    }

    htable1.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for put replication");
      }
      Result res = htable2.get(get);
      if (res.isEmpty()) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }
  }
}
