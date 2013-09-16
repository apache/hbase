/*
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test handling of changes to the number of a peer's regionservers.
 */
@Category(LargeTests.class)
public class TestReplicationChangingPeerRegionservers extends TestReplicationBase {

  private static final Log LOG = LogFactory.getLog(TestReplicationChangingPeerRegionservers.class);

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    htable1.setAutoFlush(false, true);
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for (JVMClusterUtil.RegionServerThread r :
                          utility1.getHBaseCluster().getRegionServerThreads()) {
      r.getRegionServer().getWAL().rollWriter();
    }
    utility1.truncateTable(tableName);
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call truncateTable on
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

  @Test(timeout = 300000)
  public void testChangingNumberOfPeerRegionServers() throws IOException, InterruptedException {

    LOG.info("testSimplePutDelete");
    MiniHBaseCluster peerCluster = utility2.getMiniHBaseCluster();

    doPutTest(Bytes.toBytes(1));

    int rsToStop = peerCluster.getServerWithMeta() == 0 ? 1 : 0;
    peerCluster.stopRegionServer(rsToStop);
    peerCluster.waitOnRegionServer(rsToStop);

    // Sanity check
    assertEquals(1, peerCluster.getRegionServerThreads().size());

    doPutTest(Bytes.toBytes(2));

    peerCluster.startRegionServer();

    // Sanity check
    assertEquals(2, peerCluster.getRegionServerThreads().size());

    doPutTest(Bytes.toBytes(3));

  }

  private void doPutTest(byte[] row) throws IOException, InterruptedException {
    Put put = new Put(row);
    put.add(famName, row, row);

    htable1 = new HTable(conf1, tableName);
    htable1.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
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

  }

}
