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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ReplicationTests.class, LargeTests.class })
public class TestBidirectionSerialReplicationStuck extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBidirectionSerialReplicationStuck.class);

  @Override
  protected boolean isSerialPeer() {
    return true;
  }

  @Override
  public void setUpBase() throws Exception {
    UTIL1.ensureSomeRegionServersAvailable(2);
    hbaseAdmin.balancerSwitch(false, true);
    addPeer(PEER_ID2, tableName, UTIL1, UTIL2);
    addPeer(PEER_ID2, tableName, UTIL2, UTIL1);
  }

  @Override
  public void tearDownBase() throws Exception {
    removePeer(PEER_ID2, UTIL1);
    removePeer(PEER_ID2, UTIL2);
  }

  @Test
  public void testStuck() throws Exception {
    // disable the peer cluster1 -> cluster2
    hbaseAdmin.disableReplicationPeer(PEER_ID2);
    byte[] qualifier = Bytes.toBytes("q");
    htable1.put(new Put(Bytes.toBytes("aaa-1")).addColumn(famName, qualifier, Bytes.toBytes(1)));

    // add a row to cluster2 and wait it replicate back to cluster1
    htable2.put(new Put(Bytes.toBytes("aaa-2")).addColumn(famName, qualifier, Bytes.toBytes(2)));
    UTIL1.waitFor(30000, () -> htable1.exists(new Get(Bytes.toBytes("aaa-2"))));

    // kill the region server which holds the region which contains our rows
    UTIL1.getRSForFirstRegionInTable(tableName).abort("for testing");
    // wait until the region is online
    UTIL1.waitFor(30000, () -> htable1.exists(new Get(Bytes.toBytes("aaa-2"))));

    // put a new row in cluster1
    htable1.put(new Put(Bytes.toBytes("aaa-3")).addColumn(famName, qualifier, Bytes.toBytes(3)));

    // enable peer cluster1 -> cluster2, the new row should be replicated to cluster2
    hbaseAdmin.enableReplicationPeer(PEER_ID2);
    UTIL1.waitFor(30000, () -> htable2.exists(new Get(Bytes.toBytes("aaa-3"))));
  }
}
