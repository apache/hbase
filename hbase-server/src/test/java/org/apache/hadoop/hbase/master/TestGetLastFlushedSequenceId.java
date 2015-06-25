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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Trivial test to confirm that we can get last flushed sequence id by encodedRegionName. See
 * HBASE-12715.
 */
@Category(MediumTests.class)
public class TestGetLastFlushedSequenceId {

  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();

  private final TableName tableName = TableName.valueOf(getClass().getSimpleName(), "test");

  private final byte[] family = Bytes.toBytes("f1");

  private final byte[][] families = new byte[][] { family };

  @Before
  public void setUp() throws Exception {
    testUtil.getConfiguration().setInt("hbase.regionserver.msginterval", 1000);
    testUtil.startMiniCluster(1, 1);
  }

  @After
  public void tearDown() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    testUtil.getHBaseAdmin().createNamespace(
      NamespaceDescriptor.create(tableName.getNamespaceAsString()).build());
    Table table = testUtil.createTable(tableName, families);
    table.put(new Put(Bytes.toBytes("k")).add(family, Bytes.toBytes("q"), Bytes.toBytes("v")));
    MiniHBaseCluster cluster = testUtil.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    Region region = null;
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region r : hrs.getOnlineRegions(tableName)) {
        region = r;
        break;
      }
    }
    assertNotNull(region);
    Thread.sleep(2000);
    RegionStoreSequenceIds ids =
        testUtil.getHBaseCluster().getMaster()
            .getLastSequenceId(region.getRegionInfo().getEncodedNameAsBytes());
    assertEquals(HConstants.NO_SEQNUM, ids.getLastFlushedSequenceId());
    // This will be the sequenceid just before that of the earliest edit in memstore.
    long storeSequenceId = ids.getStoreSequenceId(0).getSequenceId();
    assertTrue(storeSequenceId > 0);
    testUtil.getHBaseAdmin().flush(tableName);
    Thread.sleep(2000);
    ids =
        testUtil.getHBaseCluster().getMaster()
            .getLastSequenceId(region.getRegionInfo().getEncodedNameAsBytes());
    assertTrue(ids.getLastFlushedSequenceId() + " > " + storeSequenceId,
      ids.getLastFlushedSequenceId() > storeSequenceId);
    assertEquals(ids.getLastFlushedSequenceId(), ids.getStoreSequenceId(0).getSequenceId());
    table.close();
  }
}
