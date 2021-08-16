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
package org.apache.hadoop.hbase.compactionserver;

import java.net.InetSocketAddress;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.TestRegionFavoredNodes;
import org.apache.hadoop.hbase.testclassification.CompactionServerTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ CompactionServerTests.class, MediumTests.class })
public class TestRegionFavoredNodesWhenCompactOffload extends TestRegionFavoredNodes {
  private static HCompactionServer COMPACTION_SERVER;
  private static final int FLUSHES = 10;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionFavoredNodesWhenCompactOffload.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    try {
      checkFileSystemWithFavoredNode();
    } catch (NoSuchMethodException nm) {
      return;
    }
    TableDescriptor tableDescriptor =
        TableDescriptorBuilder.newBuilder(TABLE_NAME).setCompactionOffloadEnabled(true).build();
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder().numCompactionServers(1)
      .numDataNodes(REGION_SERVERS).numRegionServers(REGION_SERVERS).build());
    TEST_UTIL.getAdmin().switchCompactionOffload(true);
    TEST_UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
    table = TEST_UTIL.createTable(tableDescriptor, Bytes.toByteArrays(COLUMN_FAMILY),
      HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE, TEST_UTIL.getConfiguration());
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
    COMPACTION_SERVER = TEST_UTIL.getMiniHBaseCluster().getCompactionServerThreads().get(0)
      .getCompactionServer();
  }

  @Test
  public void testFavoredNodes() throws Exception {
    Assume.assumeTrue(createWithFavoredNode != null);
    InetSocketAddress[] nodes = getDataNodes();
    String[] nodeNames = new String[REGION_SERVERS];
    for (int i = 0; i < REGION_SERVERS; i++) {
      nodeNames[i] = nodes[i].getAddress().getHostAddress() + ":" + nodes[i].getPort();
    }
    updateFavoredNodes(nodes);
    // Write some data to each region and flush. Repeat some number of times to
    // get multiple files for each region.
    for (int i = 0; i < FLUSHES; i++) {
      TEST_UTIL.loadTable(table, COLUMN_FAMILY, false);
      TEST_UTIL.flush();
    }
    TEST_UTIL.compact(TABLE_NAME, true);
    TEST_UTIL.waitFor(60000, () -> {
      int hFileCount = 0;
      for (HRegion region : TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME)) {
        hFileCount += region.getStore(COLUMN_FAMILY).getStorefilesCount();

      }
      return hFileCount == HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE.length + 1;
    });
    checkFavoredNodes(nodeNames);
    // To ensure do compaction on compaction server
    TEST_UTIL.waitFor(60000, () -> COMPACTION_SERVER.requestCount.sum() > 0);
  }

}
