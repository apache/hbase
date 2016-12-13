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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.CompactingMemStore;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hdfs.MiniDFSCluster.DataNodeProperties;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ VerySlowRegionServerTests.class, LargeTests.class })
public class TestAsyncLogRolling extends AbstractTestLogRolling {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TestAsyncLogRolling.TEST_UTIL.getConfiguration();
    conf.setInt(AsyncFSWAL.ASYNC_WAL_CREATE_MAX_RETRIES, 100);
    conf.set(WALFactory.WAL_PROVIDER, "asyncfs");
    AbstractTestLogRolling.setUpBeforeClass();
  }

  @Test(timeout = 180000)
  public void testLogRollOnDatanodeDeath() throws IOException, InterruptedException {
    dfsCluster.startDataNodes(TEST_UTIL.getConfiguration(), 3, true, null, null);
    tableName = getName();
    Table table = createTestTable(tableName);
    TEST_UTIL.waitUntilAllRegionsAssigned(table.getName());
    doPut(table, 1);
    server = TEST_UTIL.getRSForFirstRegionInTable(table.getName());
    HRegionInfo hri = server.getOnlineRegions(table.getName()).get(0).getRegionInfo();
    AsyncFSWAL wal = (AsyncFSWAL) server.getWAL(hri);
    int numRolledLogFiles = AsyncFSWALProvider.getNumRolledLogFiles(wal);
    DatanodeInfo[] dnInfos = wal.getPipeline();
    DataNodeProperties dnProp = TEST_UTIL.getDFSCluster().stopDataNode(dnInfos[0].getName());
    TEST_UTIL.getDFSCluster().restartDataNode(dnProp);
    doPut(table, 2);
    assertEquals(numRolledLogFiles + 1, AsyncFSWALProvider.getNumRolledLogFiles(wal));
  }
}
