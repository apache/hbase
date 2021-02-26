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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Testcase for HBASE-20066
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestSequenceIdMonotonicallyIncreasing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSequenceIdMonotonicallyIncreasing.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final TableName NAME = TableName.valueOf("test");

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    Admin admin = UTIL.getAdmin();
    if (admin.tableExists(NAME)) {
      admin.disableTable(NAME);
      admin.deleteTable(NAME);
    }
  }

  private Table createTable(boolean multiRegions) throws IOException {
    if (multiRegions) {
      return UTIL.createTable(NAME, CF, new byte[][] { Bytes.toBytes(1) });
    } else {
      return UTIL.createTable(NAME, CF);
    }
  }

  private long getMaxSeqId(HRegionServer rs, RegionInfo region) throws IOException {
    Path walFile = ((AbstractFSWAL<?>) rs.getWAL(null)).getCurrentFileName();
    long maxSeqId = -1L;
    try (WAL.Reader reader =
      WALFactory.createReader(UTIL.getTestFileSystem(), walFile, UTIL.getConfiguration())) {
      for (;;) {
        WAL.Entry entry = reader.next();
        if (entry == null) {
          break;
        }
        if (Bytes.equals(region.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName())) {
          maxSeqId = Math.max(maxSeqId, entry.getKey().getSequenceId());
        }
      }
    }
    return maxSeqId;
  }

  @Test
  public void testSplit()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    try (Table table = createTable(false)) {
      table.put(new Put(Bytes.toBytes(0)).addColumn(CF, CQ, Bytes.toBytes(0)));
      table.put(new Put(Bytes.toBytes(1)).addColumn(CF, CQ, Bytes.toBytes(0)));
    }
    UTIL.flush(NAME);
    HRegionServer rs = UTIL.getRSForFirstRegionInTable(NAME);
    RegionInfo region = UTIL.getMiniHBaseCluster().getRegions(NAME).get(0).getRegionInfo();
    UTIL.getAdmin().splitRegionAsync(region.getRegionName(), Bytes.toBytes(1)).get(1,
      TimeUnit.MINUTES);
    long maxSeqId = getMaxSeqId(rs, region);
    RegionLocator locator = UTIL.getConnection().getRegionLocator(NAME);
    HRegionLocation locA = locator.getRegionLocation(Bytes.toBytes(0), true);
    HRegionLocation locB = locator.getRegionLocation(Bytes.toBytes(1), true);
    assertEquals(maxSeqId + 1, locA.getSeqNum());
    assertEquals(maxSeqId + 1, locB.getSeqNum());
  }

  @Test
  public void testMerge()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    try (Table table = createTable(true)) {
      table.put(new Put(Bytes.toBytes(0)).addColumn(CF, CQ, Bytes.toBytes(0)));
      table.put(new Put(Bytes.toBytes(1)).addColumn(CF, CQ, Bytes.toBytes(0)));
      table.put(new Put(Bytes.toBytes(2)).addColumn(CF, CQ, Bytes.toBytes(0)));
    }
    UTIL.flush(NAME);
    MiniHBaseCluster cluster = UTIL.getMiniHBaseCluster();
    List<HRegion> regions = cluster.getRegions(NAME);
    HRegion regionA = regions.get(0);
    HRegion regionB = regions.get(1);
    HRegionServer rsA =
      cluster.getRegionServer(cluster.getServerWith(regionA.getRegionInfo().getRegionName()));
    HRegionServer rsB =
      cluster.getRegionServer(cluster.getServerWith(regionB.getRegionInfo().getRegionName()));
    UTIL.getAdmin().mergeRegionsAsync(regionA.getRegionInfo().getRegionName(),
      regionB.getRegionInfo().getRegionName(), false).get(1, TimeUnit.MINUTES);
    long maxSeqIdA = getMaxSeqId(rsA, regionA.getRegionInfo());
    long maxSeqIdB = getMaxSeqId(rsB, regionB.getRegionInfo());
    HRegionLocation loc =
      UTIL.getConnection().getRegionLocator(NAME).getRegionLocation(Bytes.toBytes(0), true);
    assertEquals(Math.max(maxSeqIdA, maxSeqIdB) + 1, loc.getSeqNum());
  }
}
