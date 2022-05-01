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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestWideScanner {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestWideScanner.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final Logger LOG = LoggerFactory.getLogger(TestWideScanner.class);

  private static final byte[] A = Bytes.toBytes("A");
  private static final byte[] B = Bytes.toBytes("B");
  private static final byte[] C = Bytes.toBytes("C");
  private static byte[][] COLUMNS = { A, B, C };

  private static final TableDescriptor TESTTABLEDESC;
  static {
    TableDescriptorBuilder builder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("testwidescan"));
    for (byte[] cfName : new byte[][] { A, B, C }) {
      // Keep versions to help debugging.
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cfName).setMaxVersions(100)
        .setBlocksize(8 * 1024).build());
    }
    TESTTABLEDESC = builder.build();
  }

  /** HRegionInfo for root region */
  private static HRegion REGION;

  @BeforeClass
  public static void setUp() throws IOException {
    Path testDir = UTIL.getDataTestDir();
    RegionInfo hri = RegionInfoBuilder.newBuilder(TESTTABLEDESC.getTableName()).build();
    REGION =
      HBaseTestingUtility.createRegionAndWAL(hri, testDir, UTIL.getConfiguration(), TESTTABLEDESC);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (REGION != null) {
      HBaseTestingUtility.closeRegionAndWAL(REGION);
      REGION = null;
    }
    UTIL.cleanupTestDir();
  }

  private int addWideContent(HRegion region) throws IOException {
    int count = 0;
    for (char c = 'a'; c <= 'c'; c++) {
      byte[] row = Bytes.toBytes("ab" + c);
      int i, j;
      long ts = EnvironmentEdgeManager.currentTime();
      for (i = 0; i < 100; i++) {
        byte[] b = Bytes.toBytes(String.format("%10d", i));
        for (j = 0; j < 100; j++) {
          Put put = new Put(row);
          put.setDurability(Durability.SKIP_WAL);
          long ts1 = ++ts;
          put.addColumn(COLUMNS[ThreadLocalRandom.current().nextInt(COLUMNS.length)], b, ts1, b);
          region.put(put);
          count++;
        }
      }
    }
    return count;
  }

  @Test
  public void testWideScanBatching() throws IOException {
    final int batch = 256;
    int inserted = addWideContent(REGION);
    List<Cell> results = new ArrayList<>();
    Scan scan = new Scan();
    scan.addFamily(A);
    scan.addFamily(B);
    scan.addFamily(C);
    scan.readVersions(100);
    scan.setBatch(batch);
    try (InternalScanner s = REGION.getScanner(scan)) {
      int total = 0;
      int i = 0;
      boolean more;
      do {
        more = s.next(results);
        i++;
        LOG.info("iteration #" + i + ", results.size=" + results.size());

        // assert that the result set is no larger
        assertTrue(results.size() <= batch);

        total += results.size();

        if (results.size() > 0) {
          // assert that all results are from the same row
          byte[] row = CellUtil.cloneRow(results.get(0));
          for (Cell kv : results) {
            assertTrue(Bytes.equals(row, CellUtil.cloneRow(kv)));
          }
        }

        results.clear();

        // trigger ChangedReadersObservers
        Iterator<KeyValueScanner> scanners = ((RegionScannerImpl) s).storeHeap.getHeap().iterator();
        while (scanners.hasNext()) {
          StoreScanner ss = (StoreScanner) scanners.next();
          ss.updateReaders(Collections.emptyList(), Collections.emptyList());
        }
      } while (more);

      // assert that the scanner returned all values
      LOG.info("inserted " + inserted + ", scanned " + total);
      assertEquals(total, inserted);
    }
  }
}
