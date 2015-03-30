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

package org.apache.hadoop.hbase.io.encoding;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner.NextState;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPrefixTree {

  private static final String row4 = "a-b-B-2-1402397300-1402416535";
  private static final byte[] row4_bytes = Bytes.toBytes(row4);
  private static final String row3 = "a-b-A-1-1402397227-1402415999";
  private static final byte[] row3_bytes = Bytes.toBytes(row3);
  private static final String row2 = "a-b-A-1-1402329600-1402396277";
  private static final byte[] row2_bytes = Bytes.toBytes(row2);
  private static final String row1 = "a-b-A-1";
  private static final byte[] row1_bytes = Bytes.toBytes(row1);

  private final static byte[] fam = Bytes.toBytes("cf_1");
  private final static byte[] qual1 = Bytes.toBytes("qf_1");
  private final static byte[] qual2 = Bytes.toBytes("qf_2");

  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();

  private HRegion region;

  @Before
  public void setUp() throws Exception {
    TableName tableName = TableName.valueOf(getClass().getSimpleName());
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor(fam).setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE));
    HRegionInfo info = new HRegionInfo(tableName, null, null, false);
    Path path = testUtil.getDataTestDir(getClass().getSimpleName());
    region = HRegion.createHRegion(info, path, testUtil.getConfiguration(), htd);
  }

  @After
  public void tearDown() throws Exception {
    region.close(true);
    testUtil.cleanupTestDir();
  }

  @Test
  public void testHBASE11728() throws Exception {
    Put put = new Put(Bytes.toBytes("a-b-0-0"));
    put.addColumn(fam, qual1, Bytes.toBytes("c1-value"));
    region.put(put);
    put = new Put(row1_bytes);
    put.addColumn(fam, qual1, Bytes.toBytes("c1-value"));
    region.put(put);
    put = new Put(row2_bytes);
    put.addColumn(fam, qual2, Bytes.toBytes("c2-value"));
    region.put(put);
    put = new Put(row3_bytes);
    put.addColumn(fam, qual2, Bytes.toBytes("c2-value-2"));
    region.put(put);
    put = new Put(row4_bytes);
    put.addColumn(fam, qual2, Bytes.toBytes("c2-value-3"));
    region.put(put);
    region.flush(true);
    String[] rows = new String[3];
    rows[0] = row1;
    rows[1] = row2;
    rows[2] = row3;
    byte[][] val = new byte[3][];
    val[0] = Bytes.toBytes("c1-value");
    val[1] = Bytes.toBytes("c2-value");
    val[2] = Bytes.toBytes("c2-value-2");
    Scan scan = new Scan();
    scan.setStartRow(row1_bytes);
    scan.setStopRow(Bytes.toBytes("a-b-A-1:"));

    RegionScanner scanner = region.getScanner(scan);
    List<Cell> cells = new ArrayList<Cell>();
    for (int i = 0; i < 3; i++) {
      assertEquals(i < 2, NextState.hasMoreValues(scanner.next(cells)));
      CellScanner cellScanner = Result.create(cells).cellScanner();
      while (cellScanner.advance()) {
        assertEquals(rows[i], Bytes.toString(cellScanner.current().getRowArray(), cellScanner
            .current().getRowOffset(), cellScanner.current().getRowLength()));
        assertEquals(Bytes.toString(val[i]), Bytes.toString(cellScanner.current().getValueArray(),
          cellScanner.current().getValueOffset(), cellScanner.current().getValueLength()));
      }
      cells.clear();
    }
    scanner.close();

    // Add column
    scan = new Scan();
    scan.addColumn(fam, qual2);
    scan.setStartRow(row1_bytes);
    scan.setStopRow(Bytes.toBytes("a-b-A-1:"));
    scanner = region.getScanner(scan);
    for (int i = 1; i < 3; i++) {
      assertEquals(i < 2, NextState.hasMoreValues(scanner.next(cells)));
      CellScanner cellScanner = Result.create(cells).cellScanner();
      while (cellScanner.advance()) {
        assertEquals(rows[i], Bytes.toString(cellScanner.current().getRowArray(), cellScanner
            .current().getRowOffset(), cellScanner.current().getRowLength()));
      }
      cells.clear();
    }
    scanner.close();

    scan = new Scan();
    scan.addColumn(fam, qual2);
    scan.setStartRow(Bytes.toBytes("a-b-A-1-"));
    scan.setStopRow(Bytes.toBytes("a-b-A-1:"));
    scanner = region.getScanner(scan);
    for (int i = 1; i < 3; i++) {
      assertEquals(i < 2, NextState.hasMoreValues(scanner.next(cells)));
      CellScanner cellScanner = Result.create(cells).cellScanner();
      while (cellScanner.advance()) {
        assertEquals(rows[i], Bytes.toString(cellScanner.current().getRowArray(), cellScanner
            .current().getRowOffset(), cellScanner.current().getRowLength()));
      }
      cells.clear();
    }
    scanner.close();

    scan = new Scan();
    scan.addColumn(fam, qual2);
    scan.setStartRow(Bytes.toBytes("a-b-A-1-140239"));
    scan.setStopRow(Bytes.toBytes("a-b-A-1:"));
    scanner = region.getScanner(scan);
    assertFalse(NextState.hasMoreValues(scanner.next(cells)));
    assertFalse(cells.isEmpty());
    scanner.close();
  }

  @Test
  public void testHBASE12817() throws IOException {
    for (int i = 0; i < 100; i++) {
      region
          .put(new Put(Bytes.toBytes("obj" + (2900 + i))).addColumn(fam, qual1, Bytes.toBytes(i)));
    }
    region.put(new Put(Bytes.toBytes("obj299")).addColumn(fam, qual1, Bytes.toBytes("whatever")));
    region.put(new Put(Bytes.toBytes("obj29")).addColumn(fam, qual1, Bytes.toBytes("whatever")));
    region.put(new Put(Bytes.toBytes("obj2")).addColumn(fam, qual1, Bytes.toBytes("whatever")));
    region.put(new Put(Bytes.toBytes("obj3")).addColumn(fam, qual1, Bytes.toBytes("whatever")));
    region.flush(true);
    Scan scan = new Scan(Bytes.toBytes("obj29995"));
    RegionScanner scanner = region.getScanner(scan);
    List<Cell> cells = new ArrayList<Cell>();
    assertFalse(NextState.hasMoreValues(scanner.next(cells)));
    assertArrayEquals(Bytes.toBytes("obj3"), Result.create(cells).getRow());
  }
}
