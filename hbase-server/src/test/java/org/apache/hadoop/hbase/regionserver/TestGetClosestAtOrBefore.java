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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TestGet is a medley of tests of get all done up as a single test.
 * It was originally written to test a method since removed, getClosestAtOrBefore
 * but the test is retained because it runs some interesting exercises.
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestGetClosestAtOrBefore  {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGetClosestAtOrBefore.class);

  @Rule public TestName testName = new TestName();
  private static final Logger LOG = LoggerFactory.getLogger(TestGetClosestAtOrBefore.class);

  private static final byte[] T00 = Bytes.toBytes("000");
  private static final byte[] T10 = Bytes.toBytes("010");
  private static final byte[] T11 = Bytes.toBytes("011");
  private static final byte[] T12 = Bytes.toBytes("012");
  private static final byte[] T20 = Bytes.toBytes("020");
  private static final byte[] T30 = Bytes.toBytes("030");
  private static final byte[] T31 = Bytes.toBytes("031");
  private static final byte[] T35 = Bytes.toBytes("035");
  private static final byte[] T40 = Bytes.toBytes("040");

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration conf = UTIL.getConfiguration();

  @Test
  public void testUsingMetaAndBinary() throws IOException {
    Path rootdir = UTIL.getDataTestDirOnTestFS();
    // Up flush size else we bind up when we use default catalog flush of 16k.
    TableDescriptorBuilder metaBuilder = UTIL.getMetaTableDescriptorBuilder()
            .setMemStoreFlushSize(64 * 1024 * 1024);
    HRegion mr = HBaseTestingUtility.createRegionAndWAL(HRegionInfo.FIRST_META_REGIONINFO,
        rootdir, this.conf, metaBuilder.build());
    try {
      // Write rows for three tables 'A', 'B', and 'C'.
      for (char c = 'A'; c < 'D'; c++) {
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("" + c));
        final int last = 128;
        final int interval = 2;
        for (int i = 0; i <= last; i += interval) {
          RegionInfo hri = RegionInfoBuilder.newBuilder(htd.getTableName())
            .setStartKey(i == 0 ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes((byte)i))
            .setEndKey(i == last ? HConstants.EMPTY_BYTE_ARRAY :
              Bytes.toBytes((byte)i + interval)).build();
          Put put =
            MetaTableAccessor.makePutFromRegionInfo(hri, EnvironmentEdgeManager.currentTime());
          put.setDurability(Durability.SKIP_WAL);
          LOG.info("Put {}", put);
          mr.put(put);
        }
      }
      InternalScanner s = mr.getScanner(new Scan());
      try {
        List<Cell> keys = new ArrayList<>();
        while (s.next(keys)) {
          LOG.info("Scan {}", keys);
          keys.clear();
        }
      } finally {
        s.close();
      }
      findRow(mr, 'C', 44, 44);
      findRow(mr, 'C', 45, 44);
      findRow(mr, 'C', 46, 46);
      findRow(mr, 'C', 43, 42);
      mr.flush(true);
      findRow(mr, 'C', 44, 44);
      findRow(mr, 'C', 45, 44);
      findRow(mr, 'C', 46, 46);
      findRow(mr, 'C', 43, 42);
      // Now delete 'C' and make sure I don't get entries from 'B'.
      byte[] firstRowInC = RegionInfo.createRegionName(TableName.valueOf("" + 'C'),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.ZEROES, false);
      Scan scan = new Scan().withStartRow(firstRowInC);
      s = mr.getScanner(scan);
      try {
        List<Cell> keys = new ArrayList<>();
        while (s.next(keys)) {
          LOG.info("Delete {}", keys);
          mr.delete(new Delete(CellUtil.cloneRow(keys.get(0))));
          keys.clear();
        }
      } finally {
        s.close();
      }
      // Assert we get null back (pass -1).
      findRow(mr, 'C', 44, -1);
      findRow(mr, 'C', 45, -1);
      findRow(mr, 'C', 46, -1);
      findRow(mr, 'C', 43, -1);
      mr.flush(true);
      findRow(mr, 'C', 44, -1);
      findRow(mr, 'C', 45, -1);
      findRow(mr, 'C', 46, -1);
      findRow(mr, 'C', 43, -1);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(mr);
    }
  }

  /*
   * @param mr
   * @param table
   * @param rowToFind
   * @param answer Pass -1 if we're not to find anything.
   * @return Row found.
   * @throws IOException
   */
  private byte [] findRow(final Region mr, final char table,
    final int rowToFind, final int answer)
  throws IOException {
    TableName tableb = TableName.valueOf("" + table);
    // Find the row.
    byte [] tofindBytes = Bytes.toBytes((short)rowToFind);
    byte [] metaKey = HRegionInfo.createRegionName(
        tableb, tofindBytes,
      HConstants.NINES, false);
    LOG.info("find=" + new String(metaKey, StandardCharsets.UTF_8));
    Result r = UTIL.getClosestRowBefore(mr, metaKey, HConstants.CATALOG_FAMILY);
    if (answer == -1) {
      assertNull(r);
      return null;
    }
    assertTrue(Bytes.compareTo(Bytes.toBytes((short)answer),
      extractRowFromMetaRow(r.getRow())) == 0);
    return r.getRow();
  }

  private byte [] extractRowFromMetaRow(final byte [] b) {
    int firstDelimiter = Bytes.searchDelimiterIndex(b, 0, b.length,
      HConstants.DELIMITER);
    int lastDelimiter = Bytes.searchDelimiterIndexInReverse(b, 0, b.length,
      HConstants.DELIMITER);
    int length = lastDelimiter - firstDelimiter - 1;
    byte [] row = new byte[length];
    System.arraycopy(b, firstDelimiter + 1, row, 0, length);
    return row;
  }

  /**
   * Test file of multiple deletes and with deletes as final key.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-751">HBASE-751</a>
   */
  @Test
  public void testGetClosestRowBefore3() throws IOException{
    HRegion region = null;
    byte [] c0 = UTIL.COLUMNS[0];
    byte [] c1 = UTIL.COLUMNS[1];
    try {
      TableName tn = TableName.valueOf(testName.getMethodName());
      HTableDescriptor htd = UTIL.createTableDescriptor(tn);
      region = UTIL.createLocalHRegion(htd, null, null);

      Put p = new Put(T00);
      p.addColumn(c0, c0, T00);
      region.put(p);

      p = new Put(T10);
      p.addColumn(c0, c0, T10);
      region.put(p);

      p = new Put(T20);
      p.addColumn(c0, c0, T20);
      region.put(p);

      Result r = UTIL.getClosestRowBefore(region, T20, c0);
      assertTrue(Bytes.equals(T20, r.getRow()));

      Delete d = new Delete(T20);
      d.addColumn(c0, c0);
      region.delete(d);

      r = UTIL.getClosestRowBefore(region, T20, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      p = new Put(T30);
      p.addColumn(c0, c0, T30);
      region.put(p);

      r = UTIL.getClosestRowBefore(region, T30, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      d = new Delete(T30);
      d.addColumn(c0, c0);
      region.delete(d);

      r = UTIL.getClosestRowBefore(region, T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = UTIL.getClosestRowBefore(region, T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      region.flush(true);

      // try finding "010" after flush
      r = UTIL.getClosestRowBefore(region, T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = UTIL.getClosestRowBefore(region, T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Put into a different column family.  Should make it so I still get t10
      p = new Put(T20);
      p.addColumn(c1, c1, T20);
      region.put(p);

      r = UTIL.getClosestRowBefore(region, T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = UTIL.getClosestRowBefore(region, T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      region.flush(true);

      r = UTIL.getClosestRowBefore(region, T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = UTIL.getClosestRowBefore(region, T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Now try combo of memcache and mapfiles.  Delete the t20 COLUMS[1]
      // in memory; make sure we get back t10 again.
      d = new Delete(T20);
      d.addColumn(c1, c1);
      region.delete(d);
      r = UTIL.getClosestRowBefore(region, T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Ask for a value off the end of the file.  Should return t10.
      r = UTIL.getClosestRowBefore(region, T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      region.flush(true);
      r = UTIL.getClosestRowBefore(region, T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Ok.  Let the candidate come out of hfile but have delete of
      // the candidate be in memory.
      p = new Put(T11);
      p.addColumn(c0, c0, T11);
      region.put(p);
      d = new Delete(T10);
      d.addColumn(c1, c1);
      r = UTIL.getClosestRowBefore(region, T12, c0);
      assertTrue(Bytes.equals(T11, r.getRow()));
    } finally {
      if (region != null) {
        try {
          WAL wal = region.getWAL();
          region.close();
          wal.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

  /** For HBASE-694 */
  @Test
  public void testGetClosestRowBefore2() throws IOException{
    HRegion region = null;
    byte [] c0 = UTIL.COLUMNS[0];
    try {
      TableName tn = TableName.valueOf(testName.getMethodName());
      HTableDescriptor htd = UTIL.createTableDescriptor(tn);
      region = UTIL.createLocalHRegion(htd, null, null);

      Put p = new Put(T10);
      p.addColumn(c0, c0, T10);
      region.put(p);

      p = new Put(T30);
      p.addColumn(c0, c0, T30);
      region.put(p);

      p = new Put(T40);
      p.addColumn(c0, c0, T40);
      region.put(p);

      // try finding "035"
      Result r = UTIL.getClosestRowBefore(region, T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      region.flush(true);

      // try finding "035"
      r = UTIL.getClosestRowBefore(region, T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      p = new Put(T20);
      p.addColumn(c0, c0, T20);
      region.put(p);

      // try finding "035"
      r = UTIL.getClosestRowBefore(region, T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      region.flush(true);

      // try finding "035"
      r = UTIL.getClosestRowBefore(region, T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));
    } finally {
      if (region != null) {
        try {
          WAL wal = region.getWAL();
          region.close();
          wal.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

}

