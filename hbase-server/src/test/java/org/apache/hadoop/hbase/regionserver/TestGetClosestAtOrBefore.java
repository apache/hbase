/**
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner.NextState;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * TestGet is a medley of tests of get all done up as a single test.
 * This class
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestGetClosestAtOrBefore extends HBaseTestCase {
  private static final Log LOG = LogFactory.getLog(TestGetClosestAtOrBefore.class);

  private static final byte[] T00 = Bytes.toBytes("000");
  private static final byte[] T10 = Bytes.toBytes("010");
  private static final byte[] T11 = Bytes.toBytes("011");
  private static final byte[] T12 = Bytes.toBytes("012");
  private static final byte[] T20 = Bytes.toBytes("020");
  private static final byte[] T30 = Bytes.toBytes("030");
  private static final byte[] T31 = Bytes.toBytes("031");
  private static final byte[] T35 = Bytes.toBytes("035");
  private static final byte[] T40 = Bytes.toBytes("040");



  @Test
  public void testUsingMetaAndBinary() throws IOException {
    FileSystem filesystem = FileSystem.get(conf);
    Path rootdir = testDir;
    // Up flush size else we bind up when we use default catalog flush of 16k.
    fsTableDescriptors.get(TableName.META_TABLE_NAME).setMemStoreFlushSize(64 * 1024 * 1024);

    Region mr = HBaseTestingUtility.createRegionAndWAL(HRegionInfo.FIRST_META_REGIONINFO,
        rootdir, this.conf, fsTableDescriptors.get(TableName.META_TABLE_NAME));
    try {
    // Write rows for three tables 'A', 'B', and 'C'.
    for (char c = 'A'; c < 'D'; c++) {
      HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("" + c));
      final int last = 128;
      final int interval = 2;
      for (int i = 0; i <= last; i += interval) {
        HRegionInfo hri = new HRegionInfo(htd.getTableName(),
          i == 0? HConstants.EMPTY_BYTE_ARRAY: Bytes.toBytes((byte)i),
          i == last? HConstants.EMPTY_BYTE_ARRAY: Bytes.toBytes((byte)i + interval));

        Put put = MetaTableAccessor.makePutFromRegionInfo(hri);
        put.setDurability(Durability.SKIP_WAL);
        mr.put(put);
      }
    }
    InternalScanner s = mr.getScanner(new Scan());
    try {
      List<Cell> keys = new ArrayList<Cell>();
        while (NextState.hasMoreValues(s.next(keys))) {
        LOG.info(keys);
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
    byte [] firstRowInC = HRegionInfo.createRegionName(
        TableName.valueOf("" + 'C'),
        HConstants.EMPTY_BYTE_ARRAY, HConstants.ZEROES, false);
    Scan scan = new Scan(firstRowInC);
    s = mr.getScanner(scan);
    try {
      List<Cell> keys = new ArrayList<Cell>();
        while (NextState.hasMoreValues(s.next(keys))) {
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
    LOG.info("find=" + new String(metaKey));
    Result r = mr.getClosestRowBefore(metaKey, HConstants.CATALOG_FAMILY);
    if (answer == -1) {
      assertNull(r);
      return null;
    }
    assertTrue(Bytes.compareTo(Bytes.toBytes((short)answer),
      extractRowFromMetaRow(r.getRow())) == 0);
    return r.getRow();
  }

  private byte [] extractRowFromMetaRow(final byte [] b) {
    int firstDelimiter = KeyValue.getDelimiter(b, 0, b.length,
      HConstants.DELIMITER);
    int lastDelimiter = KeyValue.getDelimiterInReverse(b, 0, b.length,
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
    Region region = null;
    byte [] c0 = COLUMNS[0];
    byte [] c1 = COLUMNS[1];
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);

      Put p = new Put(T00);
      p.add(c0, c0, T00);
      region.put(p);

      p = new Put(T10);
      p.add(c0, c0, T10);
      region.put(p);

      p = new Put(T20);
      p.add(c0, c0, T20);
      region.put(p);

      Result r = region.getClosestRowBefore(T20, c0);
      assertTrue(Bytes.equals(T20, r.getRow()));

      Delete d = new Delete(T20);
      d.deleteColumn(c0, c0);
      region.delete(d);

      r = region.getClosestRowBefore(T20, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      p = new Put(T30);
      p.add(c0, c0, T30);
      region.put(p);

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      d = new Delete(T30);
      d.deleteColumn(c0, c0);
      region.delete(d);

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      region.flush(true);

      // try finding "010" after flush
      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Put into a different column family.  Should make it so I still get t10
      p = new Put(T20);
      p.add(c1, c1, T20);
      region.put(p);

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      region.flush(true);

      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Now try combo of memcache and mapfiles.  Delete the t20 COLUMS[1]
      // in memory; make sure we get back t10 again.
      d = new Delete(T20);
      d.deleteColumn(c1, c1);
      region.delete(d);
      r = region.getClosestRowBefore(T30, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Ask for a value off the end of the file.  Should return t10.
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));
      region.flush(true);
      r = region.getClosestRowBefore(T31, c0);
      assertTrue(Bytes.equals(T10, r.getRow()));

      // Ok.  Let the candidate come out of hfile but have delete of
      // the candidate be in memory.
      p = new Put(T11);
      p.add(c0, c0, T11);
      region.put(p);
      d = new Delete(T10);
      d.deleteColumn(c1, c1);
      r = region.getClosestRowBefore(T12, c0);
      assertTrue(Bytes.equals(T11, r.getRow()));
    } finally {
      if (region != null) {
        try {
          WAL wal = ((HRegion)region).getWAL();
          ((HRegion)region).close();
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
    Region region = null;
    byte [] c0 = COLUMNS[0];
    try {
      HTableDescriptor htd = createTableDescriptor(getName());
      region = createNewHRegion(htd, null, null);

      Put p = new Put(T10);
      p.add(c0, c0, T10);
      region.put(p);

      p = new Put(T30);
      p.add(c0, c0, T30);
      region.put(p);

      p = new Put(T40);
      p.add(c0, c0, T40);
      region.put(p);

      // try finding "035"
      Result r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      region.flush(true);

      // try finding "035"
      r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      p = new Put(T20);
      p.add(c0, c0, T20);
      region.put(p);

      // try finding "035"
      r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));

      region.flush(true);

      // try finding "035"
      r = region.getClosestRowBefore(T35, c0);
      assertTrue(Bytes.equals(T30, r.getRow()));
    } finally {
      if (region != null) {
        try {
          WAL wal = ((HRegion)region).getWAL();
          ((HRegion)region).close();
          wal.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }

}

