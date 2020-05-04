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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

/**
 * Tests user specifiable time stamps putting, getting and scanning.  Also
 * tests same in presence of deletes.  Test cores are written so can be
 * run against an HRegion and against an HTable: i.e. both local and remote.
 */
public class TimestampTestBase {
  private static final long T0 = 10L;
  private static final long T1 = 100L;
  private static final long T2 = 200L;

  public static final byte [] FAMILY_NAME = Bytes.toBytes("colfamily11");
  private static final byte [] QUALIFIER_NAME = Bytes.toBytes("contents");

  private static final byte [] ROW = Bytes.toBytes("row");

  interface FlushCache {
    void flushcache() throws IOException;
  }

  /*
   * Run test that delete works according to description in <a
   * href="https://issues.apache.org/jira/browse/HADOOP-1784">hadoop-1784</a>.
   * @param incommon
   * @param flusher
   * @throws IOException
   */
  public static void doTestDelete(final Table table, FlushCache flusher)
  throws IOException {
    // Add values at various timestamps (Values are timestampes as bytes).
    put(table, T0);
    put(table, T1);
    put(table, T2);
    put(table);
    // Verify that returned versions match passed timestamps.
    assertVersions(table, new long [] {HConstants.LATEST_TIMESTAMP, T2, T1});

    // If I delete w/o specifying a timestamp, this means I'm deleting the latest.
    delete(table);
    // Verify that I get back T2 through T1 -- that the latest version has been deleted.
    assertVersions(table, new long [] {T2, T1, T0});

    // Flush everything out to disk and then retry
    flusher.flushcache();
    assertVersions(table, new long [] {T2, T1, T0});

    // Now add, back a latest so I can test remove other than the latest.
    put(table);
    assertVersions(table, new long [] {HConstants.LATEST_TIMESTAMP, T2, T1});
    delete(table, T2);
    assertVersions(table, new long [] {HConstants.LATEST_TIMESTAMP, T1, T0});
    // Flush everything out to disk and then retry
    flusher.flushcache();
    assertVersions(table, new long [] {HConstants.LATEST_TIMESTAMP, T1, T0});

    // Now try deleting all from T2 back inclusive (We first need to add T2
    // back into the mix and to make things a little interesting, delete and then readd T1.
    put(table, T2);
    delete(table, T1);
    put(table, T1);

    Delete delete = new Delete(ROW);
    delete.addColumns(FAMILY_NAME, QUALIFIER_NAME, T2);
    table.delete(delete);

    // Should only be current value in set.  Assert this is so
    assertOnlyLatest(table, HConstants.LATEST_TIMESTAMP);

    // Flush everything out to disk and then redo above tests
    flusher.flushcache();
    assertOnlyLatest(table, HConstants.LATEST_TIMESTAMP);
  }

  private static void assertOnlyLatest(final Table incommon, final long currentTime)
  throws IOException {
    Get get = null;
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    get.readVersions(3);
    Result result = incommon.get(get);
    Assert.assertEquals(1, result.size());
    long time = Bytes.toLong(CellUtil.cloneValue(result.rawCells()[0]));
    Assert.assertEquals(time, currentTime);
  }

  /*
   * Assert that returned versions match passed in timestamps and that results
   * are returned in the right order.  Assert that values when converted to
   * longs match the corresponding passed timestamp.
   * @param r
   * @param tss
   * @throws IOException
   */
  public static void assertVersions(final Table incommon, final long [] tss)
  throws IOException {
    // Assert that 'latest' is what we expect.
    Get get = null;
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    Result r = incommon.get(get);
    byte [] bytes = r.getValue(FAMILY_NAME, QUALIFIER_NAME);
    long t = Bytes.toLong(bytes);
    Assert.assertEquals(tss[0], t);

    // Now assert that if we ask for multiple versions, that they come out in
    // order.
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    get.readVersions(tss.length);
    Result result = incommon.get(get);
    Cell [] kvs = result.rawCells();
    Assert.assertEquals(kvs.length, tss.length);
    for(int i=0;i<kvs.length;i++) {
      t = Bytes.toLong(CellUtil.cloneValue(kvs[i]));
      Assert.assertEquals(tss[i], t);
    }

    // Determine highest stamp to set as next max stamp
    long maxStamp = kvs[0].getTimestamp();

    // Specify a timestamp get multiple versions.
    get = new Get(ROW);
    get.addColumn(FAMILY_NAME, QUALIFIER_NAME);
    get.setTimeRange(0, maxStamp);
    get.readVersions(kvs.length - 1);
    result = incommon.get(get);
    kvs = result.rawCells();
    Assert.assertEquals(kvs.length, tss.length - 1);
    for(int i=1;i<kvs.length;i++) {
      t = Bytes.toLong(CellUtil.cloneValue(kvs[i-1]));
      Assert.assertEquals(tss[i], t);
    }

    // Test scanner returns expected version
    assertScanContentTimestamp(incommon, tss[0]);
  }

  /*
   * Run test scanning different timestamps.
   * @param incommon
   * @param flusher
   * @throws IOException
   */
  public static void doTestTimestampScanning(final Table incommon,
    final FlushCache flusher)
  throws IOException {
    // Add a couple of values for three different timestamps.
    put(incommon, T0);
    put(incommon, T1);
    put(incommon, HConstants.LATEST_TIMESTAMP);
    // Get count of latest items.
    int count = assertScanContentTimestamp(incommon,
      HConstants.LATEST_TIMESTAMP);
    // Assert I get same count when I scan at each timestamp.
    Assert.assertEquals(count, assertScanContentTimestamp(incommon, T0));
    Assert.assertEquals(count, assertScanContentTimestamp(incommon, T1));
    // Flush everything out to disk and then retry
    flusher.flushcache();
    Assert.assertEquals(count, assertScanContentTimestamp(incommon, T0));
    Assert.assertEquals(count, assertScanContentTimestamp(incommon, T1));
  }

  /*
   * Assert that the scan returns only values < timestamp.
   * @param r
   * @param ts
   * @return Count of items scanned.
   * @throws IOException
   */
  public static int assertScanContentTimestamp(final Table in, final long ts)
  throws IOException {
    Scan scan = new Scan().withStartRow(HConstants.EMPTY_START_ROW);
    scan.addFamily(FAMILY_NAME);
    scan.setTimeRange(0, ts);
    ResultScanner scanner = in.getScanner(scan);
    int count = 0;
    try {
      // TODO FIX
//      HStoreKey key = new HStoreKey();
//      TreeMap<byte [], Cell>value =
//        new TreeMap<byte [], Cell>(Bytes.BYTES_COMPARATOR);
//      while (scanner.next(key, value)) {
//        assertTrue(key.getTimestamp() <= ts);
//        // Content matches the key or HConstants.LATEST_TIMESTAMP.
//        // (Key does not match content if we 'put' with LATEST_TIMESTAMP).
//        long l = Bytes.toLong(value.get(COLUMN).getValue());
//        assertTrue(key.getTimestamp() == l ||
//          HConstants.LATEST_TIMESTAMP == l);
//        count++;
//        value.clear();
//      }
    } finally {
      scanner.close();
    }
    return count;
  }

  public static void put(final Table loader, final long ts)
  throws IOException {
    put(loader, Bytes.toBytes(ts), ts);
  }

  public static void put(final Table loader)
  throws IOException {
    long ts = HConstants.LATEST_TIMESTAMP;
    put(loader, Bytes.toBytes(ts), ts);
  }

  /*
   * Put values.
   * @param loader
   * @param bytes
   * @param ts
   * @throws IOException
   */
  public static void put(final Table loader, final byte [] bytes,
    final long ts)
  throws IOException {
    Put put = new Put(ROW, ts);
    put.setDurability(Durability.SKIP_WAL);
    put.addColumn(FAMILY_NAME, QUALIFIER_NAME, bytes);
    loader.put(put);
  }

  public static void delete(final Table loader) throws IOException {
    delete(loader, null);
  }

  public static void delete(final Table loader, final byte [] column)
  throws IOException {
    delete(loader, column, HConstants.LATEST_TIMESTAMP);
  }

  public static void delete(final Table loader, final long ts)
  throws IOException {
    delete(loader, null, ts);
  }

  public static void delete(final Table loader, final byte [] column,
      final long ts)
  throws IOException {
    Delete delete = ts == HConstants.LATEST_TIMESTAMP?
      new Delete(ROW): new Delete(ROW, ts);
    delete.addColumn(FAMILY_NAME, QUALIFIER_NAME, ts);
    loader.delete(delete);
  }

  public static Result get(final Table loader) throws IOException {
    return loader.get(new Get(ROW));
  }
}
