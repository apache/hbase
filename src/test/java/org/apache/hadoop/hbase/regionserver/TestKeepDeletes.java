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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class TestKeepDeletes extends HBaseTestCase {
  private final byte[] T0 = Bytes.toBytes("0");
  private final byte[] T1 = Bytes.toBytes("1");
  private final byte[] T2 = Bytes.toBytes("2");
  private final byte[] T3 = Bytes.toBytes("3");
  private final byte[] T4 = Bytes.toBytes("4");
  private final byte[] T5 = Bytes.toBytes("5");
  private final byte[] T6 = Bytes.toBytes("6");

  private final byte[] c0 = COLUMNS[0];
  private final byte[] c1 = COLUMNS[1];

  /**
   * Make sure that deleted rows are retained.
   * Family delete markers are deleted.
   * Column Delete markers are versioned
   * Time range scan of deleted rows are possible
   */
  public void testBasicScenario() throws Exception {
    // keep 3 versions, rows do not expire
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 3,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);
    p = new Put(T1, ts+1);
    p.add(c0, c0, T2);
    region.put(p);
    p = new Put(T1, ts+2);
    p.add(c0, c0, T3);
    region.put(p);
    p = new Put(T1, ts+4);
    p.add(c0, c0, T4);
    region.put(p);

    // now place a delete marker at ts+2
    Delete d = new Delete(T1, ts+2, null);
    region.delete(d, null, true);

    // a raw scan can see the delete markers
    // (one for each column family)
    assertEquals(3, countDeleteMarkers(region));

    // get something *before* the delete marker
    Get g = new Get(T1);
    g.setMaxVersions();
    g.setTimeRange(0L, ts+2);
    Result r = region.get(g, null);
    checkResult(r, c0, c0, T2,T1);

    // flush
    region.flushcache();

    // yep, T2 still there, T1 gone
    r = region.get(g, null);
    checkResult(r, c0, c0, T2);

    // major compact
    region.compactStores(true);
    region.compactStores(true);

    // one delete marker left (the others did not
    // have older puts)
    assertEquals(1, countDeleteMarkers(region));

    // still there (even after multiple compactions)
    r = region.get(g, null);
    checkResult(r, c0, c0, T2);

    // a timerange that includes the delete marker won't see past rows
    g.setTimeRange(0L, ts+4);
    r = region.get(g, null);
    assertTrue(r.isEmpty());

    // two more puts, this will expire the older puts.
    p = new Put(T1, ts+5);
    p.add(c0, c0, T5);
    region.put(p);
    p = new Put(T1, ts+6);
    p.add(c0, c0, T6);
    region.put(p);

    // also add an old put again
    // (which is past the max versions)
    p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);
    r = region.get(g, null);
    assertTrue(r.isEmpty());

    region.flushcache();
    region.compactStores(true);
    region.compactStores(true);

    // verify that the delete marker itself was collected
    region.put(p);
    r = region.get(g, null);
    checkResult(r, c0, c0, T1);
    assertEquals(0, countDeleteMarkers(region));
  }

  /**
   * Even when the store does not keep deletes a "raw" scan will 
   * return everything it can find (unless discarding cells is guaranteed
   * to have no effect).
   * Assuming this the desired behavior. Could also disallow "raw" scanning
   * if the store does not have KEEP_DELETED_CELLS enabled.
   * (can be changed easily)
   */
  public void testRawScanWithoutKeepingDeletes() throws Exception {
    // KEEP_DELETED_CELLS is NOT enabled
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 3,
        HConstants.FOREVER, false);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    Delete d = new Delete(T1, ts, null);
    d.deleteColumn(c0, c0, ts);
    region.delete(d, null, true);

    // scan still returns delete markers and deletes rows
    Scan s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    InternalScanner scan = region.getScanner(s);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    scan.next(kvs);
    assertEquals(2, kvs.size());

    region.flushcache();
    region.compactStores(true);

    // after compaction they are gone
    // (note that this a test with a Store without
    //  KEEP_DELETED_CELLS)
    s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    scan = region.getScanner(s);
    kvs = new ArrayList<KeyValue>();
    scan.next(kvs);
    assertTrue(kvs.isEmpty());
  }

  /**
   * basic verification of existing behavior
   */
  public void testWithoutKeepingDeletes() throws Exception {
    // KEEP_DELETED_CELLS is NOT enabled
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 3,
        HConstants.FOREVER, false);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();  
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);
    Delete d = new Delete(T1, ts+2, null);
    d.deleteColumn(c0, c0, ts);
    region.delete(d, null, true);

    // "past" get does not see rows behind delete marker
    Get g = new Get(T1);
    g.setMaxVersions();
    g.setTimeRange(0L, ts+1);
    Result r = region.get(g, null);
    assertTrue(r.isEmpty());

    // "past" scan does not see rows behind delete marker
    Scan s = new Scan();
    s.setMaxVersions();
    s.setTimeRange(0L, ts+1);
    InternalScanner scanner = region.getScanner(s);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    while(scanner.next(kvs));
    assertTrue(kvs.isEmpty());

    // flushing and minor compaction keep delete markers
    region.flushcache();
    region.compactStores();
    assertEquals(1, countDeleteMarkers(region));
    region.compactStores(true);
    // major compaction deleted it
    assertEquals(0, countDeleteMarkers(region));
  }

  /**
   * The ExplicitColumnTracker does not support "raw" scanning.
   */
  public void testRawScanWithColumns() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 3,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    Scan s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    s.addColumn(c0, c0);
    
    try {
      InternalScanner scan = region.getScanner(s);
      fail("raw scanner with columns should have failed");
    } catch (DoNotRetryIOException dnre) {
      // ok!
    }
  }

  /**
   * Verify that "raw" scanning mode return delete markers and deletes rows.
   */
  public void testRawScan() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 3,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);
    p = new Put(T1, ts+2);
    p.add(c0, c0, T2);
    region.put(p);
    p = new Put(T1, ts+4);
    p.add(c0, c0, T3);
    region.put(p);

    Delete d = new Delete(T1, ts+1, null);
    region.delete(d, null, true);

    d = new Delete(T1, ts+2, null);
    d.deleteColumn(c0, c0, ts+2);
    region.delete(d, null, true);

    d = new Delete(T1, ts+3, null);
    d.deleteColumns(c0, c0, ts+3);
    region.delete(d, null, true);

    Scan s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    InternalScanner scan = region.getScanner(s);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    scan.next(kvs);
    assertTrue(kvs.get(0).isDeleteFamily());
    assertEquals(kvs.get(1).getValue(), T3);
    assertTrue(kvs.get(2).isDelete());
    assertTrue(kvs.get(3).isDeleteType());
    assertEquals(kvs.get(4).getValue(), T2);
    assertEquals(kvs.get(5).getValue(), T1);
  }

  /**
   * Verify that delete markers are removed from an otherwise empty store.
   */
  public void testDeleteMarkerExpirationEmptyStore() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 1,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();

    Delete d = new Delete(T1, ts, null);
    d.deleteColumns(c0, c0, ts);
    region.delete(d, null, true);

    d = new Delete(T1, ts, null);
    d.deleteFamily(c0);
    region.delete(d, null, true);

    d = new Delete(T1, ts, null);
    d.deleteColumn(c0, c0, ts+1);
    region.delete(d, null, true);
    
    d = new Delete(T1, ts, null);
    d.deleteColumn(c0, c0, ts+2);
    region.delete(d, null, true);

    // 1 family marker, 1 column marker, 2 version markers
    assertEquals(4, countDeleteMarkers(region));

    // neither flush nor minor compaction removes any marker
    region.flushcache();
    assertEquals(4, countDeleteMarkers(region));
    region.compactStores(false);
    assertEquals(4, countDeleteMarkers(region));

    // major compaction removes all, since there are no puts they affect
    region.compactStores(true);
    assertEquals(0, countDeleteMarkers(region));
  }

  /**
   * Test delete marker removal from store files.
   */
  public void testDeleteMarkerExpiration() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 1,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();

    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    // a put into another store (CF) should have no effect
    p = new Put(T1, ts-10);
    p.add(c1, c0, T1);
    region.put(p);

    // all the following deletes affect the put
    Delete d = new Delete(T1, ts, null);
    d.deleteColumns(c0, c0, ts);
    region.delete(d, null, true);

    d = new Delete(T1, ts, null);
    d.deleteFamily(c0, ts);
    region.delete(d, null, true);

    d = new Delete(T1, ts, null);
    d.deleteColumn(c0, c0, ts+1);
    region.delete(d, null, true);
    
    d = new Delete(T1, ts, null);
    d.deleteColumn(c0, c0, ts+2);
    region.delete(d, null, true);

    // 1 family marker, 1 column marker, 2 version markers
    assertEquals(4, countDeleteMarkers(region));

    region.flushcache();
    assertEquals(4, countDeleteMarkers(region));
    region.compactStores(false);
    assertEquals(4, countDeleteMarkers(region));

    // another put will push out the earlier put...
    p = new Put(T1, ts+3);
    p.add(c0, c0, T1);
    region.put(p);

    region.flushcache();
    // no markers are collected, since there is an affected put
    region.compactStores(true);
    assertEquals(4, countDeleteMarkers(region));

    // the last collections collected the earlier put
    // so after this collection all markers
    region.compactStores(true);
    assertEquals(0, countDeleteMarkers(region));
  }

  /**
   * Verify correct range demarcation
   */
  public void testRanges() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 3,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    p.add(c0, c1, T1);
    p.add(c1, c0, T1);
    p.add(c1, c1, T1);
    region.put(p);

    p = new Put(T2, ts);
    p.add(c0, c0, T1);
    p.add(c0, c1, T1);
    p.add(c1, c0, T1);
    p.add(c1, c1, T1);
    region.put(p);

    p = new Put(T1, ts+1);
    p.add(c0, c0, T2);
    p.add(c0, c1, T2);
    p.add(c1, c0, T2);
    p.add(c1, c1, T2);
    region.put(p);

    p = new Put(T2, ts+1);
    p.add(c0, c0, T2);
    p.add(c0, c1, T2);
    p.add(c1, c0, T2);
    p.add(c1, c1, T2);
    region.put(p);

    Delete d = new Delete(T1, ts+1, null);
    d.deleteColumns(c0, c0, ts+1);
    region.delete(d, null, true);

    d = new Delete(T1, ts+1, null);
    d.deleteFamily(c1, ts+1);
    region.delete(d, null, true);

    d = new Delete(T2, ts+1, null);
    d.deleteFamily(c0, ts+1);
    region.delete(d, null, true);

    // add an older delete, to make sure it is filtered
    d = new Delete(T1, ts-10, null);
    d.deleteFamily(c1, ts-10);
    region.delete(d, null, true);

    // ts + 2 does NOT include the delete at ts+1
    checkGet(region, T1, c0, c0, ts+2, T2, T1);
    checkGet(region, T1, c0, c1, ts+2, T2, T1);
    checkGet(region, T1, c1, c0, ts+2, T2, T1);
    checkGet(region, T1, c1, c1, ts+2, T2, T1);

    checkGet(region, T2, c0, c0, ts+2, T2, T1);
    checkGet(region, T2, c0, c1, ts+2, T2, T1);
    checkGet(region, T2, c1, c0, ts+2, T2, T1);
    checkGet(region, T2, c1, c1, ts+2, T2, T1);

    // ts + 3 does
    checkGet(region, T1, c0, c0, ts+3);
    checkGet(region, T1, c0, c1, ts+3, T2, T1);
    checkGet(region, T1, c1, c0, ts+3);
    checkGet(region, T1, c1, c1, ts+3);

    checkGet(region, T2, c0, c0, ts+3);
    checkGet(region, T2, c0, c1, ts+3);
    checkGet(region, T2, c1, c0, ts+3, T2, T1);
    checkGet(region, T2, c1, c1, ts+3, T2, T1);
  }

  /**
   * Verify that column/version delete makers are sorted
   * with their respective puts and removed correctly by
   * versioning (i.e. not relying on the store earliestPutTS).
   */
  public void testDeleteMarkerVersioning() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 1,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    // this prevents marker collection based on earliestPut
    // (cannot keep earliest put per column in the store file)
    p = new Put(T1, ts-10);
    p.add(c0, c1, T1);
    region.put(p);
    
    Delete d = new Delete(T1, ts, null);
    // test corner case (Put and Delete have same TS)
    d.deleteColumns(c0, c0, ts);
    region.delete(d, null, true);

    d = new Delete(T1, ts+1, null);
    d.deleteColumn(c0, c0, ts+1);
    region.delete(d, null, true);
    
    d = new Delete(T1, ts+3, null);
    d.deleteColumn(c0, c0, ts+3);
    region.delete(d, null, true);

    region.flushcache();
    region.compactStores(true);
    region.compactStores(true);
    assertEquals(3, countDeleteMarkers(region));

    // add two more puts, since max version is 1
    // the 2nd put (and all delete markers following)
    // will be removed.
    p = new Put(T1, ts+2);
    p.add(c0, c0, T2);
    region.put(p);
    
    // delete, put, delete, delete, put
    assertEquals(3, countDeleteMarkers(region));

    p = new Put(T1, ts+3);
    p.add(c0, c0, T3);
    region.put(p);

    // This is potentially questionable behavior.
    // This could be changed by not letting the ScanQueryMatcher
    // return SEEK_NEXT_COL if a put is past VERSIONS, but instead
    // return SKIP if the store has KEEP_DELETED_CELLS set.
    //
    // As it stands, the 1 here is correct here.
    // There are two puts, VERSIONS is one, so after the 1st put the scanner
    // knows that there can be no more KVs (put or delete) that have any effect.
    //
    // delete, put, put | delete, delete
    assertEquals(1, countDeleteMarkers(region));

    // flush cache only sees what is in the memstore
    region.flushcache();

    // Here we have the three markers again, because the flush above
    // removed the 2nd put before the file is written.
    // So there's only one put, and hence the deletes already in the store
    // files cannot be removed safely.
    // delete, put, delete, delete
    assertEquals(3, countDeleteMarkers(region));

    region.compactStores(true);
    assertEquals(3, countDeleteMarkers(region));

    // add one more put
    p = new Put(T1, ts+4);
    p.add(c0, c0, T4);
    region.put(p);

    region.flushcache();
    // one trailing delete marker remains (but only one)
    // because delete markers do not increase the version count
    assertEquals(1, countDeleteMarkers(region));
    region.compactStores(true);
    region.compactStores(true);
    assertEquals(1, countDeleteMarkers(region));
  }

  /**
   * Verify scenarios with multiple CFs and columns
   */
  public void testWithMixedCFs() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 0, 1,
        HConstants.FOREVER, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis();

    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    p.add(c0, c1, T1);
    p.add(c1, c0, T1);
    p.add(c1, c1, T1);
    region.put(p);

    p = new Put(T2, ts+1);
    p.add(c0, c0, T2);
    p.add(c0, c1, T2);
    p.add(c1, c0, T2);
    p.add(c1, c1, T2);
    region.put(p);

    // family markers are each family
    Delete d = new Delete(T1, ts, null);
    region.delete(d, null, true);

    d = new Delete(T2, ts+1, null);
    region.delete(d, null, true);

    Scan s = new Scan(T1);
    s.setTimeRange(0, ts+1);
    InternalScanner scanner = region.getScanner(s);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    scanner.next(kvs);
    assertEquals(4, kvs.size());
    scanner.close();

    s = new Scan(T2);
    s.setTimeRange(0, ts+2);
    scanner = region.getScanner(s);
    kvs = new ArrayList<KeyValue>();
    scanner.next(kvs);
    assertEquals(4, kvs.size());
    scanner.close();
  }

  /**
   * Test keeping deleted rows together with min versions set
   * @throws Exception
   */
  public void testWithMinVersions() throws Exception {
    HTableDescriptor htd = createTableDescriptor(getName(), 3, 1000, 1, true);
    HRegion region = createNewHRegion(htd, null, null);

    long ts = System.currentTimeMillis() - 2000; // 2s in the past

    Put p = new Put(T1, ts);
    p.add(c0, c0, T3);
    region.put(p);
    p = new Put(T1, ts-1);
    p.add(c0, c0, T2);
    region.put(p);
    p = new Put(T1, ts-3);
    p.add(c0, c0, T1);
    region.put(p);
    p = new Put(T1, ts-4);
    p.add(c0, c0, T0);
    region.put(p);

    // all puts now are just retained because of min versions = 3

    // place a family delete marker
    Delete d = new Delete(T1, ts-1, null);
    region.delete(d, null, true);
    // and a column delete marker
    d = new Delete(T1, ts-2, null);
    d.deleteColumns(c0, c0, ts-1);
    region.delete(d, null, true);

    Get g = new Get(T1);
    g.setMaxVersions();
    g.setTimeRange(0L, ts-2);
    Result r = region.get(g, null);
    checkResult(r, c0, c0, T1,T0);

    // 3 families, one column delete marker
    assertEquals(4, countDeleteMarkers(region));

    region.flushcache();
    // no delete marker removes by the flush
    assertEquals(4, countDeleteMarkers(region));

    r = region.get(g, null);
    checkResult(r, c0, c0, T1);
    p = new Put(T1, ts+1);
    p.add(c0, c0, T4);
    region.put(p);
    region.flushcache();

    assertEquals(4, countDeleteMarkers(region));

    r = region.get(g, null);
    checkResult(r, c0, c0, T1);

    // this will push out the last put before
    // family delete marker
    p = new Put(T1, ts+2);
    p.add(c0, c0, T5);
    region.put(p);

    region.flushcache();
    region.compactStores(true);
    // the two family markers without puts are gone
    assertEquals(2, countDeleteMarkers(region));

    // the last compactStores updated the earliestPutTs,
    // so after the next compaction the last family delete marker is also gone
    region.compactStores(true);
    assertEquals(0, countDeleteMarkers(region));
  }

  private void checkGet(HRegion region, byte[] row, byte[] fam, byte[] col,
      long time, byte[]... vals) throws IOException {
    Get g = new Get(row);
    g.addColumn(fam, col);
    g.setMaxVersions();
    g.setTimeRange(0L, time);
    Result r = region.get(g, null);
    checkResult(r, fam, col, vals);
    
  }

  private int countDeleteMarkers(HRegion region) throws IOException {
    Scan s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    InternalScanner scan = region.getScanner(s);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    int res = 0;
    boolean hasMore;
    do {
      hasMore = scan.next(kvs);
      for (KeyValue kv : kvs) {
        if(kv.isDelete()) res++;
      }
      kvs.clear();
    } while (hasMore);
    scan.close();    
    return res;
  }

  private void checkResult(Result r, byte[] fam, byte[] col, byte[] ... vals) {
    assertEquals(r.size(), vals.length);
    List<KeyValue> kvs = r.getColumn(fam, col);
    assertEquals(kvs.size(), vals.length);
    for (int i=0;i<vals.length;i++) {
      assertEquals(kvs.get(i).getValue(), vals[i]);
    }
  }

}
