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

import static org.apache.hadoop.hbase.HBaseTestingUtility.COLUMNS;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner.NextState;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(SmallTests.class)
public class TestKeepDeletes {
  HBaseTestingUtility hbu = HBaseTestingUtility.createLocalHTU();
  private final byte[] T0 = Bytes.toBytes("0");
  private final byte[] T1 = Bytes.toBytes("1");
  private final byte[] T2 = Bytes.toBytes("2");
  private final byte[] T3 = Bytes.toBytes("3");
  private final byte[] T4 = Bytes.toBytes("4");
  private final byte[] T5 = Bytes.toBytes("5");
  private final byte[] T6 = Bytes.toBytes("6");

  private final byte[] c0 = COLUMNS[0];
  private final byte[] c1 = COLUMNS[1];

  @Rule public TestName name = new TestName();
  
  @Before
  public void setUp() throws Exception {
    /* HBASE-6832: [WINDOWS] Tests should use explicit timestamp for Puts, and not rely on
     * implicit RS timing.
     * Use an explicit timer (IncrementingEnvironmentEdge) so that the put, delete
     * compact timestamps are tracked. Otherwise, forced major compaction will not purge
     * Delete's having the same timestamp. see ScanQueryMatcher.match():
     * if (retainDeletesInOutput
     *     || (!isUserScan && (EnvironmentEdgeManager.currentTime() - timestamp)
     *     <= timeToPurgeDeletes) ... )
     *
     */
    EnvironmentEdgeManagerTestHelper.injectEdge(new IncrementingEnvironmentEdge());
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentEdgeManager.reset();
  }

  /**
   * Make sure that deleted rows are retained.
   * Family delete markers are deleted.
   * Column Delete markers are versioned
   * Time range scan of deleted rows are possible
   */
  @Test
  public void testBasicScenario() throws Exception {
    // keep 3 versions, rows do not expire
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 3,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();
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
    Delete d = new Delete(T1, ts+2);
    region.delete(d);

    // a raw scan can see the delete markers
    // (one for each column family)
    assertEquals(3, countDeleteMarkers(region));

    // get something *before* the delete marker
    Get g = new Get(T1);
    g.setMaxVersions();
    g.setTimeRange(0L, ts+2);
    Result r = region.get(g);
    checkResult(r, c0, c0, T2,T1);

    // flush
    region.flush(true);

    // yep, T2 still there, T1 gone
    r = region.get(g);
    checkResult(r, c0, c0, T2);

    // major compact
    region.compact(true);
    region.compact(true);

    // one delete marker left (the others did not
    // have older puts)
    assertEquals(1, countDeleteMarkers(region));

    // still there (even after multiple compactions)
    r = region.get(g);
    checkResult(r, c0, c0, T2);

    // a timerange that includes the delete marker won't see past rows
    g.setTimeRange(0L, ts+4);
    r = region.get(g);
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
    r = region.get(g);
    assertTrue(r.isEmpty());

    region.flush(true);
    region.compact(true);
    region.compact(true);

    // verify that the delete marker itself was collected
    region.put(p);
    r = region.get(g);
    checkResult(r, c0, c0, T1);
    assertEquals(0, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  /**
   * Even when the store does not keep deletes a "raw" scan will
   * return everything it can find (unless discarding cells is guaranteed
   * to have no effect).
   * Assuming this the desired behavior. Could also disallow "raw" scanning
   * if the store does not have KEEP_DELETED_CELLS enabled.
   * (can be changed easily)
   */
  @Test
  public void testRawScanWithoutKeepingDeletes() throws Exception {
    // KEEP_DELETED_CELLS is NOT enabled
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 3,
        HConstants.FOREVER, KeepDeletedCells.FALSE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    Delete d = new Delete(T1, ts);
    d.deleteColumn(c0, c0, ts);
    region.delete(d);

    // scan still returns delete markers and deletes rows
    Scan s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    InternalScanner scan = region.getScanner(s);
    List<Cell> kvs = new ArrayList<Cell>();
    scan.next(kvs);
    assertEquals(2, kvs.size());

    region.flush(true);
    region.compact(true);

    // after compaction they are gone
    // (note that this a test with a Store without
    //  KEEP_DELETED_CELLS)
    s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    scan = region.getScanner(s);
    kvs = new ArrayList<Cell>();
    scan.next(kvs);
    assertTrue(kvs.isEmpty());

    HRegion.closeHRegion(region);
  }

  /**
   * basic verification of existing behavior
   */
  @Test
  public void testWithoutKeepingDeletes() throws Exception {
    // KEEP_DELETED_CELLS is NOT enabled
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 3,
        HConstants.FOREVER, KeepDeletedCells.FALSE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);
    Delete d = new Delete(T1, ts+2);
    d.deleteColumn(c0, c0, ts);
    region.delete(d);

    // "past" get does not see rows behind delete marker
    Get g = new Get(T1);
    g.setMaxVersions();
    g.setTimeRange(0L, ts+1);
    Result r = region.get(g);
    assertTrue(r.isEmpty());

    // "past" scan does not see rows behind delete marker
    Scan s = new Scan();
    s.setMaxVersions();
    s.setTimeRange(0L, ts+1);
    InternalScanner scanner = region.getScanner(s);
    List<Cell> kvs = new ArrayList<Cell>();
    while (NextState.hasMoreValues(scanner.next(kvs)));
    assertTrue(kvs.isEmpty());

    // flushing and minor compaction keep delete markers
    region.flush(true);
    region.compact(false);
    assertEquals(1, countDeleteMarkers(region));
    region.compact(true);
    // major compaction deleted it
    assertEquals(0, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  /**
   * The ExplicitColumnTracker does not support "raw" scanning.
   */
  @Test
  public void testRawScanWithColumns() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 3,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    Scan s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    s.addColumn(c0, c0);

    try {
      region.getScanner(s);
      fail("raw scanner with columns should have failed");
    } catch (org.apache.hadoop.hbase.DoNotRetryIOException dnre) {
      // ok!
    }

    HRegion.closeHRegion(region);
  }

  /**
   * Verify that "raw" scanning mode return delete markers and deletes rows.
   */
  @Test
  public void testRawScan() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 3,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);
    p = new Put(T1, ts+2);
    p.add(c0, c0, T2);
    region.put(p);
    p = new Put(T1, ts+4);
    p.add(c0, c0, T3);
    region.put(p);

    Delete d = new Delete(T1, ts+1);
    region.delete(d);

    d = new Delete(T1, ts+2);
    d.deleteColumn(c0, c0, ts+2);
    region.delete(d);

    d = new Delete(T1, ts+3);
    d.deleteColumns(c0, c0, ts+3);
    region.delete(d);

    Scan s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    InternalScanner scan = region.getScanner(s);
    List<Cell> kvs = new ArrayList<Cell>();
    scan.next(kvs);
    assertEquals(8, kvs.size());
    assertTrue(CellUtil.isDeleteFamily(kvs.get(0)));
    assertArrayEquals(CellUtil.cloneValue(kvs.get(1)), T3);
    assertTrue(CellUtil.isDelete(kvs.get(2)));
    assertTrue(CellUtil.isDelete(kvs.get(3))); // .isDeleteType());
    assertArrayEquals(CellUtil.cloneValue(kvs.get(4)), T2);
    assertArrayEquals(CellUtil.cloneValue(kvs.get(5)), T1);
    // we have 3 CFs, so there are two more delete markers
    assertTrue(CellUtil.isDeleteFamily(kvs.get(6)));
    assertTrue(CellUtil.isDeleteFamily(kvs.get(7)));

    // verify that raw scans honor the passed timerange
    s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    s.setTimeRange(0, 1);
    scan = region.getScanner(s);
    kvs = new ArrayList<Cell>();
    scan.next(kvs);
    // nothing in this interval, not even delete markers
    assertTrue(kvs.isEmpty());

    // filter new delete markers
    s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    s.setTimeRange(0, ts+2);
    scan = region.getScanner(s);
    kvs = new ArrayList<Cell>();
    scan.next(kvs);
    assertEquals(4, kvs.size());
    assertTrue(CellUtil.isDeleteFamily(kvs.get(0)));
    assertArrayEquals(CellUtil.cloneValue(kvs.get(1)), T1);
    // we have 3 CFs
    assertTrue(CellUtil.isDeleteFamily(kvs.get(2)));
    assertTrue(CellUtil.isDeleteFamily(kvs.get(3)));

    // filter old delete markers
    s = new Scan();
    s.setRaw(true);
    s.setMaxVersions();
    s.setTimeRange(ts+3, ts+5);
    scan = region.getScanner(s);
    kvs = new ArrayList<Cell>();
    scan.next(kvs);
    assertEquals(2, kvs.size());
    assertArrayEquals(CellUtil.cloneValue(kvs.get(0)), T3);
    assertTrue(CellUtil.isDelete(kvs.get(1)));


    HRegion.closeHRegion(region);
  }

  /**
   * Verify that delete markers are removed from an otherwise empty store.
   */
  @Test
  public void testDeleteMarkerExpirationEmptyStore() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 1,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();

    Delete d = new Delete(T1, ts);
    d.deleteColumns(c0, c0, ts);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteFamily(c0);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteColumn(c0, c0, ts+1);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteColumn(c0, c0, ts+2);
    region.delete(d);

    // 1 family marker, 1 column marker, 2 version markers
    assertEquals(4, countDeleteMarkers(region));

    // neither flush nor minor compaction removes any marker
    region.flush(true);
    assertEquals(4, countDeleteMarkers(region));
    region.compact(false);
    assertEquals(4, countDeleteMarkers(region));

    // major compaction removes all, since there are no puts they affect
    region.compact(true);
    assertEquals(0, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  /**
   * Test delete marker removal from store files.
   */
  @Test
  public void testDeleteMarkerExpiration() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 1,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();

    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    // a put into another store (CF) should have no effect
    p = new Put(T1, ts-10);
    p.add(c1, c0, T1);
    region.put(p);

    // all the following deletes affect the put
    Delete d = new Delete(T1, ts);
    d.deleteColumns(c0, c0, ts);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteFamily(c0, ts);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteColumn(c0, c0, ts+1);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteColumn(c0, c0, ts+2);
    region.delete(d);

    // 1 family marker, 1 column marker, 2 version markers
    assertEquals(4, countDeleteMarkers(region));

    region.flush(true);
    assertEquals(4, countDeleteMarkers(region));
    region.compact(false);
    assertEquals(4, countDeleteMarkers(region));

    // another put will push out the earlier put...
    p = new Put(T1, ts+3);
    p.add(c0, c0, T1);
    region.put(p);

    region.flush(true);
    // no markers are collected, since there is an affected put
    region.compact(true);
    assertEquals(4, countDeleteMarkers(region));

    // the last collections collected the earlier put
    // so after this collection all markers
    region.compact(true);
    assertEquals(0, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  /**
   * Test delete marker removal from store files.
   */
  @Test
  public void testWithOldRow() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 1,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();

    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    // a put another (older) row in the same store
    p = new Put(T2, ts-10);
    p.add(c0, c0, T1);
    region.put(p);

    // all the following deletes affect the put
    Delete d = new Delete(T1, ts);
    d.deleteColumns(c0, c0, ts);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteFamily(c0, ts);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteColumn(c0, c0, ts+1);
    region.delete(d);

    d = new Delete(T1, ts);
    d.deleteColumn(c0, c0, ts+2);
    region.delete(d);

    // 1 family marker, 1 column marker, 2 version markers
    assertEquals(4, countDeleteMarkers(region));

    region.flush(true);
    assertEquals(4, countDeleteMarkers(region));
    region.compact(false);
    assertEquals(4, countDeleteMarkers(region));

    // another put will push out the earlier put...
    p = new Put(T1, ts+3);
    p.add(c0, c0, T1);
    region.put(p);

    region.flush(true);
    // no markers are collected, since there is an affected put
    region.compact(true);
    assertEquals(4, countDeleteMarkers(region));

    // all markers remain, since we have the older row
    // and we haven't pushed the inlined markers past MAX_VERSIONS
    region.compact(true);
    assertEquals(4, countDeleteMarkers(region));

    // another put will push out the earlier put...
    p = new Put(T1, ts+4);
    p.add(c0, c0, T1);
    region.put(p);

    // this pushed out the column and version marker
    // but the family markers remains. THIS IS A PROBLEM!
    region.compact(true);
    assertEquals(1, countDeleteMarkers(region));

    // no amount of compacting is getting this of this one
    // KEEP_DELETED_CELLS=>TTL is an option to avoid this.
    region.compact(true);
    assertEquals(1, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  /**
   * Verify correct range demarcation
   */
  @Test
  public void testRanges() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 3,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();
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

    Delete d = new Delete(T1, ts+2);
    d.deleteColumns(c0, c0, ts+2);
    region.delete(d);

    d = new Delete(T1, ts+2);
    d.deleteFamily(c1, ts+2);
    region.delete(d);

    d = new Delete(T2, ts+2);
    d.deleteFamily(c0, ts+2);
    region.delete(d);

    // add an older delete, to make sure it is filtered
    d = new Delete(T1, ts-10);
    d.deleteFamily(c1, ts-10);
    region.delete(d);

    // ts + 2 does NOT include the delete at ts+2
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

    HRegion.closeHRegion(region);
  }

  /**
   * Verify that column/version delete makers are sorted
   * with their respective puts and removed correctly by
   * versioning (i.e. not relying on the store earliestPutTS).
   */
  @Test
  public void testDeleteMarkerVersioning() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 1,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();
    Put p = new Put(T1, ts);
    p.add(c0, c0, T1);
    region.put(p);

    // this prevents marker collection based on earliestPut
    // (cannot keep earliest put per column in the store file)
    p = new Put(T1, ts-10);
    p.add(c0, c1, T1);
    region.put(p);

    Delete d = new Delete(T1, ts);
    // test corner case (Put and Delete have same TS)
    d.deleteColumns(c0, c0, ts);
    region.delete(d);

    d = new Delete(T1, ts+1);
    d.deleteColumn(c0, c0, ts+1);
    region.delete(d);

    d = new Delete(T1, ts+3);
    d.deleteColumn(c0, c0, ts+3);
    region.delete(d);

    region.flush(true);
    region.compact(true);
    region.compact(true);
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
    region.flush(true);

    // Here we have the three markers again, because the flush above
    // removed the 2nd put before the file is written.
    // So there's only one put, and hence the deletes already in the store
    // files cannot be removed safely.
    // delete, put, delete, delete
    assertEquals(3, countDeleteMarkers(region));

    region.compact(true);
    assertEquals(3, countDeleteMarkers(region));

    // add one more put
    p = new Put(T1, ts+4);
    p.add(c0, c0, T4);
    region.put(p);

    region.flush(true);
    // one trailing delete marker remains (but only one)
    // because delete markers do not increase the version count
    assertEquals(1, countDeleteMarkers(region));
    region.compact(true);
    region.compact(true);
    assertEquals(1, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  /**
   * Verify scenarios with multiple CFs and columns
   */
  public void testWithMixedCFs() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 0, 1,
        HConstants.FOREVER, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime();

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
    Delete d = new Delete(T1, ts+1);
    region.delete(d);

    d = new Delete(T2, ts+2);
    region.delete(d);

    Scan s = new Scan(T1);
    s.setTimeRange(0, ts+1);
    InternalScanner scanner = region.getScanner(s);
    List<Cell> kvs = new ArrayList<Cell>();
    scanner.next(kvs);
    assertEquals(4, kvs.size());
    scanner.close();

    s = new Scan(T2);
    s.setTimeRange(0, ts+2);
    scanner = region.getScanner(s);
    kvs = new ArrayList<Cell>();
    scanner.next(kvs);
    assertEquals(4, kvs.size());
    scanner.close();

    HRegion.closeHRegion(region);
  }

  /**
   * Test keeping deleted rows together with min versions set
   * @throws Exception
   */
  @Test
  public void testWithMinVersions() throws Exception {
    HTableDescriptor htd =
        hbu.createTableDescriptor(name.getMethodName(), 3, 1000, 1, KeepDeletedCells.TRUE);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime() - 2000; // 2s in the past

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
    Delete d = new Delete(T1, ts-1);
    region.delete(d);
    // and a column delete marker
    d = new Delete(T1, ts-2);
    d.deleteColumns(c0, c0, ts-1);
    region.delete(d);

    Get g = new Get(T1);
    g.setMaxVersions();
    g.setTimeRange(0L, ts-2);
    Result r = region.get(g);
    checkResult(r, c0, c0, T1,T0);

    // 3 families, one column delete marker
    assertEquals(4, countDeleteMarkers(region));

    region.flush(true);
    // no delete marker removes by the flush
    assertEquals(4, countDeleteMarkers(region));

    r = region.get(g);
    checkResult(r, c0, c0, T1);
    p = new Put(T1, ts+1);
    p.add(c0, c0, T4);
    region.put(p);
    region.flush(true);

    assertEquals(4, countDeleteMarkers(region));

    r = region.get(g);
    checkResult(r, c0, c0, T1);

    // this will push out the last put before
    // family delete marker
    p = new Put(T1, ts+2);
    p.add(c0, c0, T5);
    region.put(p);

    region.flush(true);
    region.compact(true);
    // the two family markers without puts are gone
    assertEquals(2, countDeleteMarkers(region));

    // the last compactStores updated the earliestPutTs,
    // so after the next compaction the last family delete marker is also gone
    region.compact(true);
    assertEquals(0, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  /**
   * Test keeping deleted rows together with min versions set
   * @throws Exception
   */
  @Test
  public void testWithTTL() throws Exception {
    HTableDescriptor htd =
        hbu.createTableDescriptor(name.getMethodName(), 1, 1000, 1, KeepDeletedCells.TTL);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    long ts = EnvironmentEdgeManager.currentTime() - 2000; // 2s in the past

    Put p = new Put(T1, ts);
    p.add(c0, c0, T3);
    region.put(p);

    // place an old row, to make the family marker expires anyway
    p = new Put(T2, ts-10);
    p.add(c0, c0, T1);
    region.put(p);

    checkGet(region, T1, c0, c0, ts+1, T3);
    // place a family delete marker
    Delete d = new Delete(T1, ts+2);
    region.delete(d);

    checkGet(region, T1, c0, c0, ts+1, T3);

    // 3 families, one column delete marker
    assertEquals(3, countDeleteMarkers(region));

    region.flush(true);
    // no delete marker removes by the flush
    assertEquals(3, countDeleteMarkers(region));

    // but the Put is gone
    checkGet(region, T1, c0, c0, ts+1);

    region.compact(true);
    // all delete marker gone
    assertEquals(0, countDeleteMarkers(region));

    HRegion.closeHRegion(region);
  }

  private void checkGet(Region region, byte[] row, byte[] fam, byte[] col,
      long time, byte[]... vals) throws IOException {
    Get g = new Get(row);
    g.addColumn(fam, col);
    g.setMaxVersions();
    g.setTimeRange(0L, time);
    Result r = region.get(g);
    checkResult(r, fam, col, vals);

  }

  private int countDeleteMarkers(Region region) throws IOException {
    Scan s = new Scan();
    s.setRaw(true);
    // use max versions from the store(s)
    s.setMaxVersions(region.getStores().iterator().next().getScanInfo().getMaxVersions());
    InternalScanner scan = region.getScanner(s);
    List<Cell> kvs = new ArrayList<Cell>();
    int res = 0;
    boolean hasMore;
    do {
      hasMore = NextState.hasMoreValues(scan.next(kvs));
      for (Cell kv : kvs) {
        if(CellUtil.isDelete(kv)) res++;
      }
      kvs.clear();
    } while (hasMore);
    scan.close();
    return res;
  }

  private void checkResult(Result r, byte[] fam, byte[] col, byte[] ... vals) {
    assertEquals(r.size(), vals.length);
    List<Cell> kvs = r.getColumnCells(fam, col);
    assertEquals(kvs.size(), vals.length);
    for (int i=0;i<vals.length;i++) {
      assertArrayEquals(CellUtil.cloneValue(kvs.get(i)), vals[i]);
    }
  }


}

