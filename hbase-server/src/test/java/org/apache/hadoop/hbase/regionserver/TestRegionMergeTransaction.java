/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

/**
 * Test the {@link RegionMergeTransaction} class against two HRegions (as
 * opposed to running cluster).
 */
@Category(SmallTests.class)
public class TestRegionMergeTransaction {
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final Path testdir = TEST_UTIL.getDataTestDir(this.getClass()
      .getName());
  private HRegion region_a;
  private HRegion region_b;
  private HRegion region_c;
  private HLog wal;
  private FileSystem fs;
  // Start rows of region_a,region_b,region_c
  private static final byte[] STARTROW_A = new byte[] { 'a', 'a', 'a' };
  private static final byte[] STARTROW_B = new byte[] { 'g', 'g', 'g' };
  private static final byte[] STARTROW_C = new byte[] { 'w', 'w', 'w' };
  private static final byte[] ENDROW = new byte[] { '{', '{', '{' };
  private static final byte[] CF = HConstants.CATALOG_FAMILY;

  @Before
  public void setup() throws IOException {
    this.fs = FileSystem.get(TEST_UTIL.getConfiguration());
    this.fs.delete(this.testdir, true);
    this.wal = HLogFactory.createHLog(fs, this.testdir, "logs",
        TEST_UTIL.getConfiguration());
    this.region_a = createRegion(this.testdir, this.wal, STARTROW_A, STARTROW_B);
    this.region_b = createRegion(this.testdir, this.wal, STARTROW_B, STARTROW_C);
    this.region_c = createRegion(this.testdir, this.wal, STARTROW_C, ENDROW);
    assert region_a != null && region_b != null && region_c != null;
    TEST_UTIL.getConfiguration().setBoolean("hbase.testing.nocluster", true);
  }

  @After
  public void teardown() throws IOException {
    for (HRegion region : new HRegion[] { region_a, region_b, region_c }) {
      if (region != null && !region.isClosed()) region.close();
      if (this.fs.exists(region.getRegionFileSystem().getRegionDir())
          && !this.fs.delete(region.getRegionFileSystem().getRegionDir(), true)) {
        throw new IOException("Failed deleting of "
            + region.getRegionFileSystem().getRegionDir());
      }
    }
    if (this.wal != null)
      this.wal.closeAndDelete();
    this.fs.delete(this.testdir, true);
  }

  /**
   * Test straight prepare works. Tries to merge on {@link #region_a} and
   * {@link #region_b}
   * @throws IOException
   */
  @Test
  public void testPrepare() throws IOException {
    prepareOnGoodRegions();
  }

  private RegionMergeTransaction prepareOnGoodRegions() throws IOException {
    RegionMergeTransaction mt = new RegionMergeTransaction(region_a, region_b,
        false);
    RegionMergeTransaction spyMT = Mockito.spy(mt);
    doReturn(false).when(spyMT).hasMergeQualifierInMeta(null,
        region_a.getRegionName());
    doReturn(false).when(spyMT).hasMergeQualifierInMeta(null,
        region_b.getRegionName());
    assertTrue(spyMT.prepare(null));
    return spyMT;
  }

  /**
   * Test merging the same region
   */
  @Test
  public void testPrepareWithSameRegion() throws IOException {
    RegionMergeTransaction mt = new RegionMergeTransaction(this.region_a,
        this.region_a, true);
    assertFalse("should not merge the same region even if it is forcible ",
        mt.prepare(null));
  }

  /**
   * Test merging two not adjacent regions under a common merge
   */
  @Test
  public void testPrepareWithRegionsNotAdjacent() throws IOException {
    RegionMergeTransaction mt = new RegionMergeTransaction(this.region_a,
        this.region_c, false);
    assertFalse("should not merge two regions if they are adjacent except it is forcible",
        mt.prepare(null));
  }

  /**
   * Test merging two not adjacent regions under a compulsory merge
   */
  @Test
  public void testPrepareWithRegionsNotAdjacentUnderCompulsory()
      throws IOException {
    RegionMergeTransaction mt = new RegionMergeTransaction(region_a, region_c,
        true);
    RegionMergeTransaction spyMT = Mockito.spy(mt);
    doReturn(false).when(spyMT).hasMergeQualifierInMeta(null,
        region_a.getRegionName());
    doReturn(false).when(spyMT).hasMergeQualifierInMeta(null,
        region_c.getRegionName());
    assertTrue("Since focible is true, should merge two regions even if they are not adjacent",
        spyMT.prepare(null));
  }

  /**
   * Pass a reference store
   */
  @Test
  public void testPrepareWithRegionsWithReference() throws IOException {
    HStore storeMock = Mockito.mock(HStore.class);
    when(storeMock.hasReferences()).thenReturn(true);
    when(storeMock.getFamily()).thenReturn(new HColumnDescriptor("cf"));
    when(storeMock.close()).thenReturn(ImmutableList.<StoreFile>of());
    this.region_a.stores.put(Bytes.toBytes(""), storeMock);
    RegionMergeTransaction mt = new RegionMergeTransaction(this.region_a,
        this.region_b, false);
    assertFalse(
        "a region should not be mergeable if it has instances of store file references",
        mt.prepare(null));
  }

  @Test
  public void testPrepareWithClosedRegion() throws IOException {
    this.region_a.close();
    RegionMergeTransaction mt = new RegionMergeTransaction(this.region_a,
        this.region_b, false);
    assertFalse(mt.prepare(null));
  }

  /**
   * Test merging regions which are merged regions and has reference in hbase:meta all
   * the same
   */
  @Test
  public void testPrepareWithRegionsWithMergeReference() throws IOException {
    RegionMergeTransaction mt = new RegionMergeTransaction(region_a, region_b,
        false);
    RegionMergeTransaction spyMT = Mockito.spy(mt);
    doReturn(true).when(spyMT).hasMergeQualifierInMeta(null,
        region_a.getRegionName());
    doReturn(true).when(spyMT).hasMergeQualifierInMeta(null,
        region_b.getRegionName());
    assertFalse(spyMT.prepare(null));
  }

  @Test
  public void testWholesomeMerge() throws IOException, InterruptedException {
    final int rowCountOfRegionA = loadRegion(this.region_a, CF, true);
    final int rowCountOfRegionB = loadRegion(this.region_b, CF, true);
    assertTrue(rowCountOfRegionA > 0 && rowCountOfRegionB > 0);
    assertEquals(rowCountOfRegionA, countRows(this.region_a));
    assertEquals(rowCountOfRegionB, countRows(this.region_b));

    // Start transaction.
    RegionMergeTransaction mt = prepareOnGoodRegions();

    // Run the execute. Look at what it returns.
    TEST_UTIL.getConfiguration().setInt(HConstants.REGIONSERVER_PORT, 0);
    Server mockServer = new HRegionServer(TEST_UTIL.getConfiguration());
    HRegion mergedRegion = mt.execute(mockServer, null);
    // Do some assertions about execution.
    assertTrue(this.fs.exists(mt.getMergesDir()));
    // Assert region_a and region_b is closed.
    assertTrue(region_a.isClosed());
    assertTrue(region_b.isClosed());

    // Assert mergedir is empty -- because its content will have been moved out
    // to be under the merged region dirs.
    assertEquals(0, this.fs.listStatus(mt.getMergesDir()).length);
    // Check merged region have correct key span.
    assertTrue(Bytes.equals(this.region_a.getStartKey(),
        mergedRegion.getStartKey()));
    assertTrue(Bytes.equals(this.region_b.getEndKey(),
        mergedRegion.getEndKey()));
    // Count rows. merged region are already open
    try {
      int mergedRegionRowCount = countRows(mergedRegion);
      assertEquals((rowCountOfRegionA + rowCountOfRegionB),
          mergedRegionRowCount);
    } finally {
      HRegion.closeHRegion(mergedRegion);
    }
    // Assert the write lock is no longer held on region_a and region_b
    assertTrue(!this.region_a.lock.writeLock().isHeldByCurrentThread());
    assertTrue(!this.region_b.lock.writeLock().isHeldByCurrentThread());
  }

  @Test
  public void testRollback() throws IOException, InterruptedException {
    final int rowCountOfRegionA = loadRegion(this.region_a, CF, true);
    final int rowCountOfRegionB = loadRegion(this.region_b, CF, true);
    assertTrue(rowCountOfRegionA > 0 && rowCountOfRegionB > 0);
    assertEquals(rowCountOfRegionA, countRows(this.region_a));
    assertEquals(rowCountOfRegionB, countRows(this.region_b));

    // Start transaction.
    RegionMergeTransaction mt = prepareOnGoodRegions();

    when(mt.createMergedRegionFromMerges(region_a, region_b,
        mt.getMergedRegionInfo())).thenThrow(
        new MockedFailedMergedRegionCreation());

    // Run the execute. Look at what it returns.
    boolean expectedException = false;
    TEST_UTIL.getConfiguration().setInt(HConstants.REGIONSERVER_PORT, 0);
    Server mockServer = new HRegionServer(TEST_UTIL.getConfiguration());
    try {
      mt.execute(mockServer, null);
    } catch (MockedFailedMergedRegionCreation e) {
      expectedException = true;
    }
    assertTrue(expectedException);
    // Run rollback
    assertTrue(mt.rollback(null, null));

    // Assert I can scan region_a and region_b.
    int rowCountOfRegionA2 = countRows(this.region_a);
    assertEquals(rowCountOfRegionA, rowCountOfRegionA2);
    int rowCountOfRegionB2 = countRows(this.region_b);
    assertEquals(rowCountOfRegionB, rowCountOfRegionB2);

    // Assert rollback cleaned up stuff in fs
    assertTrue(!this.fs.exists(HRegion.getRegionDir(this.testdir,
        mt.getMergedRegionInfo())));

    assertTrue(!this.region_a.lock.writeLock().isHeldByCurrentThread());
    assertTrue(!this.region_b.lock.writeLock().isHeldByCurrentThread());

    // Now retry the merge but do not throw an exception this time.
    assertTrue(mt.prepare(null));
    HRegion mergedRegion = mt.execute(mockServer, null);
    // Count rows. daughters are already open
    // Count rows. merged region are already open
    try {
      int mergedRegionRowCount = countRows(mergedRegion);
      assertEquals((rowCountOfRegionA + rowCountOfRegionB),
          mergedRegionRowCount);
    } finally {
      HRegion.closeHRegion(mergedRegion);
    }
    // Assert the write lock is no longer held on region_a and region_b
    assertTrue(!this.region_a.lock.writeLock().isHeldByCurrentThread());
    assertTrue(!this.region_b.lock.writeLock().isHeldByCurrentThread());
  }

  @Test
  public void testFailAfterPONR() throws IOException, KeeperException, InterruptedException {
    final int rowCountOfRegionA = loadRegion(this.region_a, CF, true);
    final int rowCountOfRegionB = loadRegion(this.region_b, CF, true);
    assertTrue(rowCountOfRegionA > 0 && rowCountOfRegionB > 0);
    assertEquals(rowCountOfRegionA, countRows(this.region_a));
    assertEquals(rowCountOfRegionB, countRows(this.region_b));

    // Start transaction.
    RegionMergeTransaction mt = prepareOnGoodRegions();
    Mockito.doThrow(new MockedFailedMergedRegionOpen())
        .when(mt)
        .openMergedRegion((Server) Mockito.anyObject(),
            (RegionServerServices) Mockito.anyObject(),
            (HRegion) Mockito.anyObject());

    // Run the execute. Look at what it returns.
    boolean expectedException = false;
    TEST_UTIL.getConfiguration().setInt(HConstants.REGIONSERVER_PORT, 0);
    Server mockServer = new HRegionServer(TEST_UTIL.getConfiguration());
    try {
      mt.execute(mockServer, null);
    } catch (MockedFailedMergedRegionOpen e) {
      expectedException = true;
    }
    assertTrue(expectedException);
    // Run rollback returns false that we should restart.
    assertFalse(mt.rollback(null, null));
    // Make sure that merged region is still in the filesystem, that
    // they have not been removed; this is supposed to be the case if we go
    // past point of no return.
    Path tableDir = this.region_a.getRegionFileSystem().getRegionDir()
        .getParent();
    Path mergedRegionDir = new Path(tableDir, mt.getMergedRegionInfo()
        .getEncodedName());
    assertTrue(TEST_UTIL.getTestFileSystem().exists(mergedRegionDir));
  }

  @Test
  public void testMeregedRegionBoundary() {
    TableName tableName =
        TableName.valueOf("testMeregedRegionBoundary");
    byte[] a = Bytes.toBytes("a");
    byte[] b = Bytes.toBytes("b");
    byte[] z = Bytes.toBytes("z");
    HRegionInfo r1 = new HRegionInfo(tableName);
    HRegionInfo r2 = new HRegionInfo(tableName, a, z);
    HRegionInfo m = RegionMergeTransaction.getMergedRegionInfo(r1, r2);
    assertTrue(Bytes.equals(m.getStartKey(), r1.getStartKey())
        && Bytes.equals(m.getEndKey(), r1.getEndKey()));

    r1 = new HRegionInfo(tableName, null, a);
    r2 = new HRegionInfo(tableName, a, z);
    m = RegionMergeTransaction.getMergedRegionInfo(r1, r2);
    assertTrue(Bytes.equals(m.getStartKey(), r1.getStartKey())
        && Bytes.equals(m.getEndKey(), r2.getEndKey()));

    r1 = new HRegionInfo(tableName, null, a);
    r2 = new HRegionInfo(tableName, z, null);
    m = RegionMergeTransaction.getMergedRegionInfo(r1, r2);
    assertTrue(Bytes.equals(m.getStartKey(), r1.getStartKey())
        && Bytes.equals(m.getEndKey(), r2.getEndKey()));

    r1 = new HRegionInfo(tableName, a, z);
    r2 = new HRegionInfo(tableName, z, null);
    m = RegionMergeTransaction.getMergedRegionInfo(r1, r2);
    assertTrue(Bytes.equals(m.getStartKey(), r1.getStartKey())
      && Bytes.equals(m.getEndKey(), r2.getEndKey()));

    r1 = new HRegionInfo(tableName, a, b);
    r2 = new HRegionInfo(tableName, b, z);
    m = RegionMergeTransaction.getMergedRegionInfo(r1, r2);
    assertTrue(Bytes.equals(m.getStartKey(), r1.getStartKey())
      && Bytes.equals(m.getEndKey(), r2.getEndKey()));
  }

  /**
   * Exception used in this class only.
   */
  @SuppressWarnings("serial")
  private class MockedFailedMergedRegionCreation extends IOException {
  }

  @SuppressWarnings("serial")
  private class MockedFailedMergedRegionOpen extends IOException {
  }

  private HRegion createRegion(final Path testdir, final HLog wal,
      final byte[] startrow, final byte[] endrow)
      throws IOException {
    // Make a region with start and end keys.
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("table"));
    HColumnDescriptor hcd = new HColumnDescriptor(CF);
    htd.addFamily(hcd);
    HRegionInfo hri = new HRegionInfo(htd.getTableName(), startrow, endrow);
    HRegion a = HRegion.createHRegion(hri, testdir,
        TEST_UTIL.getConfiguration(), htd);
    HRegion.closeHRegion(a);
    return HRegion.openHRegion(testdir, hri, htd, wal,
        TEST_UTIL.getConfiguration());
  }

  private int countRows(final HRegion r) throws IOException {
    int rowcount = 0;
    InternalScanner scanner = r.getScanner(new Scan());
    try {
      List<Cell> kvs = new ArrayList<Cell>();
      boolean hasNext = true;
      while (hasNext) {
        hasNext = scanner.next(kvs);
        if (!kvs.isEmpty())
          rowcount++;
      }
    } finally {
      scanner.close();
    }
    return rowcount;
  }

  /**
   * Load region with rows from 'aaa' to 'zzz', skip the rows which are out of
   * range of the region
   * @param r Region
   * @param f Family
   * @param flush flush the cache if true
   * @return Count of rows loaded.
   * @throws IOException
   */
  private int loadRegion(final HRegion r, final byte[] f, final boolean flush)
      throws IOException {
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          if (!HRegion.rowIsInRange(r.getRegionInfo(), k)) {
            continue;
          }
          Put put = new Put(k);
          put.add(f, null, k);
          if (r.getLog() == null)
            put.setDurability(Durability.SKIP_WAL);
          r.put(put);
          rowCount++;
        }
      }
      if (flush) {
        r.flushcache();
      }
    }
    return rowCount;
  }

}
