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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.regionserver.InternalScanner.NextState;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

/**
 * Test the {@link SplitTransaction} class against an HRegion (as opposed to
 * running cluster).
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestSplitTransaction {
  private final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final Path testdir =
    TEST_UTIL.getDataTestDir(this.getClass().getName());
  private HRegion parent;
  private WALFactory wals;
  private FileSystem fs;
  private static final byte [] STARTROW = new byte [] {'a', 'a', 'a'};
  // '{' is next ascii after 'z'.
  private static final byte [] ENDROW = new byte [] {'{', '{', '{'};
  private static final byte [] GOOD_SPLIT_ROW = new byte [] {'d', 'd', 'd'};
  private static final byte [] CF = HConstants.CATALOG_FAMILY;
  
  private static boolean preRollBackCalled = false;
  private static boolean postRollBackCalled = false;
  
  @Before public void setup() throws IOException {
    this.fs = FileSystem.get(TEST_UTIL.getConfiguration());
    TEST_UTIL.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, CustomObserver.class.getName());
    this.fs.delete(this.testdir, true);
    final Configuration walConf = new Configuration(TEST_UTIL.getConfiguration());
    FSUtils.setRootDir(walConf, this.testdir);
    this.wals = new WALFactory(walConf, null, this.getClass().getName());
    
    this.parent = createRegion(this.testdir, this.wals);
    RegionCoprocessorHost host = new RegionCoprocessorHost(this.parent, null, TEST_UTIL.getConfiguration());
    this.parent.setCoprocessorHost(host);
    TEST_UTIL.getConfiguration().setBoolean("hbase.testing.nocluster", true);
  }

  @After public void teardown() throws IOException {
    if (this.parent != null && !this.parent.isClosed()) this.parent.close();
    Path regionDir = this.parent.getRegionFileSystem().getRegionDir();
    if (this.fs.exists(regionDir) && !this.fs.delete(regionDir, true)) {
      throw new IOException("Failed delete of " + regionDir);
    }
    if (this.wals != null) {
      this.wals.close();
    }
    this.fs.delete(this.testdir, true);
  }

  @Test public void testFailAfterPONR() throws IOException, KeeperException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    SplitTransaction st = prepareGOOD_SPLIT_ROW();
    SplitTransaction spiedUponSt = spy(st);
    Mockito
        .doThrow(new MockedFailedDaughterOpen())
        .when(spiedUponSt)
        .openDaughterRegion((Server) Mockito.anyObject(),
            (HRegion) Mockito.anyObject());

    // Run the execute.  Look at what it returns.
    boolean expectedException = false;
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    try {
      spiedUponSt.execute(mockServer, null);
    } catch (IOException e) {
      if (e.getCause() != null &&
          e.getCause() instanceof MockedFailedDaughterOpen) {
        expectedException = true;
      }
    }
    assertTrue(expectedException);
    // Run rollback returns that we should restart.
    assertFalse(spiedUponSt.rollback(null, null));
    // Make sure that region a and region b are still in the filesystem, that
    // they have not been removed; this is supposed to be the case if we go
    // past point of no return.
    Path tableDir =  this.parent.getRegionFileSystem().getTableDir();
    Path daughterADir = new Path(tableDir, spiedUponSt.getFirstDaughter().getEncodedName());
    Path daughterBDir = new Path(tableDir, spiedUponSt.getSecondDaughter().getEncodedName());
    assertTrue(TEST_UTIL.getTestFileSystem().exists(daughterADir));
    assertTrue(TEST_UTIL.getTestFileSystem().exists(daughterBDir));
  }

  /**
   * Test straight prepare works.  Tries to split on {@link #GOOD_SPLIT_ROW}
   * @throws IOException
   */
  @Test public void testPrepare() throws IOException {
    prepareGOOD_SPLIT_ROW();
  }

  private SplitTransaction prepareGOOD_SPLIT_ROW() {
    return prepareGOOD_SPLIT_ROW(this.parent);
  }

  private SplitTransaction prepareGOOD_SPLIT_ROW(final HRegion parentRegion) {
    SplitTransaction st = new SplitTransaction(parentRegion, GOOD_SPLIT_ROW);
    assertTrue(st.prepare());
    return st;
  }

  /**
   * Pass a reference store
   */
  @Test public void testPrepareWithRegionsWithReference() throws IOException {
    HStore storeMock = Mockito.mock(HStore.class);
    when(storeMock.hasReferences()).thenReturn(true);
    when(storeMock.getFamily()).thenReturn(new HColumnDescriptor("cf"));
    when(storeMock.close()).thenReturn(ImmutableList.<StoreFile>of());
    this.parent.stores.put(Bytes.toBytes(""), storeMock);

    SplitTransaction st = new SplitTransaction(this.parent, GOOD_SPLIT_ROW);

    assertFalse("a region should not be splittable if it has instances of store file references",
                st.prepare());
  }

  /**
   * Pass an unreasonable split row.
   */
  @Test public void testPrepareWithBadSplitRow() throws IOException {
    // Pass start row as split key.
    SplitTransaction st = new SplitTransaction(this.parent, STARTROW);
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, HConstants.EMPTY_BYTE_ARRAY);
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, new byte [] {'A', 'A', 'A'});
    assertFalse(st.prepare());
    st = new SplitTransaction(this.parent, ENDROW);
    assertFalse(st.prepare());
  }

  @Test public void testPrepareWithClosedRegion() throws IOException {
    this.parent.close();
    SplitTransaction st = new SplitTransaction(this.parent, GOOD_SPLIT_ROW);
    assertFalse(st.prepare());
  }

  @Test public void testWholesomeSplit() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF, true);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Pretend region's blocks are not in the cache, used for
    // testWholesomeSplitWithHFileV1
    CacheConfig cacheConf = new CacheConfig(TEST_UTIL.getConfiguration());
    ((LruBlockCache) cacheConf.getBlockCache()).clearCache();

    // Start transaction.
    SplitTransaction st = prepareGOOD_SPLIT_ROW();

    // Run the execute.  Look at what it returns.
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    PairOfSameType<HRegion> daughters = st.execute(mockServer, null);
    // Do some assertions about execution.
    assertTrue(this.fs.exists(this.parent.getRegionFileSystem().getSplitsDir()));
    // Assert the parent region is closed.
    assertTrue(this.parent.isClosed());

    // Assert splitdir is empty -- because its content will have been moved out
    // to be under the daughter region dirs.
    assertEquals(0, this.fs.listStatus(this.parent.getRegionFileSystem().getSplitsDir()).length);
    // Check daughters have correct key span.
    assertTrue(Bytes.equals(this.parent.getStartKey(), daughters.getFirst().getStartKey()));
    assertTrue(Bytes.equals(GOOD_SPLIT_ROW, daughters.getFirst().getEndKey()));
    assertTrue(Bytes.equals(daughters.getSecond().getStartKey(), GOOD_SPLIT_ROW));
    assertTrue(Bytes.equals(this.parent.getEndKey(), daughters.getSecond().getEndKey()));
    // Count rows. daughters are already open
    int daughtersRowCount = 0;
    for (HRegion openRegion: daughters) {
      try {
        int count = countRows(openRegion);
        assertTrue(count > 0 && count != rowcount);
        daughtersRowCount += count;
      } finally {
        HBaseTestingUtility.closeRegionAndWAL(openRegion);
      }
    }
    assertEquals(rowcount, daughtersRowCount);
    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());
  }

  @Test
  public void testCountReferencesFailsSplit() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    HRegion spiedRegion = spy(this.parent);
    SplitTransaction st = prepareGOOD_SPLIT_ROW(spiedRegion);
    SplitTransaction spiedUponSt = spy(st);
    doThrow(new IOException("Failing split. Expected reference file count isn't equal."))
        .when(spiedUponSt).assertReferenceFileCount(anyInt(),
        eq(new Path(this.parent.getRegionFileSystem().getTableDir(),
            st.getSecondDaughter().getEncodedName())));

    // Run the execute.  Look at what it returns.
    boolean expectedException = false;
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    try {
      spiedUponSt.execute(mockServer, null);
    } catch (IOException e) {
      expectedException = true;
    }
    assertTrue(expectedException);
  }


  @Test public void testRollback() throws IOException {
    final int rowcount = TEST_UTIL.loadRegion(this.parent, CF);
    assertTrue(rowcount > 0);
    int parentRowCount = countRows(this.parent);
    assertEquals(rowcount, parentRowCount);

    // Start transaction.
    HRegion spiedRegion = spy(this.parent);
    SplitTransaction st = prepareGOOD_SPLIT_ROW(spiedRegion);
    SplitTransaction spiedUponSt = spy(st);
    doNothing().when(spiedUponSt).assertReferenceFileCount(anyInt(),
        eq(parent.getRegionFileSystem().getSplitsDir(st.getFirstDaughter())));
    when(spiedRegion.createDaughterRegionFromSplits(spiedUponSt.getSecondDaughter())).
        thenThrow(new MockedFailedDaughterCreation());
    // Run the execute.  Look at what it returns.
    boolean expectedException = false;
    Server mockServer = Mockito.mock(Server.class);
    when(mockServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    try {
      spiedUponSt.execute(mockServer, null);
    } catch (MockedFailedDaughterCreation e) {
      expectedException = true;
    }
    assertTrue(expectedException);
    // Run rollback
    assertTrue(spiedUponSt.rollback(null, null));

    // Assert I can scan parent.
    int parentRowCount2 = countRows(this.parent);
    assertEquals(parentRowCount, parentRowCount2);

    // Assert rollback cleaned up stuff in fs
    assertTrue(!this.fs.exists(HRegion.getRegionDir(this.testdir, st.getFirstDaughter())));
    assertTrue(!this.fs.exists(HRegion.getRegionDir(this.testdir, st.getSecondDaughter())));
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());

    // Now retry the split but do not throw an exception this time.
    assertTrue(st.prepare());
    PairOfSameType<HRegion> daughters = st.execute(mockServer, null);
    // Count rows. daughters are already open
    int daughtersRowCount = 0;
    for (HRegion openRegion: daughters) {
      try {
        int count = countRows(openRegion);
        assertTrue(count > 0 && count != rowcount);
        daughtersRowCount += count;
      } finally {
        HBaseTestingUtility.closeRegionAndWAL(openRegion);
      }
    }
    assertEquals(rowcount, daughtersRowCount);
    // Assert the write lock is no longer held on parent
    assertTrue(!this.parent.lock.writeLock().isHeldByCurrentThread());
    assertTrue("Rollback hooks should be called.", wasRollBackHookCalled());
  }
  
  private boolean wasRollBackHookCalled(){
    return (preRollBackCalled && postRollBackCalled);
  }

  /**
   * Exception used in this class only.
   */
  @SuppressWarnings("serial")
  private class MockedFailedDaughterCreation extends IOException {}
  private class MockedFailedDaughterOpen extends IOException {}

  private int countRows(final HRegion r) throws IOException {
    int rowcount = 0;
    InternalScanner scanner = r.getScanner(new Scan());
    try {
      List<Cell> kvs = new ArrayList<Cell>();
      boolean hasNext = true;
      while (hasNext) {
        hasNext = NextState.hasMoreValues(scanner.next(kvs));
        if (!kvs.isEmpty()) rowcount++;
      }
    } finally {
      scanner.close();
    }
    return rowcount;
  }

  HRegion createRegion(final Path testdir, final WALFactory wals)
  throws IOException {
    // Make a region with start and end keys. Use 'aaa', to 'AAA'.  The load
    // region utility will add rows between 'aaa' and 'zzz'.
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("table"));
    HColumnDescriptor hcd = new HColumnDescriptor(CF);
    htd.addFamily(hcd);
    HRegionInfo hri = new HRegionInfo(htd.getTableName(), STARTROW, ENDROW);
    HRegion r = HBaseTestingUtility.createRegionAndWAL(hri, testdir, TEST_UTIL.getConfiguration(),
        htd);
    HBaseTestingUtility.closeRegionAndWAL(r);
    return HRegion.openHRegion(testdir, hri, htd, wals.getWAL(hri.getEncodedNameAsBytes()),
      TEST_UTIL.getConfiguration());
  }
  
  public static class CustomObserver extends BaseRegionObserver{
    @Override
    public void preRollBackSplit(
        ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
      preRollBackCalled = true;
    }
    
    @Override
    public void postRollBackSplit(
        ObserverContext<RegionCoprocessorEnvironment> ctx) throws IOException {
      postRollBackCalled = true;
    }
  }

}

