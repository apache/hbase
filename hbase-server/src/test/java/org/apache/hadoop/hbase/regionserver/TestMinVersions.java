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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test Minimum Versions feature (HBASE-4071).
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestMinVersions {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMinVersions.class);

  HBaseTestingUtility hbu = new HBaseTestingUtility();
  private final byte[] T0 = Bytes.toBytes("0");
  private final byte[] T1 = Bytes.toBytes("1");
  private final byte[] T2 = Bytes.toBytes("2");
  private final byte[] T3 = Bytes.toBytes("3");
  private final byte[] T4 = Bytes.toBytes("4");
  private final byte[] T5 = Bytes.toBytes("5");

  private final byte[] c0 = COLUMNS[0];

  @Rule public TestName name = new TestName();

  /**
   * Verify behavior of getClosestBefore(...)
   */
  @Test
  public void testGetClosestBefore() throws Exception {

    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(c0)
      .setMinVersions(1).setMaxVersions(1000).setTimeToLive(1).
        setKeepDeletedCells(KeepDeletedCells.FALSE).build();

    TableDescriptor htd = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamily(cfd).build();
    HRegion region = hbu.createLocalHRegion(htd, null, null);
    try {

      // 2s in the past
      long ts = EnvironmentEdgeManager.currentTime() - 2000;

      Put p = new Put(T1, ts);
      p.addColumn(c0, c0, T1);
      region.put(p);

      p = new Put(T1, ts+1);
      p.addColumn(c0, c0, T4);
      region.put(p);

      p = new Put(T3, ts);
      p.addColumn(c0, c0, T3);
      region.put(p);

      // now make sure that getClosestBefore(...) get can
      // rows that would be expired without minVersion.
      // also make sure it gets the latest version
      Result r = hbu.getClosestRowBefore(region, T1, c0);
      checkResult(r, c0, T4);

      r = hbu.getClosestRowBefore(region, T2, c0);
      checkResult(r, c0, T4);

      // now flush/compact
      region.flush(true);
      region.compact(true);

      r = hbu.getClosestRowBefore(region, T1, c0);
      checkResult(r, c0, T4);

      r = hbu.getClosestRowBefore(region, T2, c0);
      checkResult(r, c0, T4);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  /**
   * Test mixed memstore and storefile scanning
   * with minimum versions.
   */
  @Test
  public void testStoreMemStore() throws Exception {
    // keep 3 versions minimum

    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(c0)
        .setMinVersions(3).setMaxVersions(1000).setTimeToLive(1).
        setKeepDeletedCells(KeepDeletedCells.FALSE).build();

    TableDescriptor htd = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamily(cfd).build();

    HRegion region = hbu.createLocalHRegion(htd, null, null);
    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;

    try {
      Put p = new Put(T1, ts-1);
      p.addColumn(c0, c0, T2);
      region.put(p);

      p = new Put(T1, ts-3);
      p.addColumn(c0, c0, T0);
      region.put(p);

      // now flush/compact
      region.flush(true);
      region.compact(true);

      p = new Put(T1, ts);
      p.addColumn(c0, c0, T3);
      region.put(p);

      p = new Put(T1, ts-2);
      p.addColumn(c0, c0, T1);
      region.put(p);

      p = new Put(T1, ts-3);
      p.addColumn(c0, c0, T0);
      region.put(p);

      // newest version in the memstore
      // the 2nd oldest in the store file
      // and the 3rd, 4th oldest also in the memstore

      Get g = new Get(T1);
      g.setMaxVersions();
      Result r = region.get(g); // this'll use ScanWildcardColumnTracker
      checkResult(r, c0, T3,T2,T1);

      g = new Get(T1);
      g.setMaxVersions();
      g.addColumn(c0, c0);
      r = region.get(g);  // this'll use ExplicitColumnTracker
      checkResult(r, c0, T3,T2,T1);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  /**
   * Make sure the Deletes behave as expected with minimum versions
   */
  @Test
  public void testDelete() throws Exception {
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(c0)
        .setMinVersions(3).setMaxVersions(1000).setTimeToLive(1).
        setKeepDeletedCells(KeepDeletedCells.FALSE).build();

    TableDescriptor htd = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamily(cfd).build();

    HRegion region = hbu.createLocalHRegion(htd, null, null);

    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;

    try {
      Put p = new Put(T1, ts-2);
      p.addColumn(c0, c0, T1);
      region.put(p);

      p = new Put(T1, ts-1);
      p.addColumn(c0, c0, T2);
      region.put(p);

      p = new Put(T1, ts);
      p.addColumn(c0, c0, T3);
      region.put(p);

      Delete d = new Delete(T1, ts-1);
      region.delete(d);

      Get g = new Get(T1);
      g.setMaxVersions();
      Result r = region.get(g);  // this'll use ScanWildcardColumnTracker
      checkResult(r, c0, T3);

      g = new Get(T1);
      g.setMaxVersions();
      g.addColumn(c0, c0);
      r = region.get(g);  // this'll use ExplicitColumnTracker
      checkResult(r, c0, T3);

      // now flush/compact
      region.flush(true);
      region.compact(true);

      // try again
      g = new Get(T1);
      g.setMaxVersions();
      r = region.get(g);  // this'll use ScanWildcardColumnTracker
      checkResult(r, c0, T3);

      g = new Get(T1);
      g.setMaxVersions();
      g.addColumn(c0, c0);
      r = region.get(g);  // this'll use ExplicitColumnTracker
      checkResult(r, c0, T3);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  /**
   * Make sure the memstor behaves correctly with minimum versions
   */
  @Test
  public void testMemStore() throws Exception {
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(c0)
        .setMinVersions(2).setMaxVersions(1000).setTimeToLive(1).
        setKeepDeletedCells(KeepDeletedCells.FALSE).build();

    TableDescriptor htd = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamily(cfd).build();
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;

    try {
      // 2nd version
      Put p = new Put(T1, ts-2);
      p.addColumn(c0, c0, T2);
      region.put(p);

      // 3rd version
      p = new Put(T1, ts-1);
      p.addColumn(c0, c0, T3);
      region.put(p);

      // 4th version
      p = new Put(T1, ts);
      p.addColumn(c0, c0, T4);
      region.put(p);

      // now flush/compact
      region.flush(true);
      region.compact(true);

      // now put the first version (backdated)
      p = new Put(T1, ts-3);
      p.addColumn(c0, c0, T1);
      region.put(p);

      // now the latest change is in the memstore,
      // but it is not the latest version

      Result r = region.get(new Get(T1));
      checkResult(r, c0, T4);

      Get g = new Get(T1);
      g.setMaxVersions();
      r = region.get(g); // this'll use ScanWildcardColumnTracker
      checkResult(r, c0, T4,T3);

      g = new Get(T1);
      g.setMaxVersions();
      g.addColumn(c0, c0);
      r = region.get(g);  // this'll use ExplicitColumnTracker
      checkResult(r, c0, T4,T3);

      p = new Put(T1, ts+1);
      p.addColumn(c0, c0, T5);
      region.put(p);

      // now the latest version is in the memstore

      g = new Get(T1);
      g.setMaxVersions();
      r = region.get(g);  // this'll use ScanWildcardColumnTracker
      checkResult(r, c0, T5,T4);

      g = new Get(T1);
      g.setMaxVersions();
      g.addColumn(c0, c0);
      r = region.get(g);  // this'll use ExplicitColumnTracker
      checkResult(r, c0, T5,T4);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  /**
   * Verify basic minimum versions functionality
   */
  @Test
  public void testBaseCase() throws Exception {
    // 2 version minimum, 1000 versions maximum, ttl = 1s
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(c0)
        .setMinVersions(2).setMaxVersions(1000).setTimeToLive(1).
        setKeepDeletedCells(KeepDeletedCells.FALSE).build();

    TableDescriptor htd = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamily(cfd).build();
    HRegion region = hbu.createLocalHRegion(htd, null, null);
    try {

      // 2s in the past
      long ts = EnvironmentEdgeManager.currentTime() - 2000;

       // 1st version
      Put p = new Put(T1, ts-3);
      p.addColumn(c0, c0, T1);
      region.put(p);

      // 2nd version
      p = new Put(T1, ts-2);
      p.addColumn(c0, c0, T2);
      region.put(p);

      // 3rd version
      p = new Put(T1, ts-1);
      p.addColumn(c0, c0, T3);
      region.put(p);

      // 4th version
      p = new Put(T1, ts);
      p.addColumn(c0, c0, T4);
      region.put(p);

      Result r = region.get(new Get(T1));
      checkResult(r, c0, T4);

      Get g = new Get(T1);
      g.setTimeRange(0L, ts+1);
      r = region.get(g);
      checkResult(r, c0, T4);

  // oldest version still exists
      g.setTimeRange(0L, ts-2);
      r = region.get(g);
      checkResult(r, c0, T1);

      // gets see only available versions
      // even before compactions
      g = new Get(T1);
      g.setMaxVersions();
      r = region.get(g); // this'll use ScanWildcardColumnTracker
      checkResult(r, c0, T4,T3);

      g = new Get(T1);
      g.setMaxVersions();
      g.addColumn(c0, c0);
      r = region.get(g);  // this'll use ExplicitColumnTracker
      checkResult(r, c0, T4,T3);

      // now flush
      region.flush(true);

      // with HBASE-4241 a flush will eliminate the expired rows
      g = new Get(T1);
      g.setTimeRange(0L, ts-2);
      r = region.get(g);
      assertTrue(r.isEmpty());

      // major compaction
      region.compact(true);

      // after compaction the 4th version is still available
      g = new Get(T1);
      g.setTimeRange(0L, ts+1);
      r = region.get(g);
      checkResult(r, c0, T4);

      // so is the 3rd
      g.setTimeRange(0L, ts);
      r = region.get(g);
      checkResult(r, c0, T3);

      // but the 2nd and earlier versions are gone
      g.setTimeRange(0L, ts-1);
      r = region.get(g);
      assertTrue(r.isEmpty());
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  /**
   * Verify that basic filters still behave correctly with
   * minimum versions enabled.
   */
  @Test
  public void testFilters() throws Exception {
    final byte [] c1 = COLUMNS[1];
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(c0)
        .setMinVersions(2).setMaxVersions(1000).setTimeToLive(1).
        setKeepDeletedCells(KeepDeletedCells.FALSE).build();

    ColumnFamilyDescriptor cfd2 =
      ColumnFamilyDescriptorBuilder.newBuilder(c1)
        .setMinVersions(2).setMaxVersions(1000).setTimeToLive(1).
        setKeepDeletedCells(KeepDeletedCells.FALSE).build();
    List<ColumnFamilyDescriptor> cfdList = new ArrayList();
    cfdList.add(cfd);
    cfdList.add(cfd2);

    TableDescriptor htd = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamilies(cfdList).build();
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;
    try {

      Put p = new Put(T1, ts-3);
      p.addColumn(c0, c0, T0);
      p.addColumn(c1, c1, T0);
      region.put(p);

      p = new Put(T1, ts-2);
      p.addColumn(c0, c0, T1);
      p.addColumn(c1, c1, T1);
      region.put(p);

      p = new Put(T1, ts-1);
      p.addColumn(c0, c0, T2);
      p.addColumn(c1, c1, T2);
      region.put(p);

      p = new Put(T1, ts);
      p.addColumn(c0, c0, T3);
      p.addColumn(c1, c1, T3);
      region.put(p);

      List<Long> tss = new ArrayList<>();
      tss.add(ts-1);
      tss.add(ts-2);

      // Sholud only get T2, versions is 2, so T1 is gone from user view.
      Get g = new Get(T1);
      g.addColumn(c1,c1);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      Result r = region.get(g);
      checkResult(r, c1, T2);

      // Sholud only get T2, versions is 2, so T1 is gone from user view.
      g = new Get(T1);
      g.addColumn(c0,c0);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      r = region.get(g);
      checkResult(r, c0, T2);

      // now flush/compact
      region.flush(true);
      region.compact(true);

      // After flush/compact, the result should be consistent with previous result
      g = new Get(T1);
      g.addColumn(c1,c1);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      r = region.get(g);
      checkResult(r, c1, T2);

      // After flush/compact, the result should be consistent with previous result
      g = new Get(T1);
      g.addColumn(c0,c0);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      r = region.get(g);
      checkResult(r, c0, T2);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  @Test
  public void testMinVersionsWithKeepDeletedCellsTTL() throws Exception {
    int ttl = 4;
    ColumnFamilyDescriptor cfd =
      ColumnFamilyDescriptorBuilder.newBuilder(c0)
        .setVersionsWithTimeToLive(ttl, 2).build();
    verifyVersionedCellKeyValues(ttl, cfd);

    cfd = ColumnFamilyDescriptorBuilder.newBuilder(c0)
      .setMinVersions(2)
      .setMaxVersions(Integer.MAX_VALUE)
      .setTimeToLive(ttl)
      .setKeepDeletedCells(KeepDeletedCells.TTL)
      .build();
    verifyVersionedCellKeyValues(ttl, cfd);
  }

  private void verifyVersionedCellKeyValues(int ttl, ColumnFamilyDescriptor cfd)
      throws IOException {
    TableDescriptor htd = TableDescriptorBuilder.
      newBuilder(TableName.valueOf(name.getMethodName())).setColumnFamily(cfd).build();

    HRegion region = hbu.createLocalHRegion(htd, null, null);

    try {
      long startTS = EnvironmentEdgeManager.currentTime();
      ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
      injectEdge.setValue(startTS);
      EnvironmentEdgeManager.injectEdge(injectEdge);

      long ts = startTS - 2000;
      putFourVersions(region, ts);

      Get get;
      Result result;

      //check we can still see all versions before compaction
      get = new Get(T1);
      get.readAllVersions();
      get.setTimeRange(0, ts);
      result = region.get(get);
      checkResult(result, c0, T4, T3, T2, T1);

      region.flush(true);
      region.compact(true);
      Assert.assertEquals(startTS, EnvironmentEdgeManager.currentTime());
      long expiredTime = EnvironmentEdgeManager.currentTime() - ts - 4;
      Assert.assertTrue("TTL for T1 has expired", expiredTime < (ttl * 1000));
      //check that nothing was purged yet
      verifyBeforeCompaction(region, ts);

      injectEdge.incValue(ttl * 1000);

      region.flush(true);
      region.compact(true);
      verifyAfterTtl(region, ts);
    } finally {
      HBaseTestingUtility.closeRegionAndWAL(region);
    }
  }

  private void verifyAfterTtl(HRegion region, long ts) throws IOException {
    Get get;
    Result result;
    //check that after compaction (which is after TTL) that only T1 && T2 were purged
    get = new Get(T1);
    get.readAllVersions();
    get.setTimeRange(0, ts);
    result = region.get(get);
    checkResult(result, c0, T4, T3);

    get = new Get(T1);
    get.readAllVersions();
    get.setTimeRange(0, ts - 1);
    result = region.get(get);
    checkResult(result, c0, T3);

    get = new Get(T1);
    get.readAllVersions();
    get.setTimestamp(ts - 2);
    result = region.get(get);
    checkResult(result, c0, T3);

    get = new Get(T1);
    get.readAllVersions();
    get.setTimestamp(ts - 3);
    result = region.get(get);
    Assert.assertEquals(result.getColumnCells(c0, c0).size(), 0);

    get = new Get(T1);
    get.readAllVersions();
    get.setTimeRange(0, ts - 2);
    result = region.get(get);
    Assert.assertEquals(result.getColumnCells(c0, c0).size(), 0);
  }

  private void verifyBeforeCompaction(HRegion region, long ts) throws IOException {
    Get get;
    Result result;
    get = new Get(T1);
    get.readAllVersions();
    get.setTimeRange(0, ts);
    result = region.get(get);
    checkResult(result, c0, T4, T3, T2, T1);

    get = new Get(T1);
    get.readAllVersions();
    get.setTimeRange(0, ts - 1);
    result = region.get(get);
    checkResult(result, c0, T3, T2, T1);

    get = new Get(T1);
    get.readAllVersions();
    get.setTimeRange(0, ts - 2);
    result = region.get(get);
    checkResult(result, c0, T2, T1);

    get = new Get(T1);
    get.readAllVersions();
    get.setTimeRange(0, ts - 3);
    result = region.get(get);
    checkResult(result, c0, T1);
  }

  private void putFourVersions(HRegion region, long ts) throws IOException {
    // 1st version
    Put put = new Put(T1, ts - 4);
    put.addColumn(c0, c0, T1);
    region.put(put);

    // 2nd version
    put = new Put(T1, ts - 3);
    put.addColumn(c0, c0, T2);
    region.put(put);

    // 3rd version
    put = new Put(T1, ts - 2);
    put.addColumn(c0, c0, T3);
    region.put(put);

    // 4th version
    put = new Put(T1, ts - 1);
    put.addColumn(c0, c0, T4);
    region.put(put);
  }

  private void checkResult(Result r, byte[] col, byte[] ... vals) {
    assertEquals(vals.length, r.size());
    List<Cell> kvs = r.getColumnCells(col, col);
    assertEquals(kvs.size(), vals.length);
    for (int i=0;i<vals.length;i++) {
      String expected = Bytes.toString(vals[i]);
      String actual = Bytes.toString(CellUtil.cloneValue(kvs.get(i)));
      assertTrue(expected + " was expected but doesn't match " + actual,
          CellUtil.matchingValue(kvs.get(i), vals[i]));
    }
  }

}

