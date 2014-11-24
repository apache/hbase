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

import static org.apache.hadoop.hbase.HBaseTestingUtility.COLUMNS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Test Minimum Versions feature (HBASE-4071).
 */
@Category({RegionServerTests.class, SmallTests.class})
public class TestMinVersions {
  HBaseTestingUtility hbu = HBaseTestingUtility.createLocalHTU();
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
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 1, 1000, 1, false);
    HRegion region = hbu.createLocalHRegion(htd, null, null);
    try {

      // 2s in the past
      long ts = EnvironmentEdgeManager.currentTime() - 2000;

      Put p = new Put(T1, ts);
      p.add(c0, c0, T1);
      region.put(p);

      p = new Put(T1, ts+1);
      p.add(c0, c0, T4);
      region.put(p);

      p = new Put(T3, ts);
      p.add(c0, c0, T3);
      region.put(p);

      // now make sure that getClosestBefore(...) get can
      // rows that would be expired without minVersion.
      // also make sure it gets the latest version
      Result r = region.getClosestRowBefore(T1, c0);
      checkResult(r, c0, T4);

      r = region.getClosestRowBefore(T2, c0);
      checkResult(r, c0, T4);

      // now flush/compact
      region.flushcache();
      region.compactStores(true);

      r = region.getClosestRowBefore(T1, c0);
      checkResult(r, c0, T4);

      r = region.getClosestRowBefore(T2, c0);
      checkResult(r, c0, T4);
    } finally {
      HRegion.closeHRegion(region);
    }
  }

  /**
   * Test mixed memstore and storefile scanning
   * with minimum versions.
   */
  @Test
  public void testStoreMemStore() throws Exception {
    // keep 3 versions minimum
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 3, 1000, 1, false);
    HRegion region = hbu.createLocalHRegion(htd, null, null);
    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;

    try {
      Put p = new Put(T1, ts-1);
      p.add(c0, c0, T2);
      region.put(p);

      p = new Put(T1, ts-3);
      p.add(c0, c0, T0);
      region.put(p);

      // now flush/compact
      region.flushcache();
      region.compactStores(true);

      p = new Put(T1, ts);
      p.add(c0, c0, T3);
      region.put(p);

      p = new Put(T1, ts-2);
      p.add(c0, c0, T1);
      region.put(p);

      p = new Put(T1, ts-3);
      p.add(c0, c0, T0);
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
      HRegion.closeHRegion(region);
    }
  }

  /**
   * Make sure the Deletes behave as expected with minimum versions
   */
  @Test
  public void testDelete() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 3, 1000, 1, false);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;

    try {
      Put p = new Put(T1, ts-2);
      p.add(c0, c0, T1);
      region.put(p);

      p = new Put(T1, ts-1);
      p.add(c0, c0, T2);
      region.put(p);

      p = new Put(T1, ts);
      p.add(c0, c0, T3);
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
      region.flushcache();
      region.compactStores(true);

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
      HRegion.closeHRegion(region);
    }
  }

  /**
   * Make sure the memstor behaves correctly with minimum versions
   */
  @Test
  public void testMemStore() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 2, 1000, 1, false);
    HRegion region = hbu.createLocalHRegion(htd, null, null);

    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;

    try {
      // 2nd version
      Put p = new Put(T1, ts-2);
      p.add(c0, c0, T2);
      region.put(p);

      // 3rd version
      p = new Put(T1, ts-1);
      p.add(c0, c0, T3);
      region.put(p);

      // 4th version
      p = new Put(T1, ts);
      p.add(c0, c0, T4);
      region.put(p);

      // now flush/compact
      region.flushcache();
      region.compactStores(true);

      // now put the first version (backdated)
      p = new Put(T1, ts-3);
      p.add(c0, c0, T1);
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
      p.add(c0, c0, T5);
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
      HRegion.closeHRegion(region);
    }
  }

  /**
   * Verify basic minimum versions functionality
   */
  @Test
  public void testBaseCase() throws Exception {
    // 1 version minimum, 1000 versions maximum, ttl = 1s
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 2, 1000, 1, false);
    HRegion region = hbu.createLocalHRegion(htd, null, null);
    try {

      // 2s in the past
      long ts = EnvironmentEdgeManager.currentTime() - 2000;

       // 1st version
      Put p = new Put(T1, ts-3);
      p.add(c0, c0, T1);
      region.put(p);

      // 2nd version
      p = new Put(T1, ts-2);
      p.add(c0, c0, T2);
      region.put(p);

      // 3rd version
      p = new Put(T1, ts-1);
      p.add(c0, c0, T3);
      region.put(p);

      // 4th version
      p = new Put(T1, ts);
      p.add(c0, c0, T4);
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
      region.flushcache();

      // with HBASE-4241 a flush will eliminate the expired rows
      g = new Get(T1);
      g.setTimeRange(0L, ts-2);
      r = region.get(g);
      assertTrue(r.isEmpty());

      // major compaction
      region.compactStores(true);

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
      HRegion.closeHRegion(region);
    }
  }

  /**
   * Verify that basic filters still behave correctly with
   * minimum versions enabled.
   */
  @Test
  public void testFilters() throws Exception {
    HTableDescriptor htd = hbu.createTableDescriptor(name.getMethodName(), 2, 1000, 1, false);
    HRegion region = hbu.createLocalHRegion(htd, null, null);
    final byte [] c1 = COLUMNS[1];

    // 2s in the past
    long ts = EnvironmentEdgeManager.currentTime() - 2000;
    try {

      Put p = new Put(T1, ts-3);
      p.add(c0, c0, T0);
      p.add(c1, c1, T0);
      region.put(p);

      p = new Put(T1, ts-2);
      p.add(c0, c0, T1);
      p.add(c1, c1, T1);
      region.put(p);

      p = new Put(T1, ts-1);
      p.add(c0, c0, T2);
      p.add(c1, c1, T2);
      region.put(p);

      p = new Put(T1, ts);
      p.add(c0, c0, T3);
      p.add(c1, c1, T3);
      region.put(p);

      List<Long> tss = new ArrayList<Long>();
      tss.add(ts-1);
      tss.add(ts-2);

      Get g = new Get(T1);
      g.addColumn(c1,c1);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      Result r = region.get(g);
      checkResult(r, c1, T2,T1);

      g = new Get(T1);
      g.addColumn(c0,c0);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      r = region.get(g);
      checkResult(r, c0, T2,T1);

      // now flush/compact
      region.flushcache();
      region.compactStores(true);

      g = new Get(T1);
      g.addColumn(c1,c1);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      r = region.get(g);
      checkResult(r, c1, T2);

      g = new Get(T1);
      g.addColumn(c0,c0);
      g.setFilter(new TimestampsFilter(tss));
      g.setMaxVersions();
      r = region.get(g);
      checkResult(r, c0, T2);
    } finally {
      HRegion.closeHRegion(region);
    }
  }

  private void checkResult(Result r, byte[] col, byte[] ... vals) {
    assertEquals(r.size(), vals.length);
    List<Cell> kvs = r.getColumnCells(col, col);
    assertEquals(kvs.size(), vals.length);
    for (int i=0;i<vals.length;i++) {
      assertTrue(CellUtil.matchingValue(kvs.get(i), vals[i]));
    }
  }

}

