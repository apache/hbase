/**
 * Copyright 2009 The Apache Software Foundation
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
package org.apache.hadoop.hbase.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Basic test of filters crossing region boundaries.
 * Tests filters come back with right answer.  For client-side test of
 * filter crossing boundaries, see filter tests in TestClient class.
 */
public class TestFilterAcrossRegions extends HBaseTestCase {
  public void testStop() throws IOException {
    byte [] name = Bytes.toBytes(getName());
    HTableDescriptor htd = new HTableDescriptor(name);
    htd.addFamily(new HColumnDescriptor(name));
    // Make three regions: ""-10, 10-20, 20-"".
    byte [] tenBoundary = Bytes.toBytes(10);
    byte [] twentyBoundary = Bytes.toBytes(20);
    HRegion r0 = createRegion(htd, HConstants.EMPTY_BYTE_ARRAY, tenBoundary);
    HRegion r1 = createRegion(htd, tenBoundary, twentyBoundary);
    HRegion r2 = createRegion(htd, twentyBoundary, HConstants.EMPTY_BYTE_ARRAY);
    HRegion [] regions = new HRegion [] {r0, r1, r2};
    final int max = 30;
    try {
      for (HRegion r: regions) {
        populate(Bytes.toInt(r.getStartKey()), Bytes.toInt(r.getEndKey()), r,
          max);
      }
      // Now I have 3 regions with rows of 0-9, 10-19, and 20-29.  Play with
      // scanners and filters.
      assertAllRows(regions, max);
      assertFilterStops(regions, max);
    } finally {
      for (HRegion r: regions) r.close();
    }
  }

  /*
   * Test using a rowfilter.  Test that after we go beyond wanted row, we
   * do not return any more rows.
   * @param regions
   * @param max
   * @throws IOException
   */
  private void assertFilterStops(final HRegion [] regions, final int max)
  throws IOException {
    // Count of rows seen.
    int count = 0;
    // Row at which we want to stop.
    int maximumRow = max/regions.length;
    // Count of regions seen.
    int regionCount = 0;
    for (HRegion r: regions) {
      // Make a filter that will stop inside first region.
      Scan s = createFilterStopsScanner(maximumRow);
      InternalScanner scanner = r.getScanner(s);
      List<KeyValue> results = new ArrayList<KeyValue>();
      boolean hasMore = false;
      do {
        hasMore = scanner.next(results);
        if (hasMore) count++;
        if (regionCount ==0) assertFalse(s.getFilter().filterAllRemaining());
        results.clear();
      } while (hasMore);
      if (regionCount > 0) assertTrue(s.getFilter().filterAllRemaining());
      regionCount++;
    }
    assertEquals(maximumRow - 1, count);
  }

  /*
   * @param max
   * @return A Scan with a RowFilter that has a binary comparator that does not
   * go beyond <code>max</code> (Filter is wrapped in a WhileMatchFilter so that
   * filterAllRemaining is true once we go beyond <code>max</code>).
   */
  private Scan createFilterStopsScanner(final int max) {
    Scan s = new Scan();
    Filter f = new RowFilter(CompareFilter.CompareOp.LESS,
      new BinaryComparator(Bytes.toBytes(max)));
    f = new WhileMatchFilter(f);
    s.setFilter(f);
    return s;
  }

  private void assertAllRows(final HRegion [] regions, final int max)
  throws IOException {
    int count = 0;
    for (HRegion r: regions) {
      count += scan(r, new Scan());
    }
    assertEquals(max, count);
  }

  private int scan(final HRegion r, final Scan scan) throws IOException {
    InternalScanner scanner = r.getScanner(new Scan(scan));
    int count = 0;
    List<KeyValue> results = new ArrayList<KeyValue>();
    do {
      count++;
    } while (scanner.next(results));
    return count;
  }

  private HRegion createRegion(final HTableDescriptor htd, final byte [] start,
    final byte [] end)
  throws IOException {
    HRegionInfo info = new HRegionInfo(htd, start, end, false);
    Path path = new Path(this.testDir, getName()); 
    return HRegion.createHRegion(info, path, conf);
  }

  /*
   * Add rows between start and end to region <code>r</code>
   * @param start
   * @param end
   * @param r
   * @param max
   * @throws IOException
   */
  private void populate(final int start, final int end, final HRegion r,
      final int max)
  throws IOException {
    byte [] name = r.getTableDesc().getFamiliesKeys().iterator().next();
    int s = start < 0? 0: start;
    int e = end < 0? max: end;
    for (int i = s; i < e; i++) {
      Put p = new Put(Bytes.toBytes(i));
      p.add(name, name, name);
      r.put(p);
    }
  }
}