/*
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
package org.apache.hadoop.hbase.client;

import java.util.ArrayList;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;

/**
 * Make sure the fake KVs created internally are never user visible
 * (not even to filters)
 */
@Category(SmallTests.class)
public class TestFakeKeyInFilter extends BinaryComparator {
  protected static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  public TestFakeKeyInFilter() {
    super(Bytes.toBytes("foo"));
  }

  @Override
  public int compareTo(byte[] value, int offset, int length) {
    if (value.length == 0) {
      throw new RuntimeException("Found mysterious empty row");
    }
    return 0;
  }

  /**
   * Simple way to verify the scenario.
   * There are no KVs with an empty row key in the
   * table, yet such a KV is presented to the filter.
   */
  @Test
  public void testForEmptyRowKey() throws Exception {
    byte[] table = Bytes.toBytes("testForEmptyRowKey");
    byte[] row = Bytes.toBytes("myRow");
    byte[] cf = Bytes.toBytes("myFamily");
    byte[] cq = Bytes.toBytes("myColumn");
    HTableDescriptor desc = new HTableDescriptor(table);
    desc.addFamily(new HColumnDescriptor(cf));
    HRegionInfo hri = new HRegionInfo(desc.getName(), null, null);
    HRegion region = HRegion.createHRegion(hri, FSUtils.getRootDir(UTIL.getConfiguration()), UTIL.getConfiguration(), desc);
    Put put = new Put(row);
    put.add(cf, cq, cq);
    region.put(put);
    region.flushcache();
    Scan scan = new Scan();
    scan.addColumn(cf, cq);
    ByteArrayComparable comparable = new TestFakeKeyInFilter();
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, comparable);
    scan.setFilter(filter);
    RegionScanner scanner = region.getScanner(scan);
    scanner.next(new ArrayList<KeyValue>());
    scanner.close();
    region.close();
  }

}
