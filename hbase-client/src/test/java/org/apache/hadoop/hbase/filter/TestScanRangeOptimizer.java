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
package org.apache.hadoop.hbase.filter;

import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.ClientUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.Arrays;

@Category({ MiscTests.class, SmallTests.class })
public class TestScanRangeOptimizer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestScanRangeOptimizer.class);

  @Test
  public void testOptimizePrefixFilter() {
    // scan data where rowkey starts with 'aaa'
    byte[] prefix = Bytes.toBytes("aaa");
    byte[] prefixNext = ClientUtil.calculateTheClosestNextRowKeyForPrefix(prefix); // ="aab"

    Scan scan = new Scan().setFilter(new PrefixFilter(prefix));
    ScanRangeOptimizer.optimize(scan);
    Assert.assertEquals(0, Bytes.compareTo(prefix, scan.getStartRow()));
    Assert.assertEquals(0, Bytes.compareTo(prefixNext, scan.getStopRow()));
  }

  @Test
  public void testOptimizeRowFilter() {
    // scan data where rowkey > 'hhh' and rowkey < 'mmm'
    RowFilter low = new RowFilter(CompareOperator.GREATER,
      new BinaryComparator(Bytes.toBytes("hhh")));
    RowFilter high = new RowFilter(CompareOperator.LESS,
      new BinaryComparator(Bytes.toBytes("mmm")));

    Scan scan = new Scan()
      .withStartRow(Bytes.toBytes("aaa"))
      .withStartRow(Bytes.toBytes("zzz"))
      .setFilter(new FilterListWithAND(Arrays.asList(low, high)));
    ScanRangeOptimizer.optimize(scan);
    Assert.assertEquals(0, Bytes.compareTo(Bytes.toBytes("hhh"), scan.getStartRow()));
    Assert.assertEquals(0, Bytes.compareTo(Bytes.toBytes("mmm"), scan.getStopRow()));
  }

  @Test
  public void testOptimizeFilterList() {
    // scan data where (rowkey start with 'prefix') or (rowkey >= 'hhh' and rowkey < 'mmm')
    PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("prefix"));
    FilterListWithAND and = new FilterListWithAND(Arrays.asList(
        new RowFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("hhh"))),
        new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("mmm")))));
    FilterListWithOR or = new FilterListWithOR(Arrays.asList(and, prefixFilter));

    Scan scan = new Scan().setFilter(or);
    ScanRangeOptimizer.optimize(scan);
    Assert.assertEquals(0, Bytes.compareTo(Bytes.toBytes("hhh"), scan.getStartRow()));
    Assert.assertEquals(0, Bytes.compareTo(Bytes.toBytes("prefiy"), scan.getStopRow()));
  }
}
