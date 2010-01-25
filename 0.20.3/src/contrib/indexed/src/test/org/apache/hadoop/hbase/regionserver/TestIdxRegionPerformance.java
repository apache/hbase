/*
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxQualifierType;
import org.apache.hadoop.hbase.client.idx.IdxScan;
import org.apache.hadoop.hbase.client.idx.exp.Comparison;
import org.apache.hadoop.hbase.client.idx.exp.Expression;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Tests/Demonstrates performance compared to good-old HRegion.
 */
public class TestIdxRegionPerformance extends HBaseTestCase {

  private final String DIR = "test/build/data/TestIdxRegionPerformance/";
  private static final byte[] FAMILY_1_NAME = Bytes.toBytes("family1");
  private static final byte[] INT_QUAL_NAME = Bytes.toBytes("intQual");
  private static final byte[] BYTES_QUAL_NAME = Bytes.toBytes("bytesQual");
  private static final byte[] FAMILY_2_NAME = Bytes.toBytes("family2");
  private static final byte[] CHARS_QUAL_NAME = Bytes.toBytes("charsQual");

  /**
   * Compares the Idx region performance with the HRegion performance.
   *
   * @throws java.io.IOException in case of an IO error
   */
  public void testIdxRegionPerformance() throws IOException {
    IdxColumnDescriptor family1 = new IdxColumnDescriptor(FAMILY_1_NAME);
    family1.addIndexDescriptor(new IdxIndexDescriptor(INT_QUAL_NAME,
      IdxQualifierType.INT));
    family1.addIndexDescriptor(new IdxIndexDescriptor(BYTES_QUAL_NAME,
      IdxQualifierType.BYTE_ARRAY));

    IdxColumnDescriptor family2 = new IdxColumnDescriptor(FAMILY_2_NAME);
    family2.addIndexDescriptor(new IdxIndexDescriptor(CHARS_QUAL_NAME,
      IdxQualifierType.CHAR_ARRAY));

    HTableDescriptor htableDescriptor
      = new HTableDescriptor("testIdxRegionPerformance");
    htableDescriptor.addFamily(family1);
    htableDescriptor.addFamily(family2);
    HRegionInfo info = new HRegionInfo(htableDescriptor, null, null, false);
    Path path = new Path(DIR + htableDescriptor.getNameAsString());
    IdxRegion region = TestIdxRegion.createIdxRegion(info, path, conf);

    int numberOfRows = 10000;

    Random random = new Random(2112L);
    for (int row = 0; row < numberOfRows; row++) {
      Put put = new Put(Bytes.toBytes(random.nextLong()));
      put.add(FAMILY_1_NAME, INT_QUAL_NAME, Bytes.toBytes(row));
      final String str = String.format("%010d", row % 1000);
      put.add(FAMILY_1_NAME, BYTES_QUAL_NAME, str.getBytes());
      put.add(FAMILY_2_NAME, CHARS_QUAL_NAME, Bytes.toBytes(str.toCharArray()));
      region.put(put);
    }

    region.flushcache();

    final byte[] intValue = Bytes.toBytes(numberOfRows - numberOfRows / 5);
    final byte[] charsValue =
      Bytes.toBytes(String.format("%010d", 50).toCharArray());
    final byte[] bytesValue = String.format("%010d", 990).getBytes();

    IdxScan scan = new IdxScan();
    scan.setExpression(Expression.or(
      Expression.and(
        Expression.comparison(FAMILY_1_NAME, INT_QUAL_NAME,
          Comparison.Operator.GTE, intValue),
        Expression.comparison(FAMILY_2_NAME, CHARS_QUAL_NAME,
          Comparison.Operator.LT, charsValue)),
      Expression.comparison(FAMILY_1_NAME, BYTES_QUAL_NAME,
        Comparison.Operator.GTE, bytesValue)));

    scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ONE,
      Arrays.<Filter>asList(
        new FilterList(FilterList.Operator.MUST_PASS_ALL,
          Arrays.<Filter>asList(
            new SingleColumnValueFilter(FAMILY_1_NAME, INT_QUAL_NAME,
              CompareFilter.CompareOp.GREATER_OR_EQUAL, intValue),
            new SingleColumnValueFilter(FAMILY_2_NAME, CHARS_QUAL_NAME,
              CompareFilter.CompareOp.LESS, charsValue))),
        new SingleColumnValueFilter(FAMILY_1_NAME, BYTES_QUAL_NAME,
          CompareFilter.CompareOp.GREATER_OR_EQUAL, bytesValue)
      )));

    // scan for two percent of the region
    int expectedNumberOfResults = numberOfRows / 50;

    long start = System.currentTimeMillis();
    InternalScanner scanner = region.getScanner(scan);
    List<KeyValue> results = new ArrayList<KeyValue>(expectedNumberOfResults);
    int actualResults = 0;
    while (scanner.next(results)) {
      assertEquals(3, results.size());
      results.clear();
      actualResults++;
    }
    System.out.println("Total (millis) for scanning " +
      "for 2% of the region using indexed scan: " +
      (System.currentTimeMillis() - start));
    assertEquals(expectedNumberOfResults, actualResults);
  }


}
