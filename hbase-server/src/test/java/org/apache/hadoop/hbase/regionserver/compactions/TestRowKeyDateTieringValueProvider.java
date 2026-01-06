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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRowKeyDateTieringValueProvider {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRowKeyDateTieringValueProvider.class);

  private RowKeyDateTieringValueProvider provider;
  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    provider = new RowKeyDateTieringValueProvider();
  }

  @After
  public void tearDown() {
    provider = null;
    conf = null;
  }

  @Test
  public void testInitWithValidConfig() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "(\\d{4}-\\d{2}-\\d{2})");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyy-MM-dd");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_EXTRACT_GROUP, "1");
    provider.init(conf);
    assertEquals(provider.getRowKeyPattern().pattern(), "(\\d{4}-\\d{2}-\\d{2})");
    assertEquals(provider.getDateFormat().toPattern(), "yyyy-MM-dd");
    assertEquals(Integer.valueOf(1), provider.getRowKeyRegexExtractGroup());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitWithMissingRegexPattern() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyy-MM-dd");
    provider.init(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitWithMissingDateFormat() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "(\\d{4}-\\d{2}-\\d{2})");
    provider.init(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitWithInvalidDateFormat() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "(\\d{4}-\\d{2}-\\d{2})");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "invalid-format");
    provider.init(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitWithInvalidExtractGroup() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "(\\d{4}-\\d{2}-\\d{2})");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyy-MM-dd");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_EXTRACT_GROUP, "-1");
    provider.init(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInitWithExtractGroupExceedingPatternGroups() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "(\\d{4}-\\d{2}-\\d{2})");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyy-MM-dd");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_EXTRACT_GROUP, "2"); // Only 1 group in
                                                                              // pattern
    provider.init(conf);
  }

  @Test
  public void testGetTieringValue() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "_(\\d{4}-\\d{2}-\\d{2})_");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyy-MM-dd");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_EXTRACT_GROUP, "1");
    provider.init(conf);

    String dateStr = "2023-10-15";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    long expectedTimestamp = sdf.parse(dateStr).getTime();

    String rowKeyStr = "order_" + dateStr + "_details";
    byte[] rowKey = Bytes.toBytes(rowKeyStr);
    ExtendedCell cell = PrivateCellUtil.createFirstOnRow(rowKey);
    long actualTimestamp = provider.getTieringValue(cell);

    assertEquals(expectedTimestamp, actualTimestamp);
  }

  @Test
  public void testGetTieringValueWithNonMatchingRowKey() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "_(\\d{4}-\\d{2}-\\d{2})_");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyy-MM-dd");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_EXTRACT_GROUP, "1");
    provider.init(conf);

    String rowKeyStr = "order_details_no_date";
    byte[] rowKey = Bytes.toBytes(rowKeyStr);
    ExtendedCell cell = PrivateCellUtil.createFirstOnRow(rowKey);
    long actualTimestamp = provider.getTieringValue(cell);

    assertEquals(Long.MAX_VALUE, actualTimestamp);
  }

  @Test
  public void testGetTieringValueWithInvalidDateInRowKey() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "_(\\d{14})_");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyyMMddHHmmss");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_EXTRACT_GROUP, "1");
    provider.init(conf);

    // Invalid Month (14)
    String rowKeyStr = "order_20151412124556_date";
    byte[] rowKey = Bytes.toBytes(rowKeyStr);
    ExtendedCell cell = PrivateCellUtil.createFirstOnRow(rowKey);
    long actualTimestamp = provider.getTieringValue(cell);

    assertEquals(Long.MAX_VALUE, actualTimestamp);
  }

  @Test
  public void testGetTieringValueWithNonUTF8RowKey() throws Exception {
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_PATTERN, "_(\\d{8})_");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_DATE_FORMAT, "yyyyMMdd");
    conf.set(RowKeyDateTieringValueProvider.ROWKEY_REGEX_EXTRACT_GROUP, "1");
    provider.init(conf);

    // Row key with non-UTF-8 bytes (invalid UTF-8 sequence)
    byte[] rowKey =
      new byte[] { 0x6F, 0x72, 0x64, 0x65, 0x72, 0x5F, (byte) 0xFF, (byte) 0xFE, 0x5F };
    ExtendedCell cell = PrivateCellUtil.createFirstOnRow(rowKey);
    long timestamp = provider.getTieringValue(cell);

    assertEquals(Long.MAX_VALUE, timestamp);
  }

  @Test(expected = IllegalStateException.class)
  public void testGetTieringValueWithoutInitialization() {
    String rowKeyStr = "order_9999999999999999_date";
    byte[] rowKey = Bytes.toBytes(rowKeyStr);
    ExtendedCell cell = PrivateCellUtil.createFirstOnRow(rowKey);
    provider.getTieringValue(cell);
  }
}
