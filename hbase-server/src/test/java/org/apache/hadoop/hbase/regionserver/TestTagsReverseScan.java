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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(RegionServerTests.TAG)
@Tag(LargeTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: DataBlockEncoding = {0}")
public class TestTagsReverseScan {

  private static final Logger LOG = LoggerFactory.getLogger(TestTagsReverseScan.class);

  private final static HBaseTestingUtil UTIL = new HBaseTestingUtil();

  private DataBlockEncoding encoding;

  public TestTagsReverseScan(DataBlockEncoding encoding) {
    this.encoding = encoding;
  }

  public static Stream<Arguments> parameters() {
    return Arrays.stream(DataBlockEncoding.values()).map(Arguments::of);
  }

  @BeforeAll
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniCluster(2);
  }

  @AfterAll
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test that we can do reverse scans when writing tags and using DataBlockEncoding. Fails with an
   * exception for PREFIX, DIFF, and FAST_DIFF prior to HBASE-27580
   */
  @TestTemplate
  public void testReverseScanWithDBE(TestInfo testInfo) throws IOException {
    byte[] family = Bytes.toBytes("0");

    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      testReverseScanWithDBE(testInfo, connection, family, HConstants.DEFAULT_BLOCKSIZE, 10);
    }
  }

  /**
   * Test that we can do reverse scans when writing tags and using DataBlockEncoding. Fails with an
   * exception for PREFIX, DIFF, and FAST_DIFF
   */
  @TestTemplate
  public void testReverseScanWithDBEWhenCurrentBlockUpdates(TestInfo testInfo) throws IOException {
    byte[] family = Bytes.toBytes("0");

    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      testReverseScanWithDBE(testInfo, connection, family, 1024, 30000);
    }
  }

  private void testReverseScanWithDBE(TestInfo testInfo, Connection conn, byte[] family,
    int blockSize, int maxRows) throws IOException {
    LOG.info("Running test with DBE={}", encoding);
    TableName tableName =
      TableName.valueOf(testInfo.getTestMethod().get().getName() + "-" + encoding);
    UTIL.createTable(
      TableDescriptorBuilder.newBuilder(tableName).setColumnFamily(ColumnFamilyDescriptorBuilder
        .newBuilder(family).setDataBlockEncoding(encoding).setBlocksize(blockSize).build()).build(),
      null);

    Table table = conn.getTable(tableName);

    byte[] val1 = new byte[10];
    byte[] val2 = new byte[10];
    Bytes.random(val1);
    Bytes.random(val2);

    for (int i = 0; i < maxRows; i++) {
      table.put(new Put(Bytes.toBytes(i)).addColumn(family, Bytes.toBytes(1), val1)
        .addColumn(family, Bytes.toBytes(2), val2).setTTL(600_000));
    }

    UTIL.flush(table.getName());

    Scan scan = new Scan();
    scan.setReversed(true);

    try (ResultScanner scanner = table.getScanner(scan)) {
      for (int i = maxRows - 1; i >= 0; i--) {
        Result row = scanner.next();
        assertEquals(2, row.size());

        Cell cell1 = row.getColumnLatestCell(family, Bytes.toBytes(1));
        assertTrue(CellUtil.matchingRows(cell1, Bytes.toBytes(i)));
        assertTrue(CellUtil.matchingValue(cell1, val1));

        Cell cell2 = row.getColumnLatestCell(family, Bytes.toBytes(2));
        assertTrue(CellUtil.matchingRows(cell2, Bytes.toBytes(i)));
        assertTrue(CellUtil.matchingValue(cell2, val2));
      }
    }
  }
}
