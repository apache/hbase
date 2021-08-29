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
package org.apache.hadoop.hbase.filter;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.testclassification.FilterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ FilterTests.class, MediumTests.class }) public class TestSeekHints {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static String cf = "f";
  private static String cq = "q";
  private static String table = "t";
  private static Table ht;

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSeekHints.class);

  @Rule public TestName name = new TestName();

  @BeforeClass public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.client.scanner.caching", 1000);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
      ConstantSizeRegionSplitPolicy.class.getName());
    // set no splits
    conf.setLong(HConstants.HREGION_MAX_FILESIZE, (1024L) * 1024 * 1024 * 10);

    TEST_UTIL.startMiniCluster();

    // load the mini cluster with a single table with 20 rows, with rowkeys of a single byte, 0-19.
    ht = TEST_UTIL.createTable(TableName.valueOf(table), Bytes.toBytes(cf),
      Integer.MAX_VALUE);
    for (byte b = 0; b < 20; b++) {
      Put put = new Put(new byte[] { b }).addColumn(Bytes.toBytes(cf), Bytes.toBytes(cq),
        Bytes.toBytes("value"));
      ht.put(put);
    }
    TEST_UTIL.flush();
  }

  @AfterClass public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNormalScanForwardsSeek() throws IOException {

    KeepAllButSeekFilter filter = new KeepAllButSeekFilter((byte) 10, (byte) 15);

    Scan scan = new Scan();
    scan.setFilter(filter);

    ResultScanner scanner = ht.getScanner(scan);
    List<byte[]> actualRowsList = new ArrayList<>();
    for (Result result : scanner) {
      actualRowsList.add(result.getRow());
    }
    byte[][] actualRows = actualRowsList.toArray(new byte[][] {});

    List<byte[]> expectedRowsList = new ArrayList<>();
    // Start counting up from 0, as that's the first of our test rows
    for (byte i = 0; i < 10; i++) {
      expectedRowsList.add(new byte[] { i });
    }
    // Skip rows starting from 10 and ending before 15, as the filter should remove them
    for (byte i = 15; i < 20; i++) {
      expectedRowsList.add(new byte[] { i });
    }
    byte[][] expectedRows = expectedRowsList.toArray(new byte[][] {});

    assertArrayEquals(expectedRows, actualRows);
  }

  @Test
  public void testReversedScanBackwardsSeek() throws IOException {

    KeepAllButSeekFilter filter = new KeepAllButSeekFilter((byte) 10, (byte) 5);

    Scan scan = new Scan();
    scan.setFilter(filter);
    scan.setReversed(true);

    ResultScanner scanner = ht.getScanner(scan);
    List<byte[]> actualRowsList = new ArrayList<>();
    for (Result result : scanner) {
      actualRowsList.add(result.getRow());
    }
    byte[][] actualRows = actualRowsList.toArray(new byte[][] {});

    List<byte[]> expectedRowsList = new ArrayList<>();
    // Start counting down from 19, as that's the last of our test rows
    for (byte i = 19; i > 10; i--) {
      expectedRowsList.add(new byte[] { i });
    }
    // Skip rows starting from 10 and ending before 5, as the filter should remove them
    for (byte i = 5; i >= 0; i--) {
      expectedRowsList.add(new byte[] { i });
    }
    byte[][] expectedRows = expectedRowsList.toArray(new byte[][] {});

    assertArrayEquals(expectedRows, actualRows);
  }

  public static class KeepAllButSeekFilter extends FilterBase {

    private byte seekStartRow;
    private byte seekTargetRow;

    public KeepAllButSeekFilter(byte seekStartRow, byte seekTargetRow) {
      this.seekStartRow = seekStartRow;
      this.seekTargetRow = seekTargetRow;
    }

    /* We return SEEK_NEXT_USING_HINT when we hit the specified row, but we return INCLUDE for all
     * other rows. This will wind up including the rows between our "seek" row and our "hint" row
     * only if we don't seek past them.
     */
    @Override public ReturnCode filterCell(final Cell c) throws IOException {
      byte rowKeyPrefix = CellUtil.cloneRow(c)[0];
      if (rowKeyPrefix == seekStartRow) {
        return ReturnCode.SEEK_NEXT_USING_HINT;
      }
      return ReturnCode.INCLUDE;
    }

    @Override public Cell getNextCellHint(Cell currentCell) {
      return PrivateCellUtil.createFirstOnRow(new byte[] { seekTargetRow });
    }

    @Override public byte[] toByteArray() {
      return new byte[] { seekStartRow, seekTargetRow };
    }

    public static KeepAllButSeekFilter parseFrom(final byte[] pbBytes)
      throws DeserializationException {

      return new KeepAllButSeekFilter(pbBytes[0], pbBytes[1]);
    }
  }

}
