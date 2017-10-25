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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.StoreScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class AbstractTestScanCursor {

  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * Table configuration
   */
  protected static TableName TABLE_NAME = TableName.valueOf("TestScanCursor");

  protected static int NUM_ROWS = 5;
  protected static byte[] ROW = Bytes.toBytes("testRow");
  protected static byte[][] ROWS = HTestConst.makeNAscii(ROW, NUM_ROWS);

  protected static int NUM_FAMILIES = 2;
  protected static byte[] FAMILY = Bytes.toBytes("testFamily");
  protected static byte[][] FAMILIES = HTestConst.makeNAscii(FAMILY, NUM_FAMILIES);

  protected static int NUM_QUALIFIERS = 2;
  protected static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  protected static byte[][] QUALIFIERS = HTestConst.makeNAscii(QUALIFIER, NUM_QUALIFIERS);

  protected static int VALUE_SIZE = 10;
  protected static byte[] VALUE = Bytes.createMaxByteArray(VALUE_SIZE);

  protected static final int TIMEOUT = 4000;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();

    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, TIMEOUT);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, TIMEOUT);

    // Check the timeout condition after every cell
    conf.setLong(StoreScanner.HBASE_CELLS_SCANNED_PER_HEARTBEAT_CHECK, 1);
    TEST_UTIL.startMiniCluster(1);

    createTestTable(TABLE_NAME, ROWS, FAMILIES, QUALIFIERS, VALUE);
  }

  private static void createTestTable(TableName name, byte[][] rows, byte[][] families,
      byte[][] qualifiers, byte[] cellValue) throws IOException {
    TEST_UTIL.createTable(name, families).put(createPuts(rows, families, qualifiers, cellValue));
  }

  private static List<Put> createPuts(byte[][] rows, byte[][] families, byte[][] qualifiers,
      byte[] value) throws IOException {
    List<Put> puts = new ArrayList<>();
    for (int row = 0; row < rows.length; row++) {
      Put put = new Put(rows[row]);
      for (int fam = 0; fam < families.length; fam++) {
        for (int qual = 0; qual < qualifiers.length; qual++) {
          KeyValue kv = new KeyValue(rows[row], families[fam], qualifiers[qual], qual, value);
          put.add(kv);
        }
      }
      puts.add(put);
    }
    return puts;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  public static final class SparseFilter extends FilterBase {

    private final boolean reversed;

    public SparseFilter(boolean reversed) {
      this.reversed = reversed;
    }

    @Override
    public ReturnCode filterCell(final Cell c) throws IOException {
      Threads.sleep(TIMEOUT / 2 + 100);
      return Bytes.equals(CellUtil.cloneRow(c), ROWS[reversed ? 0 : NUM_ROWS - 1])
          ? ReturnCode.INCLUDE
          : ReturnCode.SKIP;
    }

    @Override
    public byte[] toByteArray() throws IOException {
      return reversed ? new byte[] { 1 } : new byte[] { 0 };
    }

    public static Filter parseFrom(final byte[] pbBytes) {
      return new SparseFilter(pbBytes[0] != 0);
    }
  }

  protected Scan createScanWithSparseFilter() {
    return new Scan().setMaxResultSize(Long.MAX_VALUE).setCaching(Integer.MAX_VALUE)
        .setNeedCursorResult(true).setAllowPartialResults(true).setFilter(new SparseFilter(false));
  }

  protected Scan createReversedScanWithSparseFilter() {
    return new Scan().setMaxResultSize(Long.MAX_VALUE).setCaching(Integer.MAX_VALUE)
        .setReversed(true).setNeedCursorResult(true).setAllowPartialResults(true)
        .setFilter(new SparseFilter(true));
  }

  protected Scan createScanWithSizeLimit() {
    return new Scan().setMaxResultSize(1).setCaching(Integer.MAX_VALUE).setNeedCursorResult(true);
  }
}
