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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CategoryBasedTimeout;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * A base class for a test Map/Reduce job over HBase tables. The map/reduce process we're testing
 * on our tables is simple - take every row in the table, reverse the value of a particular cell,
 * and write it back to the table. Implements common components between mapred and mapreduce
 * implementations.
 */
public abstract class TestTableMapReduceBase {
  @Rule public final TestRule timeout = CategoryBasedTimeout.builder().
      withTimeout(this.getClass()).withLookingForStuckThread(true).build();
  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  protected static final TableName MULTI_REGION_TABLE_NAME = TableName.valueOf("mrtest");
  protected static final TableName TABLE_FOR_NEGATIVE_TESTS = TableName.valueOf("testfailuretable");
  protected static final byte[] INPUT_FAMILY = Bytes.toBytes("contents");
  protected static final byte[] OUTPUT_FAMILY = Bytes.toBytes("text");

  protected static final byte[][] columns = new byte[][] {
    INPUT_FAMILY,
    OUTPUT_FAMILY
  };

  /**
   * Retrieve my logger instance.
   */
  protected abstract Log getLog();

  /**
   * Handles API-specifics for setting up and executing the job.
   */
  protected abstract void runTestOnTable(Table table) throws IOException;

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    Table table =
        UTIL.createMultiRegionTable(MULTI_REGION_TABLE_NAME, new byte[][] { INPUT_FAMILY,
            OUTPUT_FAMILY });
    UTIL.loadTable(table, INPUT_FAMILY, false);
    UTIL.createTable(TABLE_FOR_NEGATIVE_TESTS, new byte[][] { INPUT_FAMILY, OUTPUT_FAMILY });
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.deleteTable(TABLE_FOR_NEGATIVE_TESTS);
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test a map/reduce against a multi-region table
   * @throws IOException
   */
  @Test
  public void testMultiRegionTable() throws IOException {
    runTestOnTable(UTIL.getConnection().getTable(MULTI_REGION_TABLE_NAME));
  }

  @Test
  public void testCombiner() throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    // force use of combiner for testing purposes
    conf.setInt("mapreduce.map.combine.minspills", 1);
    runTestOnTable(UTIL.getConnection().getTable(MULTI_REGION_TABLE_NAME));
  }

  /**
   * Implements mapper logic for use across APIs.
   */
  protected static Put map(ImmutableBytesWritable key, Result value) throws IOException {
    if (value.size() != 1) {
      throw new IOException("There should only be one input column");
    }
    Map<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>>
      cf = value.getMap();
    if(!cf.containsKey(INPUT_FAMILY)) {
      throw new IOException("Wrong input columns. Missing: '" +
        Bytes.toString(INPUT_FAMILY) + "'.");
    }

    // Get the original value and reverse it

    String originalValue = Bytes.toString(value.getValue(INPUT_FAMILY, INPUT_FAMILY));
    StringBuilder newValue = new StringBuilder(originalValue);
    newValue.reverse();

    // Now set the value to be collected

    Put outval = new Put(key.get());
    outval.addColumn(OUTPUT_FAMILY, null, Bytes.toBytes(newValue.toString()));
    return outval;
  }

  protected void verify(TableName tableName) throws IOException {
    Table table = UTIL.getConnection().getTable(tableName);
    boolean verified = false;
    long pause = UTIL.getConfiguration().getLong("hbase.client.pause", 5 * 1000);
    int numRetries = UTIL.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    for (int i = 0; i < numRetries; i++) {
      try {
        getLog().info("Verification attempt #" + i);
        verifyAttempt(table);
        verified = true;
        break;
      } catch (NullPointerException e) {
        // If here, a cell was empty. Presume its because updates came in
        // after the scanner had been opened. Wait a while and retry.
        getLog().debug("Verification attempt failed: " + e.getMessage());
      }
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        // continue
      }
    }
    assertTrue(verified);
  }

  /**
   * Looks at every value of the mapreduce output and verifies that indeed
   * the values have been reversed.
   * @param table Table to scan.
   * @throws IOException
   * @throws NullPointerException if we failed to find a cell value
   */
  private void verifyAttempt(final Table table) throws IOException, NullPointerException {
    Scan scan = new Scan();
    TableInputFormat.addColumns(scan, columns);
    ResultScanner scanner = table.getScanner(scan);
    try {
      Iterator<Result> itr = scanner.iterator();
      assertTrue(itr.hasNext());
      while(itr.hasNext()) {
        Result r = itr.next();
        if (getLog().isDebugEnabled()) {
          if (r.size() > 2 ) {
            throw new IOException("Too many results, expected 2 got " +
              r.size());
          }
        }
        byte[] firstValue = null;
        byte[] secondValue = null;
        int count = 0;
         for(Cell kv : r.listCells()) {
          if (count == 0) {
            firstValue = CellUtil.cloneValue(kv);
          }
          if (count == 1) {
            secondValue = CellUtil.cloneValue(kv);
          }
          count++;
          if (count == 2) {
            break;
          }
        }


        if (firstValue == null) {
          throw new NullPointerException(Bytes.toString(r.getRow()) +
            ": first value is null");
        }
        String first = Bytes.toString(firstValue);

        if (secondValue == null) {
          throw new NullPointerException(Bytes.toString(r.getRow()) +
            ": second value is null");
        }
        byte[] secondReversed = new byte[secondValue.length];
        for (int i = 0, j = secondValue.length - 1; j >= 0; j--, i++) {
          secondReversed[i] = secondValue[j];
        }
        String second = Bytes.toString(secondReversed);

        if (first.compareTo(second) != 0) {
          if (getLog().isDebugEnabled()) {
            getLog().debug("second key is not the reverse of first. row=" +
                Bytes.toStringBinary(r.getRow()) + ", first value=" + first +
                ", second value=" + second);
          }
          fail();
        }
      }
    } finally {
      scanner.close();
    }
  }
}
