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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Run Increment tests that use the HBase clients; {@link HTable}.
 *
 * Test is parameterized to run the slow and fast increment code paths. If fast, in the @before, we
 * do a rolling restart of the single regionserver so that it can pick up the go fast configuration.
 * Doing it this way should be faster than starting/stopping a cluster per test.
 *
 * Test takes a long time because spin up a cluster between each run -- ugh.
 */
@Category(LargeTests.class)
public class TestIncrementsFromClientSide {
  final Log LOG = LogFactory.getLog(getClass());
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  // This test depends on there being only one slave running at at a time. See the @Before
  // method where we do rolling restart.
  protected static int SLAVES = 1;
  @Rule public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MultiRowMutationEndpoint.class.getName());
    conf.setBoolean("hbase.table.sanity.checks", true); // enable for below tests
    // We need more than one region server in this test
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Test increment result when there are duplicate rpc request.
   */
  @Test
  public void testDuplicateIncrement() throws Exception {
    HTableDescriptor hdt = TEST_UTIL.createTableDescriptor("HCM-testDuplicateIncrement");
    Map<String, String> kvs = new HashMap<String, String>();
    kvs.put(HConnectionTestingUtility.SleepAtFirstRpcCall.SLEEP_TIME_CONF_KEY, "2000");
    hdt.addCoprocessor(HConnectionTestingUtility.SleepAtFirstRpcCall.class.getName(), null, 1, kvs);
    TEST_UTIL.createTable(hdt, new byte[][] { ROW }).close();

    Configuration c = new Configuration(TEST_UTIL.getConfiguration());
    c.setInt(HConstants.HBASE_CLIENT_PAUSE, 50);
    // Client will retry beacuse rpc timeout is small than the sleep time of first rpc call
    c.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 1500);

    Connection connection = ConnectionFactory.createConnection(c);
    Table t = connection.getTable(TableName.valueOf("HCM-testDuplicateIncrement"));
    if (t instanceof HTable) {
      HTable table = (HTable) t;
      table.setOperationTimeout(3 * 1000);

      try {
        Increment inc = new Increment(ROW);
        inc.addColumn(TEST_UTIL.fam1, QUALIFIER, 1);
        Result result = table.increment(inc);

        Cell [] cells = result.rawCells();
        assertEquals(1, cells.length);
        assertIncrementKey(cells[0], ROW, TEST_UTIL.fam1, QUALIFIER, 1);

        // Verify expected result
        Result readResult = table.get(new Get(ROW));
        cells = readResult.rawCells();
        assertEquals(1, cells.length);
        assertIncrementKey(cells[0], ROW, TEST_UTIL.fam1, QUALIFIER, 1);
      } finally {
        table.close();
        connection.close();
      }
    }
  }

  @Test
  public void testIncrementWithDeletes() throws Exception {
    LOG.info("Starting " + this.name.getMethodName());
    final TableName TABLENAME =
        TableName.valueOf(filterStringSoTableNameSafe(this.name.getMethodName()));
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);
    final byte[] COLUMN = Bytes.toBytes("column");

    ht.incrementColumnValue(ROW, FAMILY, COLUMN, 5);
    TEST_UTIL.flush(TABLENAME);

    Delete del = new Delete(ROW);
    ht.delete(del);

    ht.incrementColumnValue(ROW, FAMILY, COLUMN, 5);

    Get get = new Get(ROW);
    Result r = ht.get(get);
    assertEquals(1, r.size());
    assertEquals(5, Bytes.toLong(r.getValue(FAMILY, COLUMN)));
  }

  @Test
  public void testIncrementingInvalidValue() throws Exception {
    LOG.info("Starting " + this.name.getMethodName());
    final TableName TABLENAME =
        TableName.valueOf(filterStringSoTableNameSafe(this.name.getMethodName()));
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);
    final byte[] COLUMN = Bytes.toBytes("column");
    Put p = new Put(ROW);
    // write an integer here (not a Long)
    p.addColumn(FAMILY, COLUMN, Bytes.toBytes(5));
    ht.put(p);
    try {
      ht.incrementColumnValue(ROW, FAMILY, COLUMN, 5);
      fail("Should have thrown DoNotRetryIOException");
    } catch (DoNotRetryIOException iox) {
      // success
    }
    Increment inc = new Increment(ROW);
    inc.addColumn(FAMILY, COLUMN, 5);
    try {
      ht.increment(inc);
      fail("Should have thrown DoNotRetryIOException");
    } catch (DoNotRetryIOException iox) {
      // success
    }
  }

  @Test
  public void testBatchIncrementsWithReturnResultFalse() throws Exception {
    LOG.info("Starting testBatchIncrementsWithReturnResultFalse");
    final TableName TABLENAME = TableName.valueOf("testBatchAppend");
    Table table = TEST_UTIL.createTable(TABLENAME, FAMILY);
    Increment inc1 = new Increment(Bytes.toBytes("row2"));
    inc1.setReturnResults(false);
    inc1.addColumn(FAMILY, Bytes.toBytes("f1"), 1);
    Increment inc2 = new Increment(Bytes.toBytes("row2"));
    inc2.setReturnResults(false);
    inc2.addColumn(FAMILY, Bytes.toBytes("f1"), 1);
    List<Increment> incs = new ArrayList<>();
    incs.add(inc1);
    incs.add(inc2);
    Object[] results = new Object[2];
    table.batch(incs, results);
    assertTrue(results.length == 2);
    for(Object r : results) {
      Result result = (Result)r;
      assertTrue(result.isEmpty());
    }
    table.close();
  }

  @Test
  public void testIncrementInvalidArguments() throws Exception {
    LOG.info("Starting " + this.name.getMethodName());
    final TableName TABLENAME =
        TableName.valueOf(filterStringSoTableNameSafe(this.name.getMethodName()));
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);
    final byte[] COLUMN = Bytes.toBytes("column");
    try {
      // try null row
      ht.incrementColumnValue(null, FAMILY, COLUMN, 5);
      fail("Should have thrown IOException");
    } catch (IOException iox) {
      // success
    }
    try {
      // try null family
      ht.incrementColumnValue(ROW, null, COLUMN, 5);
      fail("Should have thrown IOException");
    } catch (IOException iox) {
      // success
    }
    try {
      // try null qualifier
      ht.incrementColumnValue(ROW, FAMILY, null, 5);
      fail("Should have thrown IOException");
    } catch (IOException iox) {
      // success
    }
    // try null row
    try {
      Increment incNoRow = new Increment((byte [])null);
      incNoRow.addColumn(FAMILY, COLUMN, 5);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iax) {
      // success
    } catch (NullPointerException npe) {
      // success
    }
    // try null family
    try {
      Increment incNoFamily = new Increment(ROW);
      incNoFamily.addColumn(null, COLUMN, 5);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iax) {
      // success
    }
    // try null qualifier
    try {
      Increment incNoQualifier = new Increment(ROW);
      incNoQualifier.addColumn(FAMILY, null, 5);
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException iax) {
      // success
    }
  }

  @Test
  public void testIncrementOutOfOrder() throws Exception {
    LOG.info("Starting " + this.name.getMethodName());
    final TableName TABLENAME =
        TableName.valueOf(filterStringSoTableNameSafe(this.name.getMethodName()));
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);

    byte [][] QUALIFIERS = new byte [][] {
      Bytes.toBytes("B"), Bytes.toBytes("A"), Bytes.toBytes("C")
    };

    Increment inc = new Increment(ROW);
    for (int i=0; i<QUALIFIERS.length; i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
    }
    ht.increment(inc);

    // Verify expected results
    Get get = new Get(ROW);
    Result r = ht.get(get);
    Cell [] kvs = r.rawCells();
    assertEquals(3, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[1], 1);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[0], 1);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 1);

    // Now try multiple columns again
    inc = new Increment(ROW);
    for (int i=0; i<QUALIFIERS.length; i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
    }
    ht.increment(inc);

    // Verify
    r = ht.get(get);
    kvs = r.rawCells();
    assertEquals(3, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[1], 2);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[0], 2);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 2);
  }

  @Test
  public void testIncrementOnSameColumn() throws Exception {
    LOG.info("Starting " + this.name.getMethodName());
    final byte[] TABLENAME = Bytes.toBytes(filterStringSoTableNameSafe(this.name.getMethodName()));
    Table ht = TEST_UTIL.createTable(TableName.valueOf(TABLENAME), FAMILY);

    byte[][] QUALIFIERS =
        new byte[][] { Bytes.toBytes("A"), Bytes.toBytes("B"), Bytes.toBytes("C") };

    Increment inc = new Increment(ROW);
    for (int i = 0; i < QUALIFIERS.length; i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
    }
    ht.increment(inc);

    // Verify expected results
    Get get = new Get(ROW);
    Result r = ht.get(get);
    Cell[] kvs = r.rawCells();
    assertEquals(3, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[0], 1);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[1], 1);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 1);

    // Now try multiple columns again
    inc = new Increment(ROW);
    for (int i = 0; i < QUALIFIERS.length; i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
      inc.addColumn(FAMILY, QUALIFIERS[i], 1);
    }
    ht.increment(inc);

    // Verify
    r = ht.get(get);
    kvs = r.rawCells();
    assertEquals(3, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[0], 2);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[1], 2);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 2);

    ht.close();
  }

  @Test
  public void testIncrementIncrZeroAtFirst() throws Exception {
    LOG.info("Starting " + this.name.getMethodName());
    final TableName TABLENAME =
            TableName.valueOf(filterStringSoTableNameSafe(this.name.getMethodName()));
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);

    byte[] col1 = Bytes.toBytes("col1");
    byte[] col2 = Bytes.toBytes("col2");
    byte[] col3 = Bytes.toBytes("col3");

    // Now increment zero at first time incr
    Increment inc = new Increment(ROW);
    inc.addColumn(FAMILY, col1, 0);
    ht.increment(inc);

    // Verify expected results
    Get get = new Get(ROW);
    Result r = ht.get(get);
    Cell [] kvs = r.rawCells();
    assertEquals(1, kvs.length);
    assertNotNull(kvs[0]);
    assertIncrementKey(kvs[0], ROW, FAMILY, col1, 0);

    // Now try multiple columns by different amounts
    inc = new Increment(ROW);
    inc.addColumn(FAMILY, col1, 1);
    inc.addColumn(FAMILY, col2, 0);
    inc.addColumn(FAMILY, col3, 2);
    ht.increment(inc);
    // Verify
    get = new Get(ROW);
    r = ht.get(get);
    kvs = r.rawCells();
    assertEquals(3, kvs.length);
    assertNotNull(kvs[0]);
    assertNotNull(kvs[1]);
    assertNotNull(kvs[2]);
    assertIncrementKey(kvs[0], ROW, FAMILY, col1, 1);
    assertIncrementKey(kvs[1], ROW, FAMILY, col2, 0);
    assertIncrementKey(kvs[2], ROW, FAMILY, col3, 2);
  }

  @Test
  public void testIncrement() throws Exception {
    LOG.info("Starting " + this.name.getMethodName());
    final TableName TABLENAME =
        TableName.valueOf(filterStringSoTableNameSafe(this.name.getMethodName()));
    Table ht = TEST_UTIL.createTable(TABLENAME, FAMILY);

    byte [][] ROWS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"),
        Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
        Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i")
    };
    byte [][] QUALIFIERS = new byte [][] {
        Bytes.toBytes("a"), Bytes.toBytes("b"), Bytes.toBytes("c"),
        Bytes.toBytes("d"), Bytes.toBytes("e"), Bytes.toBytes("f"),
        Bytes.toBytes("g"), Bytes.toBytes("h"), Bytes.toBytes("i")
    };

    // Do some simple single-column increments

    // First with old API
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[0], 1);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[1], 2);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[2], 3);
    ht.incrementColumnValue(ROW, FAMILY, QUALIFIERS[3], 4);

    // Now increment things incremented with old and do some new
    Increment inc = new Increment(ROW);
    inc.addColumn(FAMILY, QUALIFIERS[1], 1);
    inc.addColumn(FAMILY, QUALIFIERS[3], 1);
    inc.addColumn(FAMILY, QUALIFIERS[4], 1);
    ht.increment(inc);

    // Verify expected results
    Get get = new Get(ROW);
    Result r = ht.get(get);
    Cell [] kvs = r.rawCells();
    assertEquals(5, kvs.length);
    assertIncrementKey(kvs[0], ROW, FAMILY, QUALIFIERS[0], 1);
    assertIncrementKey(kvs[1], ROW, FAMILY, QUALIFIERS[1], 3);
    assertIncrementKey(kvs[2], ROW, FAMILY, QUALIFIERS[2], 3);
    assertIncrementKey(kvs[3], ROW, FAMILY, QUALIFIERS[3], 5);
    assertIncrementKey(kvs[4], ROW, FAMILY, QUALIFIERS[4], 1);

    // Now try multiple columns by different amounts
    inc = new Increment(ROWS[0]);
    for (int i=0;i<QUALIFIERS.length;i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], i+1);
    }
    ht.increment(inc);
    // Verify
    get = new Get(ROWS[0]);
    r = ht.get(get);
    kvs = r.rawCells();
    assertEquals(QUALIFIERS.length, kvs.length);
    for (int i=0;i<QUALIFIERS.length;i++) {
      assertIncrementKey(kvs[i], ROWS[0], FAMILY, QUALIFIERS[i], i+1);
    }

    // Re-increment them
    inc = new Increment(ROWS[0]);
    for (int i=0;i<QUALIFIERS.length;i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], i+1);
    }
    ht.increment(inc);
    // Verify
    r = ht.get(get);
    kvs = r.rawCells();
    assertEquals(QUALIFIERS.length, kvs.length);
    for (int i=0;i<QUALIFIERS.length;i++) {
      assertIncrementKey(kvs[i], ROWS[0], FAMILY, QUALIFIERS[i], 2*(i+1));
    }

    // Verify that an Increment of an amount of zero, returns current count; i.e. same as for above
    // test, that is: 2 * (i + 1).
    inc = new Increment(ROWS[0]);
    for (int i = 0; i < QUALIFIERS.length; i++) {
      inc.addColumn(FAMILY, QUALIFIERS[i], 0);
    }
    ht.increment(inc);
    r = ht.get(get);
    kvs = r.rawCells();
    assertEquals(QUALIFIERS.length, kvs.length);
    for (int i = 0; i < QUALIFIERS.length; i++) {
      assertIncrementKey(kvs[i], ROWS[0], FAMILY, QUALIFIERS[i], 2*(i+1));
    }
  }


  /**
   * Call over to the adjacent class's method of same name.
   */
  static void assertIncrementKey(Cell key, byte [] row, byte [] family,
      byte [] qualifier, long value) throws Exception {
    TestFromClientSide.assertIncrementKey(key, row, family, qualifier, value);
  }

  public static String filterStringSoTableNameSafe(final String str) {
    return str.replaceAll("\\[fast\\=(.*)\\]", ".FAST.is.$1");
  }
}
