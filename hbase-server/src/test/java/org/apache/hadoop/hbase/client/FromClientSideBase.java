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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.NonRepeatedEnvironmentEdge;
import org.apache.hadoop.hbase.util.TableDescriptorChecker;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Base for TestFromClientSide* classes.
 * Has common defines and utility used by all.
 */
@Category({LargeTests.class, ClientTests.class})
@SuppressWarnings ("deprecation")
@RunWith(Parameterized.class)
class FromClientSideBase {
  private static final Logger LOG = LoggerFactory.getLogger(FromClientSideBase.class);
  static HBaseTestingUtility TEST_UTIL;
  static byte [] ROW = Bytes.toBytes("testRow");
  static byte [] FAMILY = Bytes.toBytes("testFamily");
  static final byte[] INVALID_FAMILY = Bytes.toBytes("invalidTestFamily");
  static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  static byte [] VALUE = Bytes.toBytes("testValue");
  static int SLAVES = 1;

  // To keep the child classes happy.
  FromClientSideBase() {}

  /**
   * JUnit does not provide an easy way to run a hook after each parameterized run. Without that
   * there is no easy way to restart the test cluster after each parameterized run. Annotation
   * BeforeParam does not work either because it runs before parameterization and hence does not
   * have access to the test parameters (which is weird).
   *
   * This *hack* checks if the current instance of test cluster configuration has the passed
   * parameterized configs. In such a case, we can just reuse the cluster for test and do not need
   * to initialize from scratch. While this is a hack, it saves a ton of time for the full
   * test and de-flakes it.
   */
  protected static boolean isSameParameterizedCluster(Class<?> registryImpl, int numHedgedReqs) {
    if (TEST_UTIL == null) {
      return false;
    }
    Configuration conf = TEST_UTIL.getConfiguration();
    Class<?> confClass = conf.getClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
      ZKConnectionRegistry.class);
    int hedgedReqConfig = conf.getInt(MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY,
      MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_DEFAULT);
    return confClass.getName().equals(registryImpl.getName()) && numHedgedReqs == hedgedReqConfig;
  }

  protected static final void initialize(Class<?> registryImpl, int numHedgedReqs, Class<?>... cps)
    throws Exception {
    // initialize() is called for every unit test, however we only want to reset the cluster state
    // at the end of every parameterized run.
    if (isSameParameterizedCluster(registryImpl, numHedgedReqs)) {
      return;
    }
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    // ((Log4JLogger)RpcServer.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger)RpcClient.LOG).getLogger().setLevel(Level.ALL);
    // ((Log4JLogger)ScannerCallable.LOG).getLogger().setLevel(Level.ALL);
    // make sure that we do not get the same ts twice, see HBASE-19731 for more details.
    EnvironmentEdgeManager.injectEdge(new NonRepeatedEnvironmentEdge());
    if (TEST_UTIL != null) {
      // We reached end of a parameterized run, clean up.
      TEST_UTIL.shutdownMiniCluster();
    }
    TEST_UTIL = new HBaseTestingUtility();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      Arrays.stream(cps).map(Class::getName).toArray(String[]::new));
    conf.setBoolean(TableDescriptorChecker.TABLE_SANITY_CHECKS, true); // enable for below tests
    conf.setClass(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY, registryImpl,
        ConnectionRegistry.class);
    Preconditions.checkArgument(numHedgedReqs > 0);
    conf.setInt(MasterRegistry.MASTER_REGISTRY_HEDGED_REQS_FANOUT_KEY, numHedgedReqs);
    StartMiniClusterOption.Builder builder = StartMiniClusterOption.builder();
    // Multiple masters needed only when hedged reads for master registry are enabled.
    builder.numMasters(numHedgedReqs > 1 ? 3 : 1).numRegionServers(SLAVES);
    TEST_UTIL.startMiniCluster(builder.build());
  }

  protected static void afterClass() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  protected void deleteColumns(Table ht, String value, String keyPrefix)
    throws IOException {
    ResultScanner scanner = buildScanner(keyPrefix, value, ht);
    Iterator<Result> it = scanner.iterator();
    int count = 0;
    while (it.hasNext()) {
      Result result = it.next();
      Delete delete = new Delete(result.getRow());
      delete.addColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"));
      ht.delete(delete);
      count++;
    }
    assertEquals("Did not perform correct number of deletes", 3, count);
  }

  protected int getNumberOfRows(String keyPrefix, String value, Table ht)
    throws Exception {
    ResultScanner resultScanner = buildScanner(keyPrefix, value, ht);
    Iterator<Result> scanner = resultScanner.iterator();
    int numberOfResults = 0;
    while (scanner.hasNext()) {
      Result result = scanner.next();
      System.out.println("Got back key: " + Bytes.toString(result.getRow()));
      for (Cell kv : result.rawCells()) {
        System.out.println("kv=" + kv.toString() + ", "
          + Bytes.toString(CellUtil.cloneValue(kv)));
      }
      numberOfResults++;
    }
    return numberOfResults;
  }

  protected ResultScanner buildScanner(String keyPrefix, String value, Table ht)
    throws IOException {
    // OurFilterList allFilters = new OurFilterList();
    FilterList allFilters = new FilterList(/* FilterList.Operator.MUST_PASS_ALL */);
    allFilters.addFilter(new PrefixFilter(Bytes.toBytes(keyPrefix)));
    SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes
      .toBytes("trans-tags"), Bytes.toBytes("qual2"), CompareOperator.EQUAL, Bytes
      .toBytes(value));
    filter.setFilterIfMissing(true);
    allFilters.addFilter(filter);

    // allFilters.addFilter(new
    // RowExcludingSingleColumnValueFilter(Bytes.toBytes("trans-tags"),
    // Bytes.toBytes("qual2"), CompareOp.EQUAL, Bytes.toBytes(value)));

    Scan scan = new Scan();
    scan.addFamily(Bytes.toBytes("trans-blob"));
    scan.addFamily(Bytes.toBytes("trans-type"));
    scan.addFamily(Bytes.toBytes("trans-date"));
    scan.addFamily(Bytes.toBytes("trans-tags"));
    scan.addFamily(Bytes.toBytes("trans-group"));
    scan.setFilter(allFilters);

    return ht.getScanner(scan);
  }

  protected void putRows(Table ht, int numRows, String value, String key)
    throws IOException {
    for (int i = 0; i < numRows; i++) {
      String row = key + "_" + HBaseCommonTestingUtility.getRandomUUID().toString();
      System.out.println(String.format("Saving row: %s, with value %s", row,
        value));
      Put put = new Put(Bytes.toBytes(row));
      put.setDurability(Durability.SKIP_WAL);
      put.addColumn(Bytes.toBytes("trans-blob"), null, Bytes
        .toBytes("value for blob"));
      put.addColumn(Bytes.toBytes("trans-type"), null, Bytes.toBytes("statement"));
      put.addColumn(Bytes.toBytes("trans-date"), null, Bytes
        .toBytes("20090921010101999"));
      put.addColumn(Bytes.toBytes("trans-tags"), Bytes.toBytes("qual2"), Bytes
        .toBytes(value));
      put.addColumn(Bytes.toBytes("trans-group"), null, Bytes
        .toBytes("adhocTransactionGroupId"));
      ht.put(put);
    }
  }

  protected void assertRowCount(final Table t, final int expected) throws IOException {
    assertEquals(expected, TEST_UTIL.countRows(t));
  }

  /*
   * @param key
   * @return Scan with RowFilter that does LESS than passed key.
   */
  protected Scan createScanWithRowFilter(final byte [] key) {
    return createScanWithRowFilter(key, null, CompareOperator.LESS);
  }

  /*
   * @param key
   * @param op
   * @param startRow
   * @return Scan with RowFilter that does CompareOp op on passed key.
   */
  protected Scan createScanWithRowFilter(final byte [] key,
    final byte [] startRow, CompareOperator op) {
    // Make sure key is of some substance... non-null and > than first key.
    assertTrue(key != null && key.length > 0 &&
      Bytes.BYTES_COMPARATOR.compare(key, new byte [] {'a', 'a', 'a'}) >= 0);
    LOG.info("Key=" + Bytes.toString(key));
    Scan s = startRow == null? new Scan(): new Scan(startRow);
    Filter f = new RowFilter(op, new BinaryComparator(key));
    f = new WhileMatchFilter(f);
    s.setFilter(f);
    return s;
  }

  /**
   * Split table into multiple regions.
   * @param t Table to split.
   * @return Map of regions to servers.
   */
  protected List<HRegionLocation> splitTable(final Table t) throws IOException {
    // Split this table in two.
    Admin admin = TEST_UTIL.getAdmin();
    admin.split(t.getName());
    // Is it right closing this admin?
    admin.close();
    List<HRegionLocation> regions = waitOnSplit(t);
    assertTrue(regions.size() > 1);
    return regions;
  }

  /*
   * Wait on table split.  May return because we waited long enough on the split
   * and it didn't happen.  Caller should check.
   * @param t
   * @return Map of table regions; caller needs to check table actually split.
   */
  private List<HRegionLocation> waitOnSplit(final Table t) throws IOException {
    try (RegionLocator locator = TEST_UTIL.getConnection().getRegionLocator(t.getName())) {
      List<HRegionLocation> regions = locator.getAllRegionLocations();
      int originalCount = regions.size();
      for (int i = 0; i < TEST_UTIL.getConfiguration().getInt("hbase.test.retries", 30); i++) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        regions = locator.getAllRegionLocations();
        if (regions.size() > originalCount && allRegionsHaveHostnames(regions)) {
          break;
        }
      }
      return regions;
    }
  }

  // We need to check for null serverNames due to https://issues.apache.org/jira/browse/HBASE-26790,
  // because the null serverNames cause the ScannerCallable to fail.
  // we can remove this check once that is resolved
  private boolean allRegionsHaveHostnames(List<HRegionLocation> regions) {
    for (HRegionLocation region : regions) {
      if (region.getServerName() == null) {
        return false;
      }
    }
    return true;
  }

  protected Result getSingleScanResult(Table ht, Scan scan) throws IOException {
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    scanner.close();
    return result;
  }

  protected byte [][] makeNAscii(byte [] base, int n) {
    if(n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      byte [] tail = Bytes.toBytes(Integer.toString(i));
      ret[i] = Bytes.add(base, tail);
    }
    return ret;
  }

  protected byte [][] makeN(byte [] base, int n) {
    if (n > 256) {
      return makeNBig(base, n);
    }
    byte [][] ret = new byte[n][];
    for(int i=0;i<n;i++) {
      ret[i] = Bytes.add(base, new byte[]{(byte)i});
    }
    return ret;
  }

  protected byte [][] makeNBig(byte [] base, int n) {
    byte [][] ret = new byte[n][];
    for(int i = 0; i < n; i++) {
      int byteA = (i % 256);
      int byteB = (i >> 8);
      ret[i] = Bytes.add(base, new byte[]{(byte)byteB,(byte)byteA});
    }
    return ret;
  }

  protected long [] makeStamps(int n) {
    long [] stamps = new long[n];
    for (int i = 0; i < n; i++) {
      stamps[i] = i+1L;
    }
    return stamps;
  }

  protected static boolean equals(byte [] left, byte [] right) {
    if (left == null && right == null) {
      return true;
    }
    if (left == null && right.length == 0) {
      return true;
    }
    if (right == null && left.length == 0) {
      return true;
    }
    return Bytes.equals(left, right);
  }

  protected void assertKey(Cell key, byte [] row, byte [] family, byte [] qualifier,
      byte [] value) {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(CellUtil.cloneRow(key)) +"]",
      equals(row, CellUtil.cloneRow(key)));
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(key)) + "]",
      equals(family, CellUtil.cloneFamily(key)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(key)) + "]",
      equals(qualifier, CellUtil.cloneQualifier(key)));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(key)) + "]",
      equals(value, CellUtil.cloneValue(key)));
  }

  protected static void assertIncrementKey(Cell key, byte [] row, byte [] family,
    byte [] qualifier, long value) {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(CellUtil.cloneRow(key)) +"]",
      equals(row, CellUtil.cloneRow(key)));
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(key)) + "]",
      equals(family, CellUtil.cloneFamily(key)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(key)) + "]",
      equals(qualifier, CellUtil.cloneQualifier(key)));
    assertEquals(
      "Expected value [" + value + "] " + "Got value [" + Bytes.toLong(CellUtil.cloneValue(key))
        + "]", Bytes.toLong(CellUtil.cloneValue(key)), value);
  }

  protected void assertNumKeys(Result result, int n) throws Exception {
    assertEquals("Expected " + n + " keys but got " + result.size(), result.size(), n);
  }

  protected void assertNResult(Result result, byte [] row,
    byte [][] families, byte [][] qualifiers, byte [][] values, int [][] idxs) {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
      equals(row, result.getRow()));
    assertEquals("Expected " + idxs.length + " keys but result contains " + result.size(),
      result.size(), idxs.length);

    Cell [] keys = result.rawCells();

    for(int i=0;i<keys.length;i++) {
      byte [] family = families[idxs[i][0]];
      byte [] qualifier = qualifiers[idxs[i][1]];
      byte [] value = values[idxs[i][2]];
      Cell key = keys[i];

      byte[] famb = CellUtil.cloneFamily(key);
      byte[] qualb = CellUtil.cloneQualifier(key);
      byte[] valb = CellUtil.cloneValue(key);
      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(famb) + "]",
        equals(family, famb));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier)
          + "] " + "Got qualifier [" + Bytes.toString(qualb) + "]",
        equals(qualifier, qualb));
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(valb) + "]",
        equals(value, valb));
    }
  }

  protected void assertNResult(Result result, byte [] row,
    byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
    int start, int end) {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
      equals(row, result.getRow()));
    int expectedResults = end - start + 1;
    assertEquals(expectedResults, result.size());

    Cell[] keys = result.rawCells();

    for (int i=0; i<keys.length; i++) {
      byte [] value = values[end-i];
      long ts = stamps[end-i];
      Cell key = keys[i];

      assertTrue("(" + i + ") Expected family [" + Bytes.toString(family)
          + "] " + "Got family [" + Bytes.toString(CellUtil.cloneFamily(key)) + "]",
        CellUtil.matchingFamily(key, family));
      assertTrue("(" + i + ") Expected qualifier [" + Bytes.toString(qualifier)
          + "] " + "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(key))+ "]",
        CellUtil.matchingQualifier(key, qualifier));
      assertEquals("Expected ts [" + ts + "] " + "Got ts [" + key.getTimestamp() + "]", ts,
        key.getTimestamp());
      assertTrue("(" + i + ") Expected value [" + Bytes.toString(value) + "] "
          + "Got value [" + Bytes.toString(CellUtil.cloneValue(key)) + "]",
        CellUtil.matchingValue(key,  value));
    }
  }

  /**
   * Validate that result contains two specified keys, exactly.
   * It is assumed key A sorts before key B.
   */
  protected void assertDoubleResult(Result result, byte [] row,
    byte [] familyA, byte [] qualifierA, byte [] valueA,
    byte [] familyB, byte [] qualifierB, byte [] valueB) {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
      equals(row, result.getRow()));
    assertEquals("Expected two keys but result contains " + result.size(),
      2, result.size());
    Cell [] kv = result.rawCells();
    Cell kvA = kv[0];
    assertTrue("(A) Expected family [" + Bytes.toString(familyA) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kvA)) + "]",
      equals(familyA, CellUtil.cloneFamily(kvA)));
    assertTrue("(A) Expected qualifier [" + Bytes.toString(qualifierA) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kvA)) + "]",
      equals(qualifierA, CellUtil.cloneQualifier(kvA)));
    assertTrue("(A) Expected value [" + Bytes.toString(valueA) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kvA)) + "]",
      equals(valueA, CellUtil.cloneValue(kvA)));
    Cell kvB = kv[1];
    assertTrue("(B) Expected family [" + Bytes.toString(familyB) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kvB)) + "]",
      equals(familyB, CellUtil.cloneFamily(kvB)));
    assertTrue("(B) Expected qualifier [" + Bytes.toString(qualifierB) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kvB)) + "]",
      equals(qualifierB, CellUtil.cloneQualifier(kvB)));
    assertTrue("(B) Expected value [" + Bytes.toString(valueB) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kvB)) + "]",
      equals(valueB, CellUtil.cloneValue(kvB)));
  }

  protected void assertSingleResult(Result result, byte [] row, byte [] family,
    byte [] qualifier, byte [] value) {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
      equals(row, result.getRow()));
    assertEquals("Expected a single key but result contains " + result.size(), 1, result.size());
    Cell kv = result.rawCells()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kv)) + "]",
      equals(family, CellUtil.cloneFamily(kv)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kv)) + "]",
      equals(qualifier, CellUtil.cloneQualifier(kv)));
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kv)) + "]",
      equals(value, CellUtil.cloneValue(kv)));
  }

  protected void assertSingleResult(Result result, byte[] row, byte[] family, byte[] qualifier,
    long value) {
    assertTrue(
      "Expected row [" + Bytes.toString(row) + "] " + "Got row [" + Bytes.toString(result.getRow())
        + "]", equals(row, result.getRow()));
    assertEquals("Expected a single key but result contains " + result.size(), 1, result.size());
    Cell kv = result.rawCells()[0];
    assertTrue(
      "Expected family [" + Bytes.toString(family) + "] " + "Got family ["
        + Bytes.toString(CellUtil.cloneFamily(kv)) + "]",
      equals(family, CellUtil.cloneFamily(kv)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " + "Got qualifier ["
        + Bytes.toString(CellUtil.cloneQualifier(kv)) + "]",
      equals(qualifier, CellUtil.cloneQualifier(kv)));
    assertEquals(
      "Expected value [" + value + "] " + "Got value [" + Bytes.toLong(CellUtil.cloneValue(kv))
        + "]", value, Bytes.toLong(CellUtil.cloneValue(kv)));
  }

  protected void assertSingleResult(Result result, byte [] row, byte [] family,
    byte [] qualifier, long ts, byte [] value) {
    assertTrue("Expected row [" + Bytes.toString(row) + "] " +
        "Got row [" + Bytes.toString(result.getRow()) +"]",
      equals(row, result.getRow()));
    assertEquals("Expected a single key but result contains " + result.size(), 1, result.size());
    Cell kv = result.rawCells()[0];
    assertTrue("Expected family [" + Bytes.toString(family) + "] " +
        "Got family [" + Bytes.toString(CellUtil.cloneFamily(kv)) + "]",
      equals(family, CellUtil.cloneFamily(kv)));
    assertTrue("Expected qualifier [" + Bytes.toString(qualifier) + "] " +
        "Got qualifier [" + Bytes.toString(CellUtil.cloneQualifier(kv)) + "]",
      equals(qualifier, CellUtil.cloneQualifier(kv)));
    assertEquals("Expected ts [" + ts + "] " + "Got ts [" + kv.getTimestamp() + "]", ts,
      kv.getTimestamp());
    assertTrue("Expected value [" + Bytes.toString(value) + "] " +
        "Got value [" + Bytes.toString(CellUtil.cloneValue(kv)) + "]",
      equals(value, CellUtil.cloneValue(kv)));
  }

  protected void assertEmptyResult(Result result) throws Exception {
    assertTrue("expected an empty result but result contains " +
      result.size() + " keys", result.isEmpty());
  }

  protected void assertNullResult(Result result) throws Exception {
    assertNull("expected null result but received a non-null result", result);
  }

  protected void getVersionRangeAndVerifyGreaterThan(Table ht, byte [] row,
    byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
    int start, int end) throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.readVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }

  protected void getVersionRangeAndVerify(Table ht, byte [] row, byte [] family,
    byte [] qualifier, long [] stamps, byte [][] values, int start, int end) throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.readVersions(Integer.MAX_VALUE);
    get.setTimeRange(stamps[start], stamps[end]+1);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  protected void getAllVersionsAndVerify(Table ht, byte [] row, byte [] family,
    byte [] qualifier, long [] stamps, byte [][] values, int start, int end) throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.readVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  protected void scanVersionRangeAndVerifyGreaterThan(Table ht, byte [] row,
    byte [] family, byte [] qualifier, long [] stamps, byte [][] values,
    int start, int end) throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start+1], Long.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start+1, end);
  }

  protected void scanVersionRangeAndVerify(Table ht, byte [] row, byte [] family,
    byte [] qualifier, long [] stamps, byte [][] values, int start, int end) throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    scan.setTimeRange(stamps[start], stamps[end]+1);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  protected void scanAllVersionsAndVerify(Table ht, byte [] row, byte [] family,
    byte [] qualifier, long [] stamps, byte [][] values, int start, int end) throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNResult(result, row, family, qualifier, stamps, values, start, end);
  }

  protected void getVersionAndVerify(Table ht, byte [] row, byte [] family,
    byte [] qualifier, long stamp, byte [] value) throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimestamp(stamp);
    get.readVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  protected void getVersionAndVerifyMissing(Table ht, byte [] row, byte [] family,
    byte [] qualifier, long stamp) throws Exception {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    get.setTimestamp(stamp);
    get.readVersions(Integer.MAX_VALUE);
    Result result = ht.get(get);
    assertEmptyResult(result);
  }

  protected void scanVersionAndVerify(Table ht, byte [] row, byte [] family,
    byte [] qualifier, long stamp, byte [] value) throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimestamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, qualifier, stamp, value);
  }

  protected void scanVersionAndVerifyMissing(Table ht, byte [] row,
    byte [] family, byte [] qualifier, long stamp) throws Exception {
    Scan scan = new Scan(row);
    scan.addColumn(family, qualifier);
    scan.setTimestamp(stamp);
    scan.setMaxVersions(Integer.MAX_VALUE);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }

  protected void getTestNull(Table ht, byte [] row, byte [] family, byte [] value)
      throws Exception {
    Get get = new Get(row);
    get.addColumn(family, null);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, null, value);

    get = new Get(row);
    get.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    get.addFamily(family);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

  }

  protected void getTestNull(Table ht, byte[] row, byte[] family, long value) throws Exception {
    Get get = new Get(row);
    get.addColumn(family, null);
    Result result = ht.get(get);
    assertSingleResult(result, row, family, null, value);

    get = new Get(row);
    get.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    get.addFamily(family);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    get = new Get(row);
    result = ht.get(get);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);
  }

  protected void scanTestNull(Table ht, byte[] row, byte[] family, byte[] value)
    throws Exception {
    scanTestNull(ht, row, family, value, false);
  }

  protected void scanTestNull(Table ht, byte[] row, byte[] family, byte[] value,
    boolean isReversedScan) throws Exception {

    Scan scan = new Scan();
    scan.setReversed(isReversedScan);
    scan.addColumn(family, null);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.setReversed(isReversedScan);
    scan.addColumn(family, HConstants.EMPTY_BYTE_ARRAY);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.setReversed(isReversedScan);
    scan.addFamily(family);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

    scan = new Scan();
    scan.setReversed(isReversedScan);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, row, family, HConstants.EMPTY_BYTE_ARRAY, value);

  }

  protected void singleRowGetTest(Table ht, byte [][] ROWS, byte [][] FAMILIES,
    byte [][] QUALIFIERS, byte [][] VALUES) throws Exception {
    // Single column from memstore
    Get get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);

    // Single column from storefile
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    // Single column from storefile, family match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);

    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
      FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = ht.get(get);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
      FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });

    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addFamily(FAMILIES[2]);
    get.addFamily(FAMILIES[4]);
    get.addFamily(FAMILIES[6]);
    get.addFamily(FAMILIES[7]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] {
        {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
      });

    // Multiple columns from everywhere storefile, many family, wildcard
    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[2], QUALIFIERS[2]);
    get.addColumn(FAMILIES[2], QUALIFIERS[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[4]);
    get.addColumn(FAMILIES[6], QUALIFIERS[6]);
    get.addColumn(FAMILIES[6], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[7]);
    get.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] {
        {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
      });

    // Everything
    get = new Get(ROWS[0]);
    result = ht.get(get);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] {
        {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
      });

    // Get around inserted columns

    get = new Get(ROWS[1]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[0]);
    get.addColumn(FAMILIES[4], QUALIFIERS[3]);
    get.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = ht.get(get);
    assertEmptyResult(result);

  }

  protected void singleRowScanTest(Table ht, byte [][] ROWS, byte [][] FAMILIES,
    byte [][] QUALIFIERS, byte [][] VALUES) throws Exception {
    // Single column from memstore
    Scan scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0]);

    // Single column from storefile
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[2], QUALIFIERS[2], VALUES[2]);

    // Single column from storefile, family match
    scan = new Scan();
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[0], FAMILIES[7], QUALIFIERS[7], VALUES[7]);

    // Two columns, one from memstore one from storefile, same family,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
      FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Two columns, one from memstore one from storefile, same family,
    // explicit match
    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    result = getSingleScanResult(ht, scan);
    assertDoubleResult(result, ROWS[0], FAMILIES[4], QUALIFIERS[0], VALUES[0],
      FAMILIES[4], QUALIFIERS[4], VALUES[4]);

    // Three column, one from memstore two from storefile, different families,
    // wildcard match
    scan = new Scan();
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] { {4, 0, 0}, {4, 4, 4}, {7, 7, 7} });

    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addFamily(FAMILIES[2]);
    scan.addFamily(FAMILIES[4]);
    scan.addFamily(FAMILIES[6]);
    scan.addFamily(FAMILIES[7]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] {
        {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
      });

    // Multiple columns from everywhere storefile, many family, wildcard
    scan = new Scan();
    scan.addColumn(FAMILIES[2], QUALIFIERS[2]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[0]);
    scan.addColumn(FAMILIES[4], QUALIFIERS[4]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[6]);
    scan.addColumn(FAMILIES[6], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[7]);
    scan.addColumn(FAMILIES[7], QUALIFIERS[8]);
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] {
        {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}
      });

    // Everything
    scan = new Scan();
    result = getSingleScanResult(ht, scan);
    assertNResult(result, ROWS[0], FAMILIES, QUALIFIERS, VALUES,
      new int [][] {
        {2, 2, 2}, {2, 4, 4}, {4, 0, 0}, {4, 4, 4}, {6, 6, 6}, {6, 7, 7}, {7, 7, 7}, {9, 0, 0}
      });

    // Scan around inserted columns

    scan = new Scan(ROWS[1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addColumn(FAMILIES[4], QUALIFIERS[3]);
    scan.addColumn(FAMILIES[2], QUALIFIERS[3]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);
  }

  /**
   * Verify a single column using gets.
   * Expects family and qualifier arrays to be valid for at least
   * the range:  idx-2 < idx < idx+2
   */
  protected void getVerifySingleColumn(Table ht, byte [][] ROWS, int ROWIDX, byte [][] FAMILIES,
    int FAMILYIDX, byte [][] QUALIFIERS, int QUALIFIERIDX, byte [][] VALUES, int VALUEIDX)
    throws Exception {
    Get get = new Get(ROWS[ROWIDX]);
    Result result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX-2]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[0]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[FAMILYIDX]);
    get.addColumn(FAMILIES[FAMILYIDX+1], QUALIFIERS[1]);
    get.addColumn(FAMILIES[FAMILYIDX-2], QUALIFIERS[1]);
    get.addFamily(FAMILIES[FAMILYIDX-1]);
    get.addFamily(FAMILIES[FAMILYIDX+2]);
    result = ht.get(get);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

  }


  /**
   * Verify a single column using scanners.
   * Expects family and qualifier arrays to be valid for at least
   * the range:  idx-2 to idx+2
   * Expects row array to be valid for at least idx to idx+2
   */
  protected void scanVerifySingleColumn(Table ht, byte [][] ROWS, int ROWIDX, byte [][] FAMILIES,
    int FAMILYIDX, byte [][] QUALIFIERS, int QUALIFIERIDX, byte [][] VALUES, int VALUEIDX)
    throws Exception {
    Scan scan = new Scan();
    Result result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(ROWS[ROWIDX], ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX-1], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX]);
    scan.addFamily(FAMILIES[FAMILYIDX+1]);
    result = getSingleScanResult(ht, scan);
    assertSingleResult(result, ROWS[ROWIDX], FAMILIES[FAMILYIDX],
      QUALIFIERS[QUALIFIERIDX], VALUES[VALUEIDX]);

  }

  /**
   * Verify we do not read any values by accident around a single column
   * Same requirements as getVerifySingleColumn
   */
  protected void getVerifySingleEmpty(Table ht, byte [][] ROWS, int ROWIDX, byte [][] FAMILIES,
    int FAMILYIDX, byte [][] QUALIFIERS, int QUALIFIERIDX) throws Exception {
    Get get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[1]);
    Result result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[4]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX]);
    get.addFamily(FAMILIES[3]);
    get.addColumn(FAMILIES[4], QUALIFIERS[2]);
    get.addFamily(FAMILIES[5]);
    result = ht.get(get);
    assertEmptyResult(result);

    get = new Get(ROWS[ROWIDX+1]);
    result = ht.get(get);
    assertEmptyResult(result);

  }

  protected void scanVerifySingleEmpty(Table ht, byte [][] ROWS, int ROWIDX, byte [][] FAMILIES,
    int FAMILYIDX, byte [][] QUALIFIERS, int QUALIFIERIDX) throws Exception {
    Scan scan = new Scan(ROWS[ROWIDX+1]);
    Result result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(ROWS[ROWIDX+1],ROWS[ROWIDX+2]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan(HConstants.EMPTY_START_ROW, ROWS[ROWIDX]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

    scan = new Scan();
    scan.addColumn(FAMILIES[FAMILYIDX], QUALIFIERS[QUALIFIERIDX+1]);
    scan.addFamily(FAMILIES[FAMILYIDX-1]);
    result = getSingleScanResult(ht, scan);
    assertNullResult(result);

  }
}

