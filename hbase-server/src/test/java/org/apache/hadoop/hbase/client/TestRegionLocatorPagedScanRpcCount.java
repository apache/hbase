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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.CatalogReplicaMode;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Asserts the single-RPC promise of the paginated meta-scan path
 * ({@link MetaTableAccessor#scanMetaForTableRegions(Connection, MetaTableAccessor.Visitor, TableName, byte[], int, CatalogReplicaMode)})
 * by wrapping the meta {@link Table} so we can read {@link ScanMetrics#countOfRPCcalls} from the
 * issued {@link ResultScanner} - one per ScannerNext server response.
 * <p/>
 * Cluster runs with {@code hbase.meta.scanner.caching = 2} so the {@code limit > caching} branch is
 * exercised cheaply with a small table (5 user regions).
 */
@Category({ MediumTests.class, ClientTests.class })
public class TestRegionLocatorPagedScanRpcCount {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionLocatorPagedScanRpcCount.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("LocatorPaged");
  private static final byte[] FAMILY = Bytes.toBytes("family");

  /** Caching is small enough that {@code limit > META_CACHING} is easy to set up. */
  private static final int META_CACHING = 2;
  private static final int NUM_REGIONS = 5;

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_META_SCANNER_CACHING, META_CACHING);
    UTIL.startMiniCluster(1);
    byte[][] splitKeys = new byte[NUM_REGIONS - 1][];
    for (int i = 0; i < splitKeys.length; i++) {
      splitKeys[i] = Bytes.toBytes(Integer.toString(i + 1));
    }
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    UTIL.getAdmin().createTable(td, splitKeys);
    UTIL.waitTableAvailable(TABLE_NAME);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSingleRpcWhenLimitWithinCaching() throws Exception {
    // limit (2) <= caching (2): trivially one ScannerNext. Baseline.
    long rpcs = runPagedScanAndCountRpcs(2);
    assertEquals("expected exactly one ScannerNext RPC for limit <= caching", 1L, rpcs);
  }

  @Test
  public void testSingleRpcWhenLimitExceedsCaching() throws Exception {
    // limit (5) > caching (2): without the isPagedScan fix this would be ceil(5/2) = 3
    // ScannerNext RPCs. With the fix, getMetaScan sizes caching to limit -> 1 RPC.
    long rpcs = runPagedScanAndCountRpcs(NUM_REGIONS);
    assertEquals("expected exactly one ScannerNext RPC for paged scan even when limit > caching",
      1L, rpcs);
  }

  @Test
  public void testUnboundedPathStillUsesConfiguredCaching() throws Exception {
    // The unbounded scanMetaForTableRegions(connection, visitor, tableName) overload (no rowLimit)
    // must continue to use the configured caching (META_CACHING=2). For NUM_REGIONS=5 user
    // regions, expect ceil(5 / 2) = 3 ScannerNext batches. Asserts the isPagedScan flag did not
    // bleed into the unbounded path.
    AtomicLong rpcCount = new AtomicLong();
    Connection wrapped = wrapConnection(UTIL.getConnection(), rpcCount);
    CollectingVisitor visitor = new CollectingVisitor(TABLE_NAME);
    MetaTableAccessor.scanMetaForTableRegions(wrapped, visitor, TABLE_NAME);
    assertEquals(NUM_REGIONS, visitor.locations.size());
    long expected = (long) Math.ceil((double) NUM_REGIONS / META_CACHING);
    assertEquals(
      "unbounded scan should split across ceil(NUM_REGIONS / caching) ScannerNext batches",
      expected, rpcCount.get());
  }

  private long runPagedScanAndCountRpcs(int limit) throws IOException {
    AtomicLong rpcCount = new AtomicLong();
    Connection wrapped = wrapConnection(UTIL.getConnection(), rpcCount);
    CollectingVisitor visitor = new CollectingVisitor(TABLE_NAME);
    MetaTableAccessor.scanMetaForTableRegions(wrapped, visitor, TABLE_NAME, null, limit,
      CatalogReplicaMode.NONE);
    assertEquals("paged call returned wrong number of regions", limit, visitor.locations.size());
    return rpcCount.get();
  }

  /** Visitor that just collects every region-row's first {@link HRegionLocation}. */
  private static final class CollectingVisitor extends MetaTableAccessor.TableVisitorBase {
    final List<HRegionLocation> locations = new ArrayList<>();

    CollectingVisitor(TableName tableName) {
      super(tableName);
    }

    @Override
    public boolean visitInternal(Result rowResult) throws IOException {
      RegionLocations locs = MetaTableAccessor.getRegionLocations(rowResult);
      if (locs != null && locs.getRegionLocation() != null) {
        locations.add(locs.getRegionLocation());
      }
      return true;
    }
  }

  /**
   * Returns a delegating proxy {@link Connection} that intercepts {@code getTable(META_TABLE_NAME)}
   * and returns a wrapped {@link Table}; the wrapped Table intercepts {@code getScanner(Scan)} to
   * enable scan metrics and wrap the returned {@link ResultScanner}. The wrapped scanner intercepts
   * {@code close()} so we can read {@link ScanMetrics#countOfRPCcalls} into {@code rpcCount} before
   * the underlying scanner is closed.
   */
  private static Connection wrapConnection(Connection delegate, AtomicLong rpcCount) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (
          "getTable".equals(method.getName()) && args != null && args.length == 1
            && TableName.META_TABLE_NAME.equals(args[0])
        ) {
          Table realTable = (Table) method.invoke(delegate, args);
          return wrapTable(realTable, rpcCount);
        }
        return method.invoke(delegate, args);
      }
    };
    return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
      new Class<?>[] { Connection.class }, handler);
  }

  private static Table wrapTable(Table delegate, AtomicLong rpcCount) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (
          "getScanner".equals(method.getName()) && args != null && args.length == 1
            && args[0] instanceof Scan
        ) {
          Scan scan = (Scan) args[0];
          scan.setScanMetricsEnabled(true);
          ResultScanner realScanner = (ResultScanner) method.invoke(delegate, scan);
          return wrapScanner(realScanner, rpcCount);
        }
        return method.invoke(delegate, args);
      }
    };
    return (Table) Proxy.newProxyInstance(Table.class.getClassLoader(),
      new Class<?>[] { Table.class }, handler);
  }

  private static ResultScanner wrapScanner(ResultScanner delegate, AtomicLong rpcCount) {
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ("close".equals(method.getName())) {
          // Read metrics BEFORE closing - the metrics object is the live counter attached to the
          // scanner. After close() the underlying client scanner may detach.
          ScanMetrics metrics = delegate.getScanMetrics();
          if (metrics != null) {
            rpcCount.set(metrics.countOfRPCcalls.get());
          }
        }
        return method.invoke(delegate, args);
      }
    };
    return (ResultScanner) Proxy.newProxyInstance(ResultScanner.class.getClassLoader(),
      new Class<?>[] { ResultScanner.class }, handler);
  }
}
