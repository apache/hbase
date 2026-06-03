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

import static org.apache.hadoop.hbase.util.FutureUtils.get;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.ClientMetaTableAccessor;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

/**
 * Asserts the single-RPC promise of the paginated meta-scan path
 * ({@link ClientMetaTableAccessor#getTableHRegionLocations(AsyncTable, TableName, byte[], int)}) by
 * wrapping the meta {@link AsyncTable} so we can count {@link AdvancedScanResultConsumer#onNext}
 * invocations - one per ScannerNext server response.
 * <p/>
 * Cluster runs with {@code hbase.meta.scanner.caching = 2} so the {@code limit > caching} branch is
 * exercised cheaply with a small table (5 user regions).
 */
@Tag(MediumTests.TAG)
@Tag(ClientTests.TAG)
public class TestRegionLocatorPagedScanRpcCount {

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("LocatorPaged");
  private static final byte[] FAMILY = Bytes.toBytes("family");

  /** Caching is small enough that {@code limit > META_CACHING} is easy to set up. */
  private static final int META_CACHING = 2;
  private static final int NUM_REGIONS = 5;

  private static AsyncConnection CONN;

  @BeforeAll
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
    CONN = ConnectionFactory.createAsyncConnection(UTIL.getConfiguration()).get();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    Closeables.close(CONN, true);
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSingleRpcWhenLimitWithinCaching() throws Exception {
    // limit (2) <= caching (2): trivially one ScannerNext. Baseline.
    int rpcs = runPagedScanAndCountRpcs(2);
    assertEquals(1, rpcs, "expected exactly one ScannerNext RPC for limit <= caching");
  }

  @Test
  public void testSingleRpcWhenLimitExceedsCaching() throws Exception {
    // limit (5) > caching (2): without the isPagedScan fix this would be ceil(5/2) = 3
    // ScannerNext RPCs. With the fix, getMetaScan sizes caching to limit -> 1 RPC.
    int rpcs = runPagedScanAndCountRpcs(NUM_REGIONS);
    assertEquals(1, rpcs,
      "expected exactly one ScannerNext RPC for paged scan even when limit > caching");
  }

  @Test
  public void testUnboundedPathStillUsesConfiguredCaching() throws Exception {
    // The unbounded getTableHRegionLocations(metaTable, tableName) overload (no limit) must
    // continue to use the configured caching (META_CACHING=2). For NUM_REGIONS=5 user regions,
    // expect ceil(5 / 2) = 3 ScannerNext batches plus possibly a final empty batch when the
    // server-side scan reaches the stopRow. Assert it is strictly more than one to prove the
    // isPagedScan flag did not bleed into the unbounded path.
    AtomicInteger onNextCalls = new AtomicInteger();
    AsyncTable<AdvancedScanResultConsumer> metaTable = wrapMetaTable(onNextCalls);
    List<HRegionLocation> all =
      get(ClientMetaTableAccessor.getTableHRegionLocations(metaTable, TABLE_NAME));
    assertEquals(NUM_REGIONS, all.size());
    int rpcs = onNextCalls.get();
    assertEquals((int) Math.ceil((double) NUM_REGIONS / META_CACHING), rpcs,
      "unbounded scan should still split across "
        + "ceil(NUM_REGIONS / caching) ScannerNext batches; got " + rpcs);
  }

  private int runPagedScanAndCountRpcs(int limit) throws Exception {
    AtomicInteger onNextCalls = new AtomicInteger();
    AsyncTable<AdvancedScanResultConsumer> metaTable = wrapMetaTable(onNextCalls);
    List<HRegionLocation> page =
      get(ClientMetaTableAccessor.getTableHRegionLocations(metaTable, TABLE_NAME, null, limit));
    assertEquals(limit, page.size(), "paged call returned wrong number of regions");
    return onNextCalls.get();
  }

  /**
   * Returns a delegating proxy for the meta {@link AsyncTable} that intercepts
   * {@code scan(Scan, AdvancedScanResultConsumer)} and wraps the supplied consumer so every
   * {@link AdvancedScanResultConsumer#onNext} invocation increments {@code onNextCalls}.
   */
  @SuppressWarnings("unchecked")
  private static AsyncTable<AdvancedScanResultConsumer> wrapMetaTable(AtomicInteger onNextCalls) {
    AsyncTable<AdvancedScanResultConsumer> delegate = CONN.getTable(TableName.META_TABLE_NAME);
    InvocationHandler handler = new InvocationHandler() {
      @Override
      public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (
          "scan".equals(method.getName()) && args != null && args.length == 2
            && args[1] instanceof AdvancedScanResultConsumer
        ) {
          Scan scan = (Scan) args[0];
          AdvancedScanResultConsumer original = (AdvancedScanResultConsumer) args[1];
          AdvancedScanResultConsumer counting = new AdvancedScanResultConsumer() {
            @Override
            public void onNext(Result[] results, ScanController controller) {
              onNextCalls.incrementAndGet();
              original.onNext(results, controller);
            }

            @Override
            public void onError(Throwable error) {
              original.onError(error);
            }

            @Override
            public void onComplete() {
              original.onComplete();
            }

            @Override
            public void onHeartbeat(ScanController controller) {
              original.onHeartbeat(controller);
            }

            @Override
            public void onScanMetricsCreated(ScanMetrics scanMetrics) {
              original.onScanMetricsCreated(scanMetrics);
            }
          };
          return method.invoke(delegate, scan, counting);
        }
        return method.invoke(delegate, args);
      }
    };
    return (AsyncTable<AdvancedScanResultConsumer>) Proxy.newProxyInstance(
      AsyncTable.class.getClassLoader(), new Class<?>[] { AsyncTable.class }, handler);
  }
}
