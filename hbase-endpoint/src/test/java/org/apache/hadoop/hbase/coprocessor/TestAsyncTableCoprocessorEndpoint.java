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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTable.CoprocessorCallback;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos.ColumnAggregationService;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos.SumRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.ColumnAggregationProtos.SumResponse;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.CoprocessorTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class, CoprocessorTests.class })
public class TestAsyncTableCoprocessorEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableCoprocessorEndpoint.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME = TableName.valueOf("async_coproc");
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("cq");
  private static final int NUM_REGIONS = 10;
  private static final int NUM_ROWS = 500;

  private static AsyncConnection ASYNC_CONN;

  @Rule
  public TestName testName = new TestName();

  @Parameter
  public Supplier<AsyncTable<?>> getTable;

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { () -> ASYNC_CONN.getTable(TABLE_NAME) },
      new Supplier<?>[] { () -> ASYNC_CONN.getTable(TABLE_NAME, ForkJoinPool.commonPool()) });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      ColumnAggregationEndpoint.class.getName());
    TEST_UTIL.startMiniCluster(1);

    createTestTable();

    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
  }

  @Before
  public void setUp() throws IOException, InterruptedException, ExecutionException {
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    if (regions.size() != NUM_REGIONS) { // always ensure the table has the correct number of
                                         // regions
      TEST_UTIL.deleteTable(TABLE_NAME);

      createTestTable();
    }
  }

  private static void createTestTable() throws IOException, InterruptedException {
    Table table = TEST_UTIL.createMultiRegionTable(TABLE_NAME, FAMILY, NUM_REGIONS);

    TEST_UTIL.waitTableAvailable(TABLE_NAME);

    Random random = new Random(0);
    Set<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    while (rows.size() < NUM_ROWS) {
      byte[] row = new byte[8];
      random.nextBytes(row);
      rows.add(row);
    }

    List<Put> puts = new ArrayList<>(rows.size());
    for (byte[] row : rows) {
      Put put = new Put(row);
      put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(random.nextInt(0, 100)));
      puts.add(put);
    }
    table.put(puts);

    table.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ASYNC_CONN.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void itCallsEndpointOnRegion() {
    AsyncTable<?> table = getTable.get();

    CompletableFuture<SumResponse> future =
      table.coprocessorService(ColumnAggregationService::newStub, sum(), Bytes.fromHex("deadbeef"));

    SumResponse response = future.join();
    assertEquals("Sum should match expected value", 12714, response.getSum());
  }

  @Test
  public void itCallsEndpointsOnAllRegion() throws Exception {
    AsyncTable<?> table = getTable.get();

    Pair<CompletableFuture<Map<String, Pair<SumResponse, Throwable>>>,
      CoprocessorCallback<SumResponse>> futureAndCallback = buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, Pair<SumResponse, Throwable>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertResultsAreCorrect(responsesByRegion);
  }

  @Test
  public void itCallsEndpointOnMergedRegion() throws Exception {
    AsyncTable<?> table = getTable.get();

    // call the endpoints before merging to ensure the meta cache has the old region locations
    Pair<CompletableFuture<Map<String, Pair<SumResponse, Throwable>>>,
      CoprocessorCallback<SumResponse>> futureAndCallback = buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();
    futureAndCallback.getFirst().join();

    byte[][] allRegions = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).stream()
      .map(RegionInfo::getRegionName).toArray(byte[][]::new);
    TEST_UTIL.getAdmin().mergeRegionsAsync(allRegions, true).get();

    futureAndCallback = buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, Pair<SumResponse, Throwable>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertResultsAreCorrect(responsesByRegion);
  }

  @Test
  public void itCallsEndpointOnSplitRegions() throws Exception {
    AsyncTable<?> table = getTable.get();

    // call the endpoints before splitting to ensure the meta cache has the old region locations
    Pair<CompletableFuture<Map<String, Pair<SumResponse, Throwable>>>,
      CoprocessorCallback<SumResponse>> futureAndCallback = buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();
    futureAndCallback.getFirst().join();

    RegionInfo regionToSplit = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).stream()
      .filter(
        regionInfo -> regionInfo.getStartKey().length > 0 && regionInfo.getEndKey().length > 0)
      .findFirst().orElseThrow(IllegalAccessException::new);
    TEST_UTIL.getAdmin().splitRegionAsync(regionToSplit.getRegionName(),
      Bytes.split(regionToSplit.getStartKey(), regionToSplit.getEndKey(), 1)[1]).get();

    futureAndCallback = buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, Pair<SumResponse, Throwable>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertResultsAreCorrect(responsesByRegion);
  }

  private void
    assertResultsAreCorrect(Map<String, Pair<SumResponse, Throwable>> responsesByRegion) {
    long actualSum = 0;
    for (String region : responsesByRegion.keySet()) {
      Pair<SumResponse, Throwable> response = responsesByRegion.get(region);
      assertNull("Error for region " + region, response.getSecond());
      actualSum += response.getFirst().getSum();
    }

    assertEquals("Sum should match expected value", 24785, actualSum);
  }

  private ServiceCaller<ColumnAggregationService, SumResponse> sum() {
    return (stub, controller, callback) -> {
      SumRequest request = SumRequest.newBuilder().setFamily(ByteString.copyFrom(FAMILY))
        .setQualifier(ByteString.copyFrom(QUALIFIER)).build();
      stub.sum(controller, request, callback);
    };
  }

  private Pair<CompletableFuture<Map<String, Pair<SumResponse, Throwable>>>,
    CoprocessorCallback<SumResponse>> buildCallbackForAllRegions() throws InterruptedException {
    CompletableFuture<Map<String, Pair<SumResponse, Throwable>>> future = new CompletableFuture<>();
    Map<String, Pair<SumResponse, Throwable>> responsesByRegion = new ConcurrentHashMap<>();

    AsyncTable.CoprocessorCallback<SumResponse> callback = new CoprocessorCallback<SumResponse>() {
      @Override
      public void onRegionComplete(RegionInfo region, SumResponse resp) {
        Pair<SumResponse, Throwable> prevResp =
          responsesByRegion.putIfAbsent(region.getEncodedName(), Pair.newPair(resp, null));

        if (prevResp != null) {
          future.completeExceptionally(new IOException("Duplicate response for region "
            + region.getEncodedName() + ": " + prevResp + " and " + Pair.newPair(resp, null)));
        }
      }

      @Override
      public void onRegionError(RegionInfo region, Throwable e) {
        Pair<SumResponse, Throwable> resp = Pair.newPair(null, e);
        Pair<SumResponse, Throwable> prevResp =
          responsesByRegion.putIfAbsent(region.getEncodedName(), Pair.newPair(null, e));

        if (prevResp != null) {
          future.completeExceptionally(new IOException("Duplicate response for region "
            + region.getEncodedName() + ": " + prevResp + " and " + resp));
        }
      }

      @Override
      public void onComplete() {
        future.complete(responsesByRegion);
      }

      @Override
      public void onError(Throwable e) {
        future.completeExceptionally(e);
      }
    };

    return Pair.newPair(future, callback);
  }
}
