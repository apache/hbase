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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.time.Duration;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.AsyncTable.PartialResultCoprocessorCallback;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.ServiceCaller;
import org.apache.hadoop.hbase.client.Table;
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

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.MoreExecutors;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.ColumnAggregationProtos.ColumnAggregationService;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.ColumnAggregationProtos.SumRequest;
import org.apache.hadoop.hbase.shaded.coprocessor.protobuf.generated.ColumnAggregationProtos.SumResponse;

@RunWith(Parameterized.class)
@Category({ MediumTests.class, ClientTests.class, CoprocessorTests.class })
public class TestAsyncTableCoprocessorEndpoint {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncTableCoprocessorEndpoint.class);

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();

  private static final TableName TABLE_NAME = TableName.valueOf("async_coproc");
  private static final byte[] FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("cq");
  private static final int NUM_REGIONS = 10;
  private static final int NUM_ROWS = 500;

  private static ExecutorService EXECUTOR;

  private static AsyncConnection ASYNC_CONN;

  @Rule
  public TestName testName = new TestName();

  @Parameter
  public Supplier<AsyncTable<?>> getTable;

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Supplier<?>[] { () -> ASYNC_CONN.getTable(TABLE_NAME) },
      new Supplier<?>[] { () -> ASYNC_CONN.getTable(TABLE_NAME, EXECUTOR) });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      ColumnAggregationEndpoint.class.getName());
    TEST_UTIL.startMiniCluster(1);

    createTestTable();

    ASYNC_CONN = ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get();
    EXECUTOR = MoreExecutors.newDirectExecutorService();
  }

  @Before
  public void setUp() throws IOException, InterruptedException, ExecutionException {
    List<HRegion> regions = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME);
    if (regions.size() != NUM_REGIONS) { // always ensure the table has the correct number of
                                         // regions
      TEST_UTIL.deleteTable(TABLE_NAME);

      createTestTable();
    }
    ASYNC_CONN.clearRegionLocationCache();
  }

  private static void createTestTable() throws IOException, InterruptedException {
    createTestTable(NUM_REGIONS);
  }

  private static void createTestTable(int numRegions) throws IOException, InterruptedException {
    Table table = numRegions > 1
      ? TEST_UTIL.createMultiRegionTable(TABLE_NAME, FAMILY, numRegions)
      : TEST_UTIL.createTable(TABLE_NAME, FAMILY);

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
    EXECUTOR.shutdownNow();
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

    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>> futureAndCallback =
        buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertResultsAreCorrect(responsesByRegion);
  }

  @Test
  public void itCallsEndpointOnMergedRegion() throws Exception {
    AsyncTable<?> table = getTable.get();

    // call the endpoints before merging to ensure the meta cache has the old region locations
    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>> futureAndCallback =
        buildCallbackForAllRegions();
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

    Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertResultsAreCorrect(responsesByRegion);
  }

  @Test
  public void itCallsEndpointOnSplitRegions() throws Exception {
    AsyncTable<?> table = getTable.get();

    // call the endpoints before splitting to ensure the meta cache has the old region locations
    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>> futureAndCallback =
        buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();
    futureAndCallback.getFirst().join();

    RegionInfo regionToSplit = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).stream()
      .filter(
        regionInfo -> regionInfo.getStartKey().length > 0 && regionInfo.getEndKey().length > 0)
      .findFirst().orElseThrow(IllegalArgumentException::new);
    TEST_UTIL.getAdmin().splitRegionAsync(regionToSplit.getRegionName(),
      Bytes.split(regionToSplit.getStartKey(), regionToSplit.getEndKey(), 1)[1]).get();

    futureAndCallback = buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertResultsAreCorrect(responsesByRegion);
  }

  @Test
  public void itCallsEndpointOnRegionsWithSplitAndMerge() throws Exception {
    AsyncTable<?> table = getTable.get();

    // call the endpoints before to ensure the meta cache has the old region locations
    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>> futureAndCallback =
        buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();
    futureAndCallback.getFirst().join();

    // it takes a while for a merged region to become splitable, so simulate it by recreating the
    // table with new splits
    TEST_UTIL.deleteTable(TABLE_NAME);
    createTestTable(1);
    RegionInfo regionToSplit = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).stream().findFirst()
      .orElseThrow(IllegalArgumentException::new);
    TEST_UTIL.getAdmin().splitRegionAsync(regionToSplit.getRegionName(), Bytes.fromHex("053333"))
      .get(); // use a new split point that wasn't in the original table

    futureAndCallback = buildCallbackForAllRegions();
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertResultsAreCorrect(responsesByRegion);
  }

  @Test
  public void itCallsEndpointDuringRegionSplitProcedure() throws Exception {
    AsyncTable<?> table = getTable.get();

    RegionInfo regionToSplit = TEST_UTIL.getAdmin().getRegions(TABLE_NAME).stream()
      .filter(
        regionInfo -> regionInfo.getStartKey().length > 0 && regionInfo.getEndKey().length > 0)
      .findFirst().orElseThrow(IllegalArgumentException::new);

    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>> futureAndCallback =
        buildCallbackForAllRegions((region, prevRequestNumber) -> {
          if (region.equals(regionToSplit.getEncodedName())) {
            if (prevRequestNumber == 0) {
              return true;
            } else if (prevRequestNumber == 1) {
              try {
                TEST_UTIL.getAdmin()
                  .splitRegionAsync(regionToSplit.getRegionName(),
                    Bytes.split(regionToSplit.getStartKey(), regionToSplit.getEndKey(), 1)[1])
                  .get();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              return true;
            } else {
              return false;
            }
          } else {
            return false;
          }
        });
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    List<Pair<SumResponse, Throwable>> responsesForSplitRegion =
      responsesByRegion.get(regionToSplit.getEncodedName());
    assertEquals("Should have received 2 responses for split region", 2,
      responsesForSplitRegion.size());
    assertNull("First response for split region should not have an error",
      responsesForSplitRegion.get(0).getSecond());
    assertNull("Second response for split region should have failed due to concurrent split",
      responsesForSplitRegion.get(1).getSecond());
  }

  @Test
  public void itReturnsErrorIfGetNextCallableThrows() throws Exception {
    AsyncTable<?> table = getTable.get();

    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>> futureAndCallback =
        buildCallbackForAllRegions(((ignored1, ignored2) -> {
          throw new RuntimeException();
        }));
    table
      .coprocessorService(ColumnAggregationService::newStub, sum(), futureAndCallback.getSecond())
      .execute();

    Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion =
      futureAndCallback.getFirst().get(10, TimeUnit.SECONDS);
    assertEquals("Should have a response for every region", NUM_REGIONS, responsesByRegion.size());
    for (String region : responsesByRegion.keySet()) {
      List<Pair<SumResponse, Throwable>> responses = responsesByRegion.get(region);
      assertEquals("Should have two responses for region " + region, 2, responses.size());
      assertNull("Response for region " + region + " should have one success",
        responses.get(0).getSecond());
      assertNotNull("Response for region " + region + " should have one error",
        responses.get(1).getSecond());
    }
  }

  private void
    assertResultsAreCorrect(Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion) {
    long actualSum = 0;
    for (String region : responsesByRegion.keySet()) {
      assertEquals("Multiple responses for region " + region, 1,
        responsesByRegion.get(region).size());
      Pair<SumResponse, Throwable> response = responsesByRegion.get(region).get(0);
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

  private
    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>>
    buildCallbackForAllRegions() throws InterruptedException {
    return buildCallbackForAllRegions((region, requestNumber) -> false);
  }

  private
    Pair<CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>>,
      PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse>>
    buildCallbackForAllRegions(BiFunction<String, Integer, Boolean> hasMore)
      throws InterruptedException {
    CompletableFuture<Map<String, List<Pair<SumResponse, Throwable>>>> future =
      new CompletableFuture<>();
    Map<String, List<Pair<SumResponse, Throwable>>> responsesByRegion = new ConcurrentHashMap<>();

    AsyncTable.PartialResultCoprocessorCallback<ColumnAggregationService, SumResponse> callback =
      new AsyncTable.PartialResultCoprocessorCallback<>() {

        @Override
        public void onRegionComplete(RegionInfo region, SumResponse resp) {
          responsesByRegion.computeIfAbsent(region.getEncodedName(), r -> new ArrayList<>())
            .add(Pair.newPair(resp, null));
        }

        @Override
        public void onRegionError(RegionInfo region, Throwable e) {
          responsesByRegion.computeIfAbsent(region.getEncodedName(), r -> new ArrayList<>())
            .add(Pair.newPair(null, e));
        }

        @Override
        public void onComplete() {
          future.complete(responsesByRegion);
        }

        @Override
        public void onError(Throwable e) {
          future.completeExceptionally(e);
        }

        @Override
        public ServiceCaller<ColumnAggregationService, SumResponse>
          getNextCallable(SumResponse response, RegionInfo region) {
          int prevRequestNumber = responsesByRegion.get(region.getEncodedName()).size() - 1;
          if (hasMore.apply(region.getEncodedName(), prevRequestNumber)) {
            return sum();
          } else {
            return null;
          }
        }

        @Override
        public Duration getWaitInterval(SumResponse response, RegionInfo region) {
          return Duration.ZERO;
        }
      };

    return Pair.newPair(future, callback);
  }
}
