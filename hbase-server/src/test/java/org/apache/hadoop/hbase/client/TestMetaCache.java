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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.function.ThrowingRunnable;

import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({ MediumTests.class, ClientTests.class })
public class TestMetaCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMetaCache.class);

  private final static HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static final TableName TABLE_NAME = TableName.valueOf("test_table");
  private static final byte[] FAMILY = Bytes.toBytes("fam1");
  private static final byte[] QUALIFIER = Bytes.toBytes("qual");

  private static HRegionServer badRS;

  private Connection conn;
  private MetricsConnection metrics;
  private AsyncRegionLocator locator;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(HConstants.REGION_SERVER_IMPL, RegionServerWithFakeRpcServices.class.getName());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.waitUntilAllRegionsAssigned(connection.getMetaTableName());
    badRS = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    assertTrue(badRS.getRSRpcServices() instanceof FakeRSRpcServices);
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(TABLE_NAME)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).setMaxVersions(2).build())
      .build();
    TEST_UTIL.createTable(desc, null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(conn, true);
  }

  private void setupConnection(int retry) throws IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retry);
    conf.setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, true);
    conn = ConnectionFactory.createConnection(conf);
    AsyncConnectionImpl asyncConn = (AsyncConnectionImpl) conn.toAsyncConnection();
    locator = asyncConn.getLocator();
    metrics = asyncConn.getConnectionMetrics().get();
  }

  /**
   * Test that our cleanOverlappingRegions doesn't incorrectly remove regions from cache. Originally
   * encountered when using floorEntry rather than lowerEntry.
   */
  @Test
  public void testAddToCacheReverse() throws IOException, InterruptedException {
    setupConnection(1);
    TableName tableName = TableName.valueOf("testAddToCache");
    byte[] family = Bytes.toBytes("CF");
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    int maxSplits = 10;
    List<byte[]> splits =
      IntStream.range(1, maxSplits).mapToObj(Bytes::toBytes).collect(Collectors.toList());

    TEST_UTIL.getAdmin().createTable(td, splits.toArray(new byte[0][]));
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.waitUntilNoRegionsInTransition();

    assertEquals(splits.size() + 1, TEST_UTIL.getAdmin().getRegions(tableName).size());

    RegionLocator locatorForTable = conn.getRegionLocator(tableName);
    for (int i = maxSplits; i >= 0; i--) {
      locatorForTable.getRegionLocation(Bytes.toBytes(i));
    }

    for (int i = 0; i < maxSplits; i++) {
      assertNotNull(locator.getRegionLocationInCache(tableName, Bytes.toBytes(i)));
    }
  }

  @Test
  public void testMergeEmptyWithMetaCache() throws Throwable {
    TableName tableName = TableName.valueOf("testMergeEmptyWithMetaCache");
    byte[] family = Bytes.toBytes("CF");
    TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
    TEST_UTIL.getAdmin().createTable(td, new byte[][] { Bytes.toBytes(2), Bytes.toBytes(5) });
    TEST_UTIL.waitTableAvailable(tableName);
    TEST_UTIL.waitUntilNoRegionsInTransition();
    RegionInfo regionA = null;
    RegionInfo regionB = null;
    RegionInfo regionC = null;
    for (RegionInfo region : TEST_UTIL.getAdmin().getRegions(tableName)) {
      if (region.getStartKey().length == 0) {
        regionA = region;
      } else if (Bytes.equals(region.getStartKey(), Bytes.toBytes(2))) {
        regionB = region;
      } else if (Bytes.equals(region.getStartKey(), Bytes.toBytes(5))) {
        regionC = region;
      }
    }

    assertNotNull(regionA);
    assertNotNull(regionB);
    assertNotNull(regionC);

    TEST_UTIL.getConfiguration().setBoolean(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY,
      true);
    try (AsyncConnection asyncConn =
      ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get()) {
      AsyncConnectionImpl asyncConnImpl = (AsyncConnectionImpl) asyncConn;

      MetricsConnection asyncMetrics = asyncConnImpl.getConnectionMetrics().get();

      // warm meta cache
      asyncConn.getRegionLocator(tableName).getAllRegionLocations().get();

      assertEquals(3, TEST_UTIL.getAdmin().getRegions(tableName).size());

      // Merge the 3 regions into one
      TEST_UTIL.getAdmin().mergeRegionsAsync(
        new byte[][] { regionA.getRegionName(), regionB.getRegionName(), regionC.getRegionName() },
        false).get(30, TimeUnit.SECONDS);

      assertEquals(1, TEST_UTIL.getAdmin().getRegions(tableName).size());

      AsyncTable<?> asyncTable = asyncConn.getTable(tableName);

      // This request should cause us to cache the newly merged region.
      // As part of caching that region, it should clear out any cached merge parent regions which
      // are overlapped by the new region. That way, subsequent calls below won't fall into the
      // bug in HBASE-27650. Otherwise, a request for row 6 would always get stuck on cached
      // regionB and we'd continue to see cache misses below.
      assertTrue(
        executeAndGetNewMisses(() -> asyncTable.get(new Get(Bytes.toBytes(6))).get(), asyncMetrics)
            > 0);

      // We verify no new cache misses here due to above, which proves we've fixed up the cache
      assertEquals(0, executeAndGetNewMisses(() -> asyncTable.get(new Get(Bytes.toBytes(6))).get(),
        asyncMetrics));
    }
  }

  private long executeAndGetNewMisses(ThrowingRunnable runnable, MetricsConnection metrics)
    throws Throwable {
    long lastVal = metrics.getMetaCacheMisses();
    runnable.run();
    long curVal = metrics.getMetaCacheMisses();
    return curVal - lastVal;
  }

  @Test
  public void testPreserveMetaCacheOnException() throws Exception {
    ((FakeRSRpcServices) badRS.getRSRpcServices())
      .setExceptionInjector(new RoundRobinExceptionInjector());
    setupConnection(1);
    try (Table table = conn.getTable(TABLE_NAME)) {
      byte[] row = Bytes.toBytes("row1");

      Put put = new Put(row);
      put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(10));
      Get get = new Get(row);
      Append append = new Append(row);
      append.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(11));
      Increment increment = new Increment(row);
      increment.addColumn(FAMILY, QUALIFIER, 10);
      Delete delete = new Delete(row);
      delete.addColumn(FAMILY, QUALIFIER);
      RowMutations mutations = new RowMutations(row);
      mutations.add(put);
      mutations.add(delete);

      Exception exp;
      boolean success;
      for (int i = 0; i < 50; i++) {
        exp = null;
        success = false;
        try {
          table.put(put);
          // If at least one operation succeeded, we should have cached the region location.
          success = true;
          table.get(get);
          table.append(append);
          table.increment(increment);
          table.delete(delete);
          table.mutateRow(mutations);
        } catch (IOException ex) {
          // Only keep track of the last exception that updated the meta cache
          if (ClientExceptionsUtil.isMetaClearingException(ex) || success) {
            exp = ex;
          }
        }
        // Do not test if we did not touch the meta cache in this iteration.
        if (exp != null && ClientExceptionsUtil.isMetaClearingException(exp)) {
          assertNull(locator.getRegionLocationInCache(TABLE_NAME, row));
        } else if (success) {
          assertNotNull(locator.getRegionLocationInCache(TABLE_NAME, row));
        }
      }
    }
  }

  @Test
  public void testCacheClearingOnCallQueueTooBig() throws Exception {
    ((FakeRSRpcServices) badRS.getRSRpcServices())
      .setExceptionInjector(new CallQueueTooBigExceptionInjector());
    setupConnection(2);
    Table table = conn.getTable(TABLE_NAME);
    byte[] row = Bytes.toBytes("row1");

    Put put = new Put(row);
    put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(10));
    table.put(put);

    // obtain the client metrics
    long preGetRegionClears = metrics.getMetaCacheNumClearRegion().getCount();
    long preGetServerClears = metrics.getMetaCacheNumClearServer().getCount();

    // attempt a get on the test table
    Get get = new Get(row);
    try {
      table.get(get);
      fail("Expected CallQueueTooBigException");
    } catch (RetriesExhaustedException ree) {
      // expected
    }

    // verify that no cache clearing took place
    long postGetRegionClears = metrics.getMetaCacheNumClearRegion().getCount();
    long postGetServerClears = metrics.getMetaCacheNumClearServer().getCount();
    assertEquals(preGetRegionClears, postGetRegionClears);
    assertEquals(preGetServerClears, postGetServerClears);
  }

  public static List<Throwable> metaCachePreservingExceptions() {
    return Arrays.asList(new RegionOpeningException(" "),
      new RegionTooBusyException("Some old message"), new RpcThrottlingException(" "),
      new MultiActionResultTooLarge(" "), new RetryImmediatelyException(" "),
      new CallQueueTooBigException());
  }

  public static class RegionServerWithFakeRpcServices extends HRegionServer {
    private FakeRSRpcServices rsRpcServices;

    public RegionServerWithFakeRpcServices(Configuration conf)
      throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      this.rsRpcServices = new FakeRSRpcServices(this);
      return rsRpcServices;
    }

    public void setExceptionInjector(ExceptionInjector injector) {
      rsRpcServices.setExceptionInjector(injector);
    }
  }

  public static class FakeRSRpcServices extends RSRpcServices {

    private ExceptionInjector exceptions;

    public FakeRSRpcServices(HRegionServer rs) throws IOException {
      super(rs);
      exceptions = new RoundRobinExceptionInjector();
    }

    public void setExceptionInjector(ExceptionInjector injector) {
      this.exceptions = injector;
    }

    @Override
    public GetResponse get(final RpcController controller, final ClientProtos.GetRequest request)
      throws ServiceException {
      exceptions.throwOnGet(this, request);
      return super.get(controller, request);
    }

    @Override
    public ClientProtos.MutateResponse mutate(final RpcController controller,
      final ClientProtos.MutateRequest request) throws ServiceException {
      exceptions.throwOnMutate(this, request);
      return super.mutate(controller, request);
    }

    @Override
    public ClientProtos.ScanResponse scan(final RpcController controller,
      final ClientProtos.ScanRequest request) throws ServiceException {
      exceptions.throwOnScan(this, request);
      return super.scan(controller, request);
    }
  }

  public static abstract class ExceptionInjector {
    protected boolean isTestTable(FakeRSRpcServices rpcServices,
      HBaseProtos.RegionSpecifier regionSpec) throws ServiceException {
      try {
        return TABLE_NAME
          .equals(rpcServices.getRegion(regionSpec).getTableDescriptor().getTableName());
      } catch (IOException ioe) {
        throw new ServiceException(ioe);
      }
    }

    public abstract void throwOnGet(FakeRSRpcServices rpcServices, ClientProtos.GetRequest request)
      throws ServiceException;

    public abstract void throwOnMutate(FakeRSRpcServices rpcServices,
      ClientProtos.MutateRequest request) throws ServiceException;

    public abstract void throwOnScan(FakeRSRpcServices rpcServices,
      ClientProtos.ScanRequest request) throws ServiceException;
  }

  /**
   * Rotates through the possible cache clearing and non-cache clearing exceptions for requests.
   */
  public static class RoundRobinExceptionInjector extends ExceptionInjector {
    private int numReqs = -1;
    private int expCount = -1;
    private List<Throwable> metaCachePreservingExceptions = metaCachePreservingExceptions();

    @Override
    public void throwOnGet(FakeRSRpcServices rpcServices, ClientProtos.GetRequest request)
      throws ServiceException {
      throwSomeExceptions(rpcServices, request.getRegion());
    }

    @Override
    public void throwOnMutate(FakeRSRpcServices rpcServices, ClientProtos.MutateRequest request)
      throws ServiceException {
      throwSomeExceptions(rpcServices, request.getRegion());
    }

    @Override
    public void throwOnScan(FakeRSRpcServices rpcServices, ClientProtos.ScanRequest request)
      throws ServiceException {
      if (!request.hasScannerId()) {
        // only handle initial scan requests
        throwSomeExceptions(rpcServices, request.getRegion());
      }
    }

    /**
     * Throw some exceptions. Mostly throw exceptions which do not clear meta cache. Periodically
     * throw NotSevingRegionException which clears the meta cache.
     */
    private void throwSomeExceptions(FakeRSRpcServices rpcServices,
      HBaseProtos.RegionSpecifier regionSpec) throws ServiceException {
      if (!isTestTable(rpcServices, regionSpec)) {
        return;
      }

      numReqs++;
      // Succeed every 5 request, throw cache clearing exceptions twice every 5 requests and throw
      // meta cache preserving exceptions otherwise.
      if (numReqs % 5 == 0) {
        return;
      } else if (numReqs % 5 == 1 || numReqs % 5 == 2) {
        throw new ServiceException(new NotServingRegionException());
      }
      // Round robin between different special exceptions.
      // This is not ideal since exception types are not tied to the operation performed here,
      // But, we don't really care here if we throw MultiActionTooLargeException while doing
      // single Gets.
      expCount++;
      Throwable t =
        metaCachePreservingExceptions.get(expCount % metaCachePreservingExceptions.size());
      throw new ServiceException(t);
    }
  }

  /**
   * Throws CallQueueTooBigException for all gets.
   */
  public static class CallQueueTooBigExceptionInjector extends ExceptionInjector {
    @Override
    public void throwOnGet(FakeRSRpcServices rpcServices, ClientProtos.GetRequest request)
      throws ServiceException {
      if (isTestTable(rpcServices, request.getRegion())) {
        throw new ServiceException(new CallQueueTooBigException());
      }
    }

    @Override
    public void throwOnMutate(FakeRSRpcServices rpcServices, ClientProtos.MutateRequest request)
      throws ServiceException {
    }

    @Override
    public void throwOnScan(FakeRSRpcServices rpcServices, ClientProtos.ScanRequest request)
      throws ServiceException {
    }
  }
}
