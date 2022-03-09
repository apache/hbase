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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class, ClientTests.class})
public class TestMetaCache {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaCache.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("test_table");
  private static final byte[] FAMILY = Bytes.toBytes("fam1");
  private static final byte[] QUALIFIER = Bytes.toBytes("qual");
  private static HRegionServer badRS;
  private static final Logger LOG = LoggerFactory.getLogger(TestMetaCache.class);

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(HConstants.REGION_SERVER_IMPL,
        RegionServerWithFakeRpcServices.class.getName());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME.META_TABLE_NAME);
    badRS = TEST_UTIL.getHBaseCluster().getRegionServer(0);
    assertTrue(badRS.getRSRpcServices() instanceof FakeRSRpcServices);
    HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    fam.setMaxVersions(2);
    table.addFamily(fam);
    TEST_UTIL.createTable(table, null);
  }


  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testPreserveMetaCacheOnException() throws Exception {
    ((FakeRSRpcServices)badRS.getRSRpcServices()).setExceptionInjector(
        new RoundRobinExceptionInjector());
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set("hbase.client.retries.number", "1");
    ConnectionImplementation conn =
        (ConnectionImplementation) ConnectionFactory.createConnection(conf);
    try {
      Table table = conn.getTable(TABLE_NAME);
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
          assertNull(conn.getCachedLocation(TABLE_NAME, row));
        } else if (success) {
          assertNotNull(conn.getCachedLocation(TABLE_NAME, row));
        }
      }
    } finally {
      conn.close();
    }
  }

  @Test
  public void testClearsCacheOnScanException() throws Exception {
    ((FakeRSRpcServices)badRS.getRSRpcServices()).setExceptionInjector(
      new RoundRobinExceptionInjector());
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set("hbase.client.retries.number", "1");

    try (ConnectionImplementation conn =
      (ConnectionImplementation) ConnectionFactory.createConnection(conf);
      Table table = conn.getTable(TABLE_NAME)) {

      byte[] row = Bytes.toBytes("row2");

      Put put = new Put(row);
      put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(10));

      Scan scan = new Scan();
      scan.withStartRow(row);
      scan.setLimit(1);
      scan.setCaching(1);

      populateCache(table, row);
      assertNotNull(conn.getCachedLocation(TABLE_NAME, row));
      assertTrue(executeUntilCacheClearingException(table, scan));
      assertNull(conn.getCachedLocation(TABLE_NAME, row));

      // repopulate cache so we can test with reverse scan too
      populateCache(table, row);
      assertNotNull(conn.getCachedLocation(TABLE_NAME, row));

      // run with reverse scan
      scan.setReversed(true);
      assertTrue(executeUntilCacheClearingException(table, scan));
      assertNull(conn.getCachedLocation(TABLE_NAME, row));
    }
  }

  private void populateCache(Table table, byte[] row) {
    for (int i = 0; i < 50; i++) {
      try {
        table.get(new Get(row));
        return;
      } catch (Exception e) {
        // pass, we just want this to succeed so that region location will be cached
      }
    }
  }

  private boolean executeUntilCacheClearingException(Table table, Scan scan) {
    for (int i = 0; i < 50; i++) {
      try {
        try (ResultScanner scanner = table.getScanner(scan)) {
          scanner.next();
        }
      } catch (Exception ex) {
        // Only keep track of the last exception that updated the meta cache
        if (ClientExceptionsUtil.isMetaClearingException(ex)) {
          return true;
        }
      }
    }
    return false;
  }

  @Test
  public void testCacheClearingOnCallQueueTooBig() throws Exception {
    ((FakeRSRpcServices)badRS.getRSRpcServices()).setExceptionInjector(
        new CallQueueTooBigExceptionInjector());
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.set("hbase.client.retries.number", "2");
    conf.set(MetricsConnection.CLIENT_SIDE_METRICS_ENABLED_KEY, "true");
    ConnectionImplementation conn =
        (ConnectionImplementation) ConnectionFactory.createConnection(conf);
    try {
      Table table = conn.getTable(TABLE_NAME);
      byte[] row = Bytes.toBytes("row1");

      Put put = new Put(row);
      put.addColumn(FAMILY, QUALIFIER, Bytes.toBytes(10));
      table.put(put);

      // obtain the client metrics
      MetricsConnection metrics = conn.getConnectionMetrics();
      long preGetRegionClears = metrics.metaCacheNumClearRegion.getCount();
      long preGetServerClears = metrics.metaCacheNumClearServer.getCount();

      // attempt a get on the test table
      Get get = new Get(row);
      try {
        table.get(get);
        fail("Expected CallQueueTooBigException");
      } catch (RetriesExhaustedException ree) {
        // expected
      }

      // verify that no cache clearing took place
      long postGetRegionClears = metrics.metaCacheNumClearRegion.getCount();
      long postGetServerClears = metrics.metaCacheNumClearServer.getCount();
      assertEquals(preGetRegionClears, postGetRegionClears);
      assertEquals(preGetServerClears, postGetServerClears);
    } finally {
      conn.close();
    }
  }

  public static List<Throwable> metaCachePreservingExceptions() {
    return new ArrayList<Throwable>() {{
        add(new RegionOpeningException(" "));
        add(new RegionTooBusyException("Some old message"));
        add(new RpcThrottlingException(" "));
        add(new MultiActionResultTooLarge(" "));
        add(new RetryImmediatelyException(" "));
        add(new CallQueueTooBigException());
    }};
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
    public GetResponse get(final RpcController controller,
                           final ClientProtos.GetRequest request) throws ServiceException {
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
        return TABLE_NAME.equals(
            rpcServices.getRegion(regionSpec).getTableDescriptor().getTableName());
      } catch (IOException ioe) {
        throw new ServiceException(ioe);
      }
    }

    public abstract void throwOnGet(FakeRSRpcServices rpcServices, ClientProtos.GetRequest request)
        throws ServiceException;

    public abstract void throwOnMutate(FakeRSRpcServices rpcServices, ClientProtos.MutateRequest request)
        throws ServiceException;

    public abstract void throwOnScan(FakeRSRpcServices rpcServices, ClientProtos.ScanRequest request)
        throws ServiceException;
  }

  /**
   * Rotates through the possible cache clearing and non-cache clearing exceptions
   * for requests.
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
     * Throw some exceptions. Mostly throw exceptions which do not clear meta cache.
     * Periodically throw NotSevingRegionException which clears the meta cache.
     * @throws ServiceException
     */
    private void throwSomeExceptions(FakeRSRpcServices rpcServices,
                                     HBaseProtos.RegionSpecifier regionSpec)
        throws ServiceException {
      if (!isTestTable(rpcServices, regionSpec)) {
        return;
      }

      numReqs++;
      // Succeed every 5 request, throw cache clearing exceptions twice every 5 requests and throw
      // meta cache preserving exceptions otherwise.
      if (numReqs % 5 ==0) {
        return;
      } else if (numReqs % 5 == 1 || numReqs % 5 == 2) {
        throw new ServiceException(new NotServingRegionException());
      }
      // Round robin between different special exceptions.
      // This is not ideal since exception types are not tied to the operation performed here,
      // But, we don't really care here if we throw MultiActionTooLargeException while doing
      // single Gets.
      expCount++;
      Throwable t = metaCachePreservingExceptions.get(
          expCount % metaCachePreservingExceptions.size());
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

  @Test
  public void testUserRegionLockThrowsException() throws IOException, InterruptedException {
    ((FakeRSRpcServices)badRS.getRSRpcServices()).setExceptionInjector(new LockSleepInjector());
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 0);
    conf.setLong(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 2000);
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 2000);

    try (ConnectionImplementation conn =
            (ConnectionImplementation) ConnectionFactory.createConnection(conf)) {
      ClientThread client1 = new ClientThread(conn);
      ClientThread client2 = new ClientThread(conn);
      client1.start();
      client2.start();
      client1.join();
      client2.join();
      // One thread will get the lock but will sleep in  LockExceptionInjector#throwOnScan and
      // eventually fail since the sleep time is more than hbase client scanner timeout period.
      // Other thread will wait to acquire userRegionLock.
      // Have no idea which thread will be scheduled first. So need to check both threads.

      // Both the threads will throw exception. One thread will throw exception since after
      // acquiring user region lock, it is sleeping for 5 seconds when the scanner time out period
      // is 2 seconds.
      // Other thread will throw exception since it was not able to get hold of user region lock
      // within meta operation timeout period.
      assertNotNull(client1.getException());
      assertNotNull(client2.getException());

      assertTrue(client1.getException() instanceof LockTimeoutException
          ^ client2.getException() instanceof LockTimeoutException);
    }
  }

  private final class ClientThread extends Thread {
    private Exception exception;
    private ConnectionImplementation connection;

    private ClientThread(ConnectionImplementation connection) {
      this.connection = connection;
    }
    @Override
    public void run() {
      byte[] currentKey = HConstants.EMPTY_START_ROW;
      try {
        connection.getRegionLocation(TABLE_NAME, currentKey, true);
      } catch (IOException e) {
        LOG.error("Thread id: " + this.getId() + "  exception: ", e);
        this.exception = e;
      }
    }
    public Exception getException() {
      return exception;
    }
  }

  public static class LockSleepInjector extends ExceptionInjector {
    @Override
    public void throwOnScan(FakeRSRpcServices rpcServices, ClientProtos.ScanRequest request) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        LOG.info("Interrupted exception", e);
      }
    }

    @Override
    public void throwOnGet(FakeRSRpcServices rpcServices, ClientProtos.GetRequest request) { }

    @Override
    public void throwOnMutate(FakeRSRpcServices rpcServices, ClientProtos.MutateRequest request) { }
  }
}
