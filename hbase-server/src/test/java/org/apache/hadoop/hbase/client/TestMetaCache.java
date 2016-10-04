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

import org.apache.hadoop.hbase.shaded.com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({MediumTests.class, ClientTests.class})
public class TestMetaCache {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("test_table");
  private static final byte[] FAMILY = Bytes.toBytes("fam1");
  private static final byte[] QUALIFIER = Bytes.toBytes("qual");

  private static HRegionServer badRS;

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
      append.add(FAMILY, QUALIFIER, Bytes.toBytes(11));
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
      add(new RegionTooBusyException());
      add(new ThrottlingException(" "));
      add(new MultiActionResultTooLarge(" "));
      add(new RetryImmediatelyException(" "));
      add(new CallQueueTooBigException());
    }};
  }

  public static class RegionServerWithFakeRpcServices extends HRegionServer {
    private FakeRSRpcServices rsRpcServices;

    public RegionServerWithFakeRpcServices(Configuration conf, CoordinatedStateManager cp)
      throws IOException, InterruptedException {
      super(conf, cp);
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

    public Region getRegion(
        final HBaseProtos.RegionSpecifier regionSpecifier) throws IOException {
      return super.getRegion(regionSpecifier);
    }
  }

  public static abstract class ExceptionInjector {
    protected boolean isTestTable(FakeRSRpcServices rpcServices,
                                  HBaseProtos.RegionSpecifier regionSpec) throws ServiceException {
      try {
        return TABLE_NAME.equals(
            rpcServices.getRegion(regionSpec).getTableDesc().getTableName());
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

    public void throwOnGet(FakeRSRpcServices rpcServices, ClientProtos.GetRequest request)
        throws ServiceException {
      throwSomeExceptions(rpcServices, request.getRegion());
    }

    public void throwOnMutate(FakeRSRpcServices rpcServices, ClientProtos.MutateRequest request)
        throws ServiceException {
      throwSomeExceptions(rpcServices, request.getRegion());
    }

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
}