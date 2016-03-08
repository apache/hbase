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

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

@Category({MediumTests.class, ClientTests.class})
public class TestMetaCache {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("test_table");
  private static final byte[] FAMILY = Bytes.toBytes("fam1");
  private static final byte[] QUALIFIER = Bytes.toBytes("qual");
  private ConnectionManager.HConnectionImplementation conn;
  private HRegionServer badRS;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.set("hbase.client.retries.number", "1");
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME.META_TABLE_NAME);
  }


  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setup() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();

    cluster.getConfiguration().setStrings(HConstants.REGION_SERVER_IMPL,
      RegionServerWithFakeRpcServices.class.getName());
    JVMClusterUtil.RegionServerThread rsThread = cluster.startRegionServer();
    rsThread.waitForServerOnline();
    badRS = rsThread.getRegionServer();
    assertTrue(badRS.getRSRpcServices() instanceof FakeRSRpcServices);
    cluster.getConfiguration().setStrings(HConstants.REGION_SERVER_IMPL,
      HRegionServer.class.getName());

    assertEquals(2, cluster.getRegionServerThreads().size());

    conn = (ConnectionManager.HConnectionImplementation)ConnectionFactory.createConnection(
      TEST_UTIL.getConfiguration());
    HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    fam.setMaxVersions(2);
    table.addFamily(fam);
    try (Admin admin = conn.getAdmin()) {
      admin.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    // Nothing to do.
  }

  @Test
  public void testPreserveMetaCacheOnException() throws Exception {
    Table table = conn.getTable(TABLE_NAME);
    byte[] row = badRS.getOnlineRegions(TABLE_NAME).get(0).getRegionInfo().getStartKey();

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
      success =false;
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
      if(exp != null && ClientExceptionsUtil.isMetaClearingException(exp)) {
        assertNull(conn.getCachedLocation(TABLE_NAME, row));
      } else if (success) {
        assertNotNull(conn.getCachedLocation(TABLE_NAME, row));
      }
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

  protected static class RegionServerWithFakeRpcServices extends HRegionServer {

    public RegionServerWithFakeRpcServices(Configuration conf, CoordinatedStateManager cp)
      throws IOException, InterruptedException {
      super(conf, cp);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new FakeRSRpcServices(this);
    }
  }

  protected static class FakeRSRpcServices extends RSRpcServices {

    private int numReqs = -1;
    private int expCount = -1;
    private List<Throwable> metaCachePreservingExceptions = metaCachePreservingExceptions();

    public FakeRSRpcServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public GetResponse get(final RpcController controller,
                           final ClientProtos.GetRequest request) throws ServiceException {
      throwSomeExceptions();
      return super.get(controller, request);
    }

    @Override
    public ClientProtos.MutateResponse mutate(final RpcController controller,
                                              final ClientProtos.MutateRequest request) throws ServiceException {
      throwSomeExceptions();
      return super.mutate(controller, request);
    }

    @Override
    public ClientProtos.ScanResponse scan(final RpcController controller,
                                          final ClientProtos.ScanRequest request) throws ServiceException {
      throwSomeExceptions();
      return super.scan(controller, request);
    }

    /**
     * Throw some exceptions. Mostly throw exceptions which do not clear meta cache.
     * Periodically throw NotSevingRegionException which clears the meta cache.
     * @throws ServiceException
     */
    private void throwSomeExceptions() throws ServiceException {
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
}