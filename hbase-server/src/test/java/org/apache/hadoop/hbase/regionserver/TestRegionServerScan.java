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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster.MiniHBaseClusterRegionServer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.io.DeallocateRewriteByteBuffAllocator;
import org.apache.hadoop.hbase.io.hfile.BlockCacheFactory;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

@Category({ RegionServerTests.class, LargeTests.class })
public class TestRegionServerScan {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerScan.class);

  @Rule
  public TestName name = new TestName();

  private static final byte[] CF = Bytes.toBytes("CF");
  private static final byte[] CQ = Bytes.toBytes("CQ");
  private static final byte[] VALUE = new byte[1200];

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Configuration conf = TEST_UTIL.getConfiguration();
  private static Admin admin = null;
  static final TableName tableName = TableName.valueOf("TestRegionServerScan");
  static final byte[] r0 = Bytes.toBytes("row-0");
  static final byte[] r1 = Bytes.toBytes("row-1");
  static final byte[] r2 = Bytes.toBytes("row-2");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    /**
     * Use {@link DeallocateRewriteByteBuffAllocator} to rewrite the bytebuffers right after
     * released.
     */
    conf.set(ByteBuffAllocator.BYTEBUFF_ALLOCATOR_CLASS,
      DeallocateRewriteByteBuffAllocator.class.getName());
    conf.setBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    conf.setInt(ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY, 0);
    conf.setInt(BlockCacheFactory.BUCKET_CACHE_WRITER_THREADS_KEY, 20);
    conf.setInt(ByteBuffAllocator.BUFFER_SIZE_KEY, 2048);
    conf.set(HConstants.BUCKET_CACHE_IOENGINE_KEY, "offheap");
    conf.setInt(HConstants.BUCKET_CACHE_SIZE_KEY, 64);
    conf.setStrings(HConstants.REGION_SERVER_IMPL, MyRegionServer.class.getName());
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 30 * 60 * 1000);
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 30 * 60 * 1000);

    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 60 * 60 * 1000);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.HBASE_CLIENT_PAUSE, 10000);
    conf.setLong(StoreScanner.STORESCANNER_PREAD_MAX_BYTES, 1024 * 1024 * 1024);
    TEST_UTIL.startMiniCluster(1);
    admin = TEST_UTIL.getAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testScannWhenRpcCallContextNull() throws Exception {
    ResultScanner resultScanner = null;
    Table table = null;
    try {
      table =
          TEST_UTIL.createTable(tableName, new byte[][] { CF }, 1, 1024, null);
      putToTable(table, r0);
      putToTable(table, r1);
      putToTable(table, r2);

      admin.flush(table.getName());

      Scan scan = new Scan();
      scan.setCaching(2);
      scan.withStartRow(r0, true).withStopRow(r2, true);

      MyRSRpcServices.inTest = true;
      resultScanner = table.getScanner(scan);
      Result result = resultScanner.next();
      byte[] rowKey = result.getRow();
      assertTrue(Bytes.equals(r0, rowKey));

      result = resultScanner.next();
      rowKey = result.getRow();
      assertTrue(Bytes.equals(r1, rowKey));

      result = resultScanner.next();
      rowKey = result.getRow();
      assertTrue(Bytes.equals(r2, rowKey));
      assertNull(resultScanner.next());
      assertTrue(MyRSRpcServices.exceptionRef.get() == null);
    } finally {
      MyRSRpcServices.inTest = false;
      if (resultScanner != null) {
        resultScanner.close();
      }
      if (table != null) {
        table.close();
      }
    }
  }

  private static void putToTable(Table table, byte[] rowkey) throws IOException {
    Put put = new Put(rowkey);
    put.addColumn(CF, CQ, VALUE);
    table.put(put);
  }

  private static class MyRegionServer extends MiniHBaseClusterRegionServer {
    public MyRegionServer(Configuration conf) throws IOException, InterruptedException {
      super(conf);
    }

    @Override
    protected RSRpcServices createRpcServices() throws IOException {
      return new MyRSRpcServices(this);
    }
  }

  private static class MyRSRpcServices extends RSRpcServices {
    private static AtomicReference<Throwable> exceptionRef = new AtomicReference<Throwable>(null);
    private static volatile boolean inTest = false;

    public MyRSRpcServices(HRegionServer rs) throws IOException {
      super(rs);
    }

    @Override
    public ScanResponse scan(RpcController controller, ScanRequest request)
        throws ServiceException {
      try {
        if (!inTest) {
          return super.scan(controller, request);
        }

        HRegion region = null;
        if (request.hasRegion()) {
          region = this.getRegion(request.getRegion());
        }

        if (region != null
            && !tableName.equals(region.getTableDescriptor().getTableName())) {
          return super.scan(controller, request);
        }

        ScanResponse result = null;
        //Simulate RpcCallContext is null for test.
        Optional<RpcCall> rpcCall = RpcServer.unsetCurrentCall();
        try {
          result = super.scan(controller, request);
        } finally {
          rpcCall.ifPresent(RpcServer::setCurrentCall);
        }
        return result;
      } catch (Throwable e) {
        exceptionRef.set(e);
        throw new ServiceException(e);
      }
    }
  }

}
