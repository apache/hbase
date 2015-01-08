/*
 *
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.CountRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.CountResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.HelloRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.HelloResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.IncrementCountRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.IncrementCountResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.NoopRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.NoopResponse;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.PingRequest;
import org.apache.hadoop.hbase.coprocessor.protobuf.generated.PingProtos.PingResponse;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;

@Category({RegionServerTests.class, MediumTests.class})
public class TestServerCustomProtocol {
  private static final Log LOG = LogFactory.getLog(TestServerCustomProtocol.class);
  static final String WHOAREYOU = "Who are you?";
  static final String NOBODY = "nobody";
  static final String HELLO = "Hello, ";

  /* Test protocol implementation */
  public static class PingHandler extends PingProtos.PingService
  implements Coprocessor, CoprocessorService {
    private int counter = 0;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
      if (env instanceof RegionCoprocessorEnvironment) return;
      throw new CoprocessorException("Must be loaded on a table region!");
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
      // Nothing to do.
    }

    @Override
    public void ping(RpcController controller, PingRequest request,
        RpcCallback<PingResponse> done) {
      this.counter++;
      done.run(PingResponse.newBuilder().setPong("pong").build());
    }

    @Override
    public void count(RpcController controller, CountRequest request,
        RpcCallback<CountResponse> done) {
      done.run(CountResponse.newBuilder().setCount(this.counter).build());
    }

    @Override
    public void increment(RpcController controller,
        IncrementCountRequest request, RpcCallback<IncrementCountResponse> done) {
      this.counter += request.getDiff();
      done.run(IncrementCountResponse.newBuilder().setCount(this.counter).build());
    }

    @Override
    public void hello(RpcController controller, HelloRequest request,
        RpcCallback<HelloResponse> done) {
      if (!request.hasName()) done.run(HelloResponse.newBuilder().setResponse(WHOAREYOU).build());
      else if (request.getName().equals(NOBODY)) done.run(HelloResponse.newBuilder().build());
      else done.run(HelloResponse.newBuilder().setResponse(HELLO + request.getName()).build());
    }

    @Override
    public void noop(RpcController controller, NoopRequest request,
        RpcCallback<NoopResponse> done) {
      done.run(NoopResponse.newBuilder().build());
    }

    @Override
    public Service getService() {
      return this;
    }
  }

  private static final TableName TEST_TABLE = TableName.valueOf("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");

  private static final byte[] ROW_AB = Bytes.toBytes("abb");
  private static final byte[] ROW_BC = Bytes.toBytes("bcc");

  private static HBaseTestingUtility util = new HBaseTestingUtility();

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.getConfiguration().set(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      PingHandler.class.getName());
    util.startMiniCluster();
  }

  @Before
  public void before()  throws Exception {
    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY,
      new byte[][]{ HConstants.EMPTY_BYTE_ARRAY, ROW_B, ROW_C});

    Put puta = new Put( ROW_A );
    puta.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(puta);

    Put putb = new Put( ROW_B );
    putb.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(putb);

    Put putc = new Put( ROW_C );
    putc.add(TEST_FAMILY, Bytes.toBytes("col1"), Bytes.toBytes(1));
    table.put(putc);
  }

  @After
  public void after() throws Exception {
    util.deleteTable(TEST_TABLE);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testSingleProxy() throws Throwable {
    Table table = util.getConnection().getTable(TEST_TABLE);
    Map<byte [], String> results = ping(table, null, null);
    // There are three regions so should get back three results.
    assertEquals(3, results.size());
    for (Map.Entry<byte [], String> e: results.entrySet()) {
      assertEquals("Invalid custom protocol response", "pong", e.getValue());
    }
    hello(table, "George", HELLO + "George");
    LOG.info("Did george");
    hello(table, null, "Who are you?");
    LOG.info("Who are you");
    hello(table, NOBODY, null);
    LOG.info(NOBODY);
    Map<byte [], Integer> intResults = table.coprocessorService(PingProtos.PingService.class,
      null, null,
      new Batch.Call<PingProtos.PingService, Integer>() {
        @Override
        public Integer call(PingProtos.PingService instance) throws IOException {
          BlockingRpcCallback<PingProtos.CountResponse> rpcCallback =
            new BlockingRpcCallback<PingProtos.CountResponse>();
          instance.count(null, PingProtos.CountRequest.newBuilder().build(), rpcCallback);
          return rpcCallback.get().getCount();
        }
      });
    int count = -1;
    for (Map.Entry<byte [], Integer> e: intResults.entrySet()) {
      assertTrue(e.getValue() > 0);
      count = e.getValue();
    }
    final int diff = 5;
    intResults = table.coprocessorService(PingProtos.PingService.class,
      null, null,
      new Batch.Call<PingProtos.PingService, Integer>() {
        @Override
        public Integer call(PingProtos.PingService instance) throws IOException {
          BlockingRpcCallback<PingProtos.IncrementCountResponse> rpcCallback =
            new BlockingRpcCallback<PingProtos.IncrementCountResponse>();
          instance.increment(null, PingProtos.IncrementCountRequest.newBuilder().setDiff(diff).build(),
            rpcCallback);
          return rpcCallback.get().getCount();
        }
      });
    // There are three regions so should get back three results.
    assertEquals(3, results.size());
    for (Map.Entry<byte [], Integer> e: intResults.entrySet()) {
      assertEquals(e.getValue().intValue(), count + diff);
    }
    table.close();
  }

  private Map<byte [], String> hello(final Table table, final String send, final String response)
  throws ServiceException, Throwable {
    Map<byte [], String> results = hello(table, send);
    for (Map.Entry<byte [], String> e: results.entrySet()) {
      assertEquals("Invalid custom protocol response", response, e.getValue());
    }
    return results;
  }

  private Map<byte [], String> hello(final Table table, final String send)
  throws ServiceException, Throwable {
    return hello(table, send, null, null);
  }

  private Map<byte [], String> hello(final Table table, final String send, final byte [] start,
      final byte [] end)
  throws ServiceException, Throwable {
    return table.coprocessorService(PingProtos.PingService.class,
        start, end,
        new Batch.Call<PingProtos.PingService, String>() {
          @Override
          public String call(PingProtos.PingService instance) throws IOException {
            BlockingRpcCallback<PingProtos.HelloResponse> rpcCallback =
              new BlockingRpcCallback<PingProtos.HelloResponse>();
            PingProtos.HelloRequest.Builder builder = PingProtos.HelloRequest.newBuilder();
            if (send != null) builder.setName(send);
            instance.hello(null, builder.build(), rpcCallback);
            PingProtos.HelloResponse r = rpcCallback.get();
            return r != null && r.hasResponse()? r.getResponse(): null;
          }
        });
  }

  private Map<byte [], String> compoundOfHelloAndPing(final Table table, final byte [] start,
      final byte [] end)
  throws ServiceException, Throwable {
    return table.coprocessorService(PingProtos.PingService.class,
        start, end,
        new Batch.Call<PingProtos.PingService, String>() {
          @Override
          public String call(PingProtos.PingService instance) throws IOException {
            BlockingRpcCallback<PingProtos.HelloResponse> rpcCallback =
              new BlockingRpcCallback<PingProtos.HelloResponse>();
            PingProtos.HelloRequest.Builder builder = PingProtos.HelloRequest.newBuilder();
            // Call ping on same instance.  Use result calling hello on same instance.
            builder.setName(doPing(instance));
            instance.hello(null, builder.build(), rpcCallback);
            PingProtos.HelloResponse r = rpcCallback.get();
            return r != null && r.hasResponse()? r.getResponse(): null;
          }
        });
  }

  private Map<byte [], String> noop(final Table table, final byte [] start,
      final byte [] end)
  throws ServiceException, Throwable {
    return table.coprocessorService(PingProtos.PingService.class, start, end,
        new Batch.Call<PingProtos.PingService, String>() {
          @Override
          public String call(PingProtos.PingService instance) throws IOException {
            BlockingRpcCallback<PingProtos.NoopResponse> rpcCallback =
              new BlockingRpcCallback<PingProtos.NoopResponse>();
            PingProtos.NoopRequest.Builder builder = PingProtos.NoopRequest.newBuilder();
            instance.noop(null, builder.build(), rpcCallback);
            rpcCallback.get();
            // Looks like null is expected when void.  That is what the test below is looking for
            return null;
          }
        });
  }

  @Test
  public void testSingleMethod() throws Throwable {
    try (HTable table = (HTable) util.getConnection().getTable(TEST_TABLE)) {
      RegionLocator locator = table.getRegionLocator();
      Map<byte [], String> results = table.coprocessorService(PingProtos.PingService.class,
        null, ROW_A,
        new Batch.Call<PingProtos.PingService, String>() {
          @Override
          public String call(PingProtos.PingService instance) throws IOException {
            BlockingRpcCallback<PingProtos.PingResponse> rpcCallback =
              new BlockingRpcCallback<PingProtos.PingResponse>();
            instance.ping(null, PingProtos.PingRequest.newBuilder().build(), rpcCallback);
            return rpcCallback.get().getPong();
          }
        });
      // Should have gotten results for 1 of the three regions only since we specified
      // rows from 1 region
      assertEquals(1, results.size());
      verifyRegionResults(locator, results, ROW_A);
  
      final String name = "NAME";
      results = hello(table, name, null, ROW_A);
      // Should have gotten results for 1 of the three regions only since we specified
      // rows from 1 region
      assertEquals(1, results.size());
      verifyRegionResults(locator, results, "Hello, NAME", ROW_A);
    }
  }

  @Test
  public void testRowRange() throws Throwable {
    try (HTable table = (HTable) util.getConnection().getTable(TEST_TABLE)) {
      RegionLocator locator = table.getRegionLocator();
      for (Entry<HRegionInfo, ServerName> e: table.getRegionLocations().entrySet()) {
        LOG.info("Region " + e.getKey().getRegionNameAsString() + ", servername=" + e.getValue());
      }
      // Here are what regions looked like on a run:
      //
      // test,,1355943549657.c65d4822d8bdecc033a96451f3a0f55d.
      // test,bbb,1355943549661.110393b070dd1ed93441e0bc9b3ffb7e.
      // test,ccc,1355943549665.c3d6d125141359cbbd2a43eaff3cdf74.
  
      Map<byte [], String> results = ping(table, null, ROW_A);
      // Should contain first region only.
      assertEquals(1, results.size());
      verifyRegionResults(locator, results, ROW_A);
  
      // Test start row + empty end
      results = ping(table, ROW_BC, null);
      assertEquals(2, results.size());
      // should contain last 2 regions
      HRegionLocation loc = table.getRegionLocation(ROW_A, true);
      assertNull("Should be missing region for row aaa (prior to start row)",
        results.get(loc.getRegionInfo().getRegionName()));
      verifyRegionResults(locator, results, ROW_B);
      verifyRegionResults(locator, results, ROW_C);
  
      // test empty start + end
      results = ping(table, null, ROW_BC);
      // should contain the first 2 regions
      assertEquals(2, results.size());
      verifyRegionResults(locator, results, ROW_A);
      verifyRegionResults(locator, results, ROW_B);
      loc = table.getRegionLocation(ROW_C, true);
      assertNull("Should be missing region for row ccc (past stop row)",
          results.get(loc.getRegionInfo().getRegionName()));
  
      // test explicit start + end
      results = ping(table, ROW_AB, ROW_BC);
      // should contain first 2 regions
      assertEquals(2, results.size());
      verifyRegionResults(locator, results, ROW_A);
      verifyRegionResults(locator, results, ROW_B);
      loc = table.getRegionLocation(ROW_C, true);
      assertNull("Should be missing region for row ccc (past stop row)",
          results.get(loc.getRegionInfo().getRegionName()));
  
      // test single region
      results = ping(table, ROW_B, ROW_BC);
      // should only contain region bbb
      assertEquals(1, results.size());
      verifyRegionResults(locator, results, ROW_B);
      loc = table.getRegionLocation(ROW_A, true);
      assertNull("Should be missing region for row aaa (prior to start)",
          results.get(loc.getRegionInfo().getRegionName()));
      loc = table.getRegionLocation(ROW_C, true);
      assertNull("Should be missing region for row ccc (past stop row)",
          results.get(loc.getRegionInfo().getRegionName()));
    }
  }

  private Map<byte [], String> ping(final Table table, final byte [] start, final byte [] end)
  throws ServiceException, Throwable {
    return table.coprocessorService(PingProtos.PingService.class, start, end,
      new Batch.Call<PingProtos.PingService, String>() {
        @Override
        public String call(PingProtos.PingService instance) throws IOException {
          return doPing(instance);
        }
      });
  }

  private static String doPing(PingProtos.PingService instance) throws IOException {
    BlockingRpcCallback<PingProtos.PingResponse> rpcCallback =
        new BlockingRpcCallback<PingProtos.PingResponse>();
      instance.ping(null, PingProtos.PingRequest.newBuilder().build(), rpcCallback);
      return rpcCallback.get().getPong();
  }

  @Test
  public void testCompoundCall() throws Throwable {
    try (HTable table = (HTable) util.getConnection().getTable(TEST_TABLE)) {
      RegionLocator locator = table.getRegionLocator();
      Map<byte [], String> results = compoundOfHelloAndPing(table, ROW_A, ROW_C);
      verifyRegionResults(locator, results, "Hello, pong", ROW_A);
      verifyRegionResults(locator, results, "Hello, pong", ROW_B);
      verifyRegionResults(locator, results, "Hello, pong", ROW_C);
    }
  }

  @Test
  public void testNullCall() throws Throwable {
    try(HTable table = (HTable) util.getConnection().getTable(TEST_TABLE)) {
      RegionLocator locator = table.getRegionLocator();
      Map<byte[],String> results = hello(table, null, ROW_A, ROW_C);
      verifyRegionResults(locator, results, "Who are you?", ROW_A);
      verifyRegionResults(locator, results, "Who are you?", ROW_B);
      verifyRegionResults(locator, results, "Who are you?", ROW_C);
    }
  }

  @Test
  public void testNullReturn() throws Throwable {
    try (HTable table = (HTable) util.getConnection().getTable(TEST_TABLE)) {
      RegionLocator locator = table.getRegionLocator();
      Map<byte[],String> results = hello(table, "nobody", ROW_A, ROW_C);
      verifyRegionResults(locator, results, null, ROW_A);
      verifyRegionResults(locator, results, null, ROW_B);
      verifyRegionResults(locator, results, null, ROW_C);
    }
  }

  @Test
  public void testEmptyReturnType() throws Throwable {
    try (HTable table = (HTable) util.getConnection().getTable(TEST_TABLE)) {
      Map<byte[],String> results = noop(table, ROW_A, ROW_C);
      assertEquals("Should have results from three regions", 3, results.size());
      // all results should be null
      for (Object v : results.values()) {
        assertNull(v);
      }
    }
  }

  private void verifyRegionResults(RegionLocator table,
      Map<byte[],String> results, byte[] row) throws Exception {
    verifyRegionResults(table, results, "pong", row);
  }

  private void verifyRegionResults(RegionLocator regionLocator,
      Map<byte[], String> results, String expected, byte[] row)
  throws Exception {
    for (Map.Entry<byte [], String> e: results.entrySet()) {
      LOG.info("row=" + Bytes.toString(row) + ", expected=" + expected +
       ", result key=" + Bytes.toString(e.getKey()) +
       ", value=" + e.getValue());
    }
    HRegionLocation loc = regionLocator.getRegionLocation(row, true);
    byte[] region = loc.getRegionInfo().getRegionName();
    assertTrue("Results should contain region " +
      Bytes.toStringBinary(region) + " for row '" + Bytes.toStringBinary(row)+ "'",
      results.containsKey(region));
    assertEquals("Invalid result for row '"+Bytes.toStringBinary(row)+"'",
      expected, results.get(region));
  }
}
