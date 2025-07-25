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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ExtendedCellScannable;
import org.apache.hadoop.hbase.ExtendedCellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SingleProcessHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.DelegatingHBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, MediumTests.class })
public class TestRequestAttributes {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRequestAttributes.class);

  private static final byte[] ROW_KEY1 = Bytes.toBytes("1");
  private static final byte[] ROW_KEY2A = Bytes.toBytes("2A");
  private static final byte[] ROW_KEY2B = Bytes.toBytes("2B");
  private static final byte[] ROW_KEY3 = Bytes.toBytes("3");
  private static final byte[] ROW_KEY4 = Bytes.toBytes("4");
  private static final byte[] ROW_KEY5 = Bytes.toBytes("5");
  private static final byte[] ROW_KEY6 = Bytes.toBytes("6");
  private static final byte[] ROW_KEY7 = Bytes.toBytes("7");
  private static final byte[] ROW_KEY8 = Bytes.toBytes("8");
  private static final byte[] ROW_KEY_FACTORY_GET = Bytes.toBytes("F1");
  private static final byte[] ROW_KEY_FACTORY_SCAN = Bytes.toBytes("F2");
  private static final byte[] ROW_KEY_FACTORY_PUT = Bytes.toBytes("F3");
  private static final byte[] ROW_KEY_FACTORY_AUGMENT = Bytes.toBytes("F4");
  private static final byte[] ROW_KEY_FACTORY_PER_REQUEST = Bytes.toBytes("F5");
  private static final String FACTORY_KEY = "factoryKey";
  private static final byte[] FACTORY_VALUE = Bytes.toBytes("factoryValue");
  private static final String STATIC_KEY = "staticKey";
  private static final byte[] STATIC_VALUE = Bytes.toBytes("staticValue");
  private static final Map<String, byte[]> CONNECTION_ATTRIBUTES = new HashMap<>();
  private static final Map<String, byte[]> REQUEST_ATTRIBUTES_SCAN = addRandomRequestAttributes();
  private static final Map<String, byte[]> REQUEST_ATTRIBUTES_FACTORY_SCAN = new HashMap<>();
  private static final Map<byte[], Map<String, byte[]>> ROW_KEY_TO_REQUEST_ATTRIBUTES =
    new HashMap<>();
  static {
    CONNECTION_ATTRIBUTES.put("clientId", Bytes.toBytes("foo"));
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY1, addRandomRequestAttributes());
    Map<String, byte[]> requestAttributes2 = addRandomRequestAttributes();
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY2A, requestAttributes2);
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY2B, requestAttributes2);
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY3, addRandomRequestAttributes());
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY4, addRandomRequestAttributes());
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY5, addRandomRequestAttributes());
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY6, addRandomRequestAttributes());
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY7, addRandomRequestAttributes());
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY8, new HashMap<String, byte[]>());

    Map<String, byte[]> factoryGetAttrs = new HashMap<>();
    factoryGetAttrs.put(FACTORY_KEY, FACTORY_VALUE);
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY_FACTORY_GET, factoryGetAttrs);

    REQUEST_ATTRIBUTES_FACTORY_SCAN.put(FACTORY_KEY, FACTORY_VALUE);

    Map<String, byte[]> factoryPutAttrs = new HashMap<>();
    factoryPutAttrs.put(FACTORY_KEY, FACTORY_VALUE);
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY_FACTORY_PUT, factoryPutAttrs);

    Map<String, byte[]> factoryAugmentAttrs = new HashMap<>();
    factoryAugmentAttrs.put(STATIC_KEY, STATIC_VALUE);
    factoryAugmentAttrs.put(FACTORY_KEY, FACTORY_VALUE);
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY_FACTORY_AUGMENT, factoryAugmentAttrs);

    Map<String, byte[]> factoryPerRequestAttrs = new HashMap<>();
    factoryPerRequestAttrs.put(FACTORY_KEY, FACTORY_VALUE);
    ROW_KEY_TO_REQUEST_ATTRIBUTES.put(ROW_KEY_FACTORY_PER_REQUEST, factoryPerRequestAttrs);
  }
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(100);
  private static final byte[] FAMILY = Bytes.toBytes("0");
  private static final TableName TABLE_NAME = TableName.valueOf("testRequestAttributes");

  private static final HBaseTestingUtil TEST_UTIL = new HBaseTestingUtil();
  private static SingleProcessHBaseCluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1);
    Table table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY }, 1,
      HConstants.DEFAULT_BLOCKSIZE, AttributesCoprocessor.class.getName());
    table.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    cluster.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRequestAttributesGet() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(conn.getTableBuilder(TABLE_NAME, EXECUTOR_SERVICE),
        ROW_KEY_TO_REQUEST_ATTRIBUTES.get(ROW_KEY1)).build()) {

      table.get(new Get(ROW_KEY1));
    }
  }

  @Test
  public void testRequestAttributesMultiGet() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(conn.getTableBuilder(TABLE_NAME, EXECUTOR_SERVICE),
        ROW_KEY_TO_REQUEST_ATTRIBUTES.get(ROW_KEY2A)).build()) {
      List<Get> gets = List.of(new Get(ROW_KEY2A), new Get(ROW_KEY2B));
      table.get(gets);
    }
  }

  @Test
  public void testRequestAttributesScan() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(conn.getTableBuilder(TABLE_NAME, EXECUTOR_SERVICE),
        REQUEST_ATTRIBUTES_SCAN).build()) {
      ResultScanner scanner = table.getScanner(new Scan());
      scanner.next();
    }
  }

  @Test
  public void testRequestAttributesPut() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(conn.getTableBuilder(TABLE_NAME, EXECUTOR_SERVICE),
        ROW_KEY_TO_REQUEST_ATTRIBUTES.get(ROW_KEY3)).build()) {
      Put put = new Put(ROW_KEY3);
      put.addColumn(FAMILY, Bytes.toBytes("c"), Bytes.toBytes("v"));
      table.put(put);
    }
  }

  @Test
  public void testRequestAttributesMultiPut() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(conn.getTableBuilder(TABLE_NAME, EXECUTOR_SERVICE),
        ROW_KEY_TO_REQUEST_ATTRIBUTES.get(ROW_KEY4)).build()) {
      Put put1 = new Put(ROW_KEY4);
      put1.addColumn(FAMILY, Bytes.toBytes("c1"), Bytes.toBytes("v1"));
      Put put2 = new Put(ROW_KEY4);
      put2.addColumn(FAMILY, Bytes.toBytes("c2"), Bytes.toBytes("v2"));
      table.put(List.of(put1, put2));
    }
  }

  @Test
  public void testRequestAttributesBufferedMutate() throws IOException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      BufferedMutator bufferedMutator =
        conn.getBufferedMutator(configureRequestAttributes(new BufferedMutatorParams(TABLE_NAME),
          ROW_KEY_TO_REQUEST_ATTRIBUTES.get(ROW_KEY5)));) {
      Put put = new Put(ROW_KEY5);
      put.addColumn(FAMILY, Bytes.toBytes("c"), Bytes.toBytes("v"));
      bufferedMutator.mutate(put);
      bufferedMutator.flush();
    }
  }

  @Test
  public void testRequestAttributesExists() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(conn.getTableBuilder(TABLE_NAME, EXECUTOR_SERVICE),
        ROW_KEY_TO_REQUEST_ATTRIBUTES.get(ROW_KEY6)).build()) {

      table.exists(new Get(ROW_KEY6));
    }
  }

  @Test
  public void testRequestAttributesFromRpcController() throws IOException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setClass(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY,
      RequestMetadataControllerFactory.class, RpcControllerFactory.class);
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      BufferedMutator bufferedMutator = conn.getBufferedMutator(TABLE_NAME);) {
      Put put = new Put(ROW_KEY7);
      put.addColumn(FAMILY, Bytes.toBytes("c"), Bytes.toBytes("v"));
      bufferedMutator.mutate(put);
      bufferedMutator.flush();
    }
    conf.unset(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY);
  }

  @Test
  public void testNoRequestAttributes() throws IOException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (Connection conn = ConnectionFactory.createConnection(conf, null,
      AuthUtil.loginClient(conf), CONNECTION_ATTRIBUTES)) {
      TableBuilder tableBuilder = conn.getTableBuilder(TABLE_NAME, null);
      try (Table table = tableBuilder.build()) {
        table.get(new Get(ROW_KEY8));
      }
    }
  }

  @Test
  public void testAsyncRequestAttributesFactoryGet()
    throws IOException, ExecutionException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME).setRequestAttributesFactory(attrs -> {
        Map<String, byte[]> newAttrs = new HashMap<>(attrs);
        newAttrs.put(FACTORY_KEY, FACTORY_VALUE);
        return newAttrs;
      }).build();
      table.get(new Get(ROW_KEY_FACTORY_GET)).get();
    }
  }

  @Test
  public void testAsyncRequestAttributesFactoryScan()
    throws IOException, ExecutionException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME).setRequestAttributesFactory(attrs -> {
        Map<String, byte[]> newAttrs = new HashMap<>(attrs);
        newAttrs.put(FACTORY_KEY, FACTORY_VALUE);
        return newAttrs;
      }).build();
      List<Result> results = table
        .scanAll(
          new Scan().withStartRow(ROW_KEY_FACTORY_SCAN).withStopRow(ROW_KEY_FACTORY_SCAN, true))
        .get();
    }
  }

  @Test
  public void testAsyncRequestAttributesFactoryPut()
    throws IOException, ExecutionException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME).setRequestAttributesFactory(attrs -> {
        Map<String, byte[]> newAttrs = new HashMap<>(attrs);
        newAttrs.put(FACTORY_KEY, FACTORY_VALUE);
        return newAttrs;
      }).build();
      Put put = new Put(ROW_KEY_FACTORY_PUT);
      put.addColumn(FAMILY, Bytes.toBytes("c"), Bytes.toBytes("v"));
      table.put(put).get();
    }
  }

  @Test
  public void testAsyncRequestAttributesFactoryAugmentsStaticAttributes()
    throws IOException, ExecutionException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME)
        .setRequestAttribute(STATIC_KEY, STATIC_VALUE).setRequestAttributesFactory(attrs -> {
          Map<String, byte[]> newAttrs = new HashMap<>(attrs);
          newAttrs.put(FACTORY_KEY, FACTORY_VALUE);
          return newAttrs;
        }).build();
      table.get(new Get(ROW_KEY_FACTORY_AUGMENT)).get();
    }
  }

  @Test
  public void testAsyncRequestAttributesFactoryCalledPerRequest()
    throws IOException, ExecutionException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    AtomicInteger callCount = new AtomicInteger(0);
    try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME).setRequestAttributesFactory(attrs -> {
        callCount.incrementAndGet();
        Map<String, byte[]> newAttrs = new HashMap<>(attrs);
        newAttrs.put(FACTORY_KEY, FACTORY_VALUE);
        return newAttrs;
      }).build();
      table.get(new Get(ROW_KEY_FACTORY_PER_REQUEST)).get();
      table.get(new Get(ROW_KEY_FACTORY_PER_REQUEST)).get();
      table.get(new Get(ROW_KEY_FACTORY_PER_REQUEST)).get();
    }
    assertTrue("Factory should be called at least 3 times", callCount.get() >= 3);
  }

  @Test
  public void testAsyncRequestAttributesFactoryCalledOnInitiatingThread()
    throws IOException, ExecutionException, InterruptedException {
    Configuration conf = TEST_UTIL.getConfiguration();
    Thread testThread = Thread.currentThread();
    AtomicReference<Thread> factoryThread = new AtomicReference<>();
    try (AsyncConnection conn = ConnectionFactory.createAsyncConnection(conf).get()) {
      AsyncTable<?> table = conn.getTableBuilder(TABLE_NAME).setRequestAttributesFactory(attrs -> {
        factoryThread.set(Thread.currentThread());
        Map<String, byte[]> newAttrs = new HashMap<>(attrs);
        newAttrs.put(FACTORY_KEY, FACTORY_VALUE);
        return newAttrs;
      }).build();
      table.get(new Get(ROW_KEY_FACTORY_GET)).get();
    }
    assertEquals("Factory should be called on the initiating thread", testThread,
      factoryThread.get());
  }

  private static Map<String, byte[]> addRandomRequestAttributes() {
    Map<String, byte[]> requestAttributes = new HashMap<>();
    int j = Math.max(2, (int) (10 * Math.random()));
    for (int i = 0; i < j; i++) {
      requestAttributes.put(String.valueOf(i), Bytes.toBytes(UUID.randomUUID().toString()));
    }
    return requestAttributes;
  }

  private static TableBuilder configureRequestAttributes(TableBuilder tableBuilder,
    Map<String, byte[]> requestAttributes) {
    requestAttributes.forEach(tableBuilder::setRequestAttribute);
    return tableBuilder;
  }

  private static BufferedMutatorParams configureRequestAttributes(BufferedMutatorParams params,
    Map<String, byte[]> requestAttributes) {
    requestAttributes.forEach(params::setRequestAttribute);
    return params;
  }

  public static class RequestMetadataControllerFactory extends RpcControllerFactory {

    public RequestMetadataControllerFactory(Configuration conf) {
      super(conf);
    }

    @Override
    public HBaseRpcController newController() {
      return new RequestMetadataController(super.newController());
    }

    @Override
    public HBaseRpcController newController(ExtendedCellScanner cellScanner) {
      return new RequestMetadataController(super.newController(null, cellScanner));
    }

    @Override
    public HBaseRpcController newController(RegionInfo regionInfo,
      ExtendedCellScanner cellScanner) {
      return new RequestMetadataController(super.newController(regionInfo, cellScanner));
    }

    @Override
    public HBaseRpcController newController(final List<ExtendedCellScannable> cellIterables) {
      return new RequestMetadataController(super.newController(null, cellIterables));
    }

    @Override
    public HBaseRpcController newController(RegionInfo regionInfo,
      final List<ExtendedCellScannable> cellIterables) {
      return new RequestMetadataController(super.newController(regionInfo, cellIterables));
    }

    public static class RequestMetadataController extends DelegatingHBaseRpcController {
      private final Map<String, byte[]> requestAttributes;

      RequestMetadataController(HBaseRpcController delegate) {
        super(delegate);
        this.requestAttributes = ROW_KEY_TO_REQUEST_ATTRIBUTES.get(ROW_KEY7);
      }

      @Override
      public Map<String, byte[]> getRequestAttributes() {
        return requestAttributes;
      }
    }
  }

  public static class AttributesCoprocessor implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<? extends RegionCoprocessorEnvironment> c, Get get,
      List<Cell> result) throws IOException {
      if (!isValidRequestAttributes(getRequestAttributesForRowKey(get.getRow()))) {
        throw new IOException("Incorrect request attributes");
      }
    }

    @Override
    public boolean preScannerNext(ObserverContext<? extends RegionCoprocessorEnvironment> c,
      InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
      if (
        !isValidRequestAttributes(REQUEST_ATTRIBUTES_SCAN)
          && !isValidRequestAttributes(REQUEST_ATTRIBUTES_FACTORY_SCAN)
      ) {
        throw new IOException("Incorrect request attributes");
      }
      return hasNext;
    }

    @Override
    public void prePut(ObserverContext<? extends RegionCoprocessorEnvironment> c, Put put,
      WALEdit edit) throws IOException {
      if (!isValidRequestAttributes(getRequestAttributesForRowKey(put.getRow()))) {
        throw new IOException("Incorrect request attributes");
      }
    }

    private Map<String, byte[]> getRequestAttributesForRowKey(byte[] rowKey) {
      for (byte[] byteArray : ROW_KEY_TO_REQUEST_ATTRIBUTES.keySet()) {
        if (Arrays.equals(byteArray, rowKey)) {
          return ROW_KEY_TO_REQUEST_ATTRIBUTES.get(byteArray);
        }
      }
      return null;
    }

    private boolean isValidRequestAttributes(Map<String, byte[]> requestAttributes) {
      RpcCall rpcCall = RpcServer.getCurrentCall().get();
      Map<String, byte[]> attrs = rpcCall.getRequestAttributes();
      if (attrs.size() != requestAttributes.size()) {
        return false;
      }
      for (Map.Entry<String, byte[]> attr : attrs.entrySet()) {
        if (!requestAttributes.containsKey(attr.getKey())) {
          return false;
        }
        if (!Arrays.equals(requestAttributes.get(attr.getKey()), attr.getValue())) {
          return false;
        }
      }
      return true;
    }
  }
}
