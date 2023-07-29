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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.ipc.RpcCall;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

@Category({ ClientTests.class, MediumTests.class })
public class TestRequestAndConnectionAttributes {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRequestAndConnectionAttributes.class);

  private static final Map<String, byte[]> CONNECTION_ATTRIBUTES = new HashMap<>();
  static {
    CONNECTION_ATTRIBUTES.put("clientId", Bytes.toBytes("foo"));
  }
  private static final Map<String, byte[]> REQUEST_ATTRIBUTES = new HashMap<>();
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(100);
  private static final AtomicBoolean REQUEST_ATTRIBUTES_VALIDATED = new AtomicBoolean(false);
  private static final byte[] REQUEST_ATTRIBUTES_TEST_TABLE_CF = Bytes.toBytes("0");
  private static final TableName REQUEST_ATTRIBUTES_TEST_TABLE =
    TableName.valueOf("testRequestAttributes");

  private static HBaseTestingUtility TEST_UTIL = null;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(REQUEST_ATTRIBUTES_TEST_TABLE,
      new byte[][] { REQUEST_ATTRIBUTES_TEST_TABLE_CF }, 1, HConstants.DEFAULT_BLOCKSIZE,
      AttributesCoprocessor.class.getName());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setup() {
    REQUEST_ATTRIBUTES_VALIDATED.getAndSet(false);
  }

  @Test
  public void testConnectionAttributes() throws IOException {
    TableName tableName = TableName.valueOf("testConnectionAttributes");
    TEST_UTIL.createTable(tableName, new byte[][] { Bytes.toBytes("0") }, 1,
      HConstants.DEFAULT_BLOCKSIZE, AttributesCoprocessor.class.getName());

    Configuration conf = TEST_UTIL.getConfiguration();
    try (Connection conn = ConnectionFactory.createConnection(conf, null,
      AuthUtil.loginClient(conf), CONNECTION_ATTRIBUTES); Table table = conn.getTable(tableName)) {
      Result result = table.get(new Get(Bytes.toBytes(0)));
      assertEquals(CONNECTION_ATTRIBUTES.size(), result.size());
      for (Map.Entry<String, byte[]> attr : CONNECTION_ATTRIBUTES.entrySet()) {
        byte[] val = result.getValue(Bytes.toBytes("c"), Bytes.toBytes(attr.getKey()));
        assertEquals(Bytes.toStringBinary(attr.getValue()), Bytes.toStringBinary(val));
      }
    }
  }

  @Test
  public void testRequestAttributesGet() throws IOException {
    addRandomRequestAttributes();

    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(
        conn.getTableBuilder(REQUEST_ATTRIBUTES_TEST_TABLE, EXECUTOR_SERVICE)).build()) {

      table.get(new Get(Bytes.toBytes(0)));
    }

    assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
  }

  @Test
  public void testRequestAttributesMultiGet() throws IOException {
    assertFalse(REQUEST_ATTRIBUTES_VALIDATED.get());
    addRandomRequestAttributes();

    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(
        conn.getTableBuilder(REQUEST_ATTRIBUTES_TEST_TABLE, EXECUTOR_SERVICE)).build()) {
      List<Get> gets = ImmutableList.of(new Get(Bytes.toBytes(0)), new Get(Bytes.toBytes(1)));
      table.get(gets);
    }

    assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
  }

  @Test
  public void testRequestAttributesExists() throws IOException {
    assertFalse(REQUEST_ATTRIBUTES_VALIDATED.get());
    addRandomRequestAttributes();

    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(
        conn.getTableBuilder(REQUEST_ATTRIBUTES_TEST_TABLE, EXECUTOR_SERVICE)).build()) {

      table.exists(new Get(Bytes.toBytes(0)));
    }

    assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
  }

  @Test
  public void testRequestAttributesScan() throws IOException {
    assertFalse(REQUEST_ATTRIBUTES_VALIDATED.get());
    addRandomRequestAttributes();

    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(
        conn.getTableBuilder(REQUEST_ATTRIBUTES_TEST_TABLE, EXECUTOR_SERVICE)).build()) {
      ResultScanner scanner = table.getScanner(new Scan());
      scanner.next();
    }
    assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
  }

  @Test
  public void testRequestAttributesPut() throws IOException {
    assertFalse(REQUEST_ATTRIBUTES_VALIDATED.get());
    addRandomRequestAttributes();

    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(
        conn.getTableBuilder(REQUEST_ATTRIBUTES_TEST_TABLE, EXECUTOR_SERVICE)).build()) {
      Put put = new Put(Bytes.toBytes("a"));
      put.addColumn(REQUEST_ATTRIBUTES_TEST_TABLE_CF, Bytes.toBytes("c"), Bytes.toBytes("v"));
      table.put(put);
    }
    assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
  }

  @Test
  public void testRequestAttributesMultiPut() throws IOException {
    assertFalse(REQUEST_ATTRIBUTES_VALIDATED.get());
    addRandomRequestAttributes();

    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(
        conn.getTableBuilder(REQUEST_ATTRIBUTES_TEST_TABLE, EXECUTOR_SERVICE)).build()) {
      Put put = new Put(Bytes.toBytes("a"));
      put.addColumn(REQUEST_ATTRIBUTES_TEST_TABLE_CF, Bytes.toBytes("c"), Bytes.toBytes("v"));
      table.put(put);
    }
    assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
  }

  @Test
  public void testRequestAttributesCheckAndMutate() throws IOException {
    assertFalse(REQUEST_ATTRIBUTES_VALIDATED.get());
    addRandomRequestAttributes();

    Configuration conf = TEST_UTIL.getConfiguration();
    try (
      Connection conn = ConnectionFactory.createConnection(conf, null, AuthUtil.loginClient(conf),
        CONNECTION_ATTRIBUTES);
      Table table = configureRequestAttributes(
        conn.getTableBuilder(REQUEST_ATTRIBUTES_TEST_TABLE, EXECUTOR_SERVICE)).build()) {
      Put put = new Put(Bytes.toBytes("a"));
      put.addColumn(REQUEST_ATTRIBUTES_TEST_TABLE_CF, Bytes.toBytes("c"), Bytes.toBytes("v"));
      CheckAndMutate checkAndMutate = CheckAndMutate.newBuilder(Bytes.toBytes("a"))
        .ifEquals(REQUEST_ATTRIBUTES_TEST_TABLE_CF, Bytes.toBytes("c"), Bytes.toBytes("v"))
        .build(put);
      table.checkAndMutate(checkAndMutate);
    }
    assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
  }

  @Test
  public void testNoRequestAttributes() throws IOException {
    assertFalse(REQUEST_ATTRIBUTES_VALIDATED.get());
    TableName tableName = TableName.valueOf("testNoRequestAttributesScan");
    TEST_UTIL.createTable(tableName, new byte[][] { Bytes.toBytes("0") }, 1,
      HConstants.DEFAULT_BLOCKSIZE, AttributesCoprocessor.class.getName());

    REQUEST_ATTRIBUTES.clear();
    Configuration conf = TEST_UTIL.getConfiguration();
    try (Connection conn = ConnectionFactory.createConnection(conf, null,
      AuthUtil.loginClient(conf), CONNECTION_ATTRIBUTES)) {
      TableBuilder tableBuilder = conn.getTableBuilder(tableName, null);
      try (Table table = tableBuilder.build()) {
        table.get(new Get(Bytes.toBytes(0)));
        assertTrue(REQUEST_ATTRIBUTES_VALIDATED.get());
      }
    }
  }

  private void addRandomRequestAttributes() {
    REQUEST_ATTRIBUTES.clear();
    int j = Math.max(2, (int) (10 * Math.random()));
    for (int i = 0; i < j; i++) {
      REQUEST_ATTRIBUTES.put(String.valueOf(i), Bytes.toBytes(UUID.randomUUID().toString()));
    }
  }

  private static TableBuilder configureRequestAttributes(TableBuilder tableBuilder) {
    REQUEST_ATTRIBUTES.forEach(tableBuilder::setRequestAttribute);
    return tableBuilder;
  }

  public static class AttributesCoprocessor implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
      List<Cell> result) throws IOException {
      validateRequestAttributes();

      // for connection attrs test
      RpcCall rpcCall = RpcServer.getCurrentCall().get();
      for (HBaseProtos.NameBytesPair attr : rpcCall.getHeader().getAttributeList()) {
        result.add(c.getEnvironment().getCellBuilder().clear().setRow(get.getRow())
          .setFamily(Bytes.toBytes("r")).setQualifier(Bytes.toBytes(attr.getName()))
          .setValue(attr.getValue().toByteArray()).setType(Cell.Type.Put).setTimestamp(1).build());
      }
      for (HBaseProtos.NameBytesPair attr : rpcCall.getConnectionHeader().getAttributeList()) {
        result.add(c.getEnvironment().getCellBuilder().clear().setRow(get.getRow())
          .setFamily(Bytes.toBytes("c")).setQualifier(Bytes.toBytes(attr.getName()))
          .setValue(attr.getValue().toByteArray()).setType(Cell.Type.Put).setTimestamp(1).build());
      }
      result.sort(CellComparator.getInstance());
      c.bypass();
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c,
      InternalScanner s, List<Result> result, int limit, boolean hasNext) throws IOException {
      validateRequestAttributes();
      return hasNext;
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit)
      throws IOException {
      validateRequestAttributes();
    }

    private void validateRequestAttributes() {
      RpcCall rpcCall = RpcServer.getCurrentCall().get();
      List<HBaseProtos.NameBytesPair> attrs = rpcCall.getHeader().getAttributeList();
      if (attrs.size() != REQUEST_ATTRIBUTES.size()) {
        return;
      }
      for (HBaseProtos.NameBytesPair attr : attrs) {
        if (!REQUEST_ATTRIBUTES.containsKey(attr.getName())) {
          return;
        }
        if (!Arrays.equals(REQUEST_ATTRIBUTES.get(attr.getName()), attr.getValue().toByteArray())) {
          return;
        }
      }
      REQUEST_ATTRIBUTES_VALIDATED.getAndSet(true);
    }
  }
}
