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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
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
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

  private static HBaseTestingUtil TEST_UTIL = null;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL = new HBaseTestingUtil();
    TEST_UTIL.getConfiguration().set(RpcControllerFactory.CUSTOM_CONTROLLER_CONF_KEY,
      AttributesRpcControllerFactory.class.getName());
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
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
      assertEquals(REQUEST_ATTRIBUTES.size() + CONNECTION_ATTRIBUTES.size(), result.size());
      for (Map.Entry<String, byte[]> attr : CONNECTION_ATTRIBUTES.entrySet()) {
        byte[] val = result.getValue(Bytes.toBytes("c"), Bytes.toBytes(attr.getKey()));
        assertEquals(Bytes.toStringBinary(attr.getValue()), Bytes.toStringBinary(val));
      }
    }
  }

  @Test
  public void testRequestAttributes() throws IOException {
    TableName tableName = TableName.valueOf("testRequestAttributes");
    TEST_UTIL.createTable(tableName, new byte[][] { Bytes.toBytes("0") }, 1,
      HConstants.DEFAULT_BLOCKSIZE, AttributesCoprocessor.class.getName());

    REQUEST_ATTRIBUTES.clear();
    REQUEST_ATTRIBUTES.put("test1", Bytes.toBytes("a"));
    REQUEST_ATTRIBUTES.put("test2", Bytes.toBytes("b"));

    Configuration conf = TEST_UTIL.getConfiguration();
    try (Connection conn = ConnectionFactory.createConnection(conf, null,
      AuthUtil.loginClient(conf), CONNECTION_ATTRIBUTES); Table table = conn.getTable(tableName)) {

      Result result = table.get(new Get(Bytes.toBytes(0)));
      assertEquals(REQUEST_ATTRIBUTES.size() + CONNECTION_ATTRIBUTES.size(), result.size());
      for (Map.Entry<String, byte[]> attr : REQUEST_ATTRIBUTES.entrySet()) {
        byte[] val = result.getValue(Bytes.toBytes("r"), Bytes.toBytes(attr.getKey()));
        assertEquals(Bytes.toStringBinary(attr.getValue()), Bytes.toStringBinary(val));
      }

      REQUEST_ATTRIBUTES.put("test3", Bytes.toBytes("c"));
      result = table.get(new Get(Bytes.toBytes(0)));
      assertEquals(REQUEST_ATTRIBUTES.size() + CONNECTION_ATTRIBUTES.size(), result.size());
      for (Map.Entry<String, byte[]> attr : REQUEST_ATTRIBUTES.entrySet()) {
        byte[] val = result.getValue(Bytes.toBytes("r"), Bytes.toBytes(attr.getKey()));
        assertEquals(Bytes.toStringBinary(attr.getValue()), Bytes.toStringBinary(val));
      }
    }
  }

  @Test
  public void testNoRequestAttributes() throws IOException {
    TableName tableName = TableName.valueOf("testNoRequestAttributes");
    TEST_UTIL.createTable(tableName, new byte[][] { Bytes.toBytes("0") }, 1,
      HConstants.DEFAULT_BLOCKSIZE, AttributesCoprocessor.class.getName());

    REQUEST_ATTRIBUTES.clear();
    Configuration conf = TEST_UTIL.getConfiguration();
    try (Connection conn = ConnectionFactory.createConnection(conf, null,
      AuthUtil.loginClient(conf), CONNECTION_ATTRIBUTES)) {
      TableBuilder tableBuilder = conn.getTableBuilder(tableName, null);
      try (Table table = tableBuilder.build()) {
        Result result = table.get(new Get(Bytes.toBytes(0)));
        assertEquals(CONNECTION_ATTRIBUTES.size(), result.size());
      }
    }
  }

  public static class AttributesRpcControllerFactory extends RpcControllerFactory {

    public AttributesRpcControllerFactory(Configuration conf) {
      super(conf);
    }

    @Override
    public HBaseRpcController newController() {
      return new AttributesRpcController(super.newController());
    }

    @Override
    public HBaseRpcController newController(CellScanner cellScanner) {
      return new AttributesRpcController(super.newController(cellScanner));
    }

    @Override
    public HBaseRpcController newController(RegionInfo regionInfo, CellScanner cellScanner) {
      return new AttributesRpcController(super.newController(regionInfo, cellScanner));
    }

    @Override
    public HBaseRpcController newController(List<CellScannable> cellIterables) {
      return new AttributesRpcController(super.newController(cellIterables));
    }

    @Override
    public HBaseRpcController newController(RegionInfo regionInfo,
      List<CellScannable> cellIterables) {
      return new AttributesRpcController(super.newController(regionInfo, cellIterables));
    }
  }

  public static class AttributesRpcController extends DelegatingHBaseRpcController {

    public AttributesRpcController(HBaseRpcController delegate) {
      super(delegate);
    }

    @Override
    public Map<String, byte[]> getAttributes() {
      return REQUEST_ATTRIBUTES;
    }
  }

  public static class AttributesCoprocessor implements RegionObserver, RegionCoprocessor {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> c, Get get,
      List<Cell> result) throws IOException {
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
  }
}
