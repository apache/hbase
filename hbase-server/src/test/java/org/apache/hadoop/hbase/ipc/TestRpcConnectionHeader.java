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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos;

@Category(SmallTests.class)
public class TestRpcConnectionHeader {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcClientLeaks.class);

  @Rule
  public TestName name = new TestName();

  private final ConnectionId connectionId;

  public TestRpcConnectionHeader() throws IOException {
    User user = User.createUserForTesting(HBaseConfiguration.create(), "test", new String[] {});
    String serviceName = MasterService.getDescriptor().getName();

    connectionId = new ConnectionId(user, serviceName, Address.fromParts("localhost", 12345));
  }

  private class MyRpcConnection extends RpcConnection {
    protected MyRpcConnection(Configuration conf, Map<String, byte[]> connectionAttributes)
      throws IOException {
      super(conf, null, connectionId, "cluster-id", false, null, null, null, null,
        connectionAttributes);
    }

    @Override
    protected void callTimeout(Call call) {
    }

    @Override
    public boolean isActive() {
      return false;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void sendRequest(Call call, HBaseRpcController hrc) throws IOException {
    }

    @Override
    public void cleanupConnection() {
    }
  }

  @Test
  public void testEmptyHeaders() throws IOException {
    Configuration configuration = HBaseConfiguration.create();

    MyRpcConnection connection = new MyRpcConnection(configuration, Map.of());
    RPCProtos.ConnectionHeader connectionHeader = connection.getConnectionHeader();

    assertEquals(0, connectionHeader.getAttributeCount());
  }

  @Test
  public void testConfigHeaders() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.client.header.test", "true");

    MyRpcConnection connection = new MyRpcConnection(configuration, Map.of());
    RPCProtos.ConnectionHeader connectionHeader = connection.getConnectionHeader();

    assertEquals(1, connectionHeader.getAttributeCount());

    HBaseProtos.NameBytesPair attribute = connectionHeader.getAttribute(0);
    assertEquals("hbase.client.header.test", attribute.getName());
    assertEquals("true", attribute.getValue().toStringUtf8());
  }

  @Test
  public void testConfigHeadersNoOverride() throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.client.header.test", "true");
    configuration.set("hbase.client.header.test2", "true");

    Map<String, byte[]> attributes = Map.of("hbase.client.header.test", Bytes.toBytes("false"));

    MyRpcConnection connection = new MyRpcConnection(configuration, attributes);
    RPCProtos.ConnectionHeader connectionHeader = connection.getConnectionHeader();

    assertEquals(2, connectionHeader.getAttributeCount());

    HBaseProtos.NameBytesPair attribute0 = connectionHeader.getAttribute(0);
    assertEquals("hbase.client.header.test", attribute0.getName());
    assertEquals("false", attribute0.getValue().toStringUtf8());

    HBaseProtos.NameBytesPair attribute1 = connectionHeader.getAttribute(1);
    assertEquals("hbase.client.header.test2", attribute1.getName());
    assertEquals("true", attribute1.getValue().toStringUtf8());
  }
}
