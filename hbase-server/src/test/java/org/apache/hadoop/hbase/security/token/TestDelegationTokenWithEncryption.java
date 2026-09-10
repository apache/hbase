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
package org.apache.hadoop.hbase.security.token;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.stream.Stream;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestExtension;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
public class TestDelegationTokenWithEncryption extends SecureTestCluster {

  @BeforeAll
  public static void setUp() throws Exception {
    // enable rpc encryption
    TEST_UTIL.getConfiguration().set("hbase.rpc.protection", "privacy");
    SecureTestCluster.setUpCluster();
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Token<? extends TokenIdentifier> token = ClientTokenUtil.obtainToken(conn);
      UserGroupInformation.getCurrentUser().addToken(token);
    }
  }

  static Stream<String> parameters() {
    // Client connection supports only non-blocking RPCs (due to master registry restriction), hence
    // we only test NettyRpcClient.
    return Stream.of(NettyRpcClient.class.getName());
  }

  @ParameterizedTest(name = "{index}: rpcClientImpl={0}")
  @MethodSource("parameters")
  public void testPutGetWithDelegationToken(String rpcClientImpl, TestInfo testInfo)
    throws Exception {
    TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      rpcClientImpl);
    TableName tableName = TableName
      .valueOf(TableNameTestExtension.cleanUpTestName(testInfo.getTestMethod().get().getName()));
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    byte[] row = Bytes.toBytes("row");
    byte[] value = Bytes.toBytes("data");
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Admin admin = conn.getAdmin();
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
      admin.createTable(tableDescriptor);
      try (Table table = conn.getTable(tableName)) {
        table.put(new Put(row).addColumn(family, qualifier, value));
        Result result = table.get(new Get(row));
        assertArrayEquals(value, result.getValue(family, qualifier));
      }
    }
  }
}
