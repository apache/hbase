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

package org.apache.hadoop.hbase.security.token;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.io.IOException;

@Category({ SecurityTests.class, MediumTests.class })
public class TestDelegationTokenWithEncryption extends SecureTestCluster {

  @BeforeClass
  public static void setUp() throws Exception {
    // enable rpc encryption
    TEST_UTIL.getConfiguration().set("hbase.rpc.protection", "privacy");
    SecureTestCluster.setUp();
  }

  private void testPutGetWithDelegationToken(Class<? extends RpcClient> rpcImplClass)
      throws IOException, ServiceException {
    TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
        rpcImplClass.getName());
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
         Table table = conn.getTable(TableName.valueOf("testtable"));) {
      Put p = new Put(Bytes.toBytes("row"));
      p.addColumn(Bytes.toBytes("family"),
          Bytes.toBytes("data"), Bytes.toBytes("testdata"));
      table.put(p);
      Get g = new Get(Bytes.toBytes("row"));
      Result result = table.get(g);
      Assert.assertArrayEquals(Bytes.toBytes("testdata"),
          result.getValue(Bytes.toBytes("family"), Bytes.toBytes("data")));
    }
  }

  @Test
  public void testPutGetWithDelegationToken()  throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Token<? extends TokenIdentifier> token = TokenUtil.obtainToken(conn);
      UserGroupInformation.getCurrentUser().addToken(token);
      // create the table for test
      Admin admin = conn.getAdmin();
      HTableDescriptor tableDescriptor = new
          HTableDescriptor(new HTableDescriptor(TableName.valueOf("testtable")));
      tableDescriptor.addFamily(new HColumnDescriptor("family"));
      admin.createTable(tableDescriptor);

      testPutGetWithDelegationToken(RpcClientImpl.class);
      testPutGetWithDelegationToken(AsyncRpcClient.class);
    }
  }
}
