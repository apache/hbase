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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.GetAuthenticationTokenRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIResponse;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

@Category({ SecurityTests.class, MediumTests.class })
public class TestGenerateDelegationToken extends SecureTestCluster {

  private void testTokenAuth(Class<? extends RpcClient> rpcImplClass) throws IOException,
      ServiceException {
    TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      rpcImplClass.getName());
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
        Table table = conn.getTable(TableName.META_TABLE_NAME)) {
      CoprocessorRpcChannel rpcChannel = table.coprocessorService(HConstants.EMPTY_START_ROW);
      AuthenticationProtos.AuthenticationService.BlockingInterface service =
          AuthenticationProtos.AuthenticationService.newBlockingStub(rpcChannel);
      WhoAmIResponse response = service.whoAmI(null, WhoAmIRequest.getDefaultInstance());
      assertEquals(USERNAME, response.getUsername());
      assertEquals(AuthenticationMethod.TOKEN.name(), response.getAuthMethod());
      try {
        service.getAuthenticationToken(null, GetAuthenticationTokenRequest.getDefaultInstance());
      } catch (ServiceException e) {
        AccessDeniedException exc = (AccessDeniedException) ProtobufUtil.handleRemoteException(e);
        assertTrue(exc.getMessage().contains(
          "Token generation only allowed for Kerberos authenticated clients"));
      }
    }
  }

  @Test
  public void test() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Token<? extends TokenIdentifier> token = TokenUtil.obtainToken(conn);
      UserGroupInformation.getCurrentUser().addToken(token);
      testTokenAuth(BlockingRpcClient.class);
      testTokenAuth(NettyRpcClient.class);
    }
  }
}
