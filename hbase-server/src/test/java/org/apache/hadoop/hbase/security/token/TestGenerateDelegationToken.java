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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ SecurityTests.class, MediumTests.class })
public class TestGenerateDelegationToken extends SecureTestCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGenerateDelegationToken.class);

  @BeforeClass
  public static void setUp() throws Exception {
    SecureTestCluster.setUp();
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Token<? extends TokenIdentifier> token = ClientTokenUtil.obtainToken(conn);
      UserGroupInformation.getCurrentUser().addToken(token);
    }
  }

  @Parameters(name = "{index}: rpcClientImpl={0}")
  public static Collection<Object> parameters() {
    // Client connection supports only non-blocking RPCs (due to master registry restriction), hence
    // we only test NettyRpcClient.
    return Arrays.asList(
      new Object[] { NettyRpcClient.class.getName() });
  }

  @Parameter
  public String rpcClientImpl;

  @Before
  public void setUpBeforeMethod() {
    TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      rpcClientImpl);
  }

  @Test
  public void test() throws Exception {
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
        IOException ioe = ProtobufUtil.getRemoteException(e);
        assertThat(ioe, instanceOf(AccessDeniedException.class));
        assertThat(ioe.getMessage(),
          containsString("Token generation only allowed for Kerberos authenticated clients"));
      }
    }
  }
}
