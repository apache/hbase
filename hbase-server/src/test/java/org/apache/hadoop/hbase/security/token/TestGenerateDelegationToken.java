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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ServiceException;
import java.util.stream.Stream;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

@Tag(SecurityTests.TAG)
@Tag(MediumTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: rpcClientImpl={0}")
public class TestGenerateDelegationToken extends SecureTestCluster {

  @BeforeAll
  public static void setUp() throws Exception {
    SecureTestCluster.setUpCluster();
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
      Token<? extends TokenIdentifier> token = ClientTokenUtil.obtainToken(conn);
      UserGroupInformation.getCurrentUser().addToken(token);
    }
  }

  public static Stream<Arguments> parameters() {
    // Client connection supports only non-blocking RPCs (due to master registry restriction), hence
    // we only test NettyRpcClient.
    return Stream.of(Arguments.of(NettyRpcClient.class.getName()));
  }

  private String rpcClientImpl;

  public TestGenerateDelegationToken(String rpcClientImpl) {
    this.rpcClientImpl = rpcClientImpl;
  }

  @BeforeEach
  public void setUpBeforeMethod() {
    TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      rpcClientImpl);
  }

  @TestTemplate
  public void test() throws Exception {
    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
      Table table = conn.getTable(TableName.META_TABLE_NAME)) {
      CoprocessorRpcChannel rpcChannel = table.coprocessorService(HConstants.EMPTY_START_ROW);
      AuthenticationProtos.AuthenticationService.BlockingInterface service =
        AuthenticationProtos.AuthenticationService.newBlockingStub(rpcChannel);
      WhoAmIResponse response = service.whoAmI(null, WhoAmIRequest.getDefaultInstance());
      assertEquals(USERNAME, response.getUsername());
      assertEquals(AuthenticationMethod.TOKEN.name(), response.getAuthMethod());
      ServiceException se = assertThrows(ServiceException.class, () -> service
        .getAuthenticationToken(null, GetAuthenticationTokenRequest.getDefaultInstance()));
      assertThat(se.getCause(), instanceOf(AccessDeniedException.class));
      assertThat(se.getCause().getMessage(),
        containsString("Token generation only allowed for Kerberos authenticated clients"));
    }
  }
}
