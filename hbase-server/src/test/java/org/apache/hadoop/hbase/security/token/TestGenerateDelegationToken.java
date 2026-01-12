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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MetaTableName;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.FutureUtils;
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

import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.AuthenticationService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.GetAuthenticationTokenRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.GetAuthenticationTokenResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.WhoAmIRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AuthenticationProtos.WhoAmIResponse;

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
    return Arrays.asList(new Object[] { NettyRpcClient.class.getName() });
  }

  @Parameter
  public String rpcClientImpl;

  @Before
  public void setUpBeforeMethod() {
    TEST_UTIL.getConfiguration().set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY,
      rpcClientImpl);
  }

  private void testToken() throws Exception {
    try (AsyncConnection conn =
      ConnectionFactory.createAsyncConnection(TEST_UTIL.getConfiguration()).get()) {
      AsyncTable<?> table = conn.getTable(conn.getMetaTableName());
      WhoAmIResponse response =
        table.<AuthenticationService.Interface, WhoAmIResponse> coprocessorService(
          AuthenticationService::newStub,
          (s, c, r) -> s.whoAmI(c, WhoAmIRequest.getDefaultInstance(), r),
          HConstants.EMPTY_START_ROW).get();
      assertEquals(USERNAME, response.getUsername());
      assertEquals(AuthenticationMethod.TOKEN.name(), response.getAuthMethod());
      IOException ioe =
        assertThrows(IOException.class,
          () -> FutureUtils.get(table.<AuthenticationService.Interface,
            GetAuthenticationTokenResponse> coprocessorService(AuthenticationService::newStub,
              (s, c, r) -> s.getAuthenticationToken(c,
                GetAuthenticationTokenRequest.getDefaultInstance(), r),
              HConstants.EMPTY_START_ROW)));
      assertThat(ioe, instanceOf(AccessDeniedException.class));
      assertThat(ioe.getMessage(),
        containsString("Token generation only allowed for Kerberos authenticated clients"));
    }

  }

  /**
   * Confirm that we will use delegation token first if token and kerberos tickets are both present
   */
  @Test
  public void testTokenFirst() throws Exception {
    testToken();
  }

  /**
   * Confirm that we can connect to cluster successfully when there is only token present, i.e, no
   * kerberos ticket
   */
  @Test
  public void testOnlyToken() throws Exception {
    User user =
      User.createUserForTesting(TEST_UTIL.getConfiguration(), "no_krb_user", new String[0]);
    for (Token<? extends TokenIdentifier> token : User.getCurrent().getUGI().getCredentials()
      .getAllTokens()) {
      user.getUGI().addToken(token);
    }
    user.getUGI().doAs(new PrivilegedExceptionAction<Void>() {

      @Override
      public Void run() throws Exception {
        testToken();
        return null;
      }
    });
  }
}
