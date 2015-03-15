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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.GetAuthenticationTokenRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIRequest;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.WhoAmIResponse;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

@Category(MediumTests.class)
public class TestGenerateDelegationToken {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static LocalHBaseCluster CLUSTER;

  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri()
      .getPath());
  private static MiniKdc KDC;

  private static String HOST;

  private static String USERNAME = System.getProperty("user.name");

  private static String PRINCIPAL;

  @BeforeClass
  public static void setUp() throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    KDC = new MiniKdc(conf, new File(TEST_UTIL.getDataTestDir("kdc").toUri().getPath()));
    KDC.start();
    HOST = InetAddress.getLocalHost().getHostName();
    PRINCIPAL = USERNAME + "/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    TEST_UTIL.getConfiguration().setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniDFSCluster(1);

    HBaseKerberosUtils.setKeytabFileForTesting(KEYTAB_FILE.getAbsolutePath());
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setSecuredConfiguration(TEST_UTIL.getConfiguration());
    UserGroupInformation.setConfiguration(TEST_UTIL.getConfiguration());
    TEST_UTIL.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      TokenProvider.class.getName());
    CLUSTER = new LocalHBaseCluster(TEST_UTIL.getConfiguration(), 1);
    CLUSTER.startup();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
    }
    CLUSTER.join();
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

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
        AccessDeniedException exc = (AccessDeniedException) ProtobufUtil.getRemoteException(e);
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
      testTokenAuth(RpcClientImpl.class);
      testTokenAuth(AsyncRpcClient.class);
    }

  }
}
