/**
 *
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
package org.apache.hadoop.hbase.security;

import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getSecuredConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;

import javax.security.sasl.SaslException;

public abstract class AbstractTestSecureIPC {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri()
      .getPath());

  static final BlockingService SERVICE =
      TestRpcServiceProtos.TestProtobufRpcProto.newReflectiveBlockingService(
          new TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface() {

            @Override
            public TestProtos.EmptyResponseProto ping(RpcController controller,
                                                      TestProtos.EmptyRequestProto request)
                throws ServiceException {
              return null;
            }

            @Override
            public TestProtos.EmptyResponseProto error(RpcController controller,
                                                       TestProtos.EmptyRequestProto request)
                throws ServiceException {
              return null;
            }

            @Override
            public TestProtos.EchoResponseProto echo(RpcController controller,
                                                     TestProtos.EchoRequestProto request)
                throws ServiceException {
              if (controller instanceof PayloadCarryingRpcController) {
                PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController) controller;
                // If cells, scan them to check we are able to iterate what we were given and since
                // this is
                // an echo, just put them back on the controller creating a new block. Tests our
                // block
                // building.
                CellScanner cellScanner = pcrc.cellScanner();
                List<Cell> list = null;
                if (cellScanner != null) {
                  list = new ArrayList<Cell>();
                  try {
                    while (cellScanner.advance()) {
                      list.add(cellScanner.current());
                    }
                  } catch (IOException e) {
                    throw new ServiceException(e);
                  }
                }
                cellScanner = CellUtil.createCellScanner(list);
                ((PayloadCarryingRpcController) controller).setCellScanner(cellScanner);
              }
              return TestProtos.EchoResponseProto.newBuilder()
                  .setMessage(request.getMessage()).build();
            }
          });

  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;

  String krbKeytab;
  String krbPrincipal;
  UserGroupInformation ugi;
  Configuration clientConf;
  Configuration serverConf;

  abstract Class<? extends RpcClient> getRpcClientClass();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setUp() throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    KDC = new MiniKdc(conf, new File(TEST_UTIL.getDataTestDir("kdc").toUri().getPath()));
    KDC.start();
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setKeytabFileForTesting(KEYTAB_FILE.getAbsolutePath());
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.cleanupTestDir();
  }

  @Before
  public void setUpTest() throws Exception {
    krbKeytab = getKeytabFileForTesting();
    krbPrincipal = getPrincipalForTesting();
    ugi = loginKerberosPrincipal(krbKeytab, krbPrincipal);
    clientConf = getSecuredConfiguration();
    clientConf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, getRpcClientClass().getName());
    serverConf = getSecuredConfiguration();
  }

  @Test
  public void testRpcCallWithEnabledKerberosSaslAuth() throws Exception {
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi, ugi2);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    callRpcService(User.create(ugi2));
  }

  @Test
  public void testRpcFallbackToSimpleAuth() throws Exception {
    String clientUsername = "testuser";
    UserGroupInformation clientUgi = UserGroupInformation.createUserForTesting(clientUsername,
        new String[]{clientUsername});

    // check that the client user is insecure
    assertNotSame(ugi, clientUgi);
    assertEquals(AuthenticationMethod.SIMPLE, clientUgi.getAuthenticationMethod());
    assertEquals(clientUsername, clientUgi.getUserName());

    clientConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    serverConf.setBoolean(RpcServer.FALLBACK_TO_INSECURE_CLIENT_AUTH, true);
    callRpcService(User.create(clientUgi));
  }

  void setRpcProtection(String clientProtection, String serverProtection) {
    clientConf.set("hbase.rpc.protection", clientProtection);
    serverConf.set("hbase.rpc.protection", serverProtection);
  }

  /**
   * Test various combinations of Server and Client qops.
   * @throws Exception
   */
  @Test
  public void testSaslWithCommonQop() throws Exception {
    setRpcProtection("privacy,authentication", "authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("authentication", "privacy,authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("integrity,authentication", "privacy,authentication");
    callRpcService(User.create(ugi));
  }

  @Test
  public void testSaslNoCommonQop() throws Exception {
    exception.expect(SaslException.class);
    exception.expectMessage("No common protection layer between client and server");
    setRpcProtection("integrity", "privacy");
    callRpcService(User.create(ugi));
  }

  private UserGroupInformation loginKerberosPrincipal(String krbKeytab, String krbPrincipal)
      throws Exception {
    Configuration cnf = new Configuration();
    cnf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(cnf);
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
    return UserGroupInformation.getLoginUser();
  }

  /**
   * Sets up a RPC Server and a Client. Does a RPC checks the result. If an exception is thrown
   * from the stub, this function will throw root cause of that exception.
   */
  private void callRpcService(User clientUser) throws Exception {
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipal())
        .thenReturn(HBaseKerberosUtils.KRB_PRINCIPAL);
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);

    InetSocketAddress isa = new InetSocketAddress(HOST, 0);

    RpcServerInterface rpcServer =
        new RpcServer(null, "AbstractTestSecureIPC",
            Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)), isa,
            serverConf, new FifoRpcScheduler(serverConf, 1));
    rpcServer.start();
    try (RpcClient rpcClient = RpcClientFactory.createClient(clientConf,
        HConstants.DEFAULT_CLUSTER_ID.toString())) {
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      BlockingRpcChannel channel =
          rpcClient.createBlockingRpcChannel(
              ServerName.valueOf(address.getHostName(), address.getPort(),
                  System.currentTimeMillis()), clientUser, 0);
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub =
          TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(channel);
      List<String> results = new ArrayList<>();
      TestThread th1 = new TestThread(stub, results);
      final Throwable exception[] = new Throwable[1];
      Collections.synchronizedList(new ArrayList<Throwable>());
      Thread.UncaughtExceptionHandler exceptionHandler =
          new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread th, Throwable ex) {
              exception[0] = ex;
            }
          };
      th1.setUncaughtExceptionHandler(exceptionHandler);
      th1.start();
      th1.join();
      if (exception[0] != null) {
        // throw root cause.
        while (exception[0].getCause() != null) {
          exception[0] = exception[0].getCause();
        }
        throw (Exception) exception[0];
      }
    } finally {
      rpcServer.stop();
    }
  }

  public static class TestThread extends Thread {
      private final TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub;

      private final List<String> results;

          public TestThread(TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub, List<String> results) {
          this.stub = stub;
          this.results = results;
        }

          @Override
      public void run() {
          String result;
          try {
              result = stub.echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage(String.valueOf(
                  ThreadLocalRandom.current().nextInt())).build()).getMessage();
            } catch (ServiceException e) {
              throw new RuntimeException(e);
            }
          if (results != null) {
              synchronized (results) {
                  results.add(result);
                }
            }
        }
    }
}
