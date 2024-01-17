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
package org.apache.hadoop.hbase.security;

import java.io.File;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.crypto.tls.KeyStoreFileType;
import org.apache.hadoop.hbase.io.crypto.tls.X509KeyType;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContext;
import org.apache.hadoop.hbase.io.crypto.tls.X509TestContextProvider;
import org.apache.hadoop.hbase.io.crypto.tls.X509Util;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcServer;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category({ SecurityTests.class, LargeTests.class })
public class TestSaslTlsIPC extends AbstractTestSecureIPC {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSaslTlsIPC.class);

  private static X509TestContextProvider PROVIDER;

  @Parameterized.Parameter(0)
  public X509KeyType caKeyType;

  @Parameterized.Parameter(1)
  public X509KeyType certKeyType;

  @Parameterized.Parameter(2)
  public char[] keyPassword;

  @Parameterized.Parameter(3)
  public boolean acceptPlainText;

  @Parameterized.Parameter(4)
  public boolean clientTlsEnabled;

  private X509TestContext x509TestContext;

  @Parameterized.Parameters(
      name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}, acceptPlainText={3},"
        + " clientTlsEnabled={4}")
  public static List<Object[]> data() {
    List<Object[]> params = new ArrayList<>();
    for (X509KeyType caKeyType : X509KeyType.values()) {
      for (X509KeyType certKeyType : X509KeyType.values()) {
        for (char[] keyPassword : new char[][] { "".toCharArray(), "pa$$w0rd".toCharArray() }) {
          // do not accept plain text
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, false, true });
          // support plain text and client enables tls
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, true, true });
          // support plain text and client disables tls
          params.add(new Object[] { caKeyType, certKeyType, keyPassword, true, false });
        }
      }
    }
    return params;
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Security.addProvider(new BouncyCastleProvider());
    File dir = new File(TEST_UTIL.getDataTestDir(TestSaslTlsIPC.class.getSimpleName()).toString())
      .getCanonicalFile();
    FileUtils.forceMkdir(dir);
    initKDCAndConf();
    Configuration conf = TEST_UTIL.getConfiguration();
    // server must enable tls
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_ENABLED, true);
    // only netty support tls
    conf.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, NettyRpcClient.class,
      RpcClient.class);
    conf.setClass(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, NettyRpcServer.class,
      RpcServer.class);
    PROVIDER = new X509TestContextProvider(conf, dir);
  }

  @AfterClass
  public static void tearDownAfterClass() throws InterruptedException {
    stopKDC();
    Security.removeProvider(BouncyCastleProvider.PROVIDER_NAME);
    TEST_UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws Exception {
    x509TestContext = PROVIDER.get(caKeyType, certKeyType, keyPassword);
    x509TestContext.setConfigurations(KeyStoreFileType.JKS, KeyStoreFileType.JKS);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(X509Util.HBASE_SERVER_NETTY_TLS_SUPPORTPLAINTEXT, acceptPlainText);
    conf.setBoolean(X509Util.HBASE_CLIENT_NETTY_TLS_ENABLED, clientTlsEnabled);
    setUpPrincipalAndConf();
  }

  @After
  public void tearDown() {
    x509TestContext.clearConfigurations();
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_OCSP);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_CLR);
    x509TestContext.getConf().unset(X509Util.TLS_CONFIG_PROTOCOL);
    System.clearProperty("com.sun.net.ssl.checkRevocation");
    System.clearProperty("com.sun.security.enableCRLDP");
    Security.setProperty("ocsp.enable", Boolean.FALSE.toString());
    Security.setProperty("com.sun.security.enableCRLDP", Boolean.FALSE.toString());
  }
}
