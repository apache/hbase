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

import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.loginKerberosPrincipal;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.setSecuredConfiguration;

import java.io.File;
import java.util.Collections;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;

@RunWith(Parameterized.class)
@Category({ SecurityTests.class, MediumTests.class })
public class TestSaslTlsIPCRejectPlainText extends AbstractTestTlsRejectPlainText {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSaslTlsIPCRejectPlainText.class);

  private static File KEYTAB_FILE;

  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;
  private static UserGroupInformation UGI;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    HBaseTestingUtil util = new HBaseTestingUtil();
    UTIL = util;
    initialize();
    KEYTAB_FILE = new File(util.getDataTestDir("keytab").toUri().getPath());
    KDC = util.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    UGI = loginKerberosPrincipal(KEYTAB_FILE.getCanonicalPath(), PRINCIPAL);
    setSecuredConfiguration(util.getConfiguration());
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipals())
      .thenReturn(Collections.singletonList(HBaseKerberosUtils.KRB_PRINCIPAL));
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);
  }

  @AfterClass
  public static void tearDownAfterClass() {
    if (KDC != null) {
      KDC.stop();
    }
    cleanUp();
  }

  @Override
  protected BlockingInterface createStub() throws Exception {
    return TestProtobufRpcServiceImpl.newBlockingStub(rpcClient, rpcServer.getListenerAddress(),
      User.create(UGI));
  }
}
