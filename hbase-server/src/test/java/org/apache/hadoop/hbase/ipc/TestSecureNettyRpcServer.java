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
package org.apache.hadoop.hbase.ipc;

import java.io.File;
import java.security.PrivilegedExceptionAction;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

@Tag(RPCTests.TAG)
@Tag(MediumTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: allocatorType={0}")
public class TestSecureNettyRpcServer extends TestNettyRpcServer {

  private static File KEYTAB_FILE;
  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;
  private static UserGroupInformation UGI;

  public TestSecureNettyRpcServer(String allocatorType) {
    super(allocatorType);
  }

  public static Stream<Arguments> parameters() {
    return TestNettyRpcServer.parameters();
  }

  @BeforeEach
  public void setup() throws Exception {
    TEST_UTIL = new HBaseTestingUtility();
    KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri().getPath());
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    String principalName = PRINCIPAL + "@" + KDC.getRealm();
    HBaseKerberosUtils.setPrincipalForTesting(principalName);
    Configuration conf = TEST_UTIL.getConfiguration();
    HBaseKerberosUtils.setSecuredConfiguration(conf, principalName, principalName);
    UGI = login(KEYTAB_FILE.toString(), principalName);
    super.setup();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (KDC != null) {
      KDC.stop();
    }
    KEYTAB_FILE.delete();
    super.tearDown();
    TEST_UTIL.cleanupTestDir();
  }

  @Override
  @TestTemplate
  public void testNettyRpcServer() throws Exception {
    UGI.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        doTest(tableName);
        return null;
      }
    });
  }

  static UserGroupInformation login(String krbKeytab, String krbPrincipal) throws Exception {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
    return UserGroupInformation.getLoginUser();
  }

}
