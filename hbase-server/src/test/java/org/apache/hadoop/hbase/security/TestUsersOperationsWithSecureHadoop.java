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
package org.apache.hadoop.hbase.security;

import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getClientKeytabForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getClientPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getSecuredConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SecurityTests.class, SmallTests.class })
public class TestUsersOperationsWithSecureHadoop {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestUsersOperationsWithSecureHadoop.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final File KEYTAB_FILE = new File(TEST_UTIL.getDataTestDir("keytab").toUri()
      .getPath());

  private static MiniKdc KDC;

  private static String HOST = "localhost";

  private static String PRINCIPAL;

  private static String CLIENT_NAME;

  @BeforeClass
  public static void setUp() throws Exception {
    KDC = TEST_UTIL.setupMiniKdc(KEYTAB_FILE);
    PRINCIPAL = "hbase/" + HOST;
    CLIENT_NAME = "foo";
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL, CLIENT_NAME);
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setKeytabFileForTesting(KEYTAB_FILE.getAbsolutePath());
    HBaseKerberosUtils.setClientPrincipalForTesting(CLIENT_NAME + "@" + KDC.getRealm());
    HBaseKerberosUtils.setClientKeytabForTesting(KEYTAB_FILE.getAbsolutePath());
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.cleanupTestDir();
  }

  /**
   * test login with security enabled configuration To run this test, we must specify the following
   * system properties:
   * <p>
   * <b> hbase.regionserver.kerberos.principal </b>
   * <p>
   * <b> hbase.regionserver.keytab.file </b>
   * @throws IOException
   */
  @Test
  public void testUserLoginInSecureHadoop() throws Exception {
    // Default login is system user.
    UserGroupInformation defaultLogin = UserGroupInformation.getCurrentUser();

    String nnKeyTab = getKeytabFileForTesting();
    String dnPrincipal = getPrincipalForTesting();

    assertNotNull("KerberosKeytab was not specified", nnKeyTab);
    assertNotNull("KerberosPrincipal was not specified", dnPrincipal);

    Configuration conf = getSecuredConfiguration();
    UserGroupInformation.setConfiguration(conf);

    User.login(conf, HBaseKerberosUtils.KRB_KEYTAB_FILE, HBaseKerberosUtils.KRB_PRINCIPAL,
      "localhost");
    UserGroupInformation successLogin = UserGroupInformation.getLoginUser();
    assertFalse("ugi should be different in in case success login",
      defaultLogin.equals(successLogin));
  }

  @Test
  public void testLoginWithUserKeytabAndPrincipal() throws Exception {
    String clientKeytab = getClientKeytabForTesting();
    String clientPrincipal = getClientPrincipalForTesting();
    assertNotNull("Path for client keytab is not specified.", clientKeytab);
    assertNotNull("Client principal is not specified.", clientPrincipal);

    Configuration conf = getSecuredConfiguration();
    conf.set(AuthUtil.HBASE_CLIENT_KEYTAB_FILE, clientKeytab);
    conf.set(AuthUtil.HBASE_CLIENT_KERBEROS_PRINCIPAL, clientPrincipal);
    UserGroupInformation.setConfiguration(conf);

    UserProvider provider = UserProvider.instantiate(conf);
    assertTrue("Client principal or keytab is empty", provider.shouldLoginFromKeytab());

    provider.login(AuthUtil.HBASE_CLIENT_KEYTAB_FILE, AuthUtil.HBASE_CLIENT_KERBEROS_PRINCIPAL);
    User loginUser = provider.getCurrent();
    assertEquals(CLIENT_NAME, loginUser.getShortName());
    assertEquals(getClientPrincipalForTesting(), loginUser.getName());
  }

  @Test
  public void testAuthUtilLogin() throws Exception {
    String clientKeytab = getClientKeytabForTesting();
    String clientPrincipal = getClientPrincipalForTesting();
    Configuration conf = getSecuredConfiguration();
    conf.set(AuthUtil.HBASE_CLIENT_KEYTAB_FILE, clientKeytab);
    conf.set(AuthUtil.HBASE_CLIENT_KERBEROS_PRINCIPAL, clientPrincipal);
    UserGroupInformation.setConfiguration(conf);

    User user = AuthUtil.loginClient(conf);
    assertTrue(user.isLoginFromKeytab());
    assertEquals(CLIENT_NAME, user.getShortName());
    assertEquals(getClientPrincipalForTesting(), user.getName());
  }
}
