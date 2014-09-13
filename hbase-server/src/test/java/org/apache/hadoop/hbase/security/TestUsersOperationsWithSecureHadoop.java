/*
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

import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getConfigurationWoPrincipal;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getSecuredConfiguration;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.isKerberosPropertySetted;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SecurityTests.class, SmallTests.class})
public class TestUsersOperationsWithSecureHadoop {
  /**
   * test login with security enabled configuration
   *
   * To run this test, we must specify the following system properties:
   * <p>
   * <b> hbase.regionserver.kerberos.principal </b>
   * <p>
   * <b> hbase.regionserver.keytab.file </b>
   *
   * @throws IOException
   */
  @Test
  public void testUserLoginInSecureHadoop() throws Exception {
    UserGroupInformation defaultLogin = UserGroupInformation.getLoginUser();
    Configuration conf = getConfigurationWoPrincipal();
    User.login(conf, HBaseKerberosUtils.KRB_KEYTAB_FILE,
        HBaseKerberosUtils.KRB_PRINCIPAL, "localhost");

    UserGroupInformation failLogin = UserGroupInformation.getLoginUser();
    assertTrue("ugi should be the same in case fail login",
        defaultLogin.equals(failLogin));
    
    assumeTrue(isKerberosPropertySetted());

    String nnKeyTab = getKeytabFileForTesting();
    String dnPrincipal = getPrincipalForTesting();

    assertNotNull("KerberosKeytab was not specified", nnKeyTab);
    assertNotNull("KerberosPrincipal was not specified", dnPrincipal);

    conf = getSecuredConfiguration();
    UserGroupInformation.setConfiguration(conf);

    User.login(conf, HBaseKerberosUtils.KRB_KEYTAB_FILE,
        HBaseKerberosUtils.KRB_PRINCIPAL, "localhost");
    UserGroupInformation successLogin = UserGroupInformation.getLoginUser();
    assertFalse("ugi should be different in in case success login",
       defaultLogin.equals(successLogin));
  }
}
