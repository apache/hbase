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
package org.apache.hadoop.hbase.client;

import java.io.File;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestBasicReadWriteWithDifferentConnectionRegistriesSecure
  extends TestBasicReadWriteWithDifferentConnectionRegistries {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBasicReadWriteWithDifferentConnectionRegistriesSecure.class);

  private static final String SERVER_PRINCIPAL = "hbase/localhost";

  private static File KEYTAB_FILE;
  private static MiniKdc KDC;

  @Override
  protected Connection createConnectionFromUri(URI uri) throws Exception {
    return ConnectionFactory.createConnection(uri, UTIL.getConfiguration());
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KEYTAB_FILE = new File(UTIL.getDataTestDir("keytab").toUri().getPath());
    KDC = UTIL.setupMiniKdc(KEYTAB_FILE);
    KDC.createPrincipal(KEYTAB_FILE, SERVER_PRINCIPAL);

    final Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
    HBaseKerberosUtils.setSecuredConfiguration(conf, SERVER_PRINCIPAL + '@' + KDC.getRealm(), null);

    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
    if (KDC != null) {
      KDC.stop();
    }
    KEYTAB_FILE.delete();
  }

  @Override
  @Before
  public void setUp() throws Exception {
    SaslClientAuthenticationProviders.reset();
    super.setUp();
  }
}
