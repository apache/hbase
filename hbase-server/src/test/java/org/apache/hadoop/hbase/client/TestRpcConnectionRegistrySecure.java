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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestRpcConnectionRegistrySecure extends TestRpcConnectionRegistry {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRpcConnectionRegistrySecure.class);

  private static final String SERVER_PRINCIPAL = "hbase/localhost";

  private static File KEYTAB_FILE;
  private static MiniKdc KDC;

  @Rule
  public final TableNameTestRule name = new TableNameTestRule();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    KEYTAB_FILE = new File(UTIL.getDataTestDir("keytab").toUri().getPath());
    KDC = UTIL.setupMiniKdc(KEYTAB_FILE);
    KDC.createPrincipal(KEYTAB_FILE, SERVER_PRINCIPAL);

    final Configuration conf = UTIL.getConfiguration();
    SecureTestUtil.enableSecurity(conf);
    HBaseKerberosUtils.setSecuredConfiguration(conf, SERVER_PRINCIPAL + '@' + KDC.getRealm(), null);

    startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
    if (KDC != null) {
      KDC.stop();
    }
    KEYTAB_FILE.delete();
  }

  @Test
  public void testSecureRpc() throws Exception {
    UserGroupInformation.loginUserFromKeytab(SERVER_PRINCIPAL, KEYTAB_FILE.toString());
    final UserGroupInformation ugi = UserGroupInformation.getLoginUser();

    // This is required because SaslClientAuthenticationProviders is already initialized
    // in the process of starting up the mini cluster.
    SaslClientAuthenticationProviders.reset();

    // Make sure that existing connection are not reused.
    UTIL.invalidateConnection();

    assertTrue(ugi.doAs((PrivilegedExceptionAction<Boolean>) this::doTableTest));
  }

  private boolean doTableTest() throws IOException {
    final int count = 10;
    final byte[] cf = Bytes.toBytes("f");
    final byte[] cq = Bytes.toBytes("q");
    final TableDescriptor desc = TableDescriptorBuilder.newBuilder(name.getTableName()).build();
    int scannedRows = 0;
    try (Table table = UTIL.createTable(desc, new byte[][] { cf }, UTIL.getConfiguration())) {
      for (int i = 0; i < count; i++) {
        table.put(new Put(Bytes.toBytes(i)).addColumn(cf, cq, "0".getBytes()));
      }
      for (Result result : table.getScanner(new Scan())) {
        scannedRows++;
      }
    }
    UTIL.deleteTableIfAny(name.getTableName());
    return scannedRows == count;
  }
}
