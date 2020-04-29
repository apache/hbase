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
package org.apache.hadoop.hbase.security.provider.example;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.provider.SaslClientAuthenticationProviders;
import org.apache.hadoop.hbase.security.provider.SaslServerAuthenticationProviders;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({MediumTests.class, SecurityTests.class})
public class TestShadeSaslAuthenticationProvider {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestShadeSaslAuthenticationProvider.class);

  private static final char[] USER1_PASSWORD = "foobarbaz".toCharArray();

  static LocalHBaseCluster createCluster(HBaseTestingUtility util, File keytabFile,
      MiniKdc kdc, Map<String,char[]> userDatabase) throws Exception {
    String servicePrincipal = "hbase/localhost";
    String spnegoPrincipal = "HTTP/localhost";
    kdc.createPrincipal(keytabFile, servicePrincipal);
    util.startMiniZKCluster();

    HBaseKerberosUtils.setSecuredConfiguration(util.getConfiguration(),
        servicePrincipal + "@" + kdc.getRealm(), spnegoPrincipal + "@" + kdc.getRealm());
    HBaseKerberosUtils.setSSLConfiguration(util, TestShadeSaslAuthenticationProvider.class);

    util.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        TokenProvider.class.getName());
    util.startMiniDFSCluster(1);
    Path testDir = util.getDataTestDirOnTestFS("TestShadeSaslAuthenticationProvider");
    USER_DATABASE_FILE = new Path(testDir, "user-db.txt");

    createUserDBFile(
        USER_DATABASE_FILE.getFileSystem(CONF), USER_DATABASE_FILE, userDatabase);
    CONF.set(ShadeSaslServerAuthenticationProvider.PASSWORD_FILE_KEY,
        USER_DATABASE_FILE.toString());

    Path rootdir = new Path(testDir, "hbase-root");
    CommonFSUtils.setRootDir(CONF, rootdir);
    LocalHBaseCluster cluster = new LocalHBaseCluster(CONF, 1);
    return cluster;
  }

  static void createUserDBFile(FileSystem fs, Path p,
      Map<String,char[]> userDatabase) throws IOException {
    if (fs.exists(p)) {
      fs.delete(p, true);
    }
    try (FSDataOutputStream out = fs.create(p);
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out))) {
      for (Entry<String,char[]> e : userDatabase.entrySet()) {
        writer.write(e.getKey());
        writer.write(ShadeSaslServerAuthenticationProvider.SEPARATOR);
        writer.write(e.getValue());
        writer.newLine();
      }
    }
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Configuration CONF = UTIL.getConfiguration();
  private static LocalHBaseCluster CLUSTER;
  private static File KEYTAB_FILE;
  private static Path USER_DATABASE_FILE;

  @BeforeClass
  public static void setupCluster() throws Exception {
    KEYTAB_FILE = new File(
        UTIL.getDataTestDir("keytab").toUri().getPath());
    final MiniKdc kdc = UTIL.setupMiniKdc(KEYTAB_FILE);

    // Adds our test impls instead of creating service loader entries which
    // might inadvertently get them loaded on a real cluster.
    CONF.setStrings(SaslClientAuthenticationProviders.EXTRA_PROVIDERS_KEY,
        ShadeSaslClientAuthenticationProvider.class.getName());
    CONF.setStrings(SaslServerAuthenticationProviders.EXTRA_PROVIDERS_KEY,
        ShadeSaslServerAuthenticationProvider.class.getName());
    CONF.set(SaslClientAuthenticationProviders.SELECTOR_KEY,
        ShadeProviderSelector.class.getName());

    CLUSTER = createCluster(UTIL, KEYTAB_FILE, kdc,
        Collections.singletonMap("user1", USER1_PASSWORD));
    CLUSTER.startup();
  }

  @AfterClass
  public static void teardownCluster() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
      CLUSTER = null;
    }
    UTIL.shutdownMiniZKCluster();
  }

  @Rule
  public TestName name = new TestName();
  TableName tableName;
  String clusterId;

  @Before
  public void createTable() throws Exception {
    tableName = TableName.valueOf(name.getMethodName());

    // Create a table and write a record as the service user (hbase)
    UserGroupInformation serviceUgi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        "hbase/localhost", KEYTAB_FILE.getAbsolutePath());
    clusterId = serviceUgi.doAs(new PrivilegedExceptionAction<String>() {
      @Override public String run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(CONF);
            Admin admin = conn.getAdmin();) {
          admin.createTable(TableDescriptorBuilder
              .newBuilder(tableName)
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f1"))
              .build());

          UTIL.waitTableAvailable(tableName);

          try (Table t = conn.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes("r1"));
            p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"), Bytes.toBytes("1"));
            t.put(p);
          }

          return admin.getClusterMetrics().getClusterId();
        }
      }
    });

    assertNotNull(clusterId);
  }

  @Test
  public void testPositiveAuthentication() throws Exception {
    final Configuration clientConf = new Configuration(CONF);
    try (Connection conn = ConnectionFactory.createConnection(clientConf)) {
      UserGroupInformation user1 = UserGroupInformation.createUserForTesting(
          "user1", new String[0]);
      user1.addToken(ShadeClientTokenUtil.obtainToken(conn, "user1", USER1_PASSWORD));
      user1.doAs(new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws Exception {
          try (Table t = conn.getTable(tableName)) {
            Result r = t.get(new Get(Bytes.toBytes("r1")));
            assertNotNull(r);
            assertFalse("Should have read a non-empty Result", r.isEmpty());
            final Cell cell = r.getColumnLatestCell(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
            assertTrue("Unexpected value", CellUtil.matchingValue(cell, Bytes.toBytes("1")));

            return null;
          }
        }
      });
    }
  }

  @Test(expected = DoNotRetryIOException.class)
  public void testNegativeAuthentication() throws Exception {
    // Validate that we can read that record back out as the user with our custom auth'n
    final Configuration clientConf = new Configuration(CONF);
    clientConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    try (Connection conn = ConnectionFactory.createConnection(clientConf)) {
      UserGroupInformation user1 = UserGroupInformation.createUserForTesting(
          "user1", new String[0]);
      user1.addToken(
          ShadeClientTokenUtil.obtainToken(conn, "user1", "not a real password".toCharArray()));
      user1.doAs(new PrivilegedExceptionAction<Void>() {
        @Override public Void run() throws Exception {
          try (Connection conn = ConnectionFactory.createConnection(clientConf);
              Table t = conn.getTable(tableName)) {
            t.get(new Get(Bytes.toBytes("r1")));
            fail("Should not successfully authenticate with HBase");
            return null;
          }
        }
      });
    }
  }
}
