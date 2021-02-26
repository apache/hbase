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
package org.apache.hadoop.hbase.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import javax.management.ObjectName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testing info servers for admin acl.
 */
@Category({ MiscTests.class, MediumTests.class })
public class TestInfoServersACL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestInfoServersACL.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestInfoServersACL.class);
  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration conf;

  protected static String USERNAME;
  private static LocalHBaseCluster CLUSTER;
  private static final File KEYTAB_FILE = new File(UTIL.getDataTestDir("keytab").toUri().getPath());
  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;
  private static String HTTP_PRINCIPAL;

  @Rule
  public TestName name = new TestName();

  // user/group present in hbase.admin.acl
  private static final String USER_ADMIN_STR = "admin";

  // user with no permissions
  private static final String USER_NONE_STR = "none";

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf = UTIL.getConfiguration();
    KDC = UTIL.setupMiniKdc(KEYTAB_FILE);
    USERNAME = UserGroupInformation.getLoginUser().getShortUserName();
    PRINCIPAL = USERNAME + "/" + HOST;
    HTTP_PRINCIPAL = "HTTP/" + HOST;
    // Create principals for services and the test users
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL, HTTP_PRINCIPAL, USER_ADMIN_STR, USER_NONE_STR);
    UTIL.startMiniZKCluster();

    HBaseKerberosUtils.setSecuredConfiguration(conf,
        PRINCIPAL + "@" + KDC.getRealm(), HTTP_PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setSSLConfiguration(UTIL, TestInfoServersACL.class);

    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        TokenProvider.class.getName());
    UTIL.startMiniDFSCluster(1);
    Path rootdir = UTIL.getDataTestDirOnTestFS("TestInfoServersACL");
    CommonFSUtils.setRootDir(conf, rootdir);

    // The info servers do not run in tests by default.
    // Set them to ephemeral ports so they will start
    // setup configuration
    conf.setInt(HConstants.MASTER_INFO_PORT, 0);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, 0);

    conf.set(HttpServer.HTTP_UI_AUTHENTICATION, "kerberos");
    conf.set(HttpServer.HTTP_SPNEGO_AUTHENTICATION_PRINCIPAL_KEY, HTTP_PRINCIPAL);
    conf.set(HttpServer.HTTP_SPNEGO_AUTHENTICATION_KEYTAB_KEY, KEYTAB_FILE.getAbsolutePath());

    // ACL lists work only when "hadoop.security.authorization" is set to true
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    // only user admin will have acl access
    conf.set(HttpServer.HTTP_SPNEGO_AUTHENTICATION_ADMIN_USERS_KEY, USER_ADMIN_STR);
    //conf.set(HttpServer.FILTER_INITIALIZERS_PROPERTY, "");

    CLUSTER = new LocalHBaseCluster(conf, 1);
    CLUSTER.startup();
    CLUSTER.getActiveMaster().waitForMetaOnline();
  }

  /**
   * Helper method to shut down the cluster (if running)
   */
  @AfterClass
  public static void shutDownMiniCluster() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
      CLUSTER.join();
    }
    if (KDC != null) {
      KDC.stop();
    }
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testAuthorizedUser() throws Exception {
    UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_ADMIN_STR, KEYTAB_FILE.getAbsolutePath());
    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // Check the expected content is present in the http response
        String expectedContent = "Get Log Level";
        Pair<Integer,String> pair = getLogLevelPage();
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertTrue("expected=" + expectedContent + ", content=" + pair.getSecond(),
          pair.getSecond().contains(expectedContent));
        return null;
      }
    });
  }

  @Test
  public void testUnauthorizedUser() throws Exception {
    UserGroupInformation nonAdmin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_NONE_STR, KEYTAB_FILE.getAbsolutePath());
    nonAdmin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        Pair<Integer,String> pair = getLogLevelPage();
        assertEquals(HttpURLConnection.HTTP_FORBIDDEN, pair.getFirst().intValue());
        return null;
      }
    });
  }

  @Test
  public void testTableActionsAvailableForAdmins() throws Exception {
    final String expectedAuthorizedContent = "Actions:";
    UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_ADMIN_STR, KEYTAB_FILE.getAbsolutePath());
    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // Check the expected content is present in the http response
        Pair<Integer,String> pair = getTablePage(TableName.META_TABLE_NAME);
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertTrue("expected=" + expectedAuthorizedContent + ", content=" + pair.getSecond(),
          pair.getSecond().contains(expectedAuthorizedContent));
        return null;
      }
    });

    UserGroupInformation nonAdmin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_NONE_STR, KEYTAB_FILE.getAbsolutePath());
    nonAdmin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        Pair<Integer,String> pair = getTablePage(TableName.META_TABLE_NAME);
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertFalse("should not find=" + expectedAuthorizedContent + ", content=" +
            pair.getSecond(), pair.getSecond().contains(expectedAuthorizedContent));
        return null;
      }
    });
  }

  @Test
  public void testLogsAvailableForAdmins() throws Exception {
    final String expectedAuthorizedContent = "Directory: /logs/";
    UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_ADMIN_STR, KEYTAB_FILE.getAbsolutePath());
    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // Check the expected content is present in the http response
        Pair<Integer,String> pair = getLogsPage();
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertTrue("expected=" + expectedAuthorizedContent + ", content=" + pair.getSecond(),
          pair.getSecond().contains(expectedAuthorizedContent));
        return null;
      }
    });

    UserGroupInformation nonAdmin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_NONE_STR, KEYTAB_FILE.getAbsolutePath());
    nonAdmin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        Pair<Integer,String> pair = getLogsPage();
        assertEquals(HttpURLConnection.HTTP_FORBIDDEN, pair.getFirst().intValue());
        return null;
      }
    });
  }

  @Test
  public void testDumpActionsAvailableForAdmins() throws Exception {
    final String expectedAuthorizedContent = "Master status for";
    UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_ADMIN_STR, KEYTAB_FILE.getAbsolutePath());
    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // Check the expected content is present in the http response
        Pair<Integer,String> pair = getMasterDumpPage();
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertTrue("expected=" + expectedAuthorizedContent + ", content=" + pair.getSecond(),
          pair.getSecond().contains(expectedAuthorizedContent));
        return null;
      }
    });

    UserGroupInformation nonAdmin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_NONE_STR, KEYTAB_FILE.getAbsolutePath());
    nonAdmin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        Pair<Integer,String> pair = getMasterDumpPage();
        assertEquals(HttpURLConnection.HTTP_FORBIDDEN, pair.getFirst().intValue());
        return null;
      }
    });
  }

  @Test
  public void testStackActionsAvailableForAdmins() throws Exception {
    final String expectedAuthorizedContent = "Process Thread Dump";
    UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_ADMIN_STR, KEYTAB_FILE.getAbsolutePath());
    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // Check the expected content is present in the http response
        Pair<Integer,String> pair = getStacksPage();
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertTrue("expected=" + expectedAuthorizedContent + ", content=" + pair.getSecond(),
          pair.getSecond().contains(expectedAuthorizedContent));
        return null;
      }
    });

    UserGroupInformation nonAdmin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_NONE_STR, KEYTAB_FILE.getAbsolutePath());
    nonAdmin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        Pair<Integer,String> pair = getStacksPage();
        assertEquals(HttpURLConnection.HTTP_FORBIDDEN, pair.getFirst().intValue());
        return null;
      }
    });
  }

  @Test
  public void testJmxAvailableForAdmins() throws Exception {
    final String expectedAuthorizedContent = "Hadoop:service=HBase";
    UTIL.waitFor(30000, new Waiter.Predicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        for (ObjectName name: ManagementFactory.getPlatformMBeanServer().
          queryNames(new ObjectName("*:*"), null)) {
          if (name.toString().contains(expectedAuthorizedContent)) {
            LOG.info("{}", name);
            return true;
          }
        }
        return false;
      }
    });
    UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_ADMIN_STR, KEYTAB_FILE.getAbsolutePath());
    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // Check the expected content is present in the http response
        Pair<Integer,String> pair = getJmxPage();
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertTrue("expected=" + expectedAuthorizedContent + ", content=" + pair.getSecond(),
          pair.getSecond().contains(expectedAuthorizedContent));
        return null;
      }
    });

    UserGroupInformation nonAdmin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_NONE_STR, KEYTAB_FILE.getAbsolutePath());
    nonAdmin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        Pair<Integer,String> pair = getJmxPage();
        assertEquals(HttpURLConnection.HTTP_FORBIDDEN, pair.getFirst().intValue());
        return null;
      }
    });
  }

  @Test
  public void testMetricsAvailableForAdmins() throws Exception {
    // Looks like there's nothing exported to this, but leave it since
    // it's Hadoop2 only and will eventually be removed due to that.
    final String expectedAuthorizedContent = "";
    UserGroupInformation admin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_ADMIN_STR, KEYTAB_FILE.getAbsolutePath());
    admin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // Check the expected content is present in the http response
        Pair<Integer,String> pair = getMetricsPage();
        if (HttpURLConnection.HTTP_NOT_FOUND == pair.getFirst()) {
          // Not on hadoop 2
          return null;
        }
        assertEquals(HttpURLConnection.HTTP_OK, pair.getFirst().intValue());
        assertTrue("expected=" + expectedAuthorizedContent + ", content=" + pair.getSecond(),
          pair.getSecond().contains(expectedAuthorizedContent));
        return null;
      }
    });

    UserGroupInformation nonAdmin = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        USER_NONE_STR, KEYTAB_FILE.getAbsolutePath());
    nonAdmin.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        Pair<Integer,String> pair = getMetricsPage();
        if (HttpURLConnection.HTTP_NOT_FOUND == pair.getFirst()) {
          // Not on hadoop 2
          return null;
        }
        assertEquals(HttpURLConnection.HTTP_FORBIDDEN, pair.getFirst().intValue());
        return null;
      }
    });
  }

  private String getInfoServerHostAndPort() {
    return "http://localhost:" + CLUSTER.getActiveMaster().getInfoServer().getPort();
  }

  private Pair<Integer,String> getLogLevelPage() throws Exception {
    // Build the url which we want to connect to
    URL url = new URL(getInfoServerHostAndPort() + "/logLevel");
    return getUrlContent(url);
  }

  private Pair<Integer,String> getTablePage(TableName tn) throws Exception {
    URL url = new URL(getInfoServerHostAndPort() + "/table.jsp?name=" + tn.getNameAsString());
    return getUrlContent(url);
  }

  private Pair<Integer,String> getLogsPage() throws Exception {
    URL url = new URL(getInfoServerHostAndPort() + "/logs/");
    return getUrlContent(url);
  }

  private Pair<Integer,String> getMasterDumpPage() throws Exception {
    URL url = new URL(getInfoServerHostAndPort() + "/dump");
    return getUrlContent(url);
  }

  private Pair<Integer,String> getStacksPage() throws Exception {
    URL url = new URL(getInfoServerHostAndPort() + "/stacks");
    return getUrlContent(url);
  }

  private Pair<Integer,String> getJmxPage() throws Exception {
    URL url = new URL(getInfoServerHostAndPort() + "/jmx");
    return getUrlContent(url);
  }

  private Pair<Integer,String> getMetricsPage() throws Exception {
    URL url = new URL(getInfoServerHostAndPort() + "/metrics");
    return getUrlContent(url);
  }

  /**
   * Retrieves the content of the specified URL. The content will only be returned if the status
   * code for the operation was HTTP 200/OK.
   */
  private Pair<Integer,String> getUrlContent(URL url) throws Exception {
    try (CloseableHttpClient client = createHttpClient(
        UserGroupInformation.getCurrentUser().getUserName())) {
      CloseableHttpResponse resp = client.execute(new HttpGet(url.toURI()));
      int code = resp.getStatusLine().getStatusCode();
      if (code == HttpURLConnection.HTTP_OK) {
        return new Pair<>(code, EntityUtils.toString(resp.getEntity()));
      }
      return new Pair<>(code, null);
    }
  }

  private CloseableHttpClient createHttpClient(String clientPrincipal) throws Exception {
    // Logs in with Kerberos via GSS
    GSSManager gssManager = GSSManager.getInstance();
    // jGSS Kerberos login constant
    Oid oid = new Oid("1.2.840.113554.1.2.2");
    GSSName gssClient = gssManager.createName(clientPrincipal, GSSName.NT_USER_NAME);
    GSSCredential credential = gssManager.createCredential(
        gssClient, GSSCredential.DEFAULT_LIFETIME, oid, GSSCredential.INITIATE_ONLY);

    Lookup<AuthSchemeProvider> authRegistry = RegistryBuilder.<AuthSchemeProvider>create()
        .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, true)).build();

    BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new KerberosCredentials(credential));

    return HttpClients.custom().setDefaultAuthSchemeRegistry(authRegistry)
        .setDefaultCredentialsProvider(credentialsProvider).build();
  }
}
