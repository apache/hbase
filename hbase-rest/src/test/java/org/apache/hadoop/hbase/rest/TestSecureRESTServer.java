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
package org.apache.hadoop.hbase.rest;

import static org.apache.hadoop.hbase.rest.RESTServlet.HBASE_REST_SUPPORT_PROXYUSER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.json.JacksonJaxbJsonProvider;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.rest.model.CellModel;
import org.apache.hadoop.hbase.rest.model.CellSetModel;
import org.apache.hadoop.hbase.rest.model.RowModel;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.access.AccessControlClient;
import org.apache.hadoop.hbase.security.access.AccessControlConstants;
import org.apache.hadoop.hbase.security.access.AccessController;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for SPNEGO authentication on the HttpServer. Uses Kerby's MiniKDC and Apache
 * HttpComponents to verify that a simple Servlet is reachable via SPNEGO and unreachable w/o.
 */
@Category({MiscTests.class, MediumTests.class})
public class TestSecureRESTServer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSecureRESTServer.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSecureRESTServer.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final HBaseRESTTestingUtility REST_TEST = new HBaseRESTTestingUtility();
  private static MiniHBaseCluster CLUSTER;

  private static final String HOSTNAME = "localhost";
  private static final String CLIENT_PRINCIPAL = "client";
  private static final String WHEEL_PRINCIPAL = "wheel";
  // The principal for accepting SPNEGO authn'ed requests (*must* be HTTP/fqdn)
  private static final String SPNEGO_SERVICE_PRINCIPAL = "HTTP/" + HOSTNAME;
  // The principal we use to connect to HBase
  private static final String REST_SERVER_PRINCIPAL = "rest";
  private static final String SERVICE_PRINCIPAL = "hbase/" + HOSTNAME;

  private static URL baseUrl;
  private static MiniKdc KDC;
  private static RESTServer server;
  private static File restServerKeytab;
  private static File clientKeytab;
  private static File wheelKeytab;
  private static File serviceKeytab;

  @BeforeClass
  public static void setupServer() throws Exception {
    final File target = new File(System.getProperty("user.dir"), "target");
    assertTrue(target.exists());

    /*
     * Keytabs
     */
    File keytabDir = new File(target, TestSecureRESTServer.class.getSimpleName()
        + "_keytabs");
    if (keytabDir.exists()) {
      FileUtils.deleteDirectory(keytabDir);
    }
    keytabDir.mkdirs();
    // Keytab for HBase services (RS, Master)
    serviceKeytab = new File(keytabDir, "hbase.service.keytab");
    // The keytab for the REST server
    restServerKeytab = new File(keytabDir, "spnego.keytab");
    // Keytab for the client
    clientKeytab = new File(keytabDir, CLIENT_PRINCIPAL + ".keytab");
    // Keytab for wheel
    wheelKeytab = new File(keytabDir, WHEEL_PRINCIPAL + ".keytab");

    /*
     * Update UGI
     */
    Configuration conf = TEST_UTIL.getConfiguration();

    /*
     * Start KDC
     */
    KDC = TEST_UTIL.setupMiniKdc(serviceKeytab);
    KDC.createPrincipal(clientKeytab, CLIENT_PRINCIPAL);
    KDC.createPrincipal(wheelKeytab, WHEEL_PRINCIPAL);
    KDC.createPrincipal(serviceKeytab, SERVICE_PRINCIPAL);
    // REST server's keytab contains keys for both principals REST uses
    KDC.createPrincipal(restServerKeytab, SPNEGO_SERVICE_PRINCIPAL, REST_SERVER_PRINCIPAL);

    // Set configuration for HBase
    HBaseKerberosUtils.setPrincipalForTesting(SERVICE_PRINCIPAL + "@" + KDC.getRealm());
    HBaseKerberosUtils.setKeytabFileForTesting(serviceKeytab.getAbsolutePath());
    // Why doesn't `setKeytabFileForTesting` do this?
    conf.set("hbase.master.keytab.file", serviceKeytab.getAbsolutePath());
    conf.set("hbase.unsafe.regionserver.hostname", "localhost");
    conf.set("hbase.master.hostname", "localhost");
    HBaseKerberosUtils.setSecuredConfiguration(conf,
        SERVICE_PRINCIPAL+ "@" + KDC.getRealm(), SPNEGO_SERVICE_PRINCIPAL+ "@" + KDC.getRealm());
    setHdfsSecuredConfiguration(conf);
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        TokenProvider.class.getName(), AccessController.class.getName());
    conf.setStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
        AccessController.class.getName());
    conf.setStrings(CoprocessorHost.REGIONSERVER_COPROCESSOR_CONF_KEY,
        AccessController.class.getName());
    // Enable EXEC permission checking
    conf.setBoolean(AccessControlConstants.EXEC_PERMISSION_CHECKS_KEY, true);
    conf.set("hbase.superuser", "hbase");
    conf.set("hadoop.proxyuser.rest.hosts", "*");
    conf.set("hadoop.proxyuser.rest.users", "*");
    conf.set("hadoop.proxyuser.wheel.hosts", "*");
    conf.set("hadoop.proxyuser.wheel.users", "*");
    UserGroupInformation.setConfiguration(conf);

    updateKerberosConfiguration(conf, REST_SERVER_PRINCIPAL, SPNEGO_SERVICE_PRINCIPAL,
        restServerKeytab);

    // Start HDFS
    TEST_UTIL.startMiniCluster(StartMiniClusterOption.builder()
        .numMasters(1)
        .numRegionServers(1)
        .numZkServers(1)
        .build());

    // Start REST
    UserGroupInformation restUser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        REST_SERVER_PRINCIPAL, restServerKeytab.getAbsolutePath());
    restUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        REST_TEST.startServletContainer(conf);
        return null;
      }
    });
    baseUrl = new URL("http://localhost:" + REST_TEST.getServletPort());

    LOG.info("HTTP server started: "+ baseUrl);
    TEST_UTIL.waitTableAvailable(TableName.valueOf("hbase:acl"));

    // Let the REST server create, read, and write globally
    UserGroupInformation superuser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        SERVICE_PRINCIPAL, serviceKeytab.getAbsolutePath());
    superuser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
          AccessControlClient.grant(
              conn, REST_SERVER_PRINCIPAL, Action.CREATE, Action.READ, Action.WRITE);
        } catch (Throwable t) {
          if (t instanceof Exception) {
            throw (Exception) t;
          } else {
            throw new Exception(t);
          }
        }
        return null;
      }
    });
    instertData();
  }

  @AfterClass
  public static void stopServer() throws Exception {
    try {
      if (null != server) {
        server.stop();
      }
    } catch (Exception e) {
      LOG.info("Failed to stop info server", e);
    }
    try {
      if (CLUSTER != null) {
        CLUSTER.shutdown();
      }
    } catch (Exception e) {
      LOG.info("Failed to stop HBase cluster", e);
    }
    try {
      if (null != KDC) {
        KDC.stop();
      }
    } catch (Exception e) {
      LOG.info("Failed to stop mini KDC", e);
    }
  }

  private static void setHdfsSecuredConfiguration(Configuration conf) throws Exception {
    // Set principal+keytab configuration for HDFS
    conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
        SERVICE_PRINCIPAL + "@" + KDC.getRealm());
    conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, serviceKeytab.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
        SERVICE_PRINCIPAL + "@" + KDC.getRealm());
    conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, serviceKeytab.getAbsolutePath());
    conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        SPNEGO_SERVICE_PRINCIPAL + "@" + KDC.getRealm());
    // Enable token access for HDFS blocks
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // Only use HTTPS (required because we aren't using "secure" ports)
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    // Bind on localhost for spnego to have a chance at working
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    // Generate SSL certs
    File keystoresDir = new File(TEST_UTIL.getDataTestDir("keystore").toUri().getPath());
    keystoresDir.mkdirs();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestSecureRESTServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir.getAbsolutePath(), sslConfDir, conf, false);

    // Magic flag to tell hdfs to not fail on using ports above 1024
    conf.setBoolean("ignore.secure.ports.for.testing", true);
  }

  private static void updateKerberosConfiguration(Configuration conf,
      String serverPrincipal, String spnegoPrincipal, File serverKeytab) {
    KerberosName.setRules("DEFAULT");

    // Enable Kerberos (pre-req)
    conf.set("hbase.security.authentication", "kerberos");
    conf.set(RESTServer.REST_AUTHENTICATION_TYPE, "kerberos");
    // User to talk to HBase as
    conf.set(RESTServer.REST_KERBEROS_PRINCIPAL, serverPrincipal);
    // User to accept SPNEGO-auth'd http calls as
    conf.set("hbase.rest.authentication.kerberos.principal", spnegoPrincipal);
    // Keytab for both principals above
    conf.set(RESTServer.REST_KEYTAB_FILE, serverKeytab.getAbsolutePath());
    conf.set("hbase.rest.authentication.kerberos.keytab", serverKeytab.getAbsolutePath());
    conf.set(HBASE_REST_SUPPORT_PROXYUSER, "true");
  }

  private static void instertData() throws IOException, InterruptedException {
    // Create a table, write a row to it, grant read perms to the client
    UserGroupInformation superuser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      SERVICE_PRINCIPAL, serviceKeytab.getAbsolutePath());
    final TableName table = TableName.valueOf("publicTable");
    superuser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration())) {
          TableDescriptor desc = TableDescriptorBuilder.newBuilder(table)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f1"))
            .build();
          conn.getAdmin().createTable(desc);
          try (Table t = conn.getTable(table)) {
            Put p = new Put(Bytes.toBytes("a"));
            p.addColumn(Bytes.toBytes("f1"), new byte[0], Bytes.toBytes("1"));
            t.put(p);
          }
          AccessControlClient.grant(conn, CLIENT_PRINCIPAL, Action.READ);
        } catch (Throwable e) {
          if (e instanceof Exception) {
            throw (Exception) e;
          } else {
            throw new Exception(e);
          }
        }
        return null;
      }
    });
  }

  public void testProxy(String extraArgs, String PRINCIPAL, File keytab, int responseCode) throws Exception{
    UserGroupInformation superuser = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      SERVICE_PRINCIPAL, serviceKeytab.getAbsolutePath());
    final TableName table = TableName.valueOf("publicTable");

    // Read that row as the client
    Pair<CloseableHttpClient,HttpClientContext> pair = getClient();
    CloseableHttpClient client = pair.getFirst();
    HttpClientContext context = pair.getSecond();

    HttpGet get = new HttpGet(new URL("http://localhost:"+ REST_TEST.getServletPort()).toURI()
      + "/" + table + "/a" + extraArgs);
    get.addHeader("Accept", "application/json");
    UserGroupInformation user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
      PRINCIPAL, keytab.getAbsolutePath());
    String jsonResponse = user.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {
        try (CloseableHttpResponse response = client.execute(get, context)) {
          final int statusCode = response.getStatusLine().getStatusCode();
          assertEquals(response.getStatusLine().toString(), responseCode, statusCode);
          HttpEntity entity = response.getEntity();
          return EntityUtils.toString(entity);
        }
      }
    });
    if(responseCode == HttpURLConnection.HTTP_OK) {
      ObjectMapper mapper = new JacksonJaxbJsonProvider().locateMapper(CellSetModel.class, MediaType.APPLICATION_JSON_TYPE);
      CellSetModel model = mapper.readValue(jsonResponse, CellSetModel.class);
      assertEquals(1, model.getRows().size());
      RowModel row = model.getRows().get(0);
      assertEquals("a", Bytes.toString(row.getKey()));
      assertEquals(1, row.getCells().size());
      CellModel cell = row.getCells().get(0);
      assertEquals("1", Bytes.toString(cell.getValue()));
    }
  }

  @Test
  public void testPositiveAuthorization() throws Exception {
    testProxy("", CLIENT_PRINCIPAL, clientKeytab, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void testDoAs() throws Exception {
    testProxy("?doAs="+CLIENT_PRINCIPAL, WHEEL_PRINCIPAL, wheelKeytab, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void testDoas() throws Exception {
    testProxy("?doas="+CLIENT_PRINCIPAL, WHEEL_PRINCIPAL, wheelKeytab, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void testWithoutDoAs() throws Exception {
    testProxy("", WHEEL_PRINCIPAL, wheelKeytab, HttpURLConnection.HTTP_FORBIDDEN);
  }


  @Test
  public void testNegativeAuthorization() throws Exception {
    Pair<CloseableHttpClient,HttpClientContext> pair = getClient();
    CloseableHttpClient client = pair.getFirst();
    HttpClientContext context = pair.getSecond();

    StringEntity entity = new StringEntity(
        "{\"name\":\"test\", \"ColumnSchema\":[{\"name\":\"f\"}]}", ContentType.APPLICATION_JSON);
    HttpPut put = new HttpPut("http://localhost:"+ REST_TEST.getServletPort() + "/test/schema");
    put.setEntity(entity);


    UserGroupInformation unprivileged = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
        CLIENT_PRINCIPAL, clientKeytab.getAbsolutePath());
    unprivileged.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (CloseableHttpResponse response = client.execute(put, context)) {
          final int statusCode = response.getStatusLine().getStatusCode();
          HttpEntity entity = response.getEntity();
          assertEquals("Got response: "+ EntityUtils.toString(entity),
              HttpURLConnection.HTTP_FORBIDDEN, statusCode);
        }
        return null;
      }
    });
  }

  private Pair<CloseableHttpClient,HttpClientContext> getClient() {
    HttpClientConnectionManager pool = new PoolingHttpClientConnectionManager();
    HttpHost host = new HttpHost("localhost", REST_TEST.getServletPort());
    Registry<AuthSchemeProvider> authRegistry =
        RegistryBuilder.<AuthSchemeProvider>create().register(AuthSchemes.SPNEGO,
            new SPNegoSchemeFactory(true, true)).build();
    CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, EmptyCredentials.INSTANCE);
    AuthCache authCache = new BasicAuthCache();

    CloseableHttpClient client = HttpClients.custom()
        .setDefaultAuthSchemeRegistry(authRegistry)
        .setConnectionManager(pool).build();

    HttpClientContext context = HttpClientContext.create();
    context.setTargetHost(host);
    context.setCredentialsProvider(credentialsProvider);
    context.setAuthSchemeRegistry(authRegistry);
    context.setAuthCache(authCache);

    return new Pair<>(client, context);
  }

  private static class EmptyCredentials implements Credentials {
    public static final EmptyCredentials INSTANCE = new EmptyCredentials();

    @Override public String getPassword() {
      return null;
    }
    @Override public Principal getUserPrincipal() {
      return null;
    }
  }
}
