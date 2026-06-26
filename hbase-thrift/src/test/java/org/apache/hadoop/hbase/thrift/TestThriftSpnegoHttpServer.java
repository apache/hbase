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
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SUPPORT_PROXYUSER_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.SimpleKdcServerUtil;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.http.HttpHeaders;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.KerberosCredentials;
import org.apache.http.client.config.AuthSchemes;
import org.apache.http.config.Lookup;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.impl.auth.SPNegoSchemeFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.hbase.thirdparty.org.apache.thrift.protocol.TProtocol;
import org.apache.hbase.thirdparty.org.apache.thrift.transport.THttpClient;

/**
 * Start the HBase Thrift HTTP server on a random port through the command-line interface and talk
 * to it from client side with SPNEGO security enabled.
 */
@Tag(ClientTests.TAG)
@Tag(LargeTests.TAG)
public class TestThriftSpnegoHttpServer extends TestThriftHttpServerBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestThriftSpnegoHttpServer.class);

  private static SimpleKdcServer kdc;
  private static File serverKeytab;
  private static File spnegoServerKeytab;
  private static File clientKeytab;

  private static String clientPrincipal;
  private static String clientPrincipal2;
  private static String serverPrincipal;
  private static String spnegoServerPrincipal;

  private static void addSecurityConfigurations(Configuration conf) {
    KerberosName.setRules("DEFAULT");

    HBaseKerberosUtils.setKeytabFileForTesting(serverKeytab.getAbsolutePath());

    conf.setBoolean(THRIFT_SUPPORT_PROXYUSER_KEY, true);
    conf.setBoolean(Constants.USE_HTTP_CONF_KEY, true);

    conf.set(Constants.THRIFT_KERBEROS_PRINCIPAL_KEY, serverPrincipal);
    conf.set(Constants.THRIFT_KEYTAB_FILE_KEY, serverKeytab.getAbsolutePath());

    HBaseKerberosUtils.setSecuredConfiguration(conf, serverPrincipal, spnegoServerPrincipal);
    conf.set("hadoop.proxyuser.hbase.hosts", "*");
    conf.set("hadoop.proxyuser.hbase.groups", "*");
    conf.set(Constants.THRIFT_SPNEGO_PRINCIPAL_KEY, spnegoServerPrincipal);
    conf.set(Constants.THRIFT_SPNEGO_KEYTAB_FILE_KEY, spnegoServerKeytab.getAbsolutePath());
  }

  @BeforeAll
  public static void beforeAll() throws Exception {
    kdc = SimpleKdcServerUtil.getRunningSimpleKdcServer(
      new File(TEST_UTIL.getDataTestDir().toString()), HBaseTestingUtil::randomFreePort);
    File keytabDir = Paths.get(TEST_UTIL.getRandomDir().toString()).toAbsolutePath().toFile();
    assertTrue(keytabDir.mkdirs());

    clientPrincipal = "client@" + kdc.getKdcConfig().getKdcRealm();
    clientPrincipal2 = "client2@" + kdc.getKdcConfig().getKdcRealm();
    clientKeytab = new File(keytabDir, clientPrincipal + ".keytab");
    kdc.createAndExportPrincipals(clientKeytab, clientPrincipal, clientPrincipal2);

    String hostname = InetAddress.getLoopbackAddress().getHostName();
    serverPrincipal = "hbase/" + hostname + "@" + kdc.getKdcConfig().getKdcRealm();
    serverKeytab = new File(keytabDir, serverPrincipal.replace('/', '_') + ".keytab");

    // Setup separate SPNEGO keytab
    spnegoServerPrincipal = "HTTP/" + hostname + "@" + kdc.getKdcConfig().getKdcRealm();
    spnegoServerKeytab = new File(keytabDir, spnegoServerPrincipal.replace('/', '_') + ".keytab");
    kdc.createAndExportPrincipals(spnegoServerKeytab, spnegoServerPrincipal);
    kdc.createAndExportPrincipals(serverKeytab, serverPrincipal);

    TEST_UTIL.getConfiguration().setBoolean(Constants.USE_HTTP_CONF_KEY, true);
    addSecurityConfigurations(TEST_UTIL.getConfiguration());

    TestThriftHttpServerBase.setUpBeforeClass();
  }

  @Override
  protected Supplier<ThriftServer> getThriftServerSupplier() {
    return () -> new ThriftServer(TEST_UTIL.getConfiguration());
  }

  @AfterAll
  public static void afterAll() throws Exception {
    TestThriftHttpServerBase.tearDownAfterClass();

    try {
      if (null != kdc) {
        kdc.stop();
        kdc = null;
      }
    } catch (Exception e) {
      LOG.info("Failed to stop mini KDC", e);
    }
  }

  /**
   * Block call through to this method. It is a messy test that fails because of bad config and then
   * succeeds only the first attempt adds a table which the second attempt doesn't want to be in
   * place to succeed. Let the super impl of this test be responsible for verifying we fail if bad
   * header size.
   */
  @Disabled
  @Test
  @Override
  public void testRunThriftServerWithHeaderBufferLength() throws Exception {
    super.testRunThriftServerWithHeaderBufferLength();
  }

  private void testScanWithDifferentClients(Hbase.Client client, Hbase.Client client2)
    throws Exception {
    List<Mutation> mutations = new ArrayList<>(1);
    mutations
      .add(new Mutation(false, TestThriftServer.columnAAname, TestThriftServer.valueAname, true));
    client.mutateRow(TestThriftServer.tableAname, TestThriftServer.rowAname, mutations,
      Collections.emptyMap());

    int id = client.scannerOpen(TestThriftServer.tableAname, ByteBuffer.allocate(0),
      Collections.emptyList(), Collections.emptyMap());

    assertThrows(IOError.class, () -> client2.scannerGet(id)).printStackTrace();
    assertThrows(IOError.class, () -> client2.scannerClose(id)).printStackTrace();

    assertEquals(1, client.scannerGet(id).size());
    assertEquals(0, client.scannerGet(id).size());
    client.scannerClose(id);
  }

  @Override
  protected void talkToThriftServer(String url, int customHeaderSize) throws Exception {
    // Close httpClient and THttpClient automatically on any failures
    try (CloseableHttpClient httpClient = createHttpClient(clientPrincipal);
      THttpClient tHttpClient = new THttpClient(url, httpClient);
      CloseableHttpClient httpClient2 = createHttpClient(clientPrincipal2);
      THttpClient tHttpClient2 = new THttpClient(url, httpClient2)) {
      tHttpClient.open();
      if (customHeaderSize > 0) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < customHeaderSize; i++) {
          sb.append("a");
        }
        tHttpClient.setCustomHeader(HttpHeaders.USER_AGENT, sb.toString());
      }

      TProtocol prot = new TBinaryProtocol(tHttpClient);
      Hbase.Client client = new Hbase.Client(prot);
      List<ByteBuffer> bbs = client.getTableNames();
      LOG.info("PRE-EXISTING {}",
        bbs.stream().map(b -> Bytes.toString(b.array())).collect(Collectors.joining(",")));
      if (!bbs.isEmpty()) {
        for (ByteBuffer bb : bbs) {
          client.disableTable(bb);
          client.deleteTable(bb);
        }
      }
      TestThriftServer.createTestTables(client);
      TestThriftServer.checkTableList(client);

      TProtocol prop2 = new TBinaryProtocol(tHttpClient2);
      Hbase.Client client2 = new Hbase.Client(prop2);
      testScanWithDifferentClients(client, client2);

      TestThriftServer.dropTestTables(client);
    }
  }

  private CloseableHttpClient createHttpClient(String clientPrincipal) throws Exception {
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(clientPrincipal, clientKeytab);
    final Set<Principal> clientPrincipals = clientSubject.getPrincipals();
    // Make sure the subject has a principal
    assertFalse(clientPrincipals.isEmpty(), "Found no client principals in the clientSubject.");

    // Get a TGT for the subject (might have many, different encryption types). The first should
    // be the default encryption type.
    Set<KerberosTicket> privateCredentials =
      clientSubject.getPrivateCredentials(KerberosTicket.class);
    assertFalse(privateCredentials.isEmpty(), "Found no private credentials in the clientSubject.");
    KerberosTicket tgt = privateCredentials.iterator().next();
    assertNotNull(tgt, "No kerberos ticket found.");

    // The name of the principal
    final String clientPrincipalName = clientPrincipals.iterator().next().getName();

    return Subject.doAs(clientSubject, (PrivilegedExceptionAction<CloseableHttpClient>) () -> {
      // Logs in with Kerberos via GSS
      GSSManager gssManager = GSSManager.getInstance();
      // jGSS Kerberos login constant
      Oid oid = new Oid("1.2.840.113554.1.2.2");
      GSSName gssClient = gssManager.createName(clientPrincipalName, GSSName.NT_USER_NAME);
      GSSCredential credential = gssManager.createCredential(gssClient,
        GSSCredential.DEFAULT_LIFETIME, oid, GSSCredential.INITIATE_ONLY);

      Lookup<AuthSchemeProvider> authRegistry = RegistryBuilder.<AuthSchemeProvider> create()
        .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, true)).build();

      BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY, new KerberosCredentials(credential));

      return HttpClients.custom().setDefaultAuthSchemeRegistry(authRegistry)
        .setDefaultCredentialsProvider(credentialsProvider).build();
    });
  }
}
