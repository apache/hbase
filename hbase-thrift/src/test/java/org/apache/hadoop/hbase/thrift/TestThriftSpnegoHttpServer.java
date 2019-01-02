/*
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.thrift;

import static org.apache.hadoop.hbase.thrift.Constants.THRIFT_SUPPORT_PROXYUSER_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.security.Principal;
import java.security.PrivilegedExceptionAction;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosTicket;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
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
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Start the HBase Thrift HTTP server on a random port through the command-line
 * interface and talk to it from client side with SPNEGO security enabled.
 */
@Category({ClientTests.class, LargeTests.class})
public class TestThriftSpnegoHttpServer extends TestThriftHttpServer {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestThriftSpnegoHttpServer.class);

  private static final Logger LOG =
    LoggerFactory.getLogger(TestThriftSpnegoHttpServer.class);

  private static SimpleKdcServer kdc;
  private static File serverKeytab;
  private static File spnegoServerKeytab;
  private static File clientKeytab;

  private static String clientPrincipal;
  private static String serverPrincipal;
  private static String spnegoServerPrincipal;

  private static void setupUser(SimpleKdcServer kdc, File keytab, String principal)
      throws KrbException {
    kdc.createPrincipal(principal);
    kdc.exportPrincipal(principal, keytab);
  }

  private static SimpleKdcServer buildMiniKdc() throws Exception {
    SimpleKdcServer kdc = new SimpleKdcServer();

    final File target = new File(System.getProperty("user.dir"), "target");
    File kdcDir = new File(target, TestThriftSpnegoHttpServer.class.getSimpleName());
    if (kdcDir.exists()) {
      FileUtils.deleteDirectory(kdcDir);
    }
    kdcDir.mkdirs();
    kdc.setWorkDir(kdcDir);

    kdc.setKdcHost(HConstants.LOCALHOST);
    int kdcPort = HBaseTestingUtility.randomFreePort();
    kdc.setAllowTcp(true);
    kdc.setAllowUdp(false);
    kdc.setKdcTcpPort(kdcPort);

    LOG.info("Starting KDC server at " + HConstants.LOCALHOST + ":" + kdcPort);

    kdc.init();

    return kdc;
  }

  private static void addSecurityConfigurations(Configuration conf) {
    KerberosName.setRules("DEFAULT");

    HBaseKerberosUtils.setKeytabFileForTesting(serverKeytab.getAbsolutePath());
    HBaseKerberosUtils.setSecuredConfiguration(conf, serverPrincipal, spnegoServerPrincipal);

    conf.setBoolean(THRIFT_SUPPORT_PROXYUSER_KEY, true);
    conf.setBoolean(Constants.USE_HTTP_CONF_KEY, true);
    conf.set("hadoop.proxyuser.hbase.hosts", "*");
    conf.set("hadoop.proxyuser.hbase.groups", "*");

    conf.set(Constants.THRIFT_KERBEROS_PRINCIPAL_KEY, serverPrincipal);
    conf.set(Constants.THRIFT_KEYTAB_FILE_KEY, serverKeytab.getAbsolutePath());
    conf.set(Constants.THRIFT_SPNEGO_PRINCIPAL_KEY, spnegoServerPrincipal);
    conf.set(Constants.THRIFT_SPNEGO_KEYTAB_FILE_KEY, spnegoServerKeytab.getAbsolutePath());
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    final File target = new File(System.getProperty("user.dir"), "target");
    assertTrue(target.exists());

    File keytabDir = new File(target, TestThriftSpnegoHttpServer.class.getSimpleName() +
      "_keytabs");
    if (keytabDir.exists()) {
      FileUtils.deleteDirectory(keytabDir);
    }
    keytabDir.mkdirs();

    kdc = buildMiniKdc();
    kdc.start();

    clientPrincipal = "client@" + kdc.getKdcConfig().getKdcRealm();
    clientKeytab = new File(keytabDir, clientPrincipal + ".keytab");
    setupUser(kdc, clientKeytab, clientPrincipal);

    serverPrincipal = "hbase/" + HConstants.LOCALHOST + "@" + kdc.getKdcConfig().getKdcRealm();
    serverKeytab = new File(keytabDir, serverPrincipal.replace('/', '_') + ".keytab");
    setupUser(kdc, serverKeytab, serverPrincipal);

    spnegoServerPrincipal = "HTTP/" + HConstants.LOCALHOST + "@" + kdc.getKdcConfig().getKdcRealm();
    spnegoServerKeytab = new File(keytabDir, spnegoServerPrincipal.replace('/', '_') + ".keytab");
    setupUser(kdc, spnegoServerKeytab, spnegoServerPrincipal);

    TEST_UTIL.getConfiguration().setBoolean(Constants.USE_HTTP_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setBoolean("hbase.table.sanity.checks", false);
    addSecurityConfigurations(TEST_UTIL.getConfiguration());

    TestThriftHttpServer.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TestThriftHttpServer.tearDownAfterClass();

    try {
      if (null != kdc) {
        kdc.stop();
      }
    } catch (Exception e) {
      LOG.info("Failed to stop mini KDC", e);
    }
  }

  @Override
  protected void talkToThriftServer(String url, int customHeaderSize) throws Exception {
    // Close httpClient and THttpClient automatically on any failures
    try (
        CloseableHttpClient httpClient = createHttpClient();
        THttpClient tHttpClient = new THttpClient(url, httpClient)
    ) {
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
      if (!tableCreated) {
        TestThriftServer.createTestTables(client);
        tableCreated = true;
      }
      TestThriftServer.checkTableList(client);
    }
  }

  private CloseableHttpClient createHttpClient() throws Exception {
    final Subject clientSubject = JaasKrbUtil.loginUsingKeytab(clientPrincipal, clientKeytab);
    final Set<Principal> clientPrincipals = clientSubject.getPrincipals();
    // Make sure the subject has a principal
    assertFalse(clientPrincipals.isEmpty());

    // Get a TGT for the subject (might have many, different encryption types). The first should
    // be the default encryption type.
    Set<KerberosTicket> privateCredentials =
        clientSubject.getPrivateCredentials(KerberosTicket.class);
    assertFalse(privateCredentials.isEmpty());
    KerberosTicket tgt = privateCredentials.iterator().next();
    assertNotNull(tgt);

    // The name of the principal
    final String clientPrincipalName = clientPrincipals.iterator().next().getName();

    return Subject.doAs(clientSubject, new PrivilegedExceptionAction<CloseableHttpClient>() {
      @Override
      public CloseableHttpClient run() throws Exception {
        // Logs in with Kerberos via GSS
        GSSManager gssManager = GSSManager.getInstance();
        // jGSS Kerberos login constant
        Oid oid = new Oid("1.2.840.113554.1.2.2");
        GSSName gssClient = gssManager.createName(clientPrincipalName, GSSName.NT_USER_NAME);
        GSSCredential credential = gssManager.createCredential(gssClient,
            GSSCredential.DEFAULT_LIFETIME, oid, GSSCredential.INITIATE_ONLY);

        Lookup<AuthSchemeProvider> authRegistry = RegistryBuilder.<AuthSchemeProvider>create()
            .register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true, true))
            .build();

        BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new KerberosCredentials(credential));

        return HttpClients.custom()
            .setDefaultAuthSchemeRegistry(authRegistry)
            .setDefaultCredentialsProvider(credentialsProvider)
            .build();
      }
    });
  }
}
