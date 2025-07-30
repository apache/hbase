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

import java.io.IOException;
import java.net.HttpURLConnection;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifs;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.http.resource.JerseyResource;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class for admin ACLs with LDAP authentication on the HttpServer.
 */
@Category({ MiscTests.class, SmallTests.class })
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = LdapConstants.LDAP_SERVER_ADDR), })
@CreateDS(name = "TestLdapAdminACL", allowAnonAccess = true,
    partitions = { @CreatePartition(name = "Test_Partition", suffix = LdapConstants.LDAP_BASE_DN,
        contextEntry = @ContextEntry(entryLdif = "dn: " + LdapConstants.LDAP_BASE_DN + " \n"
          + "dc: example\n" + "objectClass: top\n" + "objectClass: domain\n\n")) })
@ApplyLdifs({ "dn: uid=bjones," + LdapConstants.LDAP_BASE_DN, "cn: Bob Jones", "sn: Jones",
  "objectClass: inetOrgPerson", "uid: bjones", "userPassword: p@ssw0rd",

  "dn: uid=jdoe," + LdapConstants.LDAP_BASE_DN, "cn: John Doe", "sn: Doe",
  "objectClass: inetOrgPerson", "uid: jdoe", "userPassword: secure123" })
public class TestLdapAdminACL extends LdapServerTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLdapAdminACL.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestLdapAdminACL.class);

  private static final String ADMIN_CREDENTIALS = "bjones:p@ssw0rd";
  private static final String NON_ADMIN_CREDENTIALS = "jdoe:secure123";
  private static final String WRONG_CREDENTIALS = "bjones:password";

  @BeforeClass
  public static void setupServer() throws Exception {
    Configuration conf = new Configuration();
    setLdapConfigurationWithACLs(conf);

    server = createTestServer(conf, InfoServer.buildAdminAcl(conf));
    server.addUnprivilegedServlet("echo", "/echo", TestHttpServer.EchoServlet.class);
    // we will reuse /jmx which is a privileged servlet
    server.addJerseyResourcePackage(JerseyResource.class.getPackage().getName(), "/jersey/*");
    server.start();

    baseUrl = getServerURL(server);
    LOG.info("HTTP server started: " + baseUrl);
  }

  private static void setLdapConfigurationWithACLs(Configuration conf) {
    setLdapConfigurations(conf);

    // Enable LDAP admin ACL
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
    conf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_INSTRUMENTATION_REQUIRES_ADMIN, true);
    conf.set(HttpServer.HTTP_LDAP_AUTHENTICATION_ADMIN_USERS_KEY, "bjones");
  }

  @Test
  public void testAdminAllowedUnprivilegedServletAccess() throws IOException {
    HttpURLConnection conn = openConnection("/echo?a=b", ADMIN_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  @Test
  public void testAdminAllowedPrivilegedServletAccess() throws IOException {
    HttpURLConnection conn = openConnection("/jmx", ADMIN_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  @Test
  public void testNonAdminAllowedUnprivilegedServletAccess() throws IOException {
    HttpURLConnection conn = openConnection("/echo?a=b", NON_ADMIN_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  @Test
  public void testNonAdminDisallowedPrivilegedServletAccess() throws IOException {
    HttpURLConnection conn = openConnection("/jmx", NON_ADMIN_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }

  @Test
  public void testWrongAuthDisallowedUnprivilegedServletAccess() throws IOException {
    HttpURLConnection conn = openConnection("/echo?a=b", WRONG_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }

  @Test
  public void testWrongAuthDisallowedPrivilegedServletAccess() throws IOException {
    HttpURLConnection conn = openConnection("/jmx", WRONG_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }
}
