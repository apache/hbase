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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.HttpURLConnection;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifs;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Test class for LDAP authentication on the HttpServer.
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
@CreateLdapServer(
    transports = { @CreateTransport(protocol = "LDAP", address = LdapConstants.LDAP_SERVER_ADDR), })
@CreateDS(name = "TestLdapHttpServer", allowAnonAccess = true,
    partitions = { @CreatePartition(name = "Test_Partition", suffix = LdapConstants.LDAP_BASE_DN,
        contextEntry = @ContextEntry(entryLdif = "dn: " + LdapConstants.LDAP_BASE_DN + " \n"
          + "dc: example\n" + "objectClass: top\n" + "objectClass: domain\n\n")) })
@ApplyLdifs({ "dn: uid=bjones," + LdapConstants.LDAP_BASE_DN, "cn: Bob Jones", "sn: Jones",
  "objectClass: inetOrgPerson", "uid: bjones", "userPassword: p@ssw0rd" })
public class TestLdapHttpServer extends LdapServerTestBase {

  private static final String BJONES_CREDENTIALS = "bjones:p@ssw0rd";
  private static final String WRONG_CREDENTIALS = "bjones:password";

  @Test
  public void testUnauthorizedClientsDisallowed() throws IOException {
    HttpURLConnection conn = openConnection("/echo?a=b", null);
    assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
  }

  @Test
  public void testAllowedClient() throws IOException {
    HttpURLConnection conn = openConnection("/echo?a=b", BJONES_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
  }

  @Test
  public void testWrongAuthClientsDisallowed() throws IOException {
    HttpURLConnection conn = openConnection("/echo?a=b", WRONG_CREDENTIALS);
    assertEquals(HttpURLConnection.HTTP_FORBIDDEN, conn.getResponseCode());
  }
}
