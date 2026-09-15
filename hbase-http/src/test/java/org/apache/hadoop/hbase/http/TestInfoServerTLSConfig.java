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
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InfoServer}'s TLS-config resolution: the 3-tier fallback chain
 * ({@code hbase.ui.ssl.server.*} → {@code hbase.ui.ssl.*} → {@code ssl.server.*}) that
 * underpins single-EKU certificate support on the UI surface, and the client-auth-mode key.
 */
@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestInfoServerTLSConfig {

  private static final String POSTFIX = "keystore.location";
  private static final String ROLE_SCOPED_KEY = "hbase.ui.ssl.server." + POSTFIX;
  private static final String HBASE_KEY = "hbase.ui.ssl." + POSTFIX;
  private static final String HADOOP_KEY = "ssl.server." + POSTFIX;

  @Test
  public void testRoleScopedTakesPrecedenceOverHBasePrefixed() {
    Configuration c = new Configuration(false);
    c.set(ROLE_SCOPED_KEY, "role-scoped-value");
    c.set(HBASE_KEY, "hbase-prefixed-value");
    c.set(HADOOP_KEY, "hadoop-prefixed-value");
    assertEquals("role-scoped-value", InfoServer.getTLSProperty(c, POSTFIX));
  }

  @Test
  public void testHBasePrefixedTakesPrecedenceOverHadoopPrefixed() {
    Configuration c = new Configuration(false);
    // No role-scoped key set — hbase-prefixed key must win.
    c.set(HBASE_KEY, "hbase-prefixed-value");
    c.set(HADOOP_KEY, "hadoop-prefixed-value");
    assertEquals("hbase-prefixed-value", InfoServer.getTLSProperty(c, POSTFIX));
  }

  @Test
  public void testFallsBackToHadoopPrefixedWhenNoOthersSet() {
    Configuration c = new Configuration(false);
    c.set(HADOOP_KEY, "hadoop-prefixed-value");
    assertEquals("hadoop-prefixed-value", InfoServer.getTLSProperty(c, POSTFIX));
  }

  @Test
  public void testReturnsDefaultWhenNoneSet() {
    Configuration c = new Configuration(false);
    assertEquals("jks", InfoServer.getTLSProperty(c, POSTFIX, "jks"));
    assertNull(InfoServer.getTLSProperty(c, POSTFIX));
  }

  @Test
  public void testGetTLSPasswordHonorsThreeTierFallback() throws Exception {
    Configuration c = new Configuration(false);
    // Configuration.getPassword returns null when unset; verifying the fallback picks each tier.
    c.set(ROLE_SCOPED_KEY.replace("keystore.location", "keystore.password"), "role-pw");
    c.set(HBASE_KEY.replace("keystore.location", "keystore.password"), "hbase-pw");
    c.set(HADOOP_KEY.replace("keystore.location", "keystore.password"), "hadoop-pw");
    assertEquals("role-pw", InfoServer.getTLSPassword(c, "keystore.password"));

    c.unset(ROLE_SCOPED_KEY.replace("keystore.location", "keystore.password"));
    assertEquals("hbase-pw", InfoServer.getTLSPassword(c, "keystore.password"));

    c.unset(HBASE_KEY.replace("keystore.location", "keystore.password"));
    assertEquals("hadoop-pw", InfoServer.getTLSPassword(c, "keystore.password"));

    c.unset(HADOOP_KEY.replace("keystore.location", "keystore.password"));
    assertNull(InfoServer.getTLSPassword(c, "keystore.password"));
  }

  @Test
  public void testClientAuthModeKeyIsRoleScoped() {
    // Guard against a "double server.server." regression: the client-auth-mode config key must
    // resolve to the single role-scoped key, not to a nested/prefixed form.
    assertEquals("hbase.ui.ssl.server.client.auth.mode", InfoServer.HBASE_UI_SSL_CLIENT_AUTH_MODE);
  }
}