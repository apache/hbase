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

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.crypto.KeyProviderTestUtils;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyStoreKeyProvider;
import org.apache.hadoop.hbase.keymeta.ManagedKeyTestBase;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.embed.ScriptingContainer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, IntegrationTests.class })
public class TestKeymetaAdminShell extends ManagedKeyTestBase implements RubyShellTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeymetaAdminShell.class);

  private final ScriptingContainer jruby = new ScriptingContainer();

  @Before
  public void setUp() throws Exception {
    final Configuration conf = TEST_UTIL.getConfiguration();
    conf.set("zookeeper.session.timeout", "6000000");
    conf.set("hbase.rpc.timeout", "6000000");
    conf.set("hbase.rpc.read.timeout", "6000000");
    conf.set("hbase.rpc.write.timeout", "6000000");
    conf.set("hbase.client.operation.timeout", "6000000");
    conf.set("hbase.client.scanner.timeout.period", "6000000");
    conf.set("hbase.ipc.client.socket.timeout.connect", "6000000");
    conf.set("hbase.ipc.client.socket.timeout.read", "6000000");
    conf.set("hbase.ipc.client.socket.timeout.write", "6000000");
    conf.set("hbase.master.start.timeout.localHBaseCluster", "6000000");
    conf.set("hbase.master.init.timeout.localHBaseCluster", "6000000");
    conf.set("hbase.client.sync.wait.timeout.msec", "6000000");
    Map<Bytes, Bytes> cust2key = new HashMap<>();
    Map<Bytes, String> cust2alias = new HashMap<>();
    String clusterId = UUID.randomUUID().toString();
    byte[] systemKey;
    String SYSTEM_KEY_ALIAS = "system-key-alias";
    String CUST1 = "cust1";
    String CUST1_ALIAS = "cust1-alias";
    String GLOB_CUST_ALIAS = "glob-cust-alias";
    String providerParams = KeyProviderTestUtils.setupTestKeyStore(TEST_UTIL, true, true, store -> {
      Properties p = new Properties();
      try {
        KeyProviderTestUtils.addEntry(conf, 128, store, CUST1_ALIAS, CUST1,
          true, cust2key, cust2alias, p);
        KeyProviderTestUtils.addEntry(conf, 128, store, GLOB_CUST_ALIAS,
          "*", true, cust2key, cust2alias, p);
        KeyProviderTestUtils.addEntry(conf, 128, store, SYSTEM_KEY_ALIAS,
          clusterId, true, cust2key, cust2alias, p);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return p;
    });
    systemKey = cust2key.get(new Bytes(clusterId.getBytes())).get();
    conf.set(HConstants.CRYPTO_MANAGED_KEY_STORE_SYSTEM_KEY_NAME_CONF_KEY,
      SYSTEM_KEY_ALIAS);
    conf.set(HConstants.CRYPTO_KEYPROVIDER_PARAMETERS_KEY, providerParams);
    RubyShellTest.setUpConfig(this);
    super.setUp();
    RubyShellTest.setUpJRubyRuntime(this);
    RubyShellTest.doTestSetup(this);
    addCustodianRubyEnvVars( jruby, "CUST1", CUST1);
  }

  @Override
  public HBaseTestingUtil getTEST_UTIL() {
    return TEST_UTIL;
  }

  @Override
  public ScriptingContainer getJRuby() {
    return jruby;
  }

  @Override
  public String getSuitePattern() {
    return "**/*_keymeta_test.rb";
  }

  @Test
  public void testRunShellTests() throws Exception {
    RubyShellTest.testRunShellTests(this);
  }

  @Override
  protected Class<? extends ManagedKeyProvider> getKeyProviderClass() {
    return ManagedKeyStoreKeyProvider.class;
  }

  public static void addCustodianRubyEnvVars(ScriptingContainer jruby, String custId,
    String custodian) {
    jruby.put("$"+custId, custodian);
    jruby.put("$"+custId+"_ALIAS", custodian+"-alias");
    jruby.put("$"+custId+"_ENCODED", Base64.getEncoder().encodeToString(custodian.getBytes()));
  }
}
