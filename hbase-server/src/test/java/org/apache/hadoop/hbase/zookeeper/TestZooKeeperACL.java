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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TestZooKeeper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ZKTests;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({ ZKTests.class, MediumTests.class })
public class TestZooKeeperACL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestZooKeeperACL.class);

  private final static Logger LOG = LoggerFactory.getLogger(TestZooKeeperACL.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static ZKWatcher zkw;
  private static boolean secureZKAvailable;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    File saslConfFile = File.createTempFile("tmp", "jaas.conf");
    FileWriter fwriter = new FileWriter(saslConfFile);

    fwriter.write("" +
      "Server {\n" +
        "org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
        "user_hbase=\"secret\";\n" +
      "};\n" +
      "Client {\n" +
        "org.apache.zookeeper.server.auth.DigestLoginModule required\n" +
        "username=\"hbase\"\n" +
        "password=\"secret\";\n" +
      "};" + "\n");
    fwriter.close();
    System.setProperty("java.security.auth.login.config",
        saslConfFile.getAbsolutePath());
    System.setProperty("zookeeper.authProvider.1",
        "org.apache.zookeeper.server.auth.SASLAuthenticationProvider");

    TEST_UTIL.getConfiguration().setInt("hbase.zookeeper.property.maxClientCnxns", 1000);

    // If Hadoop is missing HADOOP-7070 the cluster will fail to start due to
    // the JAAS configuration required by ZK being clobbered by Hadoop
    try {
      TEST_UTIL.startMiniCluster();
    } catch (IOException e) {
      LOG.warn("Hadoop is missing HADOOP-7070", e);
      secureZKAvailable = false;
      return;
    }
    zkw = new ZKWatcher(
      new Configuration(TEST_UTIL.getConfiguration()),
        TestZooKeeper.class.getName(), null);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (!secureZKAvailable) {
      return;
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws Exception {
    if (!secureZKAvailable) {
      return;
    }
    TEST_UTIL.ensureSomeRegionServersAvailable(2);
  }

  /**
   * Create a node and check its ACL. When authentication is enabled on
   * ZooKeeper, all nodes (except /hbase/root-region-server, /hbase/master
   * and /hbase/hbaseid) should be created so that only the hbase server user
   * (master or region server user) that created them can access them, and
   * this user should have all permissions on this node. For
   * /hbase/root-region-server, /hbase/master, and /hbase/hbaseid the
   * permissions should be as above, but should also be world-readable. First
   * we check the general case of /hbase nodes in the following test, and
   * then check the subset of world-readable nodes in the three tests after
   * that.
   */
  @Test
  public void testHBaseRootZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase", new Stat());
    assertEquals(1, acls.size());
    assertEquals("sasl", acls.get(0).getId().getScheme());
    assertEquals("hbase", acls.get(0).getId().getId());
    assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
  }

  /**
   * When authentication is enabled on ZooKeeper, /hbase/root-region-server
   * should be created with 2 ACLs: one specifies that the hbase user has
   * full access to the node; the other, that it is world-readable.
   */
  @Test
  public void testHBaseRootRegionServerZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/root-region-server", new Stat());
    assertEquals(2, acls.size());

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals("anyone", acls.get(0).getId().getId());
        assertEquals(ZooDefs.Perms.READ, acls.get(0).getPerms());
        foundWorldReadableAcl = true;
      }
      else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals("hbase", acls.get(1).getId().getId());
          assertEquals("sasl", acls.get(1).getId().getScheme());
          foundHBaseOwnerAcl = true;
        } else { // error: should not get here: test fails.
          assertTrue(false);
        }
      }
    }
    assertTrue(foundWorldReadableAcl);
    assertTrue(foundHBaseOwnerAcl);
  }

  /**
   * When authentication is enabled on ZooKeeper, /hbase/master should be
   * created with 2 ACLs: one specifies that the hbase user has full access
   * to the node; the other, that it is world-readable.
   */
  @Test
  public void testHBaseMasterServerZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/master", new Stat());
    assertEquals(2, acls.size());

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals("anyone", acls.get(0).getId().getId());
        assertEquals(ZooDefs.Perms.READ, acls.get(0).getPerms());
        foundWorldReadableAcl = true;
      } else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals("hbase", acls.get(1).getId().getId());
          assertEquals("sasl", acls.get(1).getId().getScheme());
          foundHBaseOwnerAcl = true;
        } else { // error: should not get here: test fails.
          assertTrue(false);
        }
      }
    }
    assertTrue(foundWorldReadableAcl);
    assertTrue(foundHBaseOwnerAcl);
  }

  /**
   * When authentication is enabled on ZooKeeper, /hbase/hbaseid should be
   * created with 2 ACLs: one specifies that the hbase user has full access
   * to the node; the other, that it is world-readable.
   */
  @Test
  public void testHBaseIDZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/hbaseid", new Stat());
    assertEquals(2, acls.size());

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals("anyone", acls.get(0).getId().getId());
        assertEquals(ZooDefs.Perms.READ, acls.get(0).getPerms());
        foundWorldReadableAcl = true;
      } else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals("hbase", acls.get(1).getId().getId());
          assertEquals("sasl", acls.get(1).getId().getScheme());
          foundHBaseOwnerAcl = true;
        } else { // error: should not get here: test fails.
          assertTrue(false);
        }
      }
    }
    assertTrue(foundWorldReadableAcl);
    assertTrue(foundHBaseOwnerAcl);
  }

  /**
   * Finally, we check the ACLs of a node outside of the /hbase hierarchy and
   * verify that its ACL is simply 'hbase:Perms.ALL'.
   */
  @Test
  public void testOutsideHBaseNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    ZKUtil.createWithParents(zkw, "/testACLNode");
    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/testACLNode", new Stat());
    assertEquals(1, acls.size());
    assertEquals("sasl", acls.get(0).getId().getScheme());
    assertEquals("hbase", acls.get(0).getId().getId());
    assertEquals(ZooDefs.Perms.ALL, acls.get(0).getPerms());
  }

  /**
   * Check if ZooKeeper JaasConfiguration is valid.
   */
  @Test
  public void testIsZooKeeperSecure() throws Exception {
    boolean testJaasConfig =
        ZKAuthentication.isSecureZooKeeper(new Configuration(TEST_UTIL.getConfiguration()));
    assertEquals(testJaasConfig, secureZKAvailable);
    // Define Jaas configuration without ZooKeeper Jaas config
    File saslConfFile = File.createTempFile("tmp", "fakeJaas.conf");
    FileWriter fwriter = new FileWriter(saslConfFile);

    fwriter.write("");
    fwriter.close();
    System.setProperty("java.security.auth.login.config",
        saslConfFile.getAbsolutePath());

    testJaasConfig = ZKAuthentication.isSecureZooKeeper(
      new Configuration(TEST_UTIL.getConfiguration()));
    assertFalse(testJaasConfig);
    saslConfFile.delete();
  }

  /**
   * Check if Programmatic way of setting zookeeper security settings is valid.
   */
  @Test
  public void testIsZooKeeperSecureWithProgrammaticConfig() throws Exception {

    javax.security.auth.login.Configuration.setConfiguration(new DummySecurityConfiguration());

    Configuration config = new Configuration(HBaseConfiguration.create());
    boolean testJaasConfig = ZKAuthentication.isSecureZooKeeper(config);
    assertFalse(testJaasConfig);

    // Now set authentication scheme to Kerberos still it should return false
    // because no configuration set
    config.set("hbase.security.authentication", "kerberos");
    testJaasConfig = ZKAuthentication.isSecureZooKeeper(config);
    assertFalse(testJaasConfig);

    // Now set programmatic options related to security
    config.set(HConstants.ZK_CLIENT_KEYTAB_FILE, "/dummy/file");
    config.set(HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, "dummy");
    config.set(HConstants.ZK_SERVER_KEYTAB_FILE, "/dummy/file");
    config.set(HConstants.ZK_SERVER_KERBEROS_PRINCIPAL, "dummy");
    testJaasConfig = ZKAuthentication.isSecureZooKeeper(config);
    assertTrue(testJaasConfig);
  }

  private static class DummySecurityConfiguration extends javax.security.auth.login.Configuration {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      return null;
    }
  }

  @Test
  public void testAdminDrainAllowedOnSecureZK() throws Exception {
    if (!secureZKAvailable) {
      return;
    }
    List<ServerName> decommissionedServers = new ArrayList<>(1);
    decommissionedServers.add(ServerName.parseServerName("ZZZ,123,123"));

    // If unable to connect to secure ZK cluster then this operation would fail.
    TEST_UTIL.getAdmin().decommissionRegionServers(decommissionedServers, false);

    decommissionedServers = TEST_UTIL.getAdmin().listDecommissionedRegionServers();
    assertEquals(1, decommissionedServers.size());
    assertEquals(ServerName.parseServerName("ZZZ,123,123"), decommissionedServers.get(0));

    TEST_UTIL.getAdmin().recommissionRegionServer(decommissionedServers.get(0), null);
    decommissionedServers = TEST_UTIL.getAdmin().listDecommissionedRegionServers();
    assertEquals(0, decommissionedServers.size());
  }

}

