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
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, MediumTests.class})
public class TestZooKeeperACL {
  private final static Log LOG = LogFactory.getLog(TestZooKeeperACL.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private static ZooKeeperWatcher zkw;
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
    zkw = new ZooKeeperWatcher(
      new Configuration(TEST_UTIL.getConfiguration()),
        TestZooKeeper.class.getName(), null);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (!secureZKAvailable) {
      return;
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
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
  @Test (timeout=30000)
  public void testHBaseRootZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase", new Stat());
    assertEquals(acls.size(),1);
    assertEquals(acls.get(0).getId().getScheme(),"sasl");
    assertEquals(acls.get(0).getId().getId(),"hbase");
    assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.ALL);
  }

  /**
   * When authentication is enabled on ZooKeeper, /hbase/root-region-server
   * should be created with 2 ACLs: one specifies that the hbase user has
   * full access to the node; the other, that it is world-readable.
   */
  @Test (timeout=30000)
  public void testHBaseRootRegionServerZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/root-region-server", new Stat());
    assertEquals(acls.size(),2);

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals(acls.get(0).getId().getId(),"anyone");
        assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.READ);
        foundWorldReadableAcl = true;
      }
      else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals(acls.get(1).getId().getId(),"hbase");
          assertEquals(acls.get(1).getId().getScheme(),"sasl");
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
  @Test (timeout=30000)
  public void testHBaseMasterServerZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/master", new Stat());
    assertEquals(acls.size(),2);

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals(acls.get(0).getId().getId(),"anyone");
        assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.READ);
        foundWorldReadableAcl = true;
      } else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals(acls.get(1).getId().getId(),"hbase");
          assertEquals(acls.get(1).getId().getScheme(),"sasl");
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
  @Test (timeout=30000)
  public void testHBaseIDZNodeACL() throws Exception {
    if (!secureZKAvailable) {
      return;
    }

    List<ACL> acls = zkw.getRecoverableZooKeeper().getZooKeeper()
        .getACL("/hbase/hbaseid", new Stat());
    assertEquals(acls.size(),2);

    boolean foundWorldReadableAcl = false;
    boolean foundHBaseOwnerAcl = false;
    for(int i = 0; i < 2; i++) {
      if (acls.get(i).getId().getScheme().equals("world") == true) {
        assertEquals(acls.get(0).getId().getId(),"anyone");
        assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.READ);
        foundWorldReadableAcl = true;
      } else {
        if (acls.get(i).getId().getScheme().equals("sasl") == true) {
          assertEquals(acls.get(1).getId().getId(),"hbase");
          assertEquals(acls.get(1).getId().getScheme(),"sasl");
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
    assertEquals(acls.size(),1);
    assertEquals(acls.get(0).getId().getScheme(),"sasl");
    assertEquals(acls.get(0).getId().getId(),"hbase");
    assertEquals(acls.get(0).getPerms(), ZooDefs.Perms.ALL);
  }

  /**
   * Check if ZooKeeper JaasConfiguration is valid.
   */
  @Test
  public void testIsZooKeeperSecure() throws Exception {
    boolean testJaasConfig = ZKUtil.isSecureZooKeeper(new Configuration(TEST_UTIL.getConfiguration()));
    assertEquals(testJaasConfig, secureZKAvailable);
    // Define Jaas configuration without ZooKeeper Jaas config
    File saslConfFile = File.createTempFile("tmp", "fakeJaas.conf");
    FileWriter fwriter = new FileWriter(saslConfFile);

    fwriter.write("");
    fwriter.close();
    System.setProperty("java.security.auth.login.config",
        saslConfFile.getAbsolutePath());

    testJaasConfig = ZKUtil.isSecureZooKeeper(new Configuration(TEST_UTIL.getConfiguration()));
    assertEquals(testJaasConfig, false);
    saslConfFile.delete();
  }
  
  /**
   * Check if Programmatic way of setting zookeeper security settings is valid.
   */
  @Test
  public void testIsZooKeeperSecureWithProgrammaticConfig() throws Exception {

    javax.security.auth.login.Configuration.setConfiguration(new DummySecurityConfiguration());

    Configuration config = new Configuration(HBaseConfiguration.create());
    boolean testJaasConfig = ZKUtil.isSecureZooKeeper(config);
    assertEquals(testJaasConfig, false);

    // Now set authentication scheme to Kerberos still it should return false
    // because no configuration set
    config.set("hbase.security.authentication", "kerberos");
    testJaasConfig = ZKUtil.isSecureZooKeeper(config);
    assertEquals(testJaasConfig, false);

    // Now set programmatic options related to security
    config.set(HConstants.ZK_CLIENT_KEYTAB_FILE, "/dummy/file");
    config.set(HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL, "dummy");
    config.set(HConstants.ZK_SERVER_KEYTAB_FILE, "/dummy/file");
    config.set(HConstants.ZK_SERVER_KERBEROS_PRINCIPAL, "dummy");
    testJaasConfig = ZKUtil.isSecureZooKeeper(config);
    assertEquals(true, testJaasConfig);
  }

  private static class DummySecurityConfiguration extends javax.security.auth.login.Configuration {
    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
      return null;
    }
  }

  @Test(timeout = 10000)
  public void testAdminDrainAllowedOnSecureZK() throws Exception {
    if (!secureZKAvailable) {
      return;
    }
    List<ServerName> drainingServers = new ArrayList<>(1);
    drainingServers.add(ServerName.parseServerName("ZZZ,123,123"));

    // If unable to connect to secure ZK cluster then this operation would fail.
    TEST_UTIL.getAdmin().drainRegionServers(drainingServers);

    drainingServers = TEST_UTIL.getAdmin().listDrainingRegionServers();
    assertEquals(1, drainingServers.size());
    assertEquals(ServerName.parseServerName("ZZZ,123,123"), drainingServers.get(0));

    TEST_UTIL.getAdmin().removeDrainFromRegionServers(drainingServers);
    drainingServers = TEST_UTIL.getAdmin().listDrainingRegionServers();
    assertEquals(0, drainingServers.size());
  }

}

