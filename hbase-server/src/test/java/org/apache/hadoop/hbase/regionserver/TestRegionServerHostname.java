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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.StartMiniClusterOption;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the hostname specification by region server
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestRegionServerHostname {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionServerHostname.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionServerHostname.class);

  private HBaseTestingUtility TEST_UTIL;

  private static final int NUM_MASTERS = 1;
  private static final int NUM_RS = 1;

  @Before
  public void setup() {
    Configuration conf = HBaseConfiguration.create();
    TEST_UTIL = new HBaseTestingUtility(conf);
  }

  @After
  public void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInvalidRegionServerHostnameAbortsServer() throws Exception {
    String invalidHostname = "hostAddr.invalid";
    TEST_UTIL.getConfiguration().set(DNS.UNSAFE_RS_HOSTNAME_KEY, invalidHostname);
    HRegionServer hrs = null;
    try {
      hrs = new HRegionServer(TEST_UTIL.getConfiguration());
    } catch (IllegalArgumentException iae) {
      assertTrue(iae.getMessage(),
        iae.getMessage().contains("Failed resolve of " + invalidHostname) ||
        iae.getMessage().contains("Problem binding to " + invalidHostname));
    }
    assertNull("Failed to validate against invalid hostname", hrs);
  }

  @Test
  public void testRegionServerHostname() throws Exception {
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface.getNetworkInterfaces();
    while (netInterfaceList.hasMoreElements()) {
      NetworkInterface ni = netInterfaceList.nextElement();
      Enumeration<InetAddress> addrList = ni.getInetAddresses();
      // iterate through host addresses and use each as hostname
      while (addrList.hasMoreElements()) {
        InetAddress addr = addrList.nextElement();
        if (addr.isLoopbackAddress() || addr.isLinkLocalAddress() || addr.isMulticastAddress() ||
            !addr.isSiteLocalAddress()) {
          continue;
        }
        String hostName = addr.getHostName();
        LOG.info("Found " + hostName + " on " + ni + ", addr=" + addr);

        TEST_UTIL.getConfiguration().set(DNS.MASTER_HOSTNAME_KEY, hostName);
        TEST_UTIL.getConfiguration().set(DNS.UNSAFE_RS_HOSTNAME_KEY, hostName);
        StartMiniClusterOption option = StartMiniClusterOption.builder()
            .numMasters(NUM_MASTERS).numRegionServers(NUM_RS).numDataNodes(NUM_RS).build();
        TEST_UTIL.startMiniCluster(option);
        try {
          ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
          List<String> servers = ZKUtil.listChildrenNoWatch(zkw, zkw.getZNodePaths().rsZNode);
          // there would be NUM_RS+1 children - one for the master
          assertTrue(servers.size() ==
            NUM_RS + (LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration())? 1: 0));
          for (String server : servers) {
            assertTrue("From zookeeper: " + server + " hostname: " + hostName,
              server.startsWith(hostName.toLowerCase(Locale.ROOT)+","));
          }
          zkw.close();
        } finally {
          TEST_UTIL.shutdownMiniCluster();
        }
      }
    }
  }

  @Test
  public void testDeprecatedConfigs() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    new HRegionServer(conf);
    conf.setBoolean(HRegionServer.RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, false);
    assertFalse(conf.getBoolean(HRegionServer.UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, true));
    conf.setBoolean(HRegionServer.RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, true);
    assertTrue(conf.getBoolean(HRegionServer.UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, false));
    conf.setBoolean(HRegionServer.UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, true);
    assertTrue(conf.getBoolean(HRegionServer.RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, false));
    conf.setBoolean(HRegionServer.UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, false);
    assertFalse(conf.getBoolean(HRegionServer.RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, true));

    conf.setBoolean(DNS.RS_HOSTNAME_KEY, false);
    assertFalse(conf.getBoolean(DNS.UNSAFE_RS_HOSTNAME_KEY, true));
    conf.setBoolean(DNS.RS_HOSTNAME_KEY, true);
    assertTrue(conf.getBoolean(DNS.UNSAFE_RS_HOSTNAME_KEY, false));
    conf.setBoolean(DNS.UNSAFE_RS_HOSTNAME_KEY, true);
    assertTrue(conf.getBoolean(DNS.RS_HOSTNAME_KEY, false));
    conf.setBoolean(DNS.UNSAFE_RS_HOSTNAME_KEY, false);
    assertFalse(conf.getBoolean(DNS.RS_HOSTNAME_KEY, true));
  }

  @Test
  public void testConflictRegionServerHostnameConfigurationsAbortServer() throws Exception {
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface.getNetworkInterfaces();
    while (netInterfaceList.hasMoreElements()) {
      NetworkInterface ni = netInterfaceList.nextElement();
      Enumeration<InetAddress> addrList = ni.getInetAddresses();
      // iterate through host addresses and use each as hostname
      while (addrList.hasMoreElements()) {
        InetAddress addr = addrList.nextElement();
        if (addr.isLoopbackAddress() || addr.isLinkLocalAddress() || addr.isMulticastAddress()) {
          continue;
        }
        String hostName = addr.getHostName();
        LOG.info("Found " + hostName + " on " + ni);

        TEST_UTIL.getConfiguration().set(DNS.MASTER_HOSTNAME_KEY, hostName);
        // "hbase.unsafe.regionserver.hostname" and "hbase.unsafe.regionserver.hostname.disable.master.reversedns"
        // are mutually exclusive. Exception should be thrown if both are used.
        TEST_UTIL.getConfiguration().set(DNS.UNSAFE_RS_HOSTNAME_KEY, hostName);
        TEST_UTIL.getConfiguration().setBoolean(HRegionServer.UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, true);
        try {
          StartMiniClusterOption option = StartMiniClusterOption.builder()
              .numMasters(NUM_MASTERS).numRegionServers(NUM_RS).numDataNodes(NUM_RS).build();
          TEST_UTIL.startMiniCluster(option);
        } catch (Exception e) {
          Throwable t1 = e.getCause();
          Throwable t2 = t1.getCause();
          assertTrue(t1.getMessage()+" - "+t2.getMessage(), t2.getMessage().contains(
            HRegionServer.UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " and " +
                DNS.UNSAFE_RS_HOSTNAME_KEY + " are mutually exclusive"));
          return;
        } finally {
          TEST_UTIL.shutdownMiniCluster();
        }
        assertTrue("Failed to validate against conflict hostname configurations", false);
      }
    }
  }

  @Test
  public void testRegionServerHostnameReportedToMaster() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HRegionServer.UNSAFE_RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY,
    true);
    StartMiniClusterOption option = StartMiniClusterOption.builder()
        .numMasters(NUM_MASTERS).numRegionServers(NUM_RS).numDataNodes(NUM_RS).build();
    TEST_UTIL.startMiniCluster(option);
    boolean tablesOnMaster = LoadBalancer.isTablesOnMaster(TEST_UTIL.getConfiguration());
    int expectedRS = NUM_RS + (tablesOnMaster? 1: 0);
    try (ZKWatcher zkw = TEST_UTIL.getZooKeeperWatcher()) {
      List<String> servers = ZKUtil.listChildrenNoWatch(zkw, zkw.getZNodePaths().rsZNode);
      assertEquals(expectedRS, servers.size());
    }
  }
}
