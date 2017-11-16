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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for the hostname specification by region server
 */
@Category({MediumTests.class})
public class TestRegionServerHostname {
  private static final Log LOG = LogFactory.getLog(TestRegionServerHostname.class);

  @Test (timeout=30000)
  public void testInvalidRegionServerHostnameAbortsServer() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;
    String invalidHostname = "hostAddr.invalid";
    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.getConfiguration().set(HRegionServer.RS_HOSTNAME_KEY, invalidHostname);
    try {
      TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    } catch (IOException ioe) {
      Throwable t1 = ioe.getCause();
      Throwable t2 = t1.getCause();
      assertTrue(t1.getMessage() + " - " + t2.getMessage(),
        t2.getMessage().contains("Failed resolve of " + invalidHostname) ||
        t2.getMessage().contains("Problem binding to " + invalidHostname));
      return;
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
    assertTrue("Failed to validate against invalid hostname", false);
  }

  @Test(timeout=120000)
  public void testRegionServerHostname() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface.getNetworkInterfaces();
    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
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
        
        TEST_UTIL.getConfiguration().set(HRegionServer.RS_HOSTNAME_KEY, hostName);
        TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
        try {
          ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
          List<String> servers = ZKUtil.listChildrenNoWatch(zkw, zkw.rsZNode);
          while (servers == null) {
            Threads.sleep(10);
            servers = ZKUtil.listChildrenNoWatch(zkw, zkw.rsZNode);
          }
          assertTrue(servers.size() == NUM_RS);
          for (String server : servers) {
            assertTrue(server.startsWith(hostName.toLowerCase()+","));
          }
          zkw.close();
        } finally {
          TEST_UTIL.shutdownMiniCluster();
        }
      }
    }
  }

  @Test(timeout=30000)
  public void testConflictRegionServerHostnameConfigurationsAbortServer() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;
    Enumeration<NetworkInterface> netInterfaceList = NetworkInterface.getNetworkInterfaces();
    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
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

        TEST_UTIL.getConfiguration().set(HRegionServer.MASTER_HOSTNAME_KEY, hostName);
        // "hbase.regionserver.hostname" and "hbase.regionserver.hostname.disable.master.reversedns"
        // are mutually exclusive. Exception should be thrown if both are used.
        TEST_UTIL.getConfiguration().set(HRegionServer.RS_HOSTNAME_KEY, hostName);
        TEST_UTIL.getConfiguration().setBoolean(HRegionServer.RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, true);
        try {
          TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
        } catch (Exception e) {
          Throwable t1 = e.getCause();
          Throwable t2 = t1.getCause();
          assertTrue(t1.getMessage()+" - "+t2.getMessage(), t2.getMessage().contains(
            HRegionServer.RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY + " and " + HRegionServer.RS_HOSTNAME_KEY +
            " are mutually exclusive"));
          return;
        } finally {
          TEST_UTIL.shutdownMiniCluster();
        }
        assertTrue("Failed to validate against conflict hostname configurations", false);
      }
    }
  }

  @Test(timeout=30000)
  public void testRegionServerHostnameReportedToMaster() throws Exception {
    final int NUM_MASTERS = 1;
    final int NUM_RS = 1;
    Configuration conf = HBaseConfiguration.create();
    HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.getConfiguration().setBoolean(HRegionServer.RS_HOSTNAME_DISABLE_MASTER_REVERSEDNS_KEY, true);
    TEST_UTIL.startMiniCluster(NUM_MASTERS, NUM_RS);
    try {
      ZooKeeperWatcher zkw = TEST_UTIL.getZooKeeperWatcher();
      List<String> servers = ZKUtil.listChildrenNoWatch(zkw, zkw.rsZNode);
      // there would be NUM_RS+1 children - one for the master
      assertTrue(servers.size() == NUM_RS);
      zkw.close();
    } finally {
      TEST_UTIL.shutdownMiniCluster();
    }
  }
}
