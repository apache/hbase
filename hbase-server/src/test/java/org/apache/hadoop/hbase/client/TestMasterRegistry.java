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

import static org.apache.hadoop.hbase.HConstants.META_REPLICAS_NUM;
import static org.junit.Assert.assertEquals;

import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestMasterRegistry {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final int META_REPLICA_COUNT = 3;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.getConfiguration().setInt(META_REPLICAS_NUM, META_REPLICA_COUNT);
    TEST_UTIL.startMiniCluster(3, 3);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Generates a string of dummy master addresses in host:port format. Every other hostname won't
   * have a port number.
   */
  private static String generateDummyMastersList(int size) {
    List<String> masters = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      masters.add(" localhost" + (i % 2 == 0 ? ":" + (1000 + i) : ""));
    }
    return Joiner.on(",").join(masters);
  }

  /**
   * Makes sure the master registry parses the master end points in the configuration correctly.
   */
  @Test
  public void testMasterAddressParsing() throws IOException {
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    int numMasters = 10;
    conf.set(HConstants.MASTER_ADDRS_KEY, generateDummyMastersList(numMasters));
    List<ServerName> parsedMasters = new ArrayList<>(MasterRegistry.parseMasterAddrs(conf));
    // Half of them would be without a port, duplicates are removed.
    assertEquals(numMasters / 2 + 1, parsedMasters.size());
    // Sort in the increasing order of port numbers.
    Collections.sort(parsedMasters, new Comparator<ServerName>() {
      @Override
      public int compare(ServerName sn1, ServerName sn2) {
        return sn1.getPort() - sn2.getPort();
      }
    });
    for (int i = 0; i < parsedMasters.size(); i++) {
      ServerName sn = parsedMasters.get(i);
      assertEquals("localhost", sn.getHostname());
      if (i == parsedMasters.size() - 1) {
        // Last entry should be the one with default port.
        assertEquals(HConstants.DEFAULT_MASTER_PORT, sn.getPort());
      } else {
        assertEquals(1000 + (2 * i), sn.getPort());
      }
    }
  }

  @Test
  public void testRegistryRPCs() throws Exception {
    HMaster activeMaster = TEST_UTIL.getHBaseCluster().getMaster();
    final MasterRegistry registry = new MasterRegistry();
    try {
      registry.init(TEST_UTIL.getConnection());
      // Add wait on all replicas being assigned before proceeding w/ test. Failed on occasion
      // because not all replicas had made it up before test started.
      TEST_UTIL.waitFor(10000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          return registry.getMetaRegionLocations().size() == META_REPLICA_COUNT;
        }
      });
      assertEquals(registry.getClusterId(), activeMaster.getClusterId());
      assertEquals(registry.getActiveMaster(), activeMaster.getServerName());
      List<HRegionLocation> metaLocations =
          Arrays.asList(registry.getMetaRegionLocations().getRegionLocations());
      List<HRegionLocation> actualMetaLocations =
          activeMaster.getMetaRegionLocationCache().getMetaRegionLocations();
      Collections.sort(metaLocations);
      Collections.sort(actualMetaLocations);
      assertEquals(actualMetaLocations, metaLocations);
      int numRs = registry.getCurrentNrHRS();
      assertEquals(TEST_UTIL.getMiniHBaseCluster().getLiveRegionServerThreads().size(), numRs);
    } finally {
      registry.close();
    }
  }
}
