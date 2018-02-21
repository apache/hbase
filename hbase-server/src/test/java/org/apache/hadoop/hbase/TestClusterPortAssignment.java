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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.BindException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.testclassification.MediumTests;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestClusterPortAssignment {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestClusterPortAssignment.class);

  /**
   * Check that we can start an HBase cluster specifying a custom set of
   * RPC and infoserver ports.
   */
  @Test
  public void testClusterPortAssignment() throws Exception {
    boolean retry = false;
    do {
      int masterPort =  HBaseTestingUtility.randomFreePort();
      int masterInfoPort =  HBaseTestingUtility.randomFreePort();
      int rsPort =  HBaseTestingUtility.randomFreePort();
      int rsInfoPort =  HBaseTestingUtility.randomFreePort();
      TEST_UTIL.getConfiguration().setBoolean(LocalHBaseCluster.ASSIGN_RANDOM_PORTS, false);
      TEST_UTIL.getConfiguration().setInt(HConstants.MASTER_PORT, masterPort);
      TEST_UTIL.getConfiguration().setInt(HConstants.MASTER_INFO_PORT, masterInfoPort);
      TEST_UTIL.getConfiguration().setInt(HConstants.REGIONSERVER_PORT, rsPort);
      TEST_UTIL.getConfiguration().setInt(HConstants.REGIONSERVER_INFO_PORT, rsInfoPort);
      try {
        MiniHBaseCluster cluster = TEST_UTIL.startMiniCluster();
        assertTrue("Cluster failed to come up", cluster.waitForActiveAndReadyMaster(30000));
        retry = false;
        assertEquals("Master RPC port is incorrect", masterPort,
          cluster.getMaster().getRpcServer().getListenerAddress().getPort());
        assertEquals("Master info port is incorrect", masterInfoPort,
          cluster.getMaster().getInfoServer().getPort());
        assertEquals("RS RPC port is incorrect", rsPort,
          cluster.getRegionServer(0).getRpcServer().getListenerAddress().getPort());
        assertEquals("RS info port is incorrect", rsInfoPort,
          cluster.getRegionServer(0).getInfoServer().getPort());
      } catch (BindException e) {
        LOG.info("Failed to bind, need to retry", e);
        retry = true;
      } finally {
        TEST_UTIL.shutdownMiniCluster();
      }
    } while (retry);
  }
}
