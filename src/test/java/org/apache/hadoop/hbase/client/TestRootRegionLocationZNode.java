/**
 * Copyright The Apache Software Foundation
 *
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


import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWrapper;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRootRegionLocationZNode {
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @Before
  public void setUp() throws Exception {
    UTIL.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void testRootRegionLocation()
      throws IOException, KeeperException, InterruptedException {
    TableServers conn =
        (TableServers)HConnectionManager.getConnection(UTIL.getConfiguration());
    ZooKeeperWrapper zk = conn.getZooKeeperWrapper();
    HRegionLocation loc = conn.locateRootRegion();
    zk.deleteZNode("/hbase/root-region-server-final");
    HRegionLocation loc2 = conn.locateRootRegion();
    assertTrue(loc.getServerAddress().equals(loc2.getServerAddress()));
  }
}
