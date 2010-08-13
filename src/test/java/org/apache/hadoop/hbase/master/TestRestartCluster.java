/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestRestartCluster {
  private static final Log LOG = LogFactory.getLog(TestRestartCluster.class);
  private static Configuration conf;
  private static HBaseTestingUtility utility;
  private static ZooKeeperWatcher zooKeeper;
  private static final byte[] TABLENAME = Bytes.toBytes("master_transitions");
  private static final byte [][] FAMILIES = new byte [][] {Bytes.toBytes("a")};


  private static final byte [][] TABLES = new byte[][] {
      Bytes.toBytes("restartTableOne"),
      Bytes.toBytes("restartTableTwo"),
      Bytes.toBytes("restartTableThree")
  };
  private static final byte [] FAMILY = Bytes.toBytes("family");

  @BeforeClass public static void beforeAllTests() throws Exception {
    conf = HBaseConfiguration.create();
    utility = new HBaseTestingUtility(conf);
  }

  @AfterClass public static void afterAllTests() throws IOException {
    utility.shutdownMiniCluster();
  }

  @Before public void setup() throws IOException {
  }

  @Test (timeout=300000)
  public void testClusterRestart() throws Exception {

    utility.getConfiguration().set("hbase.test.build.dir",
        utility.setupClusterTestBuildDir().getAbsolutePath());

    LOG.info("\n\nStarting cluster the first time");
    utility.startMiniCluster(3);

    LOG.info("\n\nCreating tables");
    for(byte [] TABLE : TABLES) {
      utility.createTable(TABLE, FAMILY);
      utility.waitTableAvailable(TABLE, 30000);
    }
    List<HRegionInfo> allRegions =
      MetaScanner.listAllRegions(utility.getConfiguration());
    assertEquals(3, allRegions.size());

    LOG.info("\n\nShutting down cluster");
    utility.getHBaseCluster().shutdown();
    utility.getHBaseCluster().join();

    LOG.info("\n\nSleeping a bit");
    Thread.sleep(2000);

    LOG.info("\n\nStarting cluster the second time");
    utility.restartHBaseCluster(3);

    allRegions = MetaScanner.listAllRegions(utility.getConfiguration());
    assertEquals(3, allRegions.size());

    LOG.info("\n\nWaiting for tables to be available");
    for(byte [] TABLE : TABLES) {
      try {
        utility.createTable(TABLE, FAMILY);
        assertTrue("Able to create table that should already exist", false);
      } catch(TableExistsException tee) {
        LOG.info("Table already exists as expected");
      }
      utility.waitTableAvailable(TABLE, 30000);
    }

    LOG.info("\n\nShutting stuff down now");
    utility.shutdownMiniCluster();

    LOG.info("\n\nDone!");
  }

  @Test (timeout=300000) public void testRestartClusterAfterKill()throws Exception {
    utility.startMiniZKCluster();
    zooKeeper = new ZooKeeperWatcher(conf, "cluster1", null);

    // create the unassigned region, throw up a region opened state for META
    String unassignedZNode = zooKeeper.assignmentZNode;
    ZKUtil.createAndFailSilent(zooKeeper, unassignedZNode);

    ZKAssign.createNodeOffline(zooKeeper, HRegionInfo.ROOT_REGIONINFO,
      HMaster.MASTER);

    ZKAssign.createNodeOffline(zooKeeper, HRegionInfo.FIRST_META_REGIONINFO,
      HMaster.MASTER);

    LOG.debug("Created UNASSIGNED zNode for ROOT and META regions in state " +
        EventType.M2ZK_REGION_OFFLINE);

    // start the HB cluster
    LOG.info("Starting HBase cluster...");
    utility.startMiniCluster(2);

    utility.createTable(TABLENAME, FAMILIES);
    LOG.info("Created a table, waiting for table to be available...");
    utility.waitTableAvailable(TABLENAME, 60*1000);

    LOG.info("Master deleted unassgined region and started up successfully.");
  }
}
