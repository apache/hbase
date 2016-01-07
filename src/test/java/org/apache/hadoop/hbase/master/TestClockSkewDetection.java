/**
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

import static org.junit.Assert.fail;

import java.net.InetAddress;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestClockSkewDetection {
  private static final Log LOG =
    LogFactory.getLog(TestClockSkewDetection.class);

  @Test
  public void testClockSkewDetection() throws Exception {
    final Configuration conf = HBaseConfiguration.create();
    ServerManager sm = new ServerManager(new Server() {
      @Override
      public CatalogTracker getCatalogTracker() {
        return null;
      }

      @Override
      public Configuration getConfiguration() {
        return conf;
      }

      @Override
      public ServerName getServerName() {
        return null;
      }

      @Override
      public ZooKeeperWatcher getZooKeeper() {
        return null;
      }

      @Override
      public void abort(String why, Throwable e) {}
      
      @Override
      public boolean isAborted() {
        return false;
      }

      @Override
      public boolean isStopped() {
        return false;
      }

      @Override
      public void stop(String why) {
      }}, null, false);

    LOG.debug("regionServerStartup 1");
    InetAddress ia1 = InetAddress.getLocalHost();
    sm.regionServerStartup(ia1, 1234, -1, System.currentTimeMillis());

    final Configuration c = HBaseConfiguration.create();
    long maxSkew = c.getLong("hbase.master.maxclockskew", 30000);
    long warningSkew = c.getLong("hbase.master.warningclockskew", 1000);

    try {
      // Master Time > Region Server Time
      LOG.debug("Test: Master Time > Region Server Time");
      LOG.debug("regionServerStartup 2");
      InetAddress ia2 = InetAddress.getLocalHost();
      sm.regionServerStartup(ia2, 1235, -1, System.currentTimeMillis() - maxSkew * 2);
      fail("HMaster should have thrown a ClockOutOfSyncException but didn't.");
    } catch (ClockOutOfSyncException e) {
      // we want an exception
      LOG.info("Recieved expected exception: " + e);
    }

    try {
      // Master Time < Region Server Time
      LOG.debug("Test: Master Time < Region Server Time");
      LOG.debug("regionServerStartup 3");
      InetAddress ia3 = InetAddress.getLocalHost();
      sm.regionServerStartup(ia3, 1236, -1, System.currentTimeMillis() + maxSkew * 2);
      fail("HMaster should have thrown a ClockOutOfSyncException but didn't.");
    } catch (ClockOutOfSyncException e) {
      // we want an exception
      LOG.info("Recieved expected exception: " + e);
    }

    // make sure values above warning threshold but below max threshold don't kill
    LOG.debug("regionServerStartup 4");
    InetAddress ia4 = InetAddress.getLocalHost();
    sm.regionServerStartup(ia4, 1237, -1, System.currentTimeMillis() - warningSkew * 2);

    // make sure values above warning threshold but below max threshold don't kill
    LOG.debug("regionServerStartup 5");
    InetAddress ia5 = InetAddress.getLocalHost();
    sm.regionServerStartup(ia5, 1238, -1, System.currentTimeMillis() + warningSkew * 2);
    
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

