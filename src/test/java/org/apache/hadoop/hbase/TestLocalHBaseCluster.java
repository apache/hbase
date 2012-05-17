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

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.zookeeper.KeeperException;

import org.junit.Test;

public class TestLocalHBaseCluster {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * Check that we can start a local HBase cluster specifying a custom master
   * and regionserver class and then cast back to those classes; also that
   * the cluster will launch and terminate cleanly. See HBASE-6011.
   */
  @Test
  public void testLocalHBaseCluster() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    MiniZooKeeperCluster zkCluster = TEST_UTIL.startMiniZKCluster();
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(zkCluster.getClientPort()));
    LocalHBaseCluster cluster = new LocalHBaseCluster(conf, 1, 1, MyHMaster.class,
      MyHRegionServer.class);
    // Can we cast back to our master class?
    try {
      ((MyHMaster)cluster.getMaster(0)).setZKCluster(zkCluster);
    } catch (ClassCastException e) {
      fail("Could not cast master to our class");
    }
    // Can we cast back to our regionserver class?
    try {
      ((MyHRegionServer)cluster.getRegionServer(0)).echo(42);
    } catch (ClassCastException e) {
      fail("Could not cast regionserver to our class");
    }
    // Does the cluster start successfully?
    try {
      cluster.startup();
      waitForClusterUp(conf);
    } catch (IOException e) {
      fail("LocalHBaseCluster did not start successfully");
    } finally {
      cluster.shutdown();
    }
  }

  private void waitForClusterUp(Configuration conf) throws IOException {
    HTable t = new HTable(conf, HConstants.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();
  }

  /**
   * A private master class similar to that used by HMasterCommandLine when
   * running in local mode.
   */
  public static class MyHMaster extends HMaster {
    private MiniZooKeeperCluster zkcluster = null;

    public MyHMaster(Configuration conf) throws IOException, KeeperException,
        InterruptedException {
      super(conf);
    }

    @Override
    public void run() {
      super.run();
      if (this.zkcluster != null) {
        try {
          this.zkcluster.shutdown();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    void setZKCluster(final MiniZooKeeperCluster zkcluster) {
      this.zkcluster = zkcluster;
    }
  }

  /**
   * A private regionserver class with a dummy method for testing casts
   */
  public static class MyHRegionServer extends HRegionServer {

    public MyHRegionServer(Configuration conf) throws IOException,
        InterruptedException {
      super(conf);
    }

    public int echo(int val) {
      return val;
    }
  }
}
