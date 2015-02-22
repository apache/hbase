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
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.zookeeper.KeeperException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, MediumTests.class})
public class TestLocalHBaseCluster {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /**
   * Check that we can start a local HBase cluster specifying a custom master
   * and regionserver class and then cast back to those classes; also that
   * the cluster will launch and terminate cleanly. See HBASE-6011. Uses the
   * HBaseTestingUtility facilities for creating a LocalHBaseCluster with
   * custom master and regionserver classes.
   */
  @Test
  public void testLocalHBaseCluster() throws Exception {
    TEST_UTIL.startMiniCluster(1, 1, null, MyHMaster.class, MyHRegionServer.class);
    // Can we cast back to our master class?
    try {
      int val = ((MyHMaster)TEST_UTIL.getHBaseCluster().getMaster(0)).echo(42);
      assertEquals(42, val);
    } catch (ClassCastException e) {
      fail("Could not cast master to our class");
    }
    // Can we cast back to our regionserver class?
    try {
      int val = ((MyHRegionServer)TEST_UTIL.getHBaseCluster().getRegionServer(0)).echo(42);
      assertEquals(42, val);
    } catch (ClassCastException e) {
      fail("Could not cast regionserver to our class");
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * A private master class similar to that used by HMasterCommandLine when
   * running in local mode.
   */
  public static class MyHMaster extends HMaster {
    public MyHMaster(Configuration conf, CoordinatedStateManager cp)
      throws IOException, KeeperException,
        InterruptedException {
      super(conf, cp);
    }

    public int echo(int val) {
      return val;
    }
  }

  /**
   * A private regionserver class with a dummy method for testing casts
   */
  public static class MyHRegionServer extends MiniHBaseCluster.MiniHBaseClusterRegionServer {

    public MyHRegionServer(Configuration conf, CoordinatedStateManager cp) throws IOException,
        InterruptedException {
      super(conf, cp);
    }

    public int echo(int val) {
      return val;
    }
  }
}
