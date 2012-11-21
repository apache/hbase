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

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestMXBean {

  private static final HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  @BeforeClass
  public static void setup() throws Exception {
    TEST_UTIL.startMiniCluster(1, 1);
  }

  @AfterClass
  public static void teardown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInfo() {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    HMaster master = TEST_UTIL.getHBaseCluster().getMaster();
    MXBeanImpl info = MXBeanImpl.init(rs);

    Assert.assertEquals(rs.getServerName().getServerName(),
        info.getServerName());
    Assert.assertEquals(rs.getCoprocessors().length,
        info.getCoprocessors().length);
    rs.getConfiguration().setInt("hbase.master.info.port",
      master.getServerName().getPort());
    Assert.assertEquals(rs.getZooKeeperWatcher().getQuorum(),
        info.getZookeeperQuorum());
  }
}
