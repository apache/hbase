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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterNotCarryTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterNotCarryTable.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMasterNotCarryTable.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static HMaster master;

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration c = UTIL.getConfiguration();
    // We use local filesystem.  Set it so it writes into the testdir.
    CommonFSUtils.setRootDir(c, UTIL.getDataTestDir());
    UTIL.startMiniZKCluster();
    master = new HMaster(UTIL.getConfiguration());
    master.start();
    // As no regionservers, only wait master to create AssignmentManager.
    while (master.getAssignmentManager() != null) {
      LOG.debug("Wait master to create AssignmentManager");
      Thread.sleep(1000);
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    master.stop("Shutdown");
    UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testMasterNotCarryTable() {
    // The default config is false
    assertFalse(LoadBalancer.isTablesOnMaster(UTIL.getConfiguration()));
    assertFalse(LoadBalancer.isSystemTablesOnlyOnMaster(UTIL.getConfiguration()));
  }

  @Test
  public void testMasterBlockCache() {
    // no need to instantiate block cache.
    assertFalse(master.getBlockCache().isPresent());
  }

  @Test
  public void testMasterMOBFileCache() {
    // no need to instantiate mob file cache.
    assertFalse(master.getMobFileCache().isPresent());
  }
}
