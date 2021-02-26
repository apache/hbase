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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Trivial test to confirm that we do not get 0 infoPort. See HBASE-12863.
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestGetInfoPort {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestGetInfoPort.class);

  private final HBaseTestingUtility testUtil = new HBaseTestingUtility();

  @Before
  public void setUp() throws Exception {
    testUtil.getConfiguration().setInt(HConstants.MASTER_INFO_PORT, 0);
    testUtil.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void test() {
    assertTrue(testUtil.getMiniHBaseCluster().getRegionServer(0).getMasterAddressTracker()
        .getMasterInfoPort() > 0);
  }
}
