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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.hadoop.hbase.regionserver.compactions.OffPeakCompactionTracker.HBASE_OFFPEAK_COMPACTION_MAX_SIZE_KEY;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({RegionServerTests.class, SmallTests.class})
public class TestOffPeakCompactionTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOffPeakCompactionTracker.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestOffPeakCompactionTracker.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @Test
  public void testOffPeakCompactionTracker() {

    Configuration conf = TEST_UTIL.getConfiguration();
    OffPeakCompactionTracker opc = new OffPeakCompactionTracker();
    opc.setMaxCompactionSize(conf);
    LOG.debug("Testing default off-peak compaction settings...");
    assertTrue(opc.tryStartOffPeakRequest());
    assertFalse(opc.tryStartOffPeakRequest());
    opc.endOffPeakRequest();
    assertTrue(opc.tryStartOffPeakRequest());
    assertFalse(opc.tryStartOffPeakRequest());

    Configuration conf1 = new Configuration();
    conf1.setInt(HBASE_OFFPEAK_COMPACTION_MAX_SIZE_KEY, 2);
    LOG.debug("Testing off-peak compaction configuration change settings ({} = {})", HBASE_OFFPEAK_COMPACTION_MAX_SIZE_KEY, 2);
    opc.onConfigurationChange(conf1);
    assertTrue(opc.tryStartOffPeakRequest());
  }
}
