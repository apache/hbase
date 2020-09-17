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
package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowRegionServerTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

/**
 * Test Export Snapshot Tool
 */
@Ignore // HBASE-24493
@Category({VerySlowRegionServerTests.class, LargeTests.class})
public class TestMobExportSnapshot extends TestExportSnapshot {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMobExportSnapshot.class);

  public static void setUpBaseConf(Configuration conf) {
    TestExportSnapshot.setUpBaseConf(conf);
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpBaseConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @Override
  protected void createTable(TableName tableName) throws Exception {
    MobSnapshotTestingUtils.createPreSplitMobTable(TEST_UTIL, tableName, 2, FAMILY);
  }

  @Override
  protected RegionPredicate getBypassRegionPredicate() {
    return new RegionPredicate() {
      @Override
      public boolean evaluate(final RegionInfo regionInfo) {
        return MobUtils.isMobRegionInfo(regionInfo);
      }
    };
  }
}
