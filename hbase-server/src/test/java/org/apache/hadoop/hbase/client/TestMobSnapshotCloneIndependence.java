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

package org.apache.hadoop.hbase.client;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.snapshot.MobSnapshotTestingUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Test to verify that the cloned table is independent of the table from which it was cloned
 */
@Category(LargeTests.class)
public class TestMobSnapshotCloneIndependence extends TestSnapshotCloneIndependence {
  private static final Log LOG = LogFactory.getLog(TestMobSnapshotCloneIndependence.class);

  /**
   * Setup the config for the cluster and start it
   * @throws Exception on failure
   */
  @BeforeClass
  public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
  }

  protected static void setupConf(Configuration conf) {
    TestSnapshotCloneIndependence.setupConf(conf);
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  @Override
  protected Table createTable(final TableName table, byte[] family) throws Exception {
    return MobSnapshotTestingUtils.createMobTable(UTIL, table, family);
  }

  @Override
  public void loadData(final Table table, byte[]... families) throws Exception {
    SnapshotTestingUtils.loadData(UTIL, table.getName(), 1000, families);
  }

  @Override
  protected int countRows(final Table table, final byte[]... families) throws Exception {
    return MobSnapshotTestingUtils.countMobRows(table, families);
  }
}