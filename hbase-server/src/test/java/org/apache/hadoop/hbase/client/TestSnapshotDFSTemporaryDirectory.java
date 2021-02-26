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

import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

/**
 * This class tests that the use of a temporary snapshot directory supports snapshot functionality
 * while the temporary directory is on the same file system as the root directory
 * <p>
 * This is an end-to-end test for the snapshot utility
 */
@Category(LargeTests.class)
public class TestSnapshotDFSTemporaryDirectory
    extends TestSnapshotTemporaryDirectory {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotDFSTemporaryDirectory.class);

  /**
   * Setup the config for the cluster
   *
   * @throws Exception on failure
   */
  @BeforeClass public static void setupCluster() throws Exception {
    setupConf(UTIL.getConfiguration());
    UTIL.startMiniCluster(NUM_RS);
    admin = UTIL.getHBaseAdmin();
  }

  private static void setupConf(Configuration conf) throws IOException {
    // disable the ui
    conf.setInt("hbase.regionsever.info.port", -1);
    // change the flush size to a small amount, regulating number of store files
    conf.setInt("hbase.hregion.memstore.flush.size", 25000);
    // so make sure we get a compaction when doing a load, but keep around some
    // files in the store
    conf.setInt("hbase.hstore.compaction.min", 10);
    conf.setInt("hbase.hstore.compactionThreshold", 10);
    // block writes if we get to 12 store files
    conf.setInt("hbase.hstore.blockingStoreFiles", 12);
    // Enable snapshot
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.set(HConstants.HBASE_REGION_SPLIT_POLICY_KEY,
        ConstantSizeRegionSplitPolicy.class.getName());

    String snapshotPath = UTIL.getDefaultRootDirPath().toString() + Path.SEPARATOR +
        UUID.randomUUID().toString() + Path.SEPARATOR + ".tmpdir" + Path.SEPARATOR;
    conf.set(SnapshotDescriptionUtils.SNAPSHOT_WORKING_DIR, "file://" + new Path(snapshotPath).toUri());
  }
}
