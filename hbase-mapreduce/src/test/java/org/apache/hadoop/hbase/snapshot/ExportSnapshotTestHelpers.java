/*
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

import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.security.HadoopSecurityEnabledUserProviderForTesting;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.security.access.SecureTestUtil;

final class ExportSnapshotTestHelpers {

  private ExportSnapshotTestHelpers() {
  }

  private static void setUpBaseConf(Configuration conf) {
    conf.setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    conf.setInt("hbase.regionserver.msginterval", 100);
    // If a single node has enough failures (default 3), resource manager will blacklist it.
    // With only 2 nodes and tests injecting faults, we don't want that.
    conf.setInt("mapreduce.job.maxtaskfailures.per.tracker", 100);
    conf.setInt(MobConstants.MOB_FILE_CACHE_SIZE_KEY, 0);
  }

  static void startCluster(HBaseTestingUtil util, boolean useTmpDir) throws Exception {
    Configuration conf = util.getConfiguration();
    setUpBaseConf(conf);
    if (useTmpDir) {
      FileSystem localFs = FileSystem.getLocal(conf);
      Path tmpDir = util.getDataTestDir(UUID.randomUUID().toString())
        .makeQualified(localFs.getUri(), localFs.getWorkingDirectory());
      conf.set(SnapshotDescriptionUtils.SNAPSHOT_WORKING_DIR, tmpDir.toUri().toString());
    }
    util.startMiniCluster(1);
    util.startMiniMapReduceCluster();
  }

  static void startSecureCluster(HBaseTestingUtil util) throws Exception {
    setUpBaseConf(util.getConfiguration());
    // Setup separate test-data directory for MR cluster and set corresponding configurations.
    // Otherwise, different test classes running MR cluster can step on each other.
    util.getDataTestDir();

    // set the always on security provider
    UserProvider.setUserProviderForTesting(util.getConfiguration(),
      HadoopSecurityEnabledUserProviderForTesting.class);

    // setup configuration
    SecureTestUtil.enableSecurity(util.getConfiguration());

    util.startMiniCluster(3);
    util.startMiniMapReduceCluster();

    // Wait for the ACL table to become available
    util.waitTableEnabled(PermissionStorage.ACL_TABLE_NAME);
  }

}
