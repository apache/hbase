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

package org.apache.hadoop.hbase.master.procedure;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.SnapshotState;

@Category({ MasterTests.class, MediumTests.class })
public class TestSnapshotProcedureSnapshotCorrupted extends TestSnapshotProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestSnapshotProcedureSnapshotCorrupted.class);

  @Test
  public void testSnapshotCorruptedAndRollback() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    SnapshotProcedure sp = new SnapshotProcedure(procExec.getEnvironment(), snapshotProto);
    procExec.submitProcedure(sp);
    TEST_UTIL.waitFor(60000,
      500,
      () -> sp.getCurrentStateId() > SnapshotState.SNAPSHOT_CONSOLIDATE_SNAPSHOT_VALUE);
    DistributedFileSystem dfs = TEST_UTIL.getDFSCluster().getFileSystem();
    Optional<HRegion> region = TEST_UTIL.getHBaseCluster().getRegions(TABLE_NAME).stream()
      .filter(r -> !r.getStoreFileList(new byte[][] { CF }).isEmpty())
      .findFirst();
    assertTrue(region.isPresent());
    region.get().getStoreFileList(new byte[][] { CF }).forEach(s -> {
        try {
          // delete real data files to trigger the CorruptedSnapshotException
          dfs.delete(new Path(s), true);
          LOG.info("delete {} to make snapshot corrupt", s);
        } catch (Exception e) {
          LOG.warn("Failed delete {} to make snapshot corrupt", s, e);
        }
      }
    );
    TEST_UTIL.waitFor(60000, () -> sp.isFailed() && sp.isFinished());
    Configuration conf = master.getConfiguration();
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(
      snapshotProto, CommonFSUtils.getRootDir(conf), conf);
    assertFalse(dfs.exists(workingDir));
    assertFalse(master.getSnapshotManager().isTakingSnapshot(TABLE_NAME));
    assertFalse(master.getSnapshotManager().isTakingAnySnapshot());
  }
}
