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
package org.apache.hadoop.hbase.master.procedure;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MasterTests.TAG)
@Tag(LargeTests.TAG)
public class TestSnapshotProcedureBypass extends TestSnapshotProcedure {

  @Test
  public void testBypassedSnapshotProcedureWorkingDirIsCleanedUp() throws Exception {
    ProcedureExecutor<MasterProcedureEnv> procExec = master.getMasterProcedureExecutor();
    SnapshotManager snapshotManager = master.getSnapshotManager();
    DelaySnapshotProcedure sp =
      new DelaySnapshotProcedure(procExec.getEnvironment(), snapshotProto);
    long procId = procExec.submitProcedure(sp);
    snapshotManager.registerSnapshotProcedure(snapshotProto, procId);

    Configuration conf = master.getConfiguration();
    Path workingDir = SnapshotDescriptionUtils.getWorkingSnapshotDir(snapshotProto,
      CommonFSUtils.getRootDir(conf), conf);
    FileSystem fs = workingDir.getFileSystem(conf);
    TEST_UTIL.waitFor(60000, 500,
      () -> fs.exists(new Path(workingDir, SnapshotDescriptionUtils.SNAPSHOTINFO_FILE)));

    List<Boolean> bypassed =
      TEST_UTIL.getHbck().bypassProcedure(Arrays.asList(procId), 1000, true, false);
    assertTrue(bypassed.get(0));
    TEST_UTIL.waitFor(120000, () -> procExec.isFinished(procId));

    TEST_UTIL.waitFor(30000, () -> !fs.exists(workingDir));
    TEST_UTIL.waitFor(30000, () -> !snapshotManager.isTakingAnySnapshot());
    assertTrue(admin.listSnapshots().isEmpty());
  }
}
