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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.assignment.AssignmentTestingUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.BeforeClass;
import org.junit.rules.TestName;

public class TestSnapshottingTableDDLProcedureBase extends TestTableDDLProcedureBase {

  protected static final int ttlForTest = 604800; // one week in seconds

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.getConfiguration().setBoolean(
      AbstractSnapshottingStateMachineTableProcedure.SNAPSHOT_BEFORE_DELETE_ENABLED_KEY, true);
    UTIL.getConfiguration().setInt(
      AbstractSnapshottingStateMachineTableProcedure.SNAPSHOT_BEFORE_DELETE_TTL_KEY, ttlForTest);
    TestTableDDLProcedureBase.setupCluster();
  }

  protected void assertSnapshotAbsent(String snapshotName) throws IOException {
    boolean found[] = new boolean[] { false };
    getMaster().getSnapshotManager().getCompletedSnapshots().forEach((desc) -> {
      if (snapshotName.equals(desc.getName())) {
        found[0] = true;
      }
    });
    assertFalse("Snapshot " + snapshotName + " should not exist", found[0]);
  }

  protected void assertSnapshotExists(String snapshotName) throws IOException {
    boolean found[] = new boolean[] { false };
    getMaster().getSnapshotManager().getCompletedSnapshots().forEach((desc) -> {
      if (snapshotName.equals(desc.getName())) {
        found[0] = true;
      }
    });
    assertTrue("Snapshot " + snapshotName + " should exist", found[0]);
  }

  protected void assertSnapshotHasTtl(String snapshotName, long ttl) throws IOException {
    boolean found[] = new boolean[] { false };
    long foundTtl[] = new long[] { -1 };
    getMaster().getSnapshotManager().getCompletedSnapshots().forEach((desc) -> {
      if (snapshotName.equals(desc.getName())) {
        found[0] = true;
        foundTtl[0] = desc.getTtl();
      }
    });
    assertTrue("Snapshot " + snapshotName + " should exist", found[0]);
    assertEquals(
      "Snapshot " + snapshotName + " has incorrect TTL: want=" + ttl + ", has=" + foundTtl[0], ttl,
      foundTtl[0]);
  }

  protected static String makeSnapshotName(TestName name) {
    return "auto_" + name.getMethodName() + "_" + EnvironmentEdgeManager.currentTime();
  }

  protected void insertData(TableName tableName, int rows, int startRow, String... cfs)
    throws IOException {
    AssignmentTestingUtil.insertData(UTIL, tableName, rows, startRow, cfs);
  }

  protected int countRows(TableName tableName, String... cfs) throws IOException {
    Scan scan = new Scan();
    for (String cf : cfs) {
      scan.addFamily(Bytes.toBytes(cf));
    }
    try (Table t = UTIL.getConnection().getTable(tableName)) {
      return HBaseTestingUtil.countRows(t, scan);
    }
  }

}
