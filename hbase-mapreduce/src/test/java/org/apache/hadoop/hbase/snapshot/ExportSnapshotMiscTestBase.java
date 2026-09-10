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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.jupiter.api.TestTemplate;

public class ExportSnapshotMiscTestBase extends ExportSnapshotTestBase {

  protected ExportSnapshotMiscTestBase(boolean mob) {
    super(mob);
  }

  @TestTemplate
  public void testExportWithTargetName() throws Exception {
    final String targetName = "testExportWithTargetName";
    testExportFileSystemState(tableName, snapshotName, targetName, tableNumFiles);
  }

  @TestTemplate
  public void testExportWithResetTtl() throws Exception {
    String suffix = mob ? methodName + "-mob" : methodName;
    TableName tableName = TableName.valueOf(suffix);
    String snapshotName = "snaptb-" + suffix;
    Long ttl = 100000L;
    // create Table
    createTable(tableName);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 50, FAMILY);
    int tableNumFiles = admin.getRegions(tableName).size();
    // take a snapshot with TTL
    Map<String, Object> props = new HashMap<>();
    props.put("TTL", ttl);
    admin.snapshot(snapshotName, tableName, props);
    Optional<Long> ttlOpt =
      admin.listSnapshots().stream().filter(s -> s.getName().equals(snapshotName))
        .map(org.apache.hadoop.hbase.client.SnapshotDescription::getTtl).findAny();
    assertTrue(ttlOpt.isPresent());
    assertEquals(ttl, ttlOpt.get());

    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles,
      getHdfsDestinationDir(), false, true);
  }

  @TestTemplate
  public void testExportExpiredSnapshot() throws Exception {
    String suffix = mob ? methodName + "-mob" : methodName;
    TableName tableName = TableName.valueOf(suffix);
    String snapshotName = "snapshot-" + suffix;
    createTable(tableName);
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 50, FAMILY);
    Map<String, Object> properties = new HashMap<>();
    properties.put("TTL", 10);
    org.apache.hadoop.hbase.client.SnapshotDescription snapshotDescription =
      new org.apache.hadoop.hbase.client.SnapshotDescription(snapshotName, tableName,
        SnapshotType.FLUSH, null, EnvironmentEdgeManager.currentTime(), -1, properties);
    admin.snapshot(snapshotDescription);
    boolean isExist =
      admin.listSnapshots().stream().anyMatch(ele -> snapshotName.equals(ele.getName()));
    assertTrue(isExist);
    TEST_UTIL.waitFor(60000,
      () -> SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDescription.getTtl(),
        snapshotDescription.getCreationTime(), EnvironmentEdgeManager.currentTime()));
    boolean isExpiredSnapshot =
      SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDescription.getTtl(),
        snapshotDescription.getCreationTime(), EnvironmentEdgeManager.currentTime());
    assertTrue(isExpiredSnapshot);
    int res = runExportSnapshot(TEST_UTIL.getConfiguration(), snapshotName, snapshotName,
      TEST_UTIL.getDefaultRootDirPath(), getHdfsDestinationDir(), false, false, false, true, true);
    assertEquals(res, AbstractHBaseTool.EXIT_FAILURE);
  }
}
