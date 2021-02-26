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
import java.util.List;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.junit.Test;

public class RestoreSnapshotFromClientAfterSplittingRegionsTestBase
    extends RestoreSnapshotFromClientTestBase {

  @Test
  public void testRestoreSnapshotAfterSplittingRegions() throws IOException, InterruptedException {
    List<RegionInfo> regionInfos = admin.getRegions(tableName);
    RegionReplicaUtil.removeNonDefaultRegions(regionInfos);

    // Split the first region
    splitRegion(regionInfos.get(0));

    // Take a snapshot
    admin.snapshot(snapshotName1, tableName);

    // Load more data
    SnapshotTestingUtils.loadData(TEST_UTIL, tableName, 500, FAMILY);

    // Split the second region
    splitRegion(regionInfos.get(1));

    // Restore the snapshot
    admin.disableTable(tableName);
    admin.restoreSnapshot(snapshotName1);
    admin.enableTable(tableName);

    verifyRowCount(TEST_UTIL, tableName, snapshot1Rows);
  }
}
