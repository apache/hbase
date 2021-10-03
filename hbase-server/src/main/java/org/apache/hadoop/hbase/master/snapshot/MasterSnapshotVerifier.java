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
package org.apache.hadoop.hbase.master.snapshot;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.snapshot.CorruptedSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotVerifyUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;


@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class MasterSnapshotVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(MasterSnapshotVerifier.class);

  private SnapshotDescription snapshot;
  private FileSystem workingDirFs;
  private TableName tableName;
  private MasterServices services;

  /**
   * @param services services for the master
   * @param snapshot snapshot to check
   * @param workingDirFs the file system containing the temporary snapshot information
   */
  public MasterSnapshotVerifier(MasterServices services,
    SnapshotDescription snapshot, FileSystem workingDirFs) {
    this.workingDirFs = workingDirFs;
    this.services = services;
    this.snapshot = snapshot;
    this.tableName = TableName.valueOf(snapshot.getTable());
  }

  /**
   * Verify that the snapshot in the directory is a valid snapshot
   * @param snapshotDir snapshot directory to check
   * @param snapshotServers {@link org.apache.hadoop.hbase.ServerName} of the servers
   *        that are involved in the snapshot
   * @throws CorruptedSnapshotException if the snapshot is invalid
   * @throws IOException if there is an unexpected connection issue to the filesystem
   */
  public void verifySnapshot(Path snapshotDir, Set<String> snapshotServers)
    throws CorruptedSnapshotException, IOException {
    verifySnapshot( true);
  }

  public void verifySnapshot(boolean verifyRegionDetails)
    throws CorruptedSnapshotException, IOException {
    List<RegionInfo> regions = services.getAssignmentManager().getTableRegions(tableName, false);

    // Remove the non-default regions
    RegionReplicaUtil.removeNonDefaultRegions(regions);

    SnapshotVerifyUtil.verifySnapshot(services.getConfiguration(), snapshot, tableName, regions,
      true, verifyRegionDetails);
  }
}
