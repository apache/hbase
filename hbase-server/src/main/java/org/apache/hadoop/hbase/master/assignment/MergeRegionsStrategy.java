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
package org.apache.hadoop.hbase.master.assignment;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

/**
 * Region merge directory creation strategy to decouple create dir logic from
 * <code></code>MergeTableRegionsProcedure</code> and allow for plugable behaviour.
 */
@InterfaceAudience.Private
public abstract class MergeRegionsStrategy {

  /**
   * Creates the resulting merging region dir and files in the file system, then updates
   * meta table information for the given region. Specific logic on where in the files system to
   * create the region structure is delegated to <method>innerMergeRegions</method> and the
   * actual <code>HRegionFileSystemWriteStrategy</code> implementation.
   * @param env the MasterProcedureEnv wrapping several meta information required.
   * @param regionsToMerge array of RegionInfo representing the regions being merged.
   * @param mergedRegion the resulting merging region.
   * @throws IOException if any error occurs while creating the region dir.
   */
  public void createMergedRegion(MasterProcedureEnv env, RegionInfo[] regionsToMerge,
    RegionInfo mergedRegion) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    final Path tabledir = CommonFSUtils.getTableDir(mfs.getRootDir(), regionsToMerge[0].getTable());
    final FileSystem fs = mfs.getFileSystem();
    HRegionFileSystem mergeRegionFs = createDirAndMergeFiles(env, fs, regionsToMerge,
      tabledir, mergedRegion);
    assert mergeRegionFs != null;
    mergeRegionFs.commitMergedRegion(mergedRegion);
    // Prepare to create merged regions
    env.getAssignmentManager().getRegionStates().
      getOrCreateRegionStateNode(mergedRegion).setState(RegionState.State.MERGING_NEW);
  }

  /**
   * Should define specific logic about where in the file system the region structure should be
   * created.
   * @param env the MasterProcedureEnv wrapping several meta information required.
   * @param fs the FileSystem instance to write the region directory.
   * @param regionsToMerge array of RegionInfo representing the regions being merged.
   * @param tableDir Path instance for the table dir.
   * @param mergedRegion the resulting merging region.
   * @return HRegionFileSystem for the resulting merging region.
   * @throws IOException if any error occurs while creating the region dir.
   */
  abstract protected HRegionFileSystem createDirAndMergeFiles(MasterProcedureEnv env, FileSystem fs,
    RegionInfo[] regionsToMerge, Path tableDir, RegionInfo mergedRegion) throws IOException;


  /**
   * Clean up a merged region on rollback after failure.
   * @param env the MasterProcedureEnv wrapping several meta information required.
   * @param regionsToMerge array of RegionInfo representing the regions being merged.
   * @param mergedRegion the resulting merging region.
   * @throws IOException if any error occurs while creating the region dir.
   */
  public void cleanupMergedRegion(MasterProcedureEnv env, RegionInfo[] regionsToMerge,
    RegionInfo mergedRegion) throws IOException {
    final MasterFileSystem mfs = env.getMasterServices().getMasterFileSystem();
    TableName tn = regionsToMerge[0].getTable();
    final Path tableDir = CommonFSUtils.getTableDir(mfs.getRootDir(), tn);
    final FileSystem fs = mfs.getFileSystem();
    // See createMergedRegion above where we specify the merge dir as being in the
    // FIRST merge parent region.
    HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
      env.getMasterConfiguration(), fs, tableDir, regionsToMerge[0], false);
    regionFs.cleanupMergedRegion(mergedRegion);
  }

}
