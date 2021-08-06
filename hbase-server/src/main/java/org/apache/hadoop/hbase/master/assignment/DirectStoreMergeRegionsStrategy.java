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

import static org.apache.hadoop.hbase.regionserver.HRegionFileSystem.REGION_WRITE_STRATEGY;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.regionserver.DirectStoreFSWriteStrategy;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.Collection;

/**
 * <code>MergeRegionStrategy</code> implementation to be used in combination with
 * <code>PersistedStoreEngine</code> to avoid renames when merging regions.
 *
 * To use it, define the following properties under table configuration:
 *
 * {TABLE_ATTRIBUTES =>
 *  {METADATA => {'hbase.hregion.file.write.strategy' =>
 *      'org.apache.hadoop.hbase.regionserver.DirectStoreFSWriteStrategy',
 *    'hbase.hregion.merge.strategy' =>
 *      'org.apache.hadoop.hbase.master.assignment.DirectStoreMergeRegionsStrategy'}}}
 *
 * This will create the resulting merging region directory straight under the table dir, instead of
 * creating it under the temporary ".merges" dir as done by the default implementation.
 *
 *
 */
@InterfaceAudience.Private
public class DirectStoreMergeRegionsStrategy extends MergeRegionsStrategy {

  /**
   * Inner logic of creating a merging region, this relies on DirectStoreMergeRegionsStrategy to
   * return the actual paths where to create the new region (avoiding temp dirs and subsequent
   * renames).
   * @param env the MasterProcedureEnv wrapping several meta information required.
   * @param fs the FileSystem instance to write the region directory.
   * @param regionsToMerge array of RegionInfo representing the regions being merged.
   * @param tableDir Path instance for the table dir.
   * @param mergedRegion the resulting merging region.
   * @return HRegionFileSystem for the resulting merging region.
   * @throws IOException if any error occurs while creating the region dir.
   */
  @Override
  public HRegionFileSystem createDirAndMergeFiles(MasterProcedureEnv env, FileSystem fs,
      RegionInfo[] regionsToMerge, Path tableDir, RegionInfo mergedRegion) throws IOException {
    //creates the resulting merge region dir directly under the table directory, instead of
    //the temp ".merges" dir
    Configuration configuration = new Configuration(env.getMasterConfiguration());
    configuration.set(REGION_WRITE_STRATEGY, DirectStoreFSWriteStrategy.class.getName());
    HRegionFileSystem mergeRegionFs = HRegionFileSystem.createRegionOnFileSystem(
      configuration, fs, tableDir, mergedRegion);
    mergeRegionFs.createMergesDir();
    for (RegionInfo ri: regionsToMerge) {
      HRegionFileSystem regionFs = HRegionFileSystem.openRegionFromFileSystem(
        configuration, fs, tableDir, ri, false);
      mergeStoreFiles(env, regionFs, mergeRegionFs, mergedRegion);
    }
    return mergeRegionFs;
  }

  private void mergeStoreFiles(MasterProcedureEnv env, HRegionFileSystem regionFs,
      HRegionFileSystem mergeRegionFs, RegionInfo mergedRegion) throws IOException {
    final TableDescriptor htd = env.getMasterServices().getTableDescriptors()
      .get(mergedRegion.getTable());
    for (ColumnFamilyDescriptor hcd : htd.getColumnFamilies()) {
      String family = hcd.getNameAsString();
      final Collection<StoreFileInfo> storeFiles = regionFs.getStoreFiles(family);
      if (storeFiles != null && storeFiles.size() > 0) {
        for (StoreFileInfo storeFileInfo : storeFiles) {
          // Create reference file(s) to parent region file here in mergedDir.
          // As this procedure is running on master, use CacheConfig.DISABLED means
          // don't cache any block.
          mergeRegionFs.mergeStoreFile(regionFs.getRegionInfo(), family,
            new HStoreFile(storeFileInfo, hcd.getBloomFilterType(), CacheConfig.DISABLED),
            mergeRegionFs.getMergesDir());
        }
      }
    }
  }

}
