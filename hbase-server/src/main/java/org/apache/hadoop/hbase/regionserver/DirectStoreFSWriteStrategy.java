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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <code>HRegionFileSystemWriteStrategy</code> implementation to be used in combination with
 * <code>PersistedStoreEngine</code> to avoid renames when splitting and merging regions.
 *
 * To use it, define the following properties under master configuration:
 * 1) <property>
 *      <name>hbase.hregion.file.write.strategy</name>
 *      <value>org.apache.hadoop.hbase.regionserver.DirectStoreFSWriteStrategy</value>
 *    </property>
 * 2) <property>
 *      <name>hbase.hregion.merge.strategy</name>
 *      <value>org.apache.hadoop.hbase.master.assignment.DirectStoreMergeRegionsStrategy</value>
 *    </property>
 *
 * This will create the resulting merging and splitting regions directory straight under
 * the table dir, instead of creating it under the temporary ".tmp" or ".merges" dirs,
 * as done by the default implementation.
 */
@InterfaceAudience.Private
public class DirectStoreFSWriteStrategy extends HRegionFileSystemWriteStrategy {
  private StoreFilePathAccessor accessor;
  private Map<String, Map<String,List<Path>>> regionSplitReferences = new HashMap<>();
  private Map<String, List<Path>> mergeReferences = new HashMap();

  public DirectStoreFSWriteStrategy(HRegionFileSystem fileSystem) throws IOException {
    super(fileSystem);
    this.accessor =  StoreFileTrackingUtils.createStoreFilePathAccessor(fileSystem.conf,
      ConnectionFactory.createConnection(fileSystem.conf));
  }

  /**
   * The parent directory where to create the splits dirs is
   * the table directory itself, in this case.
   * @return Path representing the table directory.
   */
  @Override
  public Path getParentSplitsDir() {
    return fileSystem.getTableDir();
  }

  /**
   * The parent directory where to create the merge dir is
   * the table directory itself, in this case.
   * @return Path representing the table directory.
   */
  @Override
  public Path getParentMergesDir() {
    return fileSystem.getTableDir();
  }

  /**
   * Creates the directories for the respective split daughters directly under the
   * table directory, instead of default behaviour of doing it under temp dirs, initially.
   * @param daughterA the first half of the split region
   * @param daughterB the second half of the split region
   *
   * @throws IOException if directories creation fails.
   */
  @Override
  public void createSplitsDir(RegionInfo daughterA, RegionInfo daughterB)
    throws IOException {
    Path splitdir = getParentSplitsDir();
    // splitDir doesn't exists now. No need to do an exists() call for it.
    if (!fileSystem.getFileSystem().exists(splitdir)) {
      throw new IOException("Table dir for splitting region not found:  " + splitdir);
    }
    Path daughterADir = getSplitsDir(daughterA);
    if (!fileSystem.createDir(daughterADir)) {
      throw new IOException("Failed create of " + daughterADir);
    }
    Path daughterBDir = getSplitsDir(daughterB);
    if (!fileSystem.createDir(daughterBDir)) {
      throw new IOException("Failed create of " + daughterBDir);
    }
  }

  /**
   * Just validates that merges parent, the actual table dir in this case, exists.
   * @throws IOException if table dir doesn't exist.
   */
  @Override
  public void createMergesDir() throws IOException {
    //When writing directly, avoiding renames, merges parent is the table dir itself, so it
    // should exist already, so just validate it exist then do nothing
    Path mergesdir = getParentMergesDir();
    if (!fileSystem.fs.exists(mergesdir)) {
      throw new IOException("Table dir for merging region not found: " + mergesdir);
    }
  }

  /**
   * Wraps <code>super.splitStoreFile</code>, so that it can map the resulting split reference to
   * the related split region and column family, in order to add this to 'storefile' system table
   * for the tracking logic of PersisedStoreFileManager later on <code>commitDaughterRegion</code>
   * method.
   * @param parentRegionInfo {@link RegionInfo} of the parent split region.
   * @param daughterRegionInfo {@link RegionInfo} of the resulting split region.
   * @param familyName Column Family Name
   * @param f File to split.
   * @param splitRow Split Row
   * @param top True if we are referring to the top half of the hfile.
   * @param splitPolicy A split policy instance; be careful! May not be full populated; e.g. if
   *                    this method is invoked on the Master side, then the RegionSplitPolicy will
   *                    NOT have a reference to a Region.
   * @param fs FileSystem instance for creating the actual reference file.
   * @return
   * @throws IOException
   */
  @Override
  public Path splitStoreFile(RegionInfo parentRegionInfo, RegionInfo daughterRegionInfo,
    String familyName, HStoreFile f, byte[] splitRow,
    boolean top, RegionSplitPolicy splitPolicy, FileSystem fs) throws IOException {
    Path path = super.splitStoreFile(parentRegionInfo, daughterRegionInfo,
      familyName, f, splitRow, top, splitPolicy, fs);
    Map<String,List<Path>> splitReferences = regionSplitReferences.
      get(daughterRegionInfo.getEncodedName());
    if(splitReferences==null){
      splitReferences = new HashMap<>();
      regionSplitReferences.put(daughterRegionInfo.getEncodedName(), splitReferences);
    }
    List<Path> references = splitReferences.get(familyName);
    if(references==null){
      references = new ArrayList<>();
      splitReferences.put(familyName,references);
    }
    references.add(path);
    return path;
  }

  /**
   * Wraps <code>super.mergeStoreFile</code>, so that it can map the resulting merge reference to
   * the related merged region and column family, in order to add this to 'storefile' system table
   * for the tracking logic of PersisedStoreFileManager later in <code>commitMergedRegion</code>.
   * @param regionToMerge {@link RegionInfo} for one of the regions being merged.
   * @param mergedRegion {@link RegionInfo} of the merged region
   * @param familyName Column Family Name
   * @param f File to create reference.
   * @param mergedDir
   * @param fs FileSystem instance for creating the actual reference file.
   * @return
   * @throws IOException
   */
  @Override
  public Path mergeStoreFile(RegionInfo regionToMerge, RegionInfo mergedRegion, String familyName,
      HStoreFile f, Path mergedDir, FileSystem fs) throws IOException {
    if (this.fileSystem.regionInfoForFs.equals(regionToMerge)) {
      Path path = super.mergeStoreFile(mergedRegion, regionToMerge, familyName, f, mergedDir, fs);
      List<Path> referenceFiles = mergeReferences.get(familyName);
      if (referenceFiles == null) {
        referenceFiles = new ArrayList<>();
        mergeReferences.put(familyName, referenceFiles);
      }
      referenceFiles.add(path);
      return path;
    } else {
      throw new IOException("Wrong ordering of regions parameter. "
        + "The resulting merge should be the first param.");
    }
  }

  /**
   * Do nothing. Here the split dir is the table dir itself, we cannot delete it.
   * @throws IOException
   */
  @Override
  public void cleanupSplitsDir() throws IOException {
  }

  /**
   * Do nothing. Here the merge dir is the table dir itself, we cannot delete it.
   * @throws IOException
   */
  @Override
  public void cleanupMergesDir() throws IOException {
    //do nothing, the merges dir is the store dir itself, so we cannot delete it.
  }

  /**
   * Do nothing. Here the split dir is the table dir itself, we cannot delete it.
   * @throws IOException
   */
  @Override
  public void cleanupAnySplitDetritus() throws IOException {
  }

  /**
   * Adds all reference files for the given daughter region into 'storefile' system table for
   * the tracking logic of PersisedStoreFileManager.
   * @param regionInfo the resulting daughter region to be committed.
   * @return
   * @throws IOException
   */
  @Override
  public Path commitDaughterRegion(RegionInfo regionInfo) throws IOException {
    Path regionDir = this.getSplitsDir(regionInfo);
    //The newly created region might not have any file, so no need to add into file manager tracker
    Map<String, List<Path>> splitReferences =
      regionSplitReferences.get(regionInfo.getEncodedName());
    if(splitReferences!=null) {
      for (String family : splitReferences.keySet()) {
        List<Path> referenceList = splitReferences.get(family);
        accessor.writeStoreFilePaths(regionInfo.getTable().getNameAsString(),
          regionInfo.getRegionNameAsString(), family,
          StoreFilePathUpdate.builder().withStorePaths(referenceList).build());
      }
    }
    regionSplitReferences.remove(regionInfo.getEncodedName());
    return regionDir;
  }

  /**
   * Adds all reference files for the merge result region into 'storefile' system table for
   * the tracking logic of PersisedStoreFileManager.
   * @param mergedRegionInfo merged region {@link RegionInfo}
   * @throws IOException
   */
  @Override
  public void commitMergedRegion(RegionInfo mergedRegionInfo) throws IOException {
    for(String family : mergeReferences.keySet()){
      accessor.writeStoreFilePaths(mergedRegionInfo.getTable().getNameAsString(),
        mergedRegionInfo.getRegionNameAsString(),
        family,
        StoreFilePathUpdate.builder().withStorePaths(mergeReferences.get(family)).build());
    }
    mergeReferences.clear();
  }
}
