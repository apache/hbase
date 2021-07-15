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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.Optional;

/**
 * HRegionFileSystem write strategy to decouple splits/merge create dir and commit logic
 * from <code>HRegionFileSystem</code>, allowing for a plugable behaviour.
 */
@InterfaceAudience.Private
public abstract class HRegionFileSystemWriteStrategy {

  protected HRegionFileSystem fileSystem;

  public HRegionFileSystemWriteStrategy(HRegionFileSystem fileSystem){
    this.fileSystem = fileSystem;
  }

  /**
   * Returns the directory Path for the region split resulting daughter.
   * @param hri for the split resulting daughter region.
   * @return a path under tmp dir for the resulting daughter region.
   */
  public Path getSplitsDir(final RegionInfo hri) {
    return new Path(getParentSplitsDir(), hri.getEncodedName());
  }

  /**
   * Defines the parent dir for the split dir.
   * @return
   */
  public abstract Path getParentSplitsDir();

  /**
   * Defines the parent dir for the merges dir.
   * @return
   */
  public abstract Path getParentMergesDir();

  /**
   * Returns the directory Path for the resulting merged region.
   * @param hri for the resulting merged region.
   * @return a path under tmp dir for the resulting merged region.
   */
  public Path getMergesDir(final RegionInfo hri) {
    return new Path(getParentMergesDir(), hri.getEncodedName());
  }

  /**
   * Creates the region splits directory.
   *
   * @param daughterA the first half of the split region
   * @param daughterB the second half of the split region
   *
   * @throws IOException if splits dir creation fails.
   */
  public abstract void createSplitsDir(RegionInfo daughterA, RegionInfo daughterB)
    throws IOException;

  /**
   * Create the region merges directory.
   * @throws IOException If merges dir creation fails.
   * @see HRegionFileSystem#cleanupMergesDir()
   */
  public abstract  void createMergesDir() throws IOException;

  /**
   * Completes the daughter region creation.
   * @param regionInfo the resulting daughter region to be committed.
   * @return the region path.
   * @throws IOException if any errors occur.
   */
  public abstract Path commitDaughterRegion(final RegionInfo regionInfo) throws IOException;

  /**
   * Completes the merged region creation.
   * @param mergedRegionInfo merged region {@link RegionInfo}
   * @throws IOException if any errors occur.
   */
  public abstract void commitMergedRegion(final RegionInfo mergedRegionInfo) throws IOException;

  /**
   * Write out a split reference.
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
   * @return Path to created reference.
   * @throws IOException if the split reference write fails.
   */
  public Path splitStoreFile(RegionInfo parentRegionInfo, RegionInfo daughterRegionInfo,
      String familyName, HStoreFile f, byte[] splitRow,
      boolean top, RegionSplitPolicy splitPolicy, FileSystem fs) throws IOException {
    if (splitPolicy == null || !splitPolicy.skipStoreFileRangeCheck(familyName)) {
      // Check whether the split row lies in the range of the store file
      // If it is outside the range, return directly.
      f.initReader();
      try {
        if (top) {
          //check if larger than last key.
          Cell splitKey = PrivateCellUtil.createFirstOnRow(splitRow);
          Optional<Cell> lastKey = f.getLastKey();
          // If lastKey is null means storefile is empty.
          if (!lastKey.isPresent()) {
            return null;
          }
          if (f.getComparator().compare(splitKey, lastKey.get()) > 0) {
            return null;
          }
        } else {
          //check if smaller than first key
          Cell splitKey = PrivateCellUtil.createLastOnRow(splitRow);
          Optional<Cell> firstKey = f.getFirstKey();
          // If firstKey is null means storefile is empty.
          if (!firstKey.isPresent()) {
            return null;
          }
          if (f.getComparator().compare(splitKey, firstKey.get()) < 0) {
            return null;
          }
        }
      } finally {
        f.closeStoreFile(f.getCacheConf() != null ? f.getCacheConf().shouldEvictOnClose() : true);
      }
    }

    Path splitDir = new Path(getSplitsDir(daughterRegionInfo), familyName);
    // A reference to the bottom half of the hsf store file.
    Reference r =
      top ? Reference.createTopReference(splitRow): Reference.createBottomReference(splitRow);
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_REGEX regex above.  The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String parentRegionName = parentRegionInfo.getEncodedName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(splitDir, f.getPath().getName() + "." + parentRegionName);
    return r.write(fs, p);
  }

  /**
   * Write out a merge reference under the given merges directory. Package local
   * so it doesnt leak out of regionserver.
   * @param regionToMerge {@link RegionInfo} for one of the regions being merged.
   * @param mergedRegion {@link RegionInfo} of the merged region
   * @param familyName Column Family Name
   * @param f File to create reference.
   * @param mergedDir
   * @param fs FileSystem instance for creating the actual reference file.
   * @return Path to created reference.
   * @throws IOException if the merge write fails.
   */
  public Path mergeStoreFile(RegionInfo regionToMerge, RegionInfo mergedRegion, String familyName, HStoreFile f,
      Path mergedDir, FileSystem fs) throws IOException {
    Path referenceDir = new Path(new Path(mergedDir,
      mergedRegion.getEncodedName()), familyName);
    // A whole reference to the store file.
    Reference r = Reference.createTopReference(regionToMerge.getStartKey());
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_REGEX regex above. The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String mergingRegionName = regionToMerge.getEncodedName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(referenceDir, f.getPath().getName() + "."
      + mergingRegionName);
    return r.write(fs, p);
  }

  /**
   *  Deletes left over splits dirs.
   * @throws IOException
   */
  public abstract void cleanupSplitsDir() throws IOException;

  /**
   * Deletes left over merges dirs.
   * @throws IOException
   */
  public abstract  void cleanupMergesDir() throws IOException;

  /**
   * Deletes any remaining of improperly finished splits.
   * @throws IOException
   */
  public abstract void cleanupAnySplitDetritus() throws IOException;
}
