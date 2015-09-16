/**
 *
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

package org.apache.hadoop.hbase.fs;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.RegionSplitPolicy;

@InterfaceAudience.Private
public abstract class HRegionFileSystem {
  private static final Log LOG = LogFactory.getLog(HRegionFileSystem.class);

  // blah... compat
  public enum Type { TABLE, SNAPSHOT, ARCHIVE };

  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

  // ==========================================================================
  //  PUBLIC methods - add/remove/list store files
  // ==========================================================================
  /**
   * Generate a unique temporary Path. Used in conjuction with commitStoreFile()
   * to get a safer file creation.
   * <code>
   * Path file = fs.createTempName();
   * ...StoreFile.Writer(file)...
   * fs.commitStoreFile("family", file);
   * </code>
   *
   * @return Unique {@link Path} of the temporary file
   */
  public Path createTempName() {
    return createTempName(null);
  }

  /**
   * Generate a unique temporary Path. Used in conjuction with commitStoreFile()
   * to get a safer file creation.
   * <code>
   * Path file = fs.createTempName();
   * ...StoreFile.Writer(file)...
   * fs.commitStoreFile("family", file);
   * </code>
   *
   * @param suffix extra information to append to the generated name
   * @return Unique {@link Path} of the temporary file
   */
  public abstract Path createTempName(final String suffix); // TODO REMOVE THIS


  /**
   * Move the file from a build/temp location to the main family store directory.
   * @param familyName Family that will gain the file
   * @param buildPath {@link Path} to the file to commit.
   * @return The new {@link Path} of the committed file
   * @throws IOException
   */
  public Path commitStoreFile(final String familyName, final Path buildPath) throws IOException {
    return commitStoreFile(familyName, buildPath, -1, false);
  }

  public abstract Path commitStoreFile(final String familyName, final Path buildPath,
      final long seqNum, final boolean generateNewName) throws IOException;

  /**
   * Moves multiple store files to the relative region's family store directory.
   * @param storeFiles list of store files divided by family
   * @throws IOException
   */
  public abstract void commitStoreFiles(final Map<byte[], List<StoreFile>> storeFiles)
      throws IOException;

  /**
   * Bulk load: Add a specified store file to the specified family.
   * If the source file is on the same different file-system is moved from the
   * source location to the destination location, otherwise is copied over.
   *
   * @param familyName Family that will gain the file
   * @param srcPath {@link Path} to the file to import
   * @param seqNum Bulk Load sequence number
   * @return The destination {@link Path} of the bulk loaded file
   * @throws IOException
   */
  public abstract Path bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
      throws IOException;

  /**
   * Archives the specified store file from the specified family.
   * @param familyName Family that contains the store files
   * @param filePath {@link Path} to the store file to remove
   * @throws IOException if the archiving fails
   */
  public abstract void removeStoreFile(String familyName, Path filePath)
      throws IOException;

  /**
   * Closes and archives the specified store files from the specified family.
   * @param familyName Family that contains the store files
   * @param storeFiles set of store files to remove
   * @throws IOException if the archiving fails
   */
  public abstract void removeStoreFiles(String familyName, Collection<StoreFile> storeFiles)
      throws IOException;

  public abstract Path getStoreFilePath(final String familyName, final String fileName);

  /**
   * Returns the store files available for the family.
   * This methods performs the filtering based on the valid store files.
   * @param familyName Column Family Name
   * @return a set of {@link StoreFileInfo} for the specified family.
   */
  public Collection<StoreFileInfo> getStoreFiles(final byte[] familyName) throws IOException {
    return getStoreFiles(Bytes.toString(familyName));
  }

  public Collection<StoreFileInfo> getStoreFiles(final String familyName) throws IOException {
    return getStoreFiles(familyName, true);
  }

  /**
   * Returns the store files available for the family.
   * This methods performs the filtering based on the valid store files.
   * @param familyName Column Family Name
   * @return a set of {@link StoreFileInfo} for the specified family.
   */
  public abstract Collection<StoreFileInfo> getStoreFiles(String familyName, boolean validate)
      throws IOException;

  /**
   * Return the store file information of the specified family/file.
   *
   * @param familyName Column Family Name
   * @param fileName File Name
   * @return The {@link StoreFileInfo} for the specified family/file
   */
  public abstract StoreFileInfo getStoreFileInfo(final String familyName, final String fileName)
      throws IOException;

  /**
   * @return the set of families present on disk
   * @throws IOException
   */
  public abstract Collection<String> getFamilies() throws IOException;

  // ==========================================================================
  //  ??? methods - do we still need this stuff in 2.0?
  // ==========================================================================
  public abstract void writeRecoveryCheckPoint() throws IOException;
  public abstract void cleanup() throws IOException;

  // ???
  public void logFileSystemState(Log log) {}
  public HRegionInfo getRegionInfoForFS() { return null; }
  public FileSystem getFileSystem() { return null; }
  public Path getRegionDir() { return null; }
  public Path getStoreDir(String family) { return null; }

  // remove on proc-v2 rewrite... move now in Merge/Split FS interface?
  public void cleanupDaughterRegion(final HRegionInfo regionInfo) throws IOException {}
  public void commitDaughterRegion(HRegionInfo hri) {}
  public void cleanupMergedRegion(final HRegionInfo mergedRegion) throws IOException {}
  public void commitMergedRegion(HRegionInfo hri) {}
  public void createSplitsDir() throws IOException {}
  public Path splitStoreFile(final HRegionInfo hri, final String familyName, final StoreFile f,
      final byte[] splitRow, final boolean top, RegionSplitPolicy splitPolicy)
          throws IOException { return null; }
  public Path getMergesDir() { return null; }
  public Path getMergesDir(final HRegionInfo hri) { return null; }
  public void createMergesDir() throws IOException {}
  public void assertReferenceFileCountOfSplitsDir(int expectedReferenceFileCount, HRegionInfo daughter) {}
  public void assertReferenceFileCountOfDaughterDir(int expectedReferenceFileCount, HRegionInfo daughter) {}
  public void cleanupSplitsDir() throws IOException {}
  public void cleanupMergesDir() throws IOException {}
  public Path mergeStoreFile(final HRegionInfo mergedRegion, final String familyName,
      final StoreFile f, final Path mergedDir)
      throws IOException { return null; }

  public boolean hasReferences(final String familyName) { return false; }
  public boolean hasReferences(final HTableDescriptor htd) { return false; }

  public void openFamily(final String family) throws IOException {}

  // ==========================================================================
  //  MAYBE methods
  // ==========================================================================
  public abstract HRegionInfo getRegionInfo();

  public TableName getTable() {
    return getRegionInfo().getTable();
  }

  // ==========================================================================
  //  PUBLIC methods - create/open/destroy
  // ==========================================================================
  public abstract void open(boolean rdonly) throws IOException;
  public abstract void create() throws IOException;
  public abstract void destroy() throws IOException;

  // ==========================================================================
  //  PUBLIC Static/Factory methods - create/open/destroy
  // ==========================================================================
  public static HRegionFileSystem createSnapshot(final Configuration conf,
      final HRegionInfo regionInfo) throws IOException {
    // we need a dir on open, create if we want to reuse...
    return null;
  }

  public static HRegionFileSystem create(final Configuration conf,
      final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem rfs = getInstance(conf, regionInfo);
    rfs.create();
    return rfs;
  }

  public static HRegionFileSystem destroy(final Configuration conf,
      final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem rfs = getInstance(conf, regionInfo);
    rfs.destroy();
    return rfs;
  }

  public static HRegionFileSystem open(final Configuration conf,
      final HRegionInfo regionInfo, boolean readOnly) throws IOException {
    HRegionFileSystem rfs = getInstance(conf, regionInfo);
    rfs.open(readOnly);
    return rfs;
  }

  private static HRegionFileSystem getInstance(final Configuration conf,
      final HRegionInfo regionInfo) throws IOException {
    String fsType = conf.get("hbase.fs.layout.type").toLowerCase();
    switch (fsType) {
      case "legacy":
        return new LegacyRegionFileSystem(conf, regionInfo);
      case "hierarchical":
        return new HierarchicalRegionFileSystem(conf, regionInfo);
      default:
        throw new IOException("Invalid filesystem type " + fsType);
    }
  }
}