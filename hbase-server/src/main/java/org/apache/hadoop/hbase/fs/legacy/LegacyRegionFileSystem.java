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

package org.apache.hadoop.hbase.fs.legacy;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.FSUtilsWithRetries;
import org.apache.hadoop.hbase.fs.FsContext;
import org.apache.hadoop.hbase.fs.RegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

@InterfaceAudience.Private
public class LegacyRegionFileSystem extends RegionFileSystem {
  private static final Log LOG = LogFactory.getLog(LegacyRegionFileSystem.class);

  private final Path tableDir;
  private final Path regionDir;
  private final Path mobDir;

  // regionInfo for interacting with FS (getting encodedName, etc)
  private final HRegionInfo regionInfoForFs;

  private final FSUtilsWithRetries fsWithRetries;

  public LegacyRegionFileSystem(Configuration conf, FileSystem fs, Path rootDir, HRegionInfo hri) {
    super(conf, fs, rootDir, hri);

    Path dataDir = LegacyLayout.getDataDir(rootDir);
    this.tableDir = LegacyLayout.getTableDir(dataDir, hri.getTable());
    this.regionDir = LegacyLayout.getRegionDir(tableDir, hri);
    this.mobDir = LegacyLayout.getDataDir(LegacyLayout.getMobDir(rootDir));
    this.fsWithRetries = new FSUtilsWithRetries(conf, fs);

    this.regionInfoForFs = ServerRegionReplicaUtil.getRegionInfoForFs(hri);
  }

  public Path getRegionDir() {
    return regionDir;
  }

  public Path getTableDir() {
    return tableDir;
  }

  // ==========================================================================
  //  PUBLIC Methods - Families Related
  // ==========================================================================
  @Override
  public Collection<String> getFamilies() throws IOException {
    FileSystem fs = getFileSystem();
    FileStatus[] fds = FSUtils.listStatus(fs, regionDir, new FSUtils.FamilyDirFilter(fs));
    if (fds == null) return Collections.emptyList();

    ArrayList<String> families = new ArrayList<String>(fds.length);
    for (FileStatus status: fds) {
      families.add(status.getPath().getName());
    }
    return families;
  }

  @Override
  public void deleteFamily(String familyName, boolean hasMob) throws IOException {
    // archive family store files
    byte[] bFamilyName = Bytes.toBytes(familyName);

    FileSystem fs = getFileSystem();
    HFileArchiver.archiveFamily(fs, getConfiguration(), getRegionInfo(), tableDir, bFamilyName);

    // delete the family folder
    HRegionInfo region = getRegionInfo();
    Path familyDir = new Path(tableDir, new Path(region.getEncodedName(), familyName));
    if (!fsWithRetries.deleteDir(familyDir)) {
      throw new IOException("Could not delete family "
          + familyName + " from FileSystem for region "
          + region.getRegionNameAsString() + "(" + region.getEncodedName()
          + ")");
    }

    // archive and delete mob files
    if (hasMob) {
      Path mobTableDir = LegacyLayout.getTableDir(mobDir, getTable());
      HRegionInfo mobRegionInfo = MobUtils.getMobRegionInfo(getTable());
      Path mobRegionDir = LegacyLayout.getRegionDir(mobTableDir, mobRegionInfo);
      Path mobFamilyDir = LegacyLayout.getFamilyDir(mobRegionDir, familyName);
      // archive mob family store files
      MobUtils.archiveMobStoreFiles(getConfiguration(), getFileSystem(),
          mobRegionInfo, mobFamilyDir, bFamilyName);

      if (!fsWithRetries.deleteDir(mobFamilyDir)) {
        throw new IOException("Could not delete mob store files for family "
            + familyName + " from FileSystem region "
            + mobRegionInfo.getRegionNameAsString() + "(" + mobRegionInfo.getEncodedName() + ")");
      }
    }
  }

  // ===========================================================================
  //  Temp Helpers
  // ===========================================================================
  /** @return {@link Path} to the region's temp directory, used for file creations */
  public Path getTempDir() {
    return LegacyLayout.getRegionTempDir(regionDir);
  }

  /**
   * Clean up any temp detritus that may have been left around from previous operation attempts.
   */
  public void cleanupTempDir() throws IOException {
    fsWithRetries.deleteDir(getTempDir());
  }

  // ===========================================================================
  //  Store/StoreFile Helpers
  // ===========================================================================
  /**
   * Returns the directory path of the specified family
   * @param familyName Column Family Name
   * @return {@link Path} to the directory of the specified family
   */
  public Path getStoreDir(final String familyName) {
    return LegacyLayout.getFamilyDir(getRegionDir(), familyName);
  }

  /**
   * Create the store directory for the specified family name
   * @param familyName Column Family Name
   * @return {@link Path} to the directory of the specified family
   * @throws IOException if the directory creation fails.
   */
  public Path createStoreDir(final String familyName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    if (!fsWithRetries.createDir(storeDir))
      throw new IOException("Failed creating "+storeDir);
    return storeDir;
  }

  // ==========================================================================
  //  PUBLIC Methods - Store Files related
  // ==========================================================================

  @Override
  public Collection<StoreFileInfo> getStoreFiles(final String familyName, final boolean validate)
      throws IOException {
    Path familyDir = getStoreDir(familyName);
    FileStatus[] files = FSUtils.listStatus(getFileSystem(), familyDir);
    if (files == null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No StoreFiles for: " + familyDir);
      }
      return null;
    }

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(files.length);
    for (FileStatus status: files) {
      if (validate && !StoreFileInfo.isValid(status)) {
        LOG.warn("Invalid StoreFile: " + status.getPath());
        continue;
      }
      StoreFileInfo info = ServerRegionReplicaUtil.getStoreFileInfo(getConfiguration(),
          getFileSystem(), getRegionInfo(), regionInfoForFs, familyName, status.getPath());
      storeFiles.add(info);

    }
    return storeFiles;
  }


  /**
   * Return Qualified Path of the specified family/file
   *
   * @param familyName Column Family Name
   * @param fileName File Name
   * @return The qualified Path for the specified family/file
   */
  public Path getStoreFilePath(final String familyName, final String fileName) {
    Path familyDir = getStoreDir(familyName);
    return LegacyLayout.getStoreFile(familyDir, fileName).makeQualified(getFileSystem());
  }

  /**
   * Return the store file information of the specified family/file.
   *
   * @param familyName Column Family Name
   * @param fileName File Name
   * @return The {@link StoreFileInfo} for the specified family/file
   */
  public StoreFileInfo getStoreFileInfo(final String familyName, final String fileName)
      throws IOException {
    Path familyDir = getStoreDir(familyName);
    return ServerRegionReplicaUtil.getStoreFileInfo(getConfiguration(),
      getFileSystem(), getRegionInfo(), regionInfoForFs, familyName,
      LegacyLayout.getStoreFile(familyDir, fileName));
  }

  /**
   * Returns true if the specified family has reference files
   * @param familyName Column Family Name
   * @return true if family contains reference files
   * @throws IOException
   */
  public boolean hasReferences(final String familyName) throws IOException {
    FileStatus[] files = FSUtils.listStatus(getFileSystem(), getStoreDir(familyName));
    if (files != null) {
      for(FileStatus stat: files) {
        if(stat.isDirectory()) {
          continue;
        }
        if(StoreFileInfo.isReference(stat.getPath())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check whether region has Reference file
   * @param htd table desciptor of the region
   * @return true if region has reference file
   * @throws IOException
   */
  public boolean hasReferences(final HTableDescriptor htd) throws IOException {
    for (HColumnDescriptor family : htd.getFamilies()) {
      if (hasReferences(family.getNameAsString())) {
        return true;
      }
    }
    return false;
  }


  /**
   * Generate a unique file name, used by createTempName() and commitStoreFile()
   * @param suffix extra information to append to the generated name
   * @return Unique file name
   */
  private static String generateUniqueName(final String suffix) {
    String name = UUID.randomUUID().toString().replaceAll("-", "");
    if (suffix != null) name += suffix;
    return name;
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
  public Path createTempName(final String suffix) {
    return new Path(getTempDir(), generateUniqueName(suffix));
  }

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

  /**
   * Move the file from a build/temp location to the main family store directory.
   * @param familyName Family that will gain the file
   * @param buildPath {@link Path} to the file to commit.
   * @param seqNum Sequence Number to append to the file name (less then 0 if no sequence number)
   * @param generateNewName False if you want to keep the buildPath name
   * @return The new {@link Path} of the committed file
   * @throws IOException
   */
  private Path commitStoreFile(final String familyName, final Path buildPath,
      final long seqNum, final boolean generateNewName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    if(!fsWithRetries.createDir(storeDir))
      throw new IOException("Failed creating " + storeDir);

    String name = buildPath.getName();
    if (generateNewName) {
      name = generateUniqueName((seqNum < 0) ? null : "_SeqId_" + seqNum + "_");
    }
    Path dstPath = new Path(storeDir, name);
    if (!fsWithRetries.exists(buildPath)) {
      throw new FileNotFoundException(buildPath.toString());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Committing store file " + buildPath + " as " + dstPath);
    }
    // buildPath exists, therefore not doing an exists() check.
    if (!fsWithRetries.rename(buildPath, dstPath)) {
      throw new IOException("Failed rename of " + buildPath + " to " + dstPath);
    }
    return dstPath;
  }


  /**
   * Moves multiple store files to the relative region's family store directory.
   * @param storeFiles list of store files divided by family
   * @throws IOException
   */
  public void commitStoreFiles(final Map<byte[], List<StoreFile>> storeFiles) throws IOException {
    for (Map.Entry<byte[], List<StoreFile>> es: storeFiles.entrySet()) {
      String familyName = Bytes.toString(es.getKey());
      for (StoreFile sf: es.getValue()) {
        commitStoreFile(familyName, sf.getPath());
      }
    }
  }

  /**
   * Archives the specified store file from the specified family.
   * @param familyName Family that contains the store files
   * @param filePath {@link Path} to the store file to remove
   * @throws IOException if the archiving fails
   */
  public void removeStoreFile(final String familyName, final Path filePath)
      throws IOException {
    HFileArchiver.archiveStoreFile(getConfiguration(), getFileSystem(), this.regionInfoForFs,
        this.tableDir, Bytes.toBytes(familyName), filePath);
  }

  /**
   * Closes and archives the specified store files from the specified family.
   * @param familyName Family that contains the store files
   * @param storeFiles set of store files to remove
   * @throws IOException if the archiving fails
   */
  public void removeStoreFiles(final String familyName, final Collection<StoreFile> storeFiles)
      throws IOException {
    HFileArchiver.archiveStoreFiles(getConfiguration(), getFileSystem(), this.regionInfoForFs,
        this.tableDir, Bytes.toBytes(familyName), storeFiles);
  }

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
  public Path bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
      throws IOException {
    // Copy the file if it's on another filesystem
    FileSystem fs = getFileSystem();
    FileSystem srcFs = srcPath.getFileSystem(getConfiguration());
    FileSystem desFs = fs instanceof HFileSystem ? ((HFileSystem)fs).getBackingFs() : fs;

    // We can't compare FileSystem instances as equals() includes UGI instance
    // as part of the comparison and won't work when doing SecureBulkLoad
    // TODO deal with viewFS
    if (!FSHDFSUtils.isSameHdfs(getConfiguration(), srcFs, desFs)) {
      LOG.info("Bulk-load file " + srcPath + " is on different filesystem than " +
          "the destination store. Copying file over to destination filesystem.");
      Path tmpPath = createTempName();
      FileUtil.copy(srcFs, srcPath, fs, tmpPath, false, getConfiguration());
      LOG.info("Copied " + srcPath + " to temporary path on destination filesystem: " + tmpPath);
      srcPath = tmpPath;
    }

    return commitStoreFile(familyName, srcPath, seqNum, true);
  }

  // ===========================================================================
  //  Splits Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during split operations */
  public Path getSplitsDir() {
    return LegacyLayout.getRegionSplitsDir(getRegionDir());
  }

  public Path getSplitsDir(final HRegionInfo hri) {
    return LegacyLayout.getRegionSplitsDir(getSplitsDir(), hri);
  }

  /**
   * Clean up any split detritus that may have been left around from previous split attempts.
   */
  public void cleanupSplitsDir() throws IOException {
    fsWithRetries.deleteDir(getSplitsDir());
  }

  /**
   * Clean up any split detritus that may have been left around from previous
   * split attempts.
   * Call this method on initial region deploy.
   * @throws IOException
   */
  public void cleanupAnySplitDetritus() throws IOException {
    Path splitdir = this.getSplitsDir();
    if (!fsWithRetries.exists(splitdir)) return;
    // Look at the splitdir.  It could have the encoded names of the daughter
    // regions we tried to make.  See if the daughter regions actually got made
    // out under the tabledir.  If here under splitdir still, then the split did
    // not complete.  Try and do cleanup.  This code WILL NOT catch the case
    // where we successfully created daughter a but regionserver crashed during
    // the creation of region b.  In this case, there'll be an orphan daughter
    // dir in the filesystem.  TOOD: Fix.
    FileSystem fs = getFileSystem();
    FileStatus[] daughters = FSUtils.listStatus(fs, splitdir, new FSUtils.DirFilter(fs));
    if (daughters != null) {
      for (int i = 0; i < daughters.length; ++i) {
        Path daughterDir = new Path(this.tableDir, daughters[i].getPath().getName());
        if (!fsWithRetries.deleteDir(daughterDir)) {
          throw new IOException("Failed delete of " + daughterDir);
        }
      }
    }
    cleanupSplitsDir();
    LOG.info("Cleaned up old failed split transaction detritus: " + splitdir);
  }

  /**
   * Remove daughter region
   * @param regionInfo daughter {@link HRegionInfo}
   * @throws IOException
   */
  public void cleanupDaughterRegion(final HRegionInfo regionInfo) throws IOException {
    Path regionDir = LegacyLayout.getRegionDir(tableDir, regionInfo);
    if (!fsWithRetries.deleteDir(regionDir)) {
      throw new IOException("Failed delete of " + regionDir);
    }
  }

  /**
   * Commit a daughter region, moving it from the split temporary directory
   * to the proper location in the filesystem.
   *
   * @param regionInfo                 daughter {@link org.apache.hadoop.hbase.HRegionInfo}
   * @throws IOException
   */
  public Path commitDaughterRegion(final HRegionInfo regionInfo)
      throws IOException {
    Path regionDir = LegacyLayout.getRegionDir(tableDir, regionInfo);
    Path daughterTmpDir = this.getSplitsDir(regionInfo);

    if (fsWithRetries.exists(daughterTmpDir)) {

      // Write HRI to a file in case we need to recover hbase:meta
      Path regionInfoFile = LegacyLayout.getRegionInfoFile(daughterTmpDir);
      byte[] regionInfoContent = getRegionInfoFileContent(regionInfo);
      writeRegionInfoFileContent(getConfiguration(), getFileSystem(), regionInfoFile, regionInfoContent);

      // Move the daughter temp dir to the table dir
      if (!fsWithRetries.rename(daughterTmpDir, regionDir)) {
        throw new IOException("Unable to rename " + daughterTmpDir + " to " + regionDir);
      }
    }

    return regionDir;
  }

  /**
   * Create the region splits directory.
   */
  public void createSplitsDir() throws IOException {
    createTempDir(getSplitsDir());
  }

  void createTempDir(Path dir) throws IOException {
    if (fsWithRetries.exists(dir)) {
      LOG.info("The " + dir + " directory exists.  Hence deleting it to recreate it");
      if (!fsWithRetries.deleteDir(dir)) {
        throw new IOException("Failed deletion of " + dir + " before creating them again.");
      }
    }
    // dir doesn't exists now. No need to do an exists() call for it.
    if (!fsWithRetries.createDir(dir)) {
      throw new IOException("Failed create of " + dir);
    }
  }

  /**
   * Write out a split reference. Package local so it doesnt leak out of
   * regionserver.
   * @param hri {@link HRegionInfo} of the destination
   * @param familyName Column Family Name
   * @param f File to split.
   * @param splitRow Split Row
   * @param top True if we are referring to the top half of the hfile.
   * @param splitPolicy
   * @return Path to created reference.
   * @throws IOException
   */
  public Path splitStoreFile(final HRegionInfo hri, final String familyName, final StoreFile f,
      final byte[] splitRow, final boolean top, RegionSplitPolicy splitPolicy)
          throws IOException {

    if (splitPolicy == null || !splitPolicy.skipStoreFileRangeCheck(familyName)) {
      // Check whether the split row lies in the range of the store file
      // If it is outside the range, return directly.
      try {
        if (top) {
          //check if larger than last key.
          Cell splitKey = CellUtil.createFirstOnRow(splitRow);
          Cell lastKey = f.getLastKey();
          // If lastKey is null means storefile is empty.
          if (lastKey == null) {
            return null;
          }
          if (f.getComparator().compare(splitKey, lastKey) > 0) {
            return null;
          }
        } else {
          //check if smaller than first key
          Cell splitKey = CellUtil.createLastOnRow(splitRow);
          Cell firstKey = f.getFirstKey();
          // If firstKey is null means storefile is empty.
          if (firstKey == null) {
            return null;
          }
          if (f.getComparator().compare(splitKey, firstKey) < 0) {
            return null;
          }
        }
      } finally {
        f.closeReader(true);
      }
    }

    Path splitDir = new Path(getSplitsDir(hri), familyName);
    // A reference to the bottom half of the hsf store file.
    Reference r =
      top ? Reference.createTopReference(splitRow): Reference.createBottomReference(splitRow);
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_REGEX regex above.  The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String parentRegionName = regionInfoForFs.getEncodedName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(splitDir, f.getPath().getName() + "." + parentRegionName);
    return r.write(getFileSystem(), p);
  }

  // ===========================================================================
  //  Merge Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during merge operations */
  public Path getMergesDir() {
    return LegacyLayout.getRegionMergesDir(getRegionDir());
  }

  Path getMergesDir(final HRegionInfo hri) {
    return LegacyLayout.getRegionMergesDir(getMergesDir(), hri);
  }

  /**
   * Clean up any merge detritus that may have been left around from previous merge attempts.
   */
  public void cleanupMergesDir() throws IOException {
    fsWithRetries.deleteDir(getMergesDir());
  }

  /**
   * Remove merged region
   * @param mergedRegion {@link HRegionInfo}
   * @throws IOException
   */
  public void cleanupMergedRegion(final HRegionInfo mergedRegion) throws IOException {
    Path regionDir = LegacyLayout.getRegionDir(tableDir, mergedRegion);
    if (fsWithRetries.deleteDir(regionDir)) {
      throw new IOException("Failed delete of " + regionDir);
    }
  }

  /**
   * Create the region merges directory.
   * @throws IOException If merges dir already exists or we fail to create it.
   * @see HRegionFileSystem#cleanupMergesDir()
   */
  public void createMergesDir() throws IOException {
    createTempDir(getMergesDir());
  }

  /**
   * Write out a merge reference under the given merges directory. Package local
   * so it doesnt leak out of regionserver.
   * @param mergedRegion {@link HRegionInfo} of the merged region
   * @param familyName Column Family Name
   * @param f File to create reference.
   * @param mergedDir
   * @return Path to created reference.
   * @throws IOException
   */
  public Path mergeStoreFile(final HRegionInfo mergedRegion, final String familyName,
      final StoreFile f, final Path mergedDir)
      throws IOException {
    Path referenceDir = new Path(new Path(mergedDir,
        mergedRegion.getEncodedName()), familyName);
    // A whole reference to the store file.
    Reference r = Reference.createTopReference(regionInfoForFs.getStartKey());
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_REGEX regex above. The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String mergingRegionName = regionInfoForFs.getEncodedName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(referenceDir, f.getPath().getName() + "."
        + mergingRegionName);
    return r.write(getFileSystem(), p);
  }

  /**
   * Commit a merged region, moving it from the merges temporary directory to
   * the proper location in the filesystem.
   * @param mergedRegionInfo merged region {@link HRegionInfo}
   * @throws IOException
   */
  public void commitMergedRegion(final HRegionInfo mergedRegionInfo) throws IOException {
    Path regionDir = new Path(this.tableDir, mergedRegionInfo.getEncodedName());
    Path mergedRegionTmpDir = this.getMergesDir(mergedRegionInfo);
    // Move the tmp dir in the expected location
    if (mergedRegionTmpDir != null && fsWithRetries.exists(mergedRegionTmpDir)) {
      if (!fsWithRetries.rename(mergedRegionTmpDir, regionDir)) {
        throw new IOException("Unable to rename " + mergedRegionTmpDir + " to "
            + regionDir);
      }
    }
  }

  // ===========================================================================
  //  Create/Open/Delete Helpers
  // ===========================================================================
  /**
   * Log the current state of the region
   * @param LOG log to output information
   * @throws IOException if an unexpected exception occurs
   */
  public void logFileSystemState(final Log LOG) throws IOException {
    FSUtils.logFileSystemState(getFileSystem(), this.getRegionDir(), LOG);
  }

  /**
   * @param hri
   * @return Content of the file we write out to the filesystem under a region
   * @throws IOException
   */
  private static byte[] getRegionInfoFileContent(final HRegionInfo hri) throws IOException {
    return hri.toDelimitedByteArray();
  }

  /**
   * Write the .regioninfo file on-disk.
   */
  private static void writeRegionInfoFileContent(final Configuration conf, final FileSystem fs,
      final Path regionInfoFile, final byte[] content) throws IOException {
    // First check to get the permissions
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    // Write the RegionInfo file content
    FSDataOutputStream out = FSUtils.create(conf, fs, regionInfoFile, perms, null);
    try {
      out.write(content);
    } finally {
      out.close();
    }
  }

  // ==========================================================================
  //  PUBLIC bootstrap
  // ==========================================================================
  @Override
  protected void bootstrap() throws IOException {
    fsWithRetries.createDir(getRegionDir());

    // Cleanup temporary directories
    cleanupTempDir();
    cleanupSplitsDir();
    cleanupMergesDir();

    // if it doesn't exists, Write HRI to a file, in case we need to recover hbase:meta
    checkRegionInfoOnFilesystem();
  }

  public void checkRegionInfoOnFilesystem() throws IOException {
    writeRegionInfoFileContent(getConfiguration(), getFileSystem(),
      LegacyLayout.getRegionInfoFile(getRegionDir()),
      getRegionInfoFileContent(getRegionInfo()));
  }

  @Override
  protected void destroy() throws IOException {
    fsWithRetries.deleteDir(regionDir);
  }
}
