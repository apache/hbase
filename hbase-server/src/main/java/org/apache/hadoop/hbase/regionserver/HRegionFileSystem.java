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

package org.apache.hadoop.hbase.regionserver;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * View to an on-disk Region.
 * Provides the set of methods necessary to interact with the on-disk region data.
 */
@InterfaceAudience.Private
public class HRegionFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionFileSystem.class);

  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

  /** Temporary subdirectory of the region directory used for merges. */
  public static final String REGION_MERGES_DIR = ".merges";

  /** Temporary subdirectory of the region directory used for splits. */
  public static final String REGION_SPLITS_DIR = ".splits";

  /** Temporary subdirectory of the region directory used for compaction output. */
  @VisibleForTesting static final String REGION_TEMP_DIR = ".tmp";

  private final RegionInfo regionInfo;
  //regionInfo for interacting with FS (getting encodedName, etc)
  private final RegionInfo regionInfoForFs;
  private final Configuration conf;
  private final Path tableDir;
  private final FileSystem fs;

  /**
   * In order to handle NN connectivity hiccups, one need to retry non-idempotent operation at the
   * client level.
   */
  private final int hdfsClientRetriesNumber;
  private final int baseSleepBeforeRetries;
  private static final int DEFAULT_HDFS_CLIENT_RETRIES_NUMBER = 10;
  private static final int DEFAULT_BASE_SLEEP_BEFORE_RETRIES = 1000;

  /**
   * Create a view to the on-disk region
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} that contains the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region
   */
  HRegionFileSystem(final Configuration conf, final FileSystem fs, final Path tableDir,
      final RegionInfo regionInfo) {
    this.fs = fs;
    this.conf = conf;
    this.tableDir = tableDir;
    this.regionInfo = regionInfo;
    this.regionInfoForFs = ServerRegionReplicaUtil.getRegionInfoForFs(regionInfo);
    this.hdfsClientRetriesNumber = conf.getInt("hdfs.client.retries.number",
      DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
    this.baseSleepBeforeRetries = conf.getInt("hdfs.client.sleep.before.retries",
      DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
 }

  /** @return the underlying {@link FileSystem} */
  public FileSystem getFileSystem() {
    return this.fs;
  }

  /** @return the {@link RegionInfo} that describe this on-disk region view */
  public RegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  public RegionInfo getRegionInfoForFS() {
    return this.regionInfoForFs;
  }

  /** @return {@link Path} to the region's root directory. */
  public Path getTableDir() {
    return this.tableDir;
  }

  /** @return {@link Path} to the region directory. */
  public Path getRegionDir() {
    return new Path(this.tableDir, this.regionInfoForFs.getEncodedName());
  }

  // ===========================================================================
  //  Temp Helpers
  // ===========================================================================
  /** @return {@link Path} to the region's temp directory, used for file creations */
  Path getTempDir() {
    return new Path(getRegionDir(), REGION_TEMP_DIR);
  }

  /**
   * Clean up any temp detritus that may have been left around from previous operation attempts.
   */
  void cleanupTempDir() throws IOException {
    deleteDir(getTempDir());
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
    return new Path(this.getRegionDir(), familyName);
  }

  /**
   * Create the store directory for the specified family name
   * @param familyName Column Family Name
   * @return {@link Path} to the directory of the specified family
   * @throws IOException if the directory creation fails.
   */
  Path createStoreDir(final String familyName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    if(!fs.exists(storeDir) && !createDir(storeDir))
      throw new IOException("Failed creating "+storeDir);
    return storeDir;
  }

  /**
   * Set the directory of CF to the specified storage policy. <br>
   * <i>"LAZY_PERSIST"</i>, <i>"ALL_SSD"</i>, <i>"ONE_SSD"</i>, <i>"HOT"</i>, <i>"WARM"</i>,
   * <i>"COLD"</i> <br>
   * <br>
   * See {@link org.apache.hadoop.hdfs.protocol.HdfsConstants} for more details.
   * @param familyName The name of column family.
   * @param policyName The name of the storage policy: 'HOT', 'COLD', etc.
   * See see hadoop 2.6+ org.apache.hadoop.hdfs.protocol.HdfsConstants for possible list e.g
   * 'COLD', 'WARM', 'HOT', 'ONE_SSD', 'ALL_SSD', 'LAZY_PERSIST'.
   */
  public void setStoragePolicy(String familyName, String policyName) {
    FSUtils.setStoragePolicy(this.fs, getStoreDir(familyName), policyName);
  }

  /**
   * Get the storage policy of the directory of CF.
   * @param familyName The name of column family.
   * @return Storage policy name, or {@code null} if not using {@link HFileSystem} or exception
   *         thrown when trying to get policy
   */
  @Nullable
  public String getStoragePolicyName(String familyName) {
    if (this.fs instanceof HFileSystem) {
      Path storeDir = getStoreDir(familyName);
      return ((HFileSystem) this.fs).getStoragePolicyName(storeDir);
    }

    return null;
  }

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
  public Collection<StoreFileInfo> getStoreFiles(final String familyName, final boolean validate)
      throws IOException {
    Path familyDir = getStoreDir(familyName);
    FileStatus[] files = FSUtils.listStatus(this.fs, familyDir);
    if (files == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No StoreFiles for: " + familyDir);
      }
      return null;
    }

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<>(files.length);
    for (FileStatus status: files) {
      if (validate && !StoreFileInfo.isValid(status)) {
        LOG.warn("Invalid StoreFile: " + status.getPath());
        continue;
      }
      StoreFileInfo info = ServerRegionReplicaUtil.getStoreFileInfo(conf, fs, regionInfo,
        regionInfoForFs, familyName, status.getPath());
      storeFiles.add(info);

    }
    return storeFiles;
  }

  /**
   * Returns the store files' LocatedFileStatus which available for the family.
   * This methods performs the filtering based on the valid store files.
   * @param familyName Column Family Name
   * @return a list of store files' LocatedFileStatus for the specified family.
   */
  public static List<LocatedFileStatus> getStoreFilesLocatedStatus(
      final HRegionFileSystem regionfs, final String familyName,
      final boolean validate) throws IOException {
    Path familyDir = regionfs.getStoreDir(familyName);
    List<LocatedFileStatus> locatedFileStatuses = FSUtils.listLocatedStatus(
        regionfs.getFileSystem(), familyDir);
    if (locatedFileStatuses == null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("No StoreFiles for: " + familyDir);
      }
      return null;
    }

    List<LocatedFileStatus> validStoreFiles = Lists.newArrayList();
    for (LocatedFileStatus status : locatedFileStatuses) {
      if (validate && !StoreFileInfo.isValid(status)) {
        LOG.warn("Invalid StoreFile: " + status.getPath());
      } else {
        validStoreFiles.add(status);
      }
    }
    return validStoreFiles;
  }

  /**
   * Return Qualified Path of the specified family/file
   *
   * @param familyName Column Family Name
   * @param fileName File Name
   * @return The qualified Path for the specified family/file
   */
  Path getStoreFilePath(final String familyName, final String fileName) {
    Path familyDir = getStoreDir(familyName);
    return new Path(familyDir, fileName).makeQualified(fs.getUri(), fs.getWorkingDirectory());
  }

  /**
   * Return the store file information of the specified family/file.
   *
   * @param familyName Column Family Name
   * @param fileName File Name
   * @return The {@link StoreFileInfo} for the specified family/file
   */
  StoreFileInfo getStoreFileInfo(final String familyName, final String fileName)
      throws IOException {
    Path familyDir = getStoreDir(familyName);
    return ServerRegionReplicaUtil.getStoreFileInfo(conf, fs, regionInfo,
      regionInfoForFs, familyName, new Path(familyDir, fileName));
  }

  /**
   * Returns true if the specified family has reference files
   * @param familyName Column Family Name
   * @return true if family contains reference files
   * @throws IOException
   */
  public boolean hasReferences(final String familyName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    FileStatus[] files = FSUtils.listStatus(fs, storeDir);
    if (files != null) {
      for(FileStatus stat: files) {
        if(stat.isDirectory()) {
          continue;
        }
        if(StoreFileInfo.isReference(stat.getPath())) {
          if (LOG.isTraceEnabled()) LOG.trace("Reference " + stat.getPath());
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
  public boolean hasReferences(final TableDescriptor htd) throws IOException {
    for (ColumnFamilyDescriptor family : htd.getColumnFamilies()) {
      if (hasReferences(family.getNameAsString())) {
        return true;
      }
    }
    return false;
  }

  /**
   * @return the set of families present on disk
   * @throws IOException
   */
  public Collection<String> getFamilies() throws IOException {
    FileStatus[] fds = FSUtils.listStatus(fs, getRegionDir(), new FSUtils.FamilyDirFilter(fs));
    if (fds == null) return null;

    ArrayList<String> families = new ArrayList<>(fds.length);
    for (FileStatus status: fds) {
      families.add(status.getPath().getName());
    }

    return families;
  }

  /**
   * Remove the region family from disk, archiving the store files.
   * @param familyName Column Family Name
   * @throws IOException if an error occours during the archiving
   */
  public void deleteFamily(final String familyName) throws IOException {
    // archive family store files
    HFileArchiver.archiveFamily(fs, conf, regionInfoForFs, tableDir, Bytes.toBytes(familyName));

    // delete the family folder
    Path familyDir = getStoreDir(familyName);
    if(fs.exists(familyDir) && !deleteDir(familyDir))
      throw new IOException("Could not delete family " + familyName
          + " from FileSystem for region " + regionInfoForFs.getRegionNameAsString() + "("
          + regionInfoForFs.getEncodedName() + ")");
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
    Path dstPath = preCommitStoreFile(familyName, buildPath, -1, false);
    return commitStoreFile(buildPath, dstPath);
  }

  /**
   * Generate the filename in the main family store directory for moving the file from a build/temp
   *  location.
   * @param familyName Family that will gain the file
   * @param buildPath {@link Path} to the file to commit.
   * @param seqNum Sequence Number to append to the file name (less then 0 if no sequence number)
   * @param generateNewName False if you want to keep the buildPath name
   * @return The new {@link Path} of the to be committed file
   * @throws IOException
   */
  private Path preCommitStoreFile(final String familyName, final Path buildPath,
      final long seqNum, final boolean generateNewName) throws IOException {
    Path storeDir = getStoreDir(familyName);
    if(!fs.exists(storeDir) && !createDir(storeDir))
      throw new IOException("Failed creating " + storeDir);

    String name = buildPath.getName();
    if (generateNewName) {
      name = generateUniqueName((seqNum < 0) ? null : "_SeqId_" + seqNum + "_");
    }
    Path dstPath = new Path(storeDir, name);
    if (!fs.exists(buildPath)) {
      throw new FileNotFoundException(buildPath.toString());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Committing " + buildPath + " as " + dstPath);
    }
    return dstPath;
  }

  /*
   * Moves file from staging dir to region dir
   * @param buildPath {@link Path} to the file to commit.
   * @param dstPath {@link Path} to the file under region dir
   * @return The {@link Path} of the committed file
   * @throws IOException
   */
  Path commitStoreFile(final Path buildPath, Path dstPath) throws IOException {
    // buildPath exists, therefore not doing an exists() check.
    if (!rename(buildPath, dstPath)) {
      throw new IOException("Failed rename of " + buildPath + " to " + dstPath);
    }
    return dstPath;
  }

  /**
   * Archives the specified store file from the specified family.
   * @param familyName Family that contains the store files
   * @param filePath {@link Path} to the store file to remove
   * @throws IOException if the archiving fails
   */
  public void removeStoreFile(final String familyName, final Path filePath)
      throws IOException {
    HFileArchiver.archiveStoreFile(this.conf, this.fs, this.regionInfoForFs,
        this.tableDir, Bytes.toBytes(familyName), filePath);
  }

  /**
   * Closes and archives the specified store files from the specified family.
   * @param familyName Family that contains the store files
   * @param storeFiles set of store files to remove
   * @throws IOException if the archiving fails
   */
  public void removeStoreFiles(String familyName, Collection<HStoreFile> storeFiles)
      throws IOException {
    HFileArchiver.archiveStoreFiles(this.conf, this.fs, this.regionInfoForFs,
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
  Pair<Path, Path> bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
      throws IOException {
    // Copy the file if it's on another filesystem
    FileSystem srcFs = srcPath.getFileSystem(conf);
    srcPath = srcFs.resolvePath(srcPath);
    FileSystem realSrcFs = srcPath.getFileSystem(conf);
    FileSystem desFs = fs instanceof HFileSystem ? ((HFileSystem)fs).getBackingFs() : fs;

    // We can't compare FileSystem instances as equals() includes UGI instance
    // as part of the comparison and won't work when doing SecureBulkLoad
    // TODO deal with viewFS
    if (!FSHDFSUtils.isSameHdfs(conf, realSrcFs, desFs)) {
      LOG.info("Bulk-load file " + srcPath + " is on different filesystem than " +
          "the destination store. Copying file over to destination filesystem.");
      Path tmpPath = createTempName();
      FileUtil.copy(realSrcFs, srcPath, fs, tmpPath, false, conf);
      LOG.info("Copied " + srcPath + " to temporary path on destination filesystem: " + tmpPath);
      srcPath = tmpPath;
    }

    return new Pair<>(srcPath, preCommitStoreFile(familyName, srcPath, seqNum, true));
  }

  // ===========================================================================
  //  Splits Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during split operations */
  Path getSplitsDir() {
    return new Path(getRegionDir(), REGION_SPLITS_DIR);
  }

  public Path getSplitsDir(final RegionInfo hri) {
    return new Path(getSplitsDir(), hri.getEncodedName());
  }

  /**
   * Clean up any split detritus that may have been left around from previous split attempts.
   */
  void cleanupSplitsDir() throws IOException {
    deleteDir(getSplitsDir());
  }

  /**
   * Clean up any split detritus that may have been left around from previous
   * split attempts.
   * Call this method on initial region deploy.
   * @throws IOException
   */
  void cleanupAnySplitDetritus() throws IOException {
    Path splitdir = this.getSplitsDir();
    if (!fs.exists(splitdir)) return;
    // Look at the splitdir.  It could have the encoded names of the daughter
    // regions we tried to make.  See if the daughter regions actually got made
    // out under the tabledir.  If here under splitdir still, then the split did
    // not complete.  Try and do cleanup.  This code WILL NOT catch the case
    // where we successfully created daughter a but regionserver crashed during
    // the creation of region b.  In this case, there'll be an orphan daughter
    // dir in the filesystem.  TOOD: Fix.
    FileStatus[] daughters = FSUtils.listStatus(fs, splitdir, new FSUtils.DirFilter(fs));
    if (daughters != null) {
      for (FileStatus daughter: daughters) {
        Path daughterDir = new Path(getTableDir(), daughter.getPath().getName());
        if (fs.exists(daughterDir) && !deleteDir(daughterDir)) {
          throw new IOException("Failed delete of " + daughterDir);
        }
      }
    }
    cleanupSplitsDir();
    LOG.info("Cleaned up old failed split transaction detritus: " + splitdir);
  }

  /**
   * Remove daughter region
   * @param regionInfo daughter {@link RegionInfo}
   * @throws IOException
   */
  void cleanupDaughterRegion(final RegionInfo regionInfo) throws IOException {
    Path regionDir = new Path(this.tableDir, regionInfo.getEncodedName());
    if (this.fs.exists(regionDir) && !deleteDir(regionDir)) {
      throw new IOException("Failed delete of " + regionDir);
    }
  }

  /**
   * Commit a daughter region, moving it from the split temporary directory
   * to the proper location in the filesystem.
   *
   * @param regionInfo daughter {@link org.apache.hadoop.hbase.client.RegionInfo}
   * @throws IOException
   */
  public Path commitDaughterRegion(final RegionInfo regionInfo)
      throws IOException {
    Path regionDir = new Path(this.tableDir, regionInfo.getEncodedName());
    Path daughterTmpDir = this.getSplitsDir(regionInfo);

    if (fs.exists(daughterTmpDir)) {

      // Write HRI to a file in case we need to recover hbase:meta
      Path regionInfoFile = new Path(daughterTmpDir, REGION_INFO_FILE);
      byte[] regionInfoContent = getRegionInfoFileContent(regionInfo);
      writeRegionInfoFileContent(conf, fs, regionInfoFile, regionInfoContent);

      // Move the daughter temp dir to the table dir
      if (!rename(daughterTmpDir, regionDir)) {
        throw new IOException("Unable to rename " + daughterTmpDir + " to " + regionDir);
      }
    }

    return regionDir;
  }

  /**
   * Create the region splits directory.
   */
  public void createSplitsDir(RegionInfo daughterA, RegionInfo daughterB) throws IOException {
    Path splitdir = getSplitsDir();
    if (fs.exists(splitdir)) {
      LOG.info("The " + splitdir + " directory exists.  Hence deleting it to recreate it");
      if (!deleteDir(splitdir)) {
        throw new IOException("Failed deletion of " + splitdir + " before creating them again.");
      }
    }
    // splitDir doesn't exists now. No need to do an exists() call for it.
    if (!createDir(splitdir)) {
      throw new IOException("Failed create of " + splitdir);
    }
    Path daughterATmpDir = getSplitsDir(daughterA);
    if (!createDir(daughterATmpDir)) {
      throw new IOException("Failed create of " + daughterATmpDir);
    }
    Path daughterBTmpDir = getSplitsDir(daughterB);
    if (!createDir(daughterBTmpDir)) {
      throw new IOException("Failed create of " + daughterBTmpDir);
    }
  }

  /**
   * Write out a split reference. Package local so it doesnt leak out of
   * regionserver.
   * @param hri {@link RegionInfo} of the destination
   * @param familyName Column Family Name
   * @param f File to split.
   * @param splitRow Split Row
   * @param top True if we are referring to the top half of the hfile.
   * @param splitPolicy A split policy instance; be careful! May not be full populated; e.g. if
   *                    this method is invoked on the Master side, then the RegionSplitPolicy will
   *                    NOT have a reference to a Region.
   * @return Path to created reference.
   * @throws IOException
   */
  public Path splitStoreFile(RegionInfo hri, String familyName, HStoreFile f, byte[] splitRow,
      boolean top, RegionSplitPolicy splitPolicy) throws IOException {
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
    return r.write(fs, p);
  }

  // ===========================================================================
  //  Merge Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during merge operations */
  public Path getMergesDir() {
    return new Path(getRegionDir(), REGION_MERGES_DIR);
  }

  Path getMergesDir(final RegionInfo hri) {
    return new Path(getMergesDir(), hri.getEncodedName());
  }

  /**
   * Clean up any merge detritus that may have been left around from previous merge attempts.
   */
  void cleanupMergesDir() throws IOException {
    deleteDir(getMergesDir());
  }

  /**
   * Remove merged region
   * @param mergedRegion {@link RegionInfo}
   * @throws IOException
   */
  public void cleanupMergedRegion(final RegionInfo mergedRegion) throws IOException {
    Path regionDir = new Path(this.tableDir, mergedRegion.getEncodedName());
    if (this.fs.exists(regionDir) && !this.fs.delete(regionDir, true)) {
      throw new IOException("Failed delete of " + regionDir);
    }
  }

  static boolean mkdirs(FileSystem fs, Configuration conf, Path dir) throws IOException {
    if (FSUtils.isDistributedFileSystem(fs) ||
        !conf.getBoolean(HConstants.ENABLE_DATA_FILE_UMASK, false)) {
      return fs.mkdirs(dir);
    }
    FsPermission perms = FSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    return fs.mkdirs(dir, perms);
  }

  /**
   * Create the region merges directory.
   * @throws IOException If merges dir already exists or we fail to create it.
   * @see HRegionFileSystem#cleanupMergesDir()
   */
  public void createMergesDir() throws IOException {
    Path mergesdir = getMergesDir();
    if (fs.exists(mergesdir)) {
      LOG.info("The " + mergesdir
          + " directory exists.  Hence deleting it to recreate it");
      if (!fs.delete(mergesdir, true)) {
        throw new IOException("Failed deletion of " + mergesdir
            + " before creating them again.");
      }
    }
    if (!mkdirs(fs, conf, mergesdir))
      throw new IOException("Failed create of " + mergesdir);
  }

  /**
   * Write out a merge reference under the given merges directory. Package local
   * so it doesnt leak out of regionserver.
   * @param mergedRegion {@link RegionInfo} of the merged region
   * @param familyName Column Family Name
   * @param f File to create reference.
   * @param mergedDir
   * @return Path to created reference.
   * @throws IOException
   */
  public Path mergeStoreFile(RegionInfo mergedRegion, String familyName, HStoreFile f,
      Path mergedDir) throws IOException {
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
    return r.write(fs, p);
  }

  /**
   * Commit a merged region, moving it from the merges temporary directory to
   * the proper location in the filesystem.
   * @param mergedRegionInfo merged region {@link RegionInfo}
   * @throws IOException
   */
  public void commitMergedRegion(final RegionInfo mergedRegionInfo) throws IOException {
    Path regionDir = new Path(this.tableDir, mergedRegionInfo.getEncodedName());
    Path mergedRegionTmpDir = this.getMergesDir(mergedRegionInfo);
    // Move the tmp dir in the expected location
    if (mergedRegionTmpDir != null && fs.exists(mergedRegionTmpDir)) {
      if (!fs.rename(mergedRegionTmpDir, regionDir)) {
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
  void logFileSystemState(final Logger LOG) throws IOException {
    FSUtils.logFileSystemState(fs, this.getRegionDir(), LOG);
  }

  /**
   * @param hri
   * @return Content of the file we write out to the filesystem under a region
   * @throws IOException
   */
  private static byte[] getRegionInfoFileContent(final RegionInfo hri) throws IOException {
    return RegionInfo.toDelimitedByteArray(hri);
  }

  /**
   * Create a {@link RegionInfo} from the serialized version on-disk.
   * @param fs {@link FileSystem} that contains the Region Info file
   * @param regionDir {@link Path} to the Region Directory that contains the Info file
   * @return An {@link RegionInfo} instance gotten from the Region Info file.
   * @throws IOException if an error occurred during file open/read operation.
   */
  public static RegionInfo loadRegionInfoFileContent(final FileSystem fs, final Path regionDir)
      throws IOException {
    FSDataInputStream in = fs.open(new Path(regionDir, REGION_INFO_FILE));
    try {
      return RegionInfo.parseFrom(in);
    } finally {
      in.close();
    }
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

  /**
   * Write out an info file under the stored region directory. Useful recovering mangled regions.
   * If the regionInfo already exists on-disk, then we fast exit.
   */
  void checkRegionInfoOnFilesystem() throws IOException {
    // Compose the content of the file so we can compare to length in filesystem. If not same,
    // rewrite it (it may have been written in the old format using Writables instead of pb). The
    // pb version is much shorter -- we write now w/o the toString version -- so checking length
    // only should be sufficient. I don't want to read the file every time to check if it pb
    // serialized.
    byte[] content = getRegionInfoFileContent(regionInfoForFs);

    // Verify if the region directory exists before opening a region. We need to do this since if
    // the region directory doesn't exist we will re-create the region directory and a new HRI
    // when HRegion.openHRegion() is called.
    try {
      FileStatus status = fs.getFileStatus(getRegionDir());
    } catch (FileNotFoundException e) {
      LOG.warn(getRegionDir() + " doesn't exist for region: " + regionInfoForFs.getEncodedName() +
          " on table " + regionInfo.getTable());
    }

    try {
      Path regionInfoFile = new Path(getRegionDir(), REGION_INFO_FILE);
      FileStatus status = fs.getFileStatus(regionInfoFile);
      if (status != null && status.getLen() == content.length) {
        // Then assume the content good and move on.
        // NOTE: that the length is not sufficient to define the the content matches.
        return;
      }

      LOG.info("Rewriting .regioninfo file at: " + regionInfoFile);
      if (!fs.delete(regionInfoFile, false)) {
        throw new IOException("Unable to remove existing " + regionInfoFile);
      }
    } catch (FileNotFoundException e) {
      LOG.warn(REGION_INFO_FILE + " file not found for region: " + regionInfoForFs.getEncodedName() +
          " on table " + regionInfo.getTable());
    }

    // Write HRI to a file in case we need to recover hbase:meta
    writeRegionInfoOnFilesystem(content, true);
  }

  /**
   * Write out an info file under the region directory. Useful recovering mangled regions.
   * @param useTempDir indicate whether or not using the region .tmp dir for a safer file creation.
   */
  private void writeRegionInfoOnFilesystem(boolean useTempDir) throws IOException {
    byte[] content = getRegionInfoFileContent(regionInfoForFs);
    writeRegionInfoOnFilesystem(content, useTempDir);
  }

  /**
   * Write out an info file under the region directory. Useful recovering mangled regions.
   * @param regionInfoContent serialized version of the {@link RegionInfo}
   * @param useTempDir indicate whether or not using the region .tmp dir for a safer file creation.
   */
  private void writeRegionInfoOnFilesystem(final byte[] regionInfoContent,
      final boolean useTempDir) throws IOException {
    Path regionInfoFile = new Path(getRegionDir(), REGION_INFO_FILE);
    if (useTempDir) {
      // Create in tmpDir and then move into place in case we crash after
      // create but before close. If we don't successfully close the file,
      // subsequent region reopens will fail the below because create is
      // registered in NN.

      // And then create the file
      Path tmpPath = new Path(getTempDir(), REGION_INFO_FILE);

      // If datanode crashes or if the RS goes down just before the close is called while trying to
      // close the created regioninfo file in the .tmp directory then on next
      // creation we will be getting AlreadyCreatedException.
      // Hence delete and create the file if exists.
      if (FSUtils.isExists(fs, tmpPath)) {
        FSUtils.delete(fs, tmpPath, true);
      }

      // Write HRI to a file in case we need to recover hbase:meta
      writeRegionInfoFileContent(conf, fs, tmpPath, regionInfoContent);

      // Move the created file to the original path
      if (fs.exists(tmpPath) &&  !rename(tmpPath, regionInfoFile)) {
        throw new IOException("Unable to rename " + tmpPath + " to " + regionInfoFile);
      }
    } else {
      // Write HRI to a file in case we need to recover hbase:meta
      writeRegionInfoFileContent(conf, fs, regionInfoFile, regionInfoContent);
    }
  }

  /**
   * Create a new Region on file-system.
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} from which to add the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region to be added
   * @throws IOException if the region creation fails due to a FileSystem exception.
   */
  public static HRegionFileSystem createRegionOnFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final RegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);

    // We only create a .regioninfo and the region directory if this is the default region replica
    if (regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      Path regionDir = regionFs.getRegionDir();
      if (fs.exists(regionDir)) {
        LOG.warn("Trying to create a region that already exists on disk: " + regionDir);
        throw new IOException("The specified region already exists on disk: " + regionDir);
      }

      // Create the region directory
      if (!createDirOnFileSystem(fs, conf, regionDir)) {
        LOG.warn("Unable to create the region directory: " + regionDir);
        throw new IOException("Unable to create region directory: " + regionDir);
      }

      // Write HRI to a file in case we need to recover hbase:meta
      regionFs.writeRegionInfoOnFilesystem(false);
    } else {
      if (LOG.isDebugEnabled())
        LOG.debug("Skipping creation of .regioninfo file for " + regionInfo);
    }
    return regionFs;
  }

  /**
   * Open Region from file-system.
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} from which to add the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region to be added
   * @param readOnly True if you don't want to edit the region data
   * @throws IOException if the region creation fails due to a FileSystem exception.
   */
  public static HRegionFileSystem openRegionFromFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final RegionInfo regionInfo, boolean readOnly)
      throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regionDir = regionFs.getRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("Trying to open a region that do not exists on disk: " + regionDir);
      throw new IOException("The specified region do not exists on disk: " + regionDir);
    }

    if (!readOnly) {
      // Cleanup temporary directories
      regionFs.cleanupTempDir();
      regionFs.cleanupSplitsDir();
      regionFs.cleanupMergesDir();

      // If it doesn't exists, Write HRI to a file, in case we need to recover hbase:meta
      // Only create HRI if we are the default replica
      if (regionInfo.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
        regionFs.checkRegionInfoOnFilesystem();
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skipping creation of .regioninfo file for " + regionInfo);
        }
      }
    }

    return regionFs;
  }

  /**
   * Remove the region from the table directory, archiving the region's hfiles.
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} from which to remove the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link RegionInfo} for region to be deleted
   * @throws IOException if the request cannot be completed
   */
  public static void deleteRegionFromFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final RegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regionDir = regionFs.getRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("Trying to delete a region that do not exists on disk: " + regionDir);
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("DELETING region " + regionDir);
    }

    // Archive region
    Path rootDir = FSUtils.getRootDir(conf);
    HFileArchiver.archiveRegion(fs, rootDir, tableDir, regionDir);

    // Delete empty region dir
    if (!fs.delete(regionDir, true)) {
      LOG.warn("Failed delete of " + regionDir);
    }
  }

  /**
   * Creates a directory. Assumes the user has already checked for this directory existence.
   * @param dir
   * @return the result of fs.mkdirs(). In case underlying fs throws an IOException, it checks
   *         whether the directory exists or not, and returns true if it exists.
   * @throws IOException
   */
  boolean createDir(Path dir) throws IOException {
    int i = 0;
    IOException lastIOE = null;
    do {
      try {
        return mkdirs(fs, conf, dir);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(dir)) return true; // directory is present
        try {
          sleepBeforeRetry("Create Directory", i+1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);
    throw new IOException("Exception in createDir", lastIOE);
  }

  /**
   * Renames a directory. Assumes the user has already checked for this directory existence.
   * @param srcpath
   * @param dstPath
   * @return true if rename is successful.
   * @throws IOException
   */
  boolean rename(Path srcpath, Path dstPath) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.rename(srcpath, dstPath);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(srcpath) && fs.exists(dstPath)) return true; // successful move
        // dir is not there, retry after some time.
        try {
          sleepBeforeRetry("Rename Directory", i+1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in rename", lastIOE);
  }

  /**
   * Deletes a directory. Assumes the user has already checked for this directory existence.
   * @param dir
   * @return true if the directory is deleted.
   * @throws IOException
   */
  boolean deleteDir(Path dir) throws IOException {
    IOException lastIOE = null;
    int i = 0;
    do {
      try {
        return fs.delete(dir, true);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (!fs.exists(dir)) return true;
        // dir is there, retry deleting after some time.
        try {
          sleepBeforeRetry("Delete Directory", i+1);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in DeleteDir", lastIOE);
  }

  /**
   * sleeping logic; handles the interrupt exception.
   */
  private void sleepBeforeRetry(String msg, int sleepMultiplier) throws InterruptedException {
    sleepBeforeRetry(msg, sleepMultiplier, baseSleepBeforeRetries, hdfsClientRetriesNumber);
  }

  /**
   * Creates a directory for a filesystem and configuration object. Assumes the user has already
   * checked for this directory existence.
   * @param fs
   * @param conf
   * @param dir
   * @return the result of fs.mkdirs(). In case underlying fs throws an IOException, it checks
   *         whether the directory exists or not, and returns true if it exists.
   * @throws IOException
   */
  private static boolean createDirOnFileSystem(FileSystem fs, Configuration conf, Path dir)
      throws IOException {
    int i = 0;
    IOException lastIOE = null;
    int hdfsClientRetriesNumber = conf.getInt("hdfs.client.retries.number",
      DEFAULT_HDFS_CLIENT_RETRIES_NUMBER);
    int baseSleepBeforeRetries = conf.getInt("hdfs.client.sleep.before.retries",
      DEFAULT_BASE_SLEEP_BEFORE_RETRIES);
    do {
      try {
        return fs.mkdirs(dir);
      } catch (IOException ioe) {
        lastIOE = ioe;
        if (fs.exists(dir)) return true; // directory is present
        try {
          sleepBeforeRetry("Create Directory", i+1, baseSleepBeforeRetries, hdfsClientRetriesNumber);
        } catch (InterruptedException e) {
          throw (InterruptedIOException)new InterruptedIOException().initCause(e);
        }
      }
    } while (++i <= hdfsClientRetriesNumber);

    throw new IOException("Exception in createDir", lastIOE);
  }

  /**
   * sleeping logic for static methods; handles the interrupt exception. Keeping a static version
   * for this to avoid re-looking for the integer values.
   */
  private static void sleepBeforeRetry(String msg, int sleepMultiplier, int baseSleepBeforeRetries,
      int hdfsClientRetriesNumber) throws InterruptedException {
    if (sleepMultiplier > hdfsClientRetriesNumber) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(msg + ", retries exhausted");
      }
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(msg + ", sleeping " + baseSleepBeforeRetries + " times " + sleepMultiplier);
    }
    Thread.sleep((long)baseSleepBeforeRetries * sleepMultiplier);
  }
}
