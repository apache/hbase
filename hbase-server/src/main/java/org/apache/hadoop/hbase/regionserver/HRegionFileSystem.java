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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * View to an on-disk Region.
 * Provides the set of methods necessary to interact with the on-disk region data.
 */
@InterfaceAudience.Private
public class HRegionFileSystem {
  public static final Log LOG = LogFactory.getLog(HRegionFileSystem.class);

  /** Name of the region info file that resides just under the region directory. */
  public final static String REGION_INFO_FILE = ".regioninfo";

  /** Temporary subdirectory of the region directory used for merges. */
  public static final String REGION_MERGES_DIR = ".merges";

  /** Temporary subdirectory of the region directory used for splits. */
  public static final String REGION_SPLITS_DIR = ".splits";

  /** Temporary subdirectory of the region directory used for compaction output. */
  private static final String REGION_TEMP_DIR = ".tmp";

  private final HRegionInfo regionInfo;
  private final Configuration conf;
  private final Path tableDir;
  private final FileSystem fs;

  /**
   * Create a view to the on-disk region
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} that contains the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link HRegionInfo} for region
   */
  HRegionFileSystem(final Configuration conf, final FileSystem fs,
      final Path tableDir, final HRegionInfo regionInfo) {
    this.fs = fs;
    this.conf = conf;
    this.tableDir = tableDir;
    this.regionInfo = regionInfo;
  }

  /** @return the underlying {@link FileSystem} */
  public FileSystem getFileSystem() {
    return this.fs;
  }

  /** @return the {@link HRegionInfo} that describe this on-disk region view */
  public HRegionInfo getRegionInfo() {
    return this.regionInfo;
  }

  /** @return {@link Path} to the region's root directory. */
  public Path getTableDir() {
    return this.tableDir;
  }

  /** @return {@link Path} to the region directory. */
  public Path getRegionDir() {
    return new Path(this.tableDir, this.regionInfo.getEncodedName());
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
    FSUtils.deleteDirectory(fs, getTempDir());
  }

  // ===========================================================================
  //  Store/StoreFile Helpers
  // ===========================================================================
  /**
   * Returns the directory path of the specified family
   * @param familyName Column Family Name
   * @return {@link Path} to the directory of the specified family
   */
  Path getStoreDir(final String familyName) {
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
    if (!fs.exists(storeDir) && !fs.mkdirs(storeDir)) {
      throw new IOException("Failed create of: " + storeDir);
    }
    return storeDir;
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

  /**
   * Returns the store files available for the family.
   * This methods performs the filtering based on the valid store files.
   * @param familyName Column Family Name
   * @return a set of {@link StoreFileInfo} for the specified family.
   */
  public Collection<StoreFileInfo> getStoreFiles(final String familyName) throws IOException {
    Path familyDir = getStoreDir(familyName);
    FileStatus[] files = FSUtils.listStatus(this.fs, familyDir);
    if (files == null) return null;

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(files.length);
    for (FileStatus status: files) {
      if (!StoreFileInfo.isValid(status)) continue;

      storeFiles.add(new StoreFileInfo(this.conf, this.fs, status));
    }
    return storeFiles;
  }

  /**
   * Returns true if the specified family has reference files
   * @param familyName Column Family Name
   * @return true if family contains reference files
   * @throws IOException
   */
  public boolean hasReferences(final String familyName) throws IOException {
    FileStatus[] files = FSUtils.listStatus(fs, getStoreDir(familyName),
      new PathFilter () {
        public boolean accept(Path path) {
          return StoreFileInfo.isReference(path);
        }
      }
    );
    return files != null && files.length > 0;
  }

  /**
   * @return the set of families present on disk
   * @throws IOException
   */
  public Collection<String> getFamilies() throws IOException {
    FileStatus[] fds = FSUtils.listStatus(fs, getRegionDir(), new FSUtils.FamilyDirFilter(fs));
    if (fds == null) return null;

    ArrayList<String> families = new ArrayList<String>(fds.length);
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
    HFileArchiver.archiveFamily(fs, conf, regionInfo, tableDir, Bytes.toBytes(familyName));

    // delete the family folder
    Path familyDir = getStoreDir(familyName);
    if (!fs.delete(familyDir, true)) {
      throw new IOException("Could not delete family " + familyName +
        " from FileSystem for region " + regionInfo.getRegionNameAsString() +
        "(" + regionInfo.getEncodedName() + ")");
    }
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
    fs.mkdirs(storeDir);
    String name = buildPath.getName();
    if (generateNewName) {
      name = generateUniqueName((seqNum < 0) ? null : "_SeqId_" + seqNum + "_");
    }
    Path dstPath = new Path(storeDir, name);
    if (!fs.exists(buildPath)) {
      throw new FileNotFoundException(buildPath.toString());
    }
    LOG.debug("Committing store file " + buildPath + " as " + dstPath);
    if (!fs.rename(buildPath, dstPath)) {
      throw new IOException("Failed rename of " + buildPath + " to " + dstPath);
    }
    return dstPath;
  }

  /**
   * Moves multiple store files to the relative region's family store directory.
   * @param storeFiles list of store files divided by family
   * @throws IOException
   */
  void commitStoreFiles(final Map<byte[], List<StoreFile>> storeFiles) throws IOException {
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
    HFileArchiver.archiveStoreFile(this.conf, this.fs, this.regionInfo,
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
    HFileArchiver.archiveStoreFiles(this.conf, this.fs, this.regionInfo,
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
  Path bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
      throws IOException {
    // Copy the file if it's on another filesystem
    FileSystem srcFs = srcPath.getFileSystem(conf);
    FileSystem desFs = fs instanceof HFileSystem ? ((HFileSystem)fs).getBackingFs() : fs;

    // We can't compare FileSystem instances as equals() includes UGI instance
    // as part of the comparison and won't work when doing SecureBulkLoad
    // TODO deal with viewFS
    if (!srcFs.getUri().equals(desFs.getUri())) {
      LOG.info("Bulk-load file " + srcPath + " is on different filesystem than " +
          "the destination store. Copying file over to destination filesystem.");
      Path tmpPath = createTempName();
      FileUtil.copy(srcFs, srcPath, fs, tmpPath, false, conf);
      LOG.info("Copied " + srcPath + " to temporary path on destination filesystem: " + tmpPath);
      srcPath = tmpPath;
    }

    return commitStoreFile(familyName, srcPath, seqNum, true);
  }

  // ===========================================================================
  //  Splits Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during split operations */
  Path getSplitsDir() {
    return new Path(getRegionDir(), REGION_SPLITS_DIR);
  }

  Path getSplitsDir(final HRegionInfo hri) {
    return new Path(getSplitsDir(), hri.getEncodedName());
  }

  /**
   * Clean up any split detritus that may have been left around from previous split attempts.
   */
  void cleanupSplitsDir() throws IOException {
    FSUtils.deleteDirectory(fs, getSplitsDir());
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
        if (fs.exists(daughterDir) && !fs.delete(daughterDir, true)) {
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
  void cleanupDaughterRegion(final HRegionInfo regionInfo) throws IOException {
    Path regionDir = new Path(this.tableDir, regionInfo.getEncodedName());
    if (this.fs.exists(regionDir) && !this.fs.delete(regionDir, true)) {
      throw new IOException("Failed delete of " + regionDir);
    }
  }

  /**
   * Commit a daughter region, moving it from the split temporary directory
   * to the proper location in the filesystem.
   * @param regionInfo daughter {@link HRegionInfo}
   * @throws IOException
   */
  Path commitDaughterRegion(final HRegionInfo regionInfo) throws IOException {
    Path regionDir = new Path(this.tableDir, regionInfo.getEncodedName());
    Path daughterTmpDir = this.getSplitsDir(regionInfo);
    if (fs.exists(daughterTmpDir) && !fs.rename(daughterTmpDir, regionDir)) {
      throw new IOException("Unable to rename " + daughterTmpDir + " to " + regionDir);
    }
    return regionDir;
  }

  /**
   * Create the region splits directory.
   */
  void createSplitsDir() throws IOException {
    Path splitdir = getSplitsDir();
    if (fs.exists(splitdir)) {
      LOG.info("The " + splitdir + " directory exists.  Hence deleting it to recreate it");
      if (!fs.delete(splitdir, true)) {
        throw new IOException("Failed deletion of " + splitdir
            + " before creating them again.");
      }
    }
    if (!fs.mkdirs(splitdir)) {
      throw new IOException("Failed create of " + splitdir);
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
   * @return Path to created reference.
   * @throws IOException
   */
  Path splitStoreFile(final HRegionInfo hri, final String familyName,
      final StoreFile f, final byte[] splitRow, final boolean top) throws IOException {
    Path splitDir = new Path(getSplitsDir(hri), familyName);
    // A reference to the bottom half of the hsf store file.
    Reference r =
      top ? Reference.createTopReference(splitRow): Reference.createBottomReference(splitRow);
    // Add the referred-to regions name as a dot separated suffix.
    // See REF_NAME_REGEX regex above.  The referred-to regions name is
    // up in the path of the passed in <code>f</code> -- parentdir is family,
    // then the directory above is the region name.
    String parentRegionName = regionInfo.getEncodedName();
    // Write reference with same file id only with the other region name as
    // suffix and into the new region location (under same family).
    Path p = new Path(splitDir, f.getPath().getName() + "." + parentRegionName);
    return r.write(fs, p);
  }

  // ===========================================================================
  //  Merge Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during merge operations */
  Path getMergesDir() {
    return new Path(getRegionDir(), REGION_MERGES_DIR);
  }

  /**
   * Clean up any merge detritus that may have been left around from previous merge attempts.
   */
  void cleanupMergesDir() throws IOException {
    FSUtils.deleteDirectory(fs, getMergesDir());
  }

  // ===========================================================================
  //  Create/Open/Delete Helpers
  // ===========================================================================
  /**
   * Log the current state of the region
   * @param LOG log to output information
   * @throws IOException if an unexpected exception occurs
   */
  void logFileSystemState(final Log LOG) throws IOException {
    FSUtils.logFileSystemState(fs, this.getRegionDir(), LOG);
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
   * Create a {@link HRegionInfo} from the serialized version on-disk.
   * @param fs {@link FileSystem} that contains the Region Info file
   * @param regionDir {@link Path} to the Region Directory that contains the Info file
   * @return An {@link HRegionInfo} instance gotten from the Region Info file.
   * @throws IOException if an error occurred during file open/read operation.
   */
  public static HRegionInfo loadRegionInfoFileContent(final FileSystem fs, final Path regionDir)
      throws IOException {
    FSDataInputStream in = fs.open(new Path(regionDir, REGION_INFO_FILE));
    try {
      return HRegionInfo.parseFrom(in);
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
    FSDataOutputStream out = FSUtils.create(fs, regionInfoFile, perms);
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
    byte[] content = getRegionInfoFileContent(regionInfo);
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
      LOG.warn(REGION_INFO_FILE + " file not found for region: " + regionInfo.getEncodedName());
    }

    // Write HRI to a file in case we need to recover .META.
    writeRegionInfoOnFilesystem(content, true);
  }

  /**
   * Write out an info file under the region directory. Useful recovering mangled regions.
   * @param useTempDir indicate whether or not using the region .tmp dir for a safer file creation.
   */
  private void writeRegionInfoOnFilesystem(boolean useTempDir) throws IOException {
    byte[] content = getRegionInfoFileContent(regionInfo);
    writeRegionInfoOnFilesystem(content, useTempDir);
  }

  /**
   * Write out an info file under the region directory. Useful recovering mangled regions.
   * @param regionInfoContent serialized version of the {@link HRegionInfo}
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

      // Write HRI to a file in case we need to recover .META.
      writeRegionInfoFileContent(conf, fs, tmpPath, regionInfoContent);

      // Move the created file to the original path
      if (!fs.rename(tmpPath, regionInfoFile)) {
        throw new IOException("Unable to rename " + tmpPath + " to " + regionInfoFile);
      }
    } else {
      // Write HRI to a file in case we need to recover .META.
      writeRegionInfoFileContent(conf, fs, regionInfoFile, regionInfoContent);
    }
  }

  /**
   * Create a new Region on file-system.
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} from which to add the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link HRegionInfo} for region to be added
   * @throws IOException if the region creation fails due to a FileSystem exception.
   */
  public static HRegionFileSystem createRegionOnFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regionDir = regionFs.getRegionDir();

    if (fs.exists(regionDir)) {
      LOG.warn("Trying to create a region that already exists on disk: " + regionDir);
      throw new IOException("The specified region already exists on disk: " + regionDir);
    }

    // Create the region directory
    if (!fs.mkdirs(regionFs.getRegionDir())) {
      LOG.warn("Unable to create the region directory: " + regionDir);
      throw new IOException("Unable to create region directory: " + regionDir);
    }

    // Write HRI to a file in case we need to recover .META.
    regionFs.writeRegionInfoOnFilesystem(false);
    return regionFs;
  }

  /**
   * Open Region from file-system.
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} from which to add the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link HRegionInfo} for region to be added
   * @param readOnly True if you don't want to edit the region data
   * @throws IOException if the region creation fails due to a FileSystem exception.
   */
  public static HRegionFileSystem openRegionFromFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo, boolean readOnly)
      throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regionDir = regionFs.getRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("Trying to open a region that do not exists on disk: " + regionDir);
      throw new IOException("The specified region do not exists on disk: " + regionDir);
    }

    if (readOnly) {
      // Cleanup temporary directories
      regionFs.cleanupTempDir();
      regionFs.cleanupSplitsDir();
      regionFs.cleanupMergesDir();

      // if it doesn't exists, Write HRI to a file, in case we need to recover .META.
      regionFs.checkRegionInfoOnFilesystem();
    }

    return regionFs;
  }

  /**
   * Remove the region from the table directory, archiving the region's hfiles.
   * @param conf the {@link Configuration} to use
   * @param fs {@link FileSystem} from which to remove the region
   * @param tableDir {@link Path} to where the table is being stored
   * @param regionInfo {@link HRegionInfo} for region to be deleted
   * @throws IOException if the request cannot be completed
   */
  public static void deleteRegionFromFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
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
}
