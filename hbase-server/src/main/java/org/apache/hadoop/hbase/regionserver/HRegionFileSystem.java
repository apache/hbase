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
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.HFileLink;
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
  public Path getTempDir() {
    return new Path(getRegionDir(), REGION_TEMP_DIR);
  }

  /**
   * Clean up any temp detritus that may have been left around from previous operation attempts.
   */
  public void cleanupTempDir() throws IOException {
    FSUtils.deleteDirectory(fs, getTempDir());
  }

  // ===========================================================================
  //  Splits Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during split operations */
  public Path getSplitsDir() {
    return new Path(getRegionDir(), REGION_SPLITS_DIR);
  }

  /**
   * Clean up any split detritus that may have been left around from previous split attempts.
   */
  public void cleanupSplitsDir() throws IOException {
    FSUtils.deleteDirectory(fs, getSplitsDir());
  }

  // ===========================================================================
  //  Merge Helpers
  // ===========================================================================
  /** @return {@link Path} to the temp directory used during merge operations */
  public Path getMergesDir() {
    return new Path(getRegionDir(), REGION_MERGES_DIR);
  }

  /**
   * Clean up any merge detritus that may have been left around from previous merge attempts.
   */
  public void cleanupMergesDir() throws IOException {
    FSUtils.deleteDirectory(fs, getMergesDir());
  }

  // ===========================================================================
  //  Create/Open/Delete Helpers
  // ===========================================================================
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
   * @throws IOException if the region creation fails due to a FileSystem exception.
   */
  public static HRegionFileSystem openRegionFromFileSystem(final Configuration conf,
      final FileSystem fs, final Path tableDir, final HRegionInfo regionInfo) throws IOException {
    HRegionFileSystem regionFs = new HRegionFileSystem(conf, fs, tableDir, regionInfo);
    Path regionDir = regionFs.getRegionDir();

    if (!fs.exists(regionDir)) {
      LOG.warn("Trying to open a region that do not exists on disk: " + regionDir);
      throw new IOException("The specified region do not exists on disk: " + regionDir);
    }

    // Cleanup temporary directories
    regionFs.cleanupTempDir();
    regionFs.cleanupSplitsDir();
    regionFs.cleanupMergesDir();
    // if it doesn't exists, Write HRI to a file, in case we need to recover .META.
    regionFs.checkRegionInfoOnFilesystem();
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