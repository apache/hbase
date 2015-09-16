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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

import com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class LegacyRegionFileSystem extends HRegionFileSystem {
  private static final Log LOG = LogFactory.getLog(LegacyRegionFileSystem.class);

  /** Temporary subdirectory of the region directory used for compaction output. */
  protected static final String REGION_TEMP_DIR = ".tmp";

  private final Configuration conf;
  private final HRegionInfo hri;
  private final Path regionDir;
  private final FileSystem fs;

  public LegacyRegionFileSystem(Configuration conf, HRegionInfo hri) {
    this.fs = FSUtils.getCurrentFileSystem(conf);
    this.hri = hri;
    this.conf = conf;
    this.regionDir = getRegionDir(getTableDir(getRootDir()));
  }

  // ==========================================================================
  //  ??? methods - do we still need this stuff in 2.0?
  // ==========================================================================
  @Override
  public void writeRecoveryCheckPoint() throws IOException {
    //checkRegionInfoOnFilesystem
  }

  @Override
  public void cleanup() throws IOException {
    /*
      // Remove temporary data left over from old regions
      status.setStatus("Cleaning up temporary data from old regions");
      fs.cleanupTempDir();

      status.setStatus("Cleaning up detritus from prior splits");
      // Get rid of any splits or merges that were lost in-progress.  Clean out
      // these directories here on open.  We may be opening a region that was
      // being split but we crashed in the middle of it all.
      fs.cleanupAnySplitDetritus();
      fs.cleanupMergesDir();
    */
  }

  // ==========================================================================
  //  PUBLIC methods - create/open/destroy
  // ==========================================================================
  @Override
  public void open(boolean rdonly) throws IOException {
  }

  @Override
  public void create() throws IOException {
  }

  @Override
  public void destroy() throws IOException {
  }

  // ==========================================================================
  //  PUBLIC methods - add/remove/list store files
  // ==========================================================================
  public Path createTempName(final String suffix) {
    return new Path(getTempDir(), generateUniqueName(suffix));
  }

  public Path commitStoreFile(final String familyName, final Path buildPath,
      final long seqNum, final boolean generateNewName) throws IOException {
    return null;
  }

  public void commitStoreFiles(final Map<byte[], List<StoreFile>> storeFiles)
      throws IOException {
  }

  public Path bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
      throws IOException {
    return null;
  }

  public void removeStoreFile(String familyName, Path filePath)
      throws IOException {
  }

  public void removeStoreFiles(String familyName, Collection<StoreFile> storeFiles)
      throws IOException {
  }

  public Path getStoreFilePath(final String familyName, final String fileName) {
    return null;
  }

  /**
   * Returns the store files available for the family.
   * This methods performs the filtering based on the valid store files.
   * @param familyName Column Family Name
   * @return a set of {@link StoreFileInfo} for the specified family.
   */
  public Collection<StoreFileInfo> getStoreFiles(final String familyName, final boolean validate)
      throws IOException {
    /*
    Path familyDir = getStoreDir(this.regionDir, familyName);
    FileStatus[] files = FSUtils.listStatus(this.fs, familyDir);
    if (files == null) {
      LOG.debug("No StoreFiles for: " + familyDir);
      return null;
    }

    ArrayList<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(files.length);
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
    */
    return null;
  }

  public StoreFileInfo getStoreFileInfo(final String familyName, final String fileName)
      throws IOException {
    return null;
  }

  /**
   * @return the set of families present on disk
   * @throws IOException
   */
  public Collection<String> getFamilies() throws IOException {
    FileStatus[] fds = FSUtils.listStatus(fs, this.regionDir, new FamilyDirFilter(fs));
    if (fds == null) return null;

    ArrayList<String> families = new ArrayList<String>(fds.length);
    for (FileStatus status: fds) {
      families.add(status.getPath().getName());
    }

    return families;
  }

  // ==========================================================================
  //  PROTECTED Internals
  // ==========================================================================

  /**
   * Generate a unique file name, used by createTempName() and commitStoreFile()
   * @param suffix extra information to append to the generated name
   * @return Unique file name
   */
  protected static String generateUniqueName(final String suffix) {
    String name = UUID.randomUUID().toString().replaceAll("-", "");
    if (suffix != null) name += suffix;
    return name;
  }

  public HRegionInfo getRegionInfo() {
    return hri;
  }

  // ==========================================================================
  //  PROTECTED FS-Layout Internals
  // ==========================================================================
  /** @return {@link Path} to the region's temp directory, used for file creations */
  Path getTempDir() {
    return new Path(getRegionDir(), REGION_TEMP_DIR);
  }

  protected Path getRootDir() {
    return new Path("/");
  }

  protected Path getTableDir(Path baseDir) {
    TableName table = getTable();
    return new Path(baseDir, new Path(table.getNamespaceAsString(), table.getQualifierAsString()));
  }

  protected Path getRegionDir(Path baseDir) {
    return new Path(baseDir, getRegionInfo().getEncodedName());
  }

  protected Path getStoreDir(Path baseDir, String familyName) {
    return new Path(baseDir, familyName);
  }

  protected Path getStoreFilePath(Path baseDir, String family, String name) {
    return getStoreFilePath(getStoreDir(baseDir, family), name);
  }

  protected Path getStoreFilePath(Path baseDir, String name) {
    return new Path(baseDir, name);
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getClass().getName(), regionDir);
  }

  /**
   * Filter for all dirs that are legal column family names.  This is generally used for colfam
   * dirs &lt;hbase.rootdir&gt;/&lt;tabledir&gt;/&lt;regiondir&gt;/&lt;colfamdir&gt;.
   */
  private static class FamilyDirFilter implements PathFilter {
    final FileSystem fs;

    public FamilyDirFilter(FileSystem fs) {
      this.fs = fs;
    }

    @Override
    public boolean accept(Path rd) {
      try {
        // throws IAE if invalid
        HColumnDescriptor.isLegalFamilyName(Bytes.toBytes(rd.getName()));
      } catch (IllegalArgumentException iae) {
        // path name is an invalid family name and thus is excluded.
        return false;
      }

      try {
        return fs.getFileStatus(rd).isDirectory();
      } catch (IOException ioe) {
        // Maybe the file was moved or the fs was disconnected.
        LOG.warn("Skipping file " + rd +" due to IOException", ioe);
        return false;
      }
    }
  }
}
