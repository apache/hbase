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
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.legacy.LegacyRegionFileSystem;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;

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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.backup.HFileArchiver;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.io.Reference;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSHDFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;

@InterfaceAudience.Private
public abstract class RegionFileSystem {
  private static Log LOG = LogFactory.getLog(RegionFileSystem.class);

  private final Configuration conf;
  private final HRegionInfo hri;
  private final FileSystem fs;
  private final Path rootDir;

  protected RegionFileSystem(Configuration conf, FileSystem fs, Path rootDir, HRegionInfo hri) {
    this.conf = conf;
    this.rootDir = rootDir;
    this.hri = hri;
    this.fs = fs;
  }

  public Configuration getConfiguration() { return conf; }
  public FileSystem getFileSystem() { return fs; }
  public Path getRootDir() { return rootDir; }

  public HRegionInfo getRegionInfo() { return hri; }
  public TableName getTable() { return getRegionInfo().getTable(); }

  // ==========================================================================
  //  PUBLIC Interfaces - Visitors
  // ==========================================================================
  public interface StoreFileVisitor {
    void storeFile(HRegionInfo region, String family, StoreFileInfo storeFile)
       throws IOException;
  }

  public void visitStoreFiles(final StoreFileVisitor visitor) throws IOException {
    for (String familyName: getFamilies()) {
      for (StoreFileInfo storeFile: getStoreFiles(familyName)) {
        visitor.storeFile(getRegionInfo(), familyName, storeFile);
      }
    }
  }

  // ==========================================================================
  //  PUBLIC Methods - Families Related
  // ==========================================================================

  /**
   * @return the set of families present on disk
   * @throws IOException
   */
  public abstract Collection<String> getFamilies() throws IOException;

  public void deleteFamily(byte[] familyName, boolean hasMob) throws IOException {
    deleteFamily(Bytes.toString(familyName), hasMob);
  }

  public abstract void deleteFamily(String familyName, boolean hasMob) throws IOException;

  // ==========================================================================
  //  PUBLIC Methods - Store Files related
  // ==========================================================================

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

  // ==========================================================================
  //  PUBLIC bootstrap
  // ==========================================================================
  protected abstract void bootstrap() throws IOException;
  protected abstract void destroy() throws IOException;

  // ==========================================================================
  //  NOOOOO
  // ==========================================================================
  public abstract void checkRegionInfoOnFilesystem() throws IOException;
  public abstract Path getRegionDir();
  public abstract Path getTableDir();

  public abstract Path getTempDir();

  public HRegionInfo getRegionInfoForFS() { return hri; }

  public abstract Path getStoreDir(final String familyName);
  public abstract Path createTempName();
  public abstract Path createStoreDir(final String familyName) throws IOException;
  public abstract Path bulkLoadStoreFile(final String familyName, Path srcPath, long seqNum)
      throws IOException;

  public abstract void cleanupTempDir() throws IOException;
  public abstract void cleanupSplitsDir() throws IOException;
  public abstract void cleanupMergesDir() throws IOException;
  public abstract void cleanupAnySplitDetritus() throws IOException;

  public abstract Path commitDaughterRegion(final HRegionInfo regionInfo)
      throws IOException;
  public abstract void commitMergedRegion(final HRegionInfo mergedRegionInfo) throws IOException;
  public abstract StoreFileInfo getStoreFileInfo(final String familyName, final String fileName)
      throws IOException;

  public abstract Path commitStoreFile(final String familyName, final Path buildPath) throws IOException;
  public abstract void commitStoreFiles(final Map<byte[], List<StoreFile>> storeFiles) throws IOException;

  public abstract void removeStoreFile(final String familyName, final Path filePath)
      throws IOException;
  public abstract void removeStoreFiles(final String familyName, final Collection<StoreFile> storeFiles)
      throws IOException;

  public abstract boolean hasReferences(final String familyName) throws IOException;
  public abstract boolean hasReferences(final HTableDescriptor htd) throws IOException;

  public abstract Path getStoreFilePath(final String familyName, final String fileName);

  public abstract void logFileSystemState(final Log LOG) throws IOException;

  public abstract void createSplitsDir() throws IOException;
  public abstract Path getSplitsDir();
  public abstract Path getSplitsDir(final HRegionInfo hri);

  public abstract Path getMergesDir();
  public abstract void createMergesDir() throws IOException;

  public abstract Path mergeStoreFile(final HRegionInfo mergedRegion, final String familyName,
      final StoreFile f, final Path mergedDir)
      throws IOException;

  public abstract void cleanupMergedRegion(final HRegionInfo mergedRegion) throws IOException;

  public abstract Path splitStoreFile(final HRegionInfo hri, final String familyName,
      final StoreFile f, final byte[] splitRow, final boolean top, RegionSplitPolicy splitPolicy)
          throws IOException;

  public abstract void cleanupDaughterRegion(final HRegionInfo regionInfo) throws IOException;

  public static HRegionInfo loadRegionInfoFileContent(FileSystem fs, Path regionDir)
      throws IOException {
    FSDataInputStream in = fs.open(new Path(regionDir, ".regioninfo"));
    try {
      return HRegionInfo.parseFrom(in);
    } finally {
      in.close();
    }
  }

  // ==========================================================================
  //  PUBLIC
  // ==========================================================================
  public static RegionFileSystem open(Configuration conf, HRegionInfo regionInfo, boolean bootstrap)
      throws IOException {
    return open(conf, FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf),
        regionInfo, bootstrap);
  }

  public static RegionFileSystem open(Configuration conf, FileSystem fs, Path rootDir,
      HRegionInfo regionInfo, boolean bootstrap) throws IOException {
    // Cover both bases, the old way of setting default fs and the new.
    // We're supposed to run on 0.20 and 0.21 anyways.
    fs = rootDir.getFileSystem(conf);
    FSUtils.setFsDefault(conf, new Path(fs.getUri()));
    // make sure the fs has the same conf
    fs.setConf(conf);

    RegionFileSystem rfs = getInstance(conf, fs, rootDir, regionInfo);
    if (bootstrap) {
      // TODO: are bootstrap and create two different things?
      // should switch to bootstrap & read-only
      // legacy region wants to recover the .regioninfo :(
      rfs.bootstrap();
    }
    return rfs;
  }

  public static void destroy(Configuration conf, HRegionInfo regionInfo) throws IOException {
    destroy(conf, FSUtils.getCurrentFileSystem(conf), FSUtils.getRootDir(conf), regionInfo);
  }

  public static void destroy(Configuration conf, FileSystem fs,
      Path rootDir, HRegionInfo regionInfo) throws IOException {
    getInstance(conf, fs, rootDir, regionInfo).destroy();
  }

  private static RegionFileSystem getInstance(Configuration conf, FileSystem fs,
      Path rootDir, HRegionInfo regionInfo) throws IOException {
    String fsType = conf.get("hbase.fs.layout.type", "legacy").toLowerCase();
    switch (fsType) {
      case "legacy":
        return new LegacyRegionFileSystem(conf, fs, rootDir, regionInfo);
      default:
        throw new IOException("Invalid filesystem type " + fsType);
    }
  }
}
