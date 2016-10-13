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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.fs.legacy.LegacyPathIdentifier;
import org.apache.hadoop.hbase.fs.legacy.LegacyRegionStorage;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;

import org.apache.hadoop.hbase.regionserver.*;

@InterfaceAudience.Private
public abstract class RegionStorage<IDENTIFIER extends StorageIdentifier> {
  private static Log LOG = LogFactory.getLog(RegionStorage.class);

  private final Configuration conf;
  private final HRegionInfo hri;
  private final FileSystem fs;
  private final IDENTIFIER rootContainer;

  protected RegionStorage(Configuration conf, FileSystem fs, IDENTIFIER rootContainer, HRegionInfo hri) {
    this.conf = conf;
    this.rootContainer = rootContainer;
    this.hri = hri;
    this.fs = fs;
  }

  /* Probably remove */ 
  public Configuration getConfiguration() { return conf; }
  /* TODO Definitely remove */
  public FileSystem getFileSystem() { return fs; }
  public IDENTIFIER getRootContainer() { return rootContainer; }

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
  // TODO refactor to checkRegionInfoInStorage or remove
  public abstract void checkRegionInfoOnFilesystem() throws IOException;
  /**
   * Get an opaque handle to the backing storage associated with this region.
   *
   */
  public abstract IDENTIFIER getRegionContainer();
  
  public abstract IDENTIFIER getTableContainer();

  public abstract IDENTIFIER getTempContainer();

  public HRegionInfo getRegionInfoForFS() { return hri; }

  /**
   * If region exists on the Storage
   * @return true, if region related artifacts (dirs, files) present on storage
   * @throws IOException
   */
  public abstract boolean exists() throws IOException;

  /**
   * Retrieve a referene to the backing storage associated with a particular family within this region.
   */
  public abstract IDENTIFIER getStoreContainer(final String familyName);
   
  public abstract IDENTIFIER getTempIdentifier();
  public abstract IDENTIFIER createStoreContainer(final String familyName) throws IOException;
  /**
   * Move the given identifier into given family and return a StoreFile object with properly
   * initialized info and reader objects.
   */
  public abstract StoreFile bulkLoadStoreFile(final String familyName, IDENTIFIER srcPath, long seqNum, final CacheConfig cacheConfig, final BloomType cfBloomType, final RegionCoprocessorHost coprocessorHost)
      throws IOException;

  public abstract void cleanupTempContainer() throws IOException;
  public abstract void cleanupSplitsContainer() throws IOException;
  public abstract void cleanupMergesContainer() throws IOException;
  public abstract void cleanupAnySplitDetritus() throws IOException;

  public abstract IDENTIFIER commitDaughterRegion(final HRegionInfo regionInfo)
      throws IOException;
  public abstract void commitMergedRegion(final HRegionInfo mergedRegionInfo) throws IOException;
  public abstract StoreFileInfo getStoreFileInfo(final String familyName, final String fileName)
      throws IOException;

  public abstract StoreFile commitStoreFile(final String familyName, final IDENTIFIER uncommitted, final CacheConfig cacheConf, final BloomType cfBloomType, final RegionCoprocessorHost coprocessorHost) throws IOException;
  public abstract void commitStoreFiles(final Map<byte[], List<StoreFile>> storeFiles) throws IOException;

  public abstract void removeStoreFiles(final String familyName, final Collection<StoreFile> storeFiles)
      throws IOException;

  public abstract boolean hasReferences(final String familyName) throws IOException;
  public abstract boolean hasReferences(final HTableDescriptor htd) throws IOException;

  public abstract IDENTIFIER getStoreFileStorageIdentifier(final String familyName, final String fileName);

  public abstract long getStoreFileLen(final StoreFile store) throws IOException;

  public abstract void logFileSystemState(final Log LOG) throws IOException;

  public abstract void createSplitsContainer() throws IOException;
  public abstract IDENTIFIER getSplitsContainer();
  public abstract IDENTIFIER getSplitsContainer(final HRegionInfo hri);

  public abstract IDENTIFIER getMergesContainer();
  public abstract void createMergesContainer() throws IOException;

  public abstract IDENTIFIER mergeStoreFile(final HRegionInfo mergedRegion, final String familyName,
      final StoreFile f, final IDENTIFIER mergedContainer)
      throws IOException;

  public abstract void cleanupMergedRegion(final HRegionInfo mergedRegion) throws IOException;

  public abstract IDENTIFIER splitStoreFile(final HRegionInfo hri, final String familyName,
      final StoreFile f, final byte[] splitRow, final boolean top, RegionSplitPolicy splitPolicy)
          throws IOException;

  public abstract void cleanupDaughterRegion(final HRegionInfo regionInfo) throws IOException;

  // ==========================================================================
  //  PUBLIC
  // ==========================================================================
  public static RegionStorage open(Configuration conf, StorageIdentifier regionContainer, boolean bootstrap) throws IOException {
    RegionStorage rs = getInstance(conf, FSUtils.getCurrentFileSystem(conf), new LegacyPathIdentifier(FSUtils.getRootDir(conf)),
        regionContainer);
    if (bootstrap) {
      rs.bootstrap();
    }
    return rs;
  }

  public static RegionStorage open(Configuration conf, HRegionInfo regionInfo, boolean bootstrap)
      throws IOException {
    return open(conf, FSUtils.getCurrentFileSystem(conf), new LegacyPathIdentifier(FSUtils.getRootDir(conf)),
        regionInfo, bootstrap);
  }

  /* TODO remove this method and have the RegionStorage implementation determine the root container */
  public static RegionStorage open(Configuration conf, FileSystem fs, StorageIdentifier rootContainer,
      HRegionInfo regionInfo, boolean bootstrap) throws IOException {

    if (rootContainer instanceof LegacyPathIdentifier) {
      // Cover both bases, the old way of setting default fs and the new.
      // We're supposed to run on 0.20 and 0.21 anyways.
      fs = ((LegacyPathIdentifier)rootContainer).path.getFileSystem(conf);
      FSUtils.setFsDefault(conf, new Path(fs.getUri()));
      // make sure the fs has the same conf
      fs.setConf(conf);
    } else {
      LOG.debug("skipping override of the default FS, since the root container is not a LegacyPathIdentifier.");
    }

    RegionStorage rfs = getInstance(conf, fs, rootContainer, regionInfo);
    if (bootstrap) {
      // TODO: are bootstrap and create two different things?
      // should switch to bootstrap & read-only
      // legacy region wants to recover the .regioninfo :(
      rfs.bootstrap();
    }
    return rfs;
  }

  public static void destroy(Configuration conf, HRegionInfo regionInfo) throws IOException {
    destroy(conf, FSUtils.getCurrentFileSystem(conf), new LegacyPathIdentifier(FSUtils.getRootDir(conf)), regionInfo);
  }

  public static void destroy(Configuration conf, FileSystem fs,
      StorageIdentifier rootContainer, HRegionInfo regionInfo) throws IOException {
    getInstance(conf, fs, rootContainer, regionInfo).destroy();
  }

  private static RegionStorage getInstance(Configuration conf, FileSystem fs,
      StorageIdentifier rootContainer, HRegionInfo regionInfo) throws IOException {
    String fsType = conf.get("hbase.fs.storage.type", "legacy").toLowerCase();
    switch (fsType) {
      case "legacy":
        return new LegacyRegionStorage(conf, fs, (LegacyPathIdentifier)rootContainer, regionInfo);
      default:
        throw new IOException("Invalid filesystem type " + fsType);
    }
  }

  private static RegionStorage getInstance(Configuration conf, FileSystem fs,
      StorageIdentifier rootContainer, StorageIdentifier regionContainer) throws IOException {
    String storageType = conf.get("hbase.storage.type", "legacy").toLowerCase();
    switch (storageType) {
      case "legacy":
        return new LegacyRegionStorage(conf, fs, (LegacyPathIdentifier)rootContainer, (LegacyPathIdentifier)regionContainer);
      default:
        throw new IOException("Invalid filesystem type " + storageType);
    }
  }
}
