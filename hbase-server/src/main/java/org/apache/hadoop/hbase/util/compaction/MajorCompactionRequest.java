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
package org.apache.hadoop.hbase.util.compaction;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
class MajorCompactionRequest {

  private static final Logger LOG = LoggerFactory.getLogger(MajorCompactionRequest.class);

  protected final Configuration configuration;
  protected final RegionInfo region;
  private Set<String> stores;

  MajorCompactionRequest(Configuration configuration, RegionInfo region) {
    this.configuration = configuration;
    this.region = region;
  }

  @VisibleForTesting
  MajorCompactionRequest(Configuration configuration, RegionInfo region,
      Set<String> stores) {
    this(configuration, region);
    this.stores = stores;
  }

  static Optional<MajorCompactionRequest> newRequest(Configuration configuration, RegionInfo info,
      Set<String> stores, long timestamp) throws IOException {
    MajorCompactionRequest request =
        new MajorCompactionRequest(configuration, info, stores);
    return request.createRequest(configuration, stores, timestamp);
  }

  RegionInfo getRegion() {
    return region;
  }

  Set<String> getStores() {
    return stores;
  }

  void setStores(Set<String> stores) {
    this.stores = stores;
  }

  @VisibleForTesting
  Optional<MajorCompactionRequest> createRequest(Configuration configuration,
      Set<String> stores, long timestamp) throws IOException {
    Set<String> familiesToCompact = getStoresRequiringCompaction(stores, timestamp);
    MajorCompactionRequest request = null;
    if (!familiesToCompact.isEmpty()) {
      request = new MajorCompactionRequest(configuration, region, familiesToCompact);
    }
    return Optional.ofNullable(request);
  }

  Set<String> getStoresRequiringCompaction(Set<String> requestedStores, long timestamp)
      throws IOException {
    try(Connection connection = getConnection(configuration)) {
      HRegionFileSystem fileSystem = getFileSystem(connection);
      Set<String> familiesToCompact = Sets.newHashSet();
      for (String family : requestedStores) {
        if (shouldCFBeCompacted(fileSystem, family, timestamp)) {
          familiesToCompact.add(family);
        }
      }
      return familiesToCompact;
    }
  }

  boolean shouldCFBeCompacted(HRegionFileSystem fileSystem, String family, long ts)
      throws IOException {

    // do we have any store files?
    Collection<StoreFileInfo> storeFiles = fileSystem.getStoreFiles(family);
    if (storeFiles == null) {
      LOG.info("Excluding store: " + family + " for compaction for region:  " + fileSystem
          .getRegionInfo().getEncodedName(), " has no store files");
      return false;
    }
    // check for reference files
    if (fileSystem.hasReferences(family) && familyHasReferenceFile(fileSystem, family, ts)) {
      LOG.info("Including store: " + family + " with: " + storeFiles.size()
          + " files for compaction for region: " + fileSystem.getRegionInfo().getEncodedName());
      return true;
    }
    // check store file timestamps
    boolean includeStore = this.shouldIncludeStore(fileSystem, family, storeFiles, ts);
    if (!includeStore) {
      LOG.info("Excluding store: " + family + " for compaction for region:  " + fileSystem
          .getRegionInfo().getEncodedName() + " already compacted");
    }
    return includeStore;
  }

  protected boolean shouldIncludeStore(HRegionFileSystem fileSystem, String family,
      Collection<StoreFileInfo> storeFiles, long ts) throws IOException {

    for (StoreFileInfo storeFile : storeFiles) {
      if (storeFile.getModificationTime() < ts) {
        LOG.info("Including store: " + family + " with: " + storeFiles.size()
            + " files for compaction for region: "
            + fileSystem.getRegionInfo().getEncodedName());
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  Connection getConnection(Configuration configuration) throws IOException {
    return ConnectionFactory.createConnection(configuration);
  }

  protected boolean familyHasReferenceFile(HRegionFileSystem fileSystem, String family, long ts)
      throws IOException {
    List<Path> referenceFiles =
        getReferenceFilePaths(fileSystem.getFileSystem(), fileSystem.getStoreDir(family));
    for (Path referenceFile : referenceFiles) {
      FileStatus status = fileSystem.getFileSystem().getFileLinkStatus(referenceFile);
      if (status.getModificationTime() < ts) {
        LOG.info("Including store: " + family + " for compaction for region:  " + fileSystem
            .getRegionInfo().getEncodedName() + " (reference store files)");
        return true;
      }
    }
    return false;

  }

  @VisibleForTesting
  List<Path> getReferenceFilePaths(FileSystem fileSystem, Path familyDir)
      throws IOException {
    return FSUtils.getReferenceFilePaths(fileSystem, familyDir);
  }

  @VisibleForTesting
  HRegionFileSystem getFileSystem(Connection connection) throws IOException {
    Admin admin = connection.getAdmin();
    return HRegionFileSystem.openRegionFromFileSystem(admin.getConfiguration(),
        FSUtils.getCurrentFileSystem(admin.getConfiguration()),
        FSUtils.getTableDir(FSUtils.getRootDir(admin.getConfiguration()), region.getTable()),
        region, true);
  }

  @Override
  public String toString() {
    return "region: " + region.getEncodedName() + " store(s): " + stores;
  }
}
