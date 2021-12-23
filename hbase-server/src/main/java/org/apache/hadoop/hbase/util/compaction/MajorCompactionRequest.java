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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileInfo;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
class MajorCompactionRequest {

  private static final Logger LOG = LoggerFactory.getLogger(MajorCompactionRequest.class);

  protected final Connection connection;
  protected final RegionInfo region;
  private Set<String> stores;

  MajorCompactionRequest(Connection connection, RegionInfo region) {
    this.connection = connection;
    this.region = region;
  }

  MajorCompactionRequest(Connection connection, RegionInfo region,
      Set<String> stores) {
    this(connection, region);
    this.stores = stores;
  }

  static Optional<MajorCompactionRequest> newRequest(Connection connection, RegionInfo info,
      Set<String> stores, long timestamp) throws IOException {
    MajorCompactionRequest request =
        new MajorCompactionRequest(connection, info, stores);
    return request.createRequest(connection, stores, timestamp);
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

  Optional<MajorCompactionRequest> createRequest(Connection connection,
      Set<String> stores, long timestamp) throws IOException {
    Set<String> familiesToCompact = getStoresRequiringCompaction(stores, timestamp);
    MajorCompactionRequest request = null;
    if (!familiesToCompact.isEmpty()) {
      request = new MajorCompactionRequest(connection, region, familiesToCompact);
    }
    return Optional.ofNullable(request);
  }

  Set<String> getStoresRequiringCompaction(Set<String> requestedStores, long timestamp)
      throws IOException {
    HRegionFileSystem fileSystem = getFileSystem();
    Set<String> familiesToCompact = Sets.newHashSet();
    for (String family : requestedStores) {
      if (shouldCFBeCompacted(fileSystem, family, timestamp)) {
        familiesToCompact.add(family);
      }
    }
    return familiesToCompact;
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

  List<Path> getReferenceFilePaths(FileSystem fileSystem, Path familyDir)
      throws IOException {
    return FSUtils.getReferenceFilePaths(fileSystem, familyDir);
  }

  HRegionFileSystem getFileSystem() throws IOException {
    try (Admin admin = connection.getAdmin()) {
      return HRegionFileSystem.openRegionFromFileSystem(admin.getConfiguration(),
        CommonFSUtils.getCurrentFileSystem(admin.getConfiguration()),
        CommonFSUtils.getTableDir(CommonFSUtils.getRootDir(admin.getConfiguration()),
          region.getTable()), region, true);
    }
  }

  @Override
  public String toString() {
    return "region: " + region.getEncodedName() + " store(s): " + stores;
  }
}
