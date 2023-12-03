/*
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableCollection;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;

/**
 * Implementation of {@link StoreFileManager} for {@link DualFileStoreEngine}. Not thread-safe.
 */
@InterfaceAudience.Private
class DualFileStoreFileManager extends DefaultStoreFileManager {
  /**
   * List of store files that include the latest put cells inside this store. This is an
   * immutable list that is atomically replaced when its contents change.
   */
  private volatile ImmutableList<HStoreFile> latestVersionStoreFiles = ImmutableList.of();

  public DualFileStoreFileManager(CellComparator cellComparator,
    Comparator<HStoreFile> storeFileComparator, Configuration conf,
    CompactionConfiguration comConf) {
    super(cellComparator, storeFileComparator, conf, comConf);
  }

  private List <HStoreFile> extractHasLatestVersionFiles(Collection<HStoreFile> storeFiles)
    throws IOException {
    List <HStoreFile> hasLatestVersionFiles = new ArrayList<>(storeFiles.size());
    for (HStoreFile file : storeFiles) {
      file.initReader();
      if (file.hasLatestVersion()) {
        hasLatestVersionFiles.add(file);
      }
    }
    return hasLatestVersionFiles;
  }

  @Override
  public void loadFiles(List<HStoreFile> storeFiles) throws IOException {
    super.loadFiles(storeFiles);
    this.latestVersionStoreFiles = ImmutableList.sortedCopyOf(getStoreFileComparator(),
      extractHasLatestVersionFiles(storeFiles));
  }

  @Override
  public void insertNewFiles(Collection<HStoreFile> sfs) throws IOException {
    super.insertNewFiles(sfs);
    this.latestVersionStoreFiles =
      ImmutableList.sortedCopyOf(getStoreFileComparator(),
        Iterables.concat(this.latestVersionStoreFiles, extractHasLatestVersionFiles(sfs)));
  }

  @Override
  public ImmutableCollection<HStoreFile> clearFiles() {
    latestVersionStoreFiles = ImmutableList.of();
    return super.clearFiles();
  }

  @Override
  public void addCompactionResults(Collection<HStoreFile> newCompactedFiles,
    Collection<HStoreFile> results) throws IOException {
    Collection<HStoreFile>  newFilesHasLatestVersion= extractHasLatestVersionFiles(results);
    this.latestVersionStoreFiles = ImmutableList.sortedCopyOf(getStoreFileComparator(), Iterables
      .concat(Iterables.filter(latestVersionStoreFiles,
        sf -> !newCompactedFiles.contains(sf)), newFilesHasLatestVersion));
    super.addCompactionResults(newCompactedFiles, results);
  }

  @Override
  public Collection<HStoreFile> getFilesForScan(byte[] startRow, boolean includeStartRow,
    byte[] stopRow, boolean includeStopRow, boolean onlyLatestVersion) {
    if (onlyLatestVersion) {
      return latestVersionStoreFiles;
    }
    return super.getFilesForScan(startRow, includeStartRow, stopRow, includeStopRow, false);
  }
}
