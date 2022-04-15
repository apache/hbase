/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This Chore, every time it runs, will clear the unsused HFiles in the data
 * folder.
 */
@InterfaceAudience.Private
public class BrokenStoreFileCleaner extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(BrokenStoreFileCleaner.class);
  public static final String BROKEN_STOREFILE_CLEANER_ENABLED =
      "hbase.region.broken.storefilecleaner.enabled";
  public static final boolean DEFAULT_BROKEN_STOREFILE_CLEANER_ENABLED = false;
  public static final String BROKEN_STOREFILE_CLEANER_TTL =
      "hbase.region.broken.storefilecleaner.ttl";
  public static final long DEFAULT_BROKEN_STOREFILE_CLEANER_TTL = 1000 * 60 * 60 * 12; //12h
  public static final String BROKEN_STOREFILE_CLEANER_DELAY =
      "hbase.region.broken.storefilecleaner.delay";
  public static final int DEFAULT_BROKEN_STOREFILE_CLEANER_DELAY = 1000 * 60 * 60 * 2; //2h
  public static final String BROKEN_STOREFILE_CLEANER_DELAY_JITTER =
      "hbase.region.broken.storefilecleaner.delay.jitter";
  public static final double DEFAULT_BROKEN_STOREFILE_CLEANER_DELAY_JITTER = 0.25D;
  public static final String BROKEN_STOREFILE_CLEANER_PERIOD =
      "hbase.region.broken.storefilecleaner.period";
  public static final int DEFAULT_BROKEN_STOREFILE_CLEANER_PERIOD = 1000 * 60 * 60 * 6; //6h

  private HRegionServer regionServer;
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private long fileTtl;

  public BrokenStoreFileCleaner(final int delay, final int period, final Stoppable stopper,
    Configuration conf, HRegionServer regionServer) {
    super("BrokenStoreFileCleaner", stopper, period, delay);
    this.regionServer = regionServer;
    setEnabled(
      conf.getBoolean(BROKEN_STOREFILE_CLEANER_ENABLED, DEFAULT_BROKEN_STOREFILE_CLEANER_ENABLED));
    fileTtl = conf.getLong(BROKEN_STOREFILE_CLEANER_TTL, DEFAULT_BROKEN_STOREFILE_CLEANER_TTL);
  }

  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  public boolean getEnabled() {
    return this.enabled.get();
  }

  @Override
  public void chore() {
    if (getEnabled()) {
      long start = EnvironmentEdgeManager.currentTime();
      AtomicLong deletedFiles = new AtomicLong(0);
      AtomicLong failedDeletes = new AtomicLong(0);
      for (HRegion region : regionServer.getRegions()) {
        for (HStore store : region.getStores()) {
          //only do cleanup in stores not using tmp directories
          if (store.getStoreEngine().requireWritingToTmpDirFirst()) {
            continue;
          }
          Path storePath =
              new Path(region.getRegionFileSystem().getRegionDir(), store.getColumnFamilyName());

          try {
            List<FileStatus> fsStoreFiles =
              Arrays.asList(region.getRegionFileSystem().fs.listStatus(storePath));
            fsStoreFiles.forEach(
              file -> cleanFileIfNeeded(file, store, deletedFiles, failedDeletes));
          } catch (IOException e) {
            LOG.warn("Failed to list files in {}, cleanup is skipped there",storePath);
            continue;
          }
        }
      }
      LOG.debug(
        "BrokenStoreFileCleaner on {} run for: {}ms. It deleted {} files and tried but failed "
        + "to delete {}",
        regionServer.getServerName().getServerName(), EnvironmentEdgeManager.currentTime() - start,
        deletedFiles.get(), failedDeletes.get());
    } else {
      LOG.trace("Broken storefile Cleaner chore disabled! Not cleaning.");
    }
  }

  private void cleanFileIfNeeded(FileStatus file, HStore store,
    AtomicLong deletedFiles, AtomicLong failedDeletes) {
    if(file.isDirectory()){
      LOG.trace("This is a Directory {}, skip cleanup", file.getPath());
      return;
    }

    if(!validate(file.getPath())){
      LOG.trace("Invalid file {}, skip cleanup", file.getPath());
      return;
    }

    if(!isOldEnough(file)){
      LOG.trace("Fresh file {}, skip cleanup", file.getPath());
      return;
    }

    if(isActiveStorefile(file, store)){
      LOG.trace("Actively used storefile file {}, skip cleanup", file.getPath());
      return;
    }

    // Compacted files can still have readers and are cleaned by a separate chore, so they have to
    // be skipped here
    if(isCompactedFile(file, store)){
      LOG.trace("Cleanup is done by a different chore for file {}, skip cleanup", file.getPath());
      return;
    }

    if(isCompactionResultFile(file, store)){
      LOG.trace("The file is the result of an ongoing compaction {}, skip cleanup", file.getPath());
      return;
    }

    deleteFile(file, store, deletedFiles, failedDeletes);
  }

  private boolean isCompactionResultFile(FileStatus file, HStore store) {
    return store.getStoreFilesBeingWritten().contains(file.getPath());
  }

  // Compacted files can still have readers and are cleaned by a separate chore, so they have to
  // be skipped here
  private boolean isCompactedFile(FileStatus file, HStore store) {
    return store.getStoreEngine().getStoreFileManager().getCompactedfiles().stream()
      .anyMatch(sf -> sf.getPath().equals(file.getPath()));
  }

  private boolean isActiveStorefile(FileStatus file, HStore store) {
    return store.getStoreEngine().getStoreFileManager().getStorefiles().stream()
      .anyMatch(sf -> sf.getPath().equals(file.getPath()));
  }

  boolean validate(Path file) {
    if (HFileLink.isBackReferencesDir(file) || HFileLink.isBackReferencesDir(file.getParent())) {
      return true;
    }
    return StoreFileInfo.validateStoreFileName(file.getName());
  }

  boolean isOldEnough(FileStatus file){
    return file.getModificationTime() + fileTtl < EnvironmentEdgeManager.currentTime();
  }

  private void deleteFile(FileStatus file, HStore store, AtomicLong deletedFiles,
    AtomicLong failedDeletes) {
    Path filePath = file.getPath();
    LOG.debug("Removing {} from store", filePath);
    try {
      boolean success = store.getFileSystem().delete(filePath, false);
      if (!success) {
        failedDeletes.incrementAndGet();
        LOG.warn("Attempted to delete:" + filePath
            + ", but couldn't. Attempt to delete on next pass.");
      }
      else{
        deletedFiles.incrementAndGet();
      }
    } catch (IOException e) {
      e = e instanceof RemoteException ?
          ((RemoteException)e).unwrapRemoteException() : e;
      LOG.warn("Error while deleting: " + filePath, e);
    }
  }

}
