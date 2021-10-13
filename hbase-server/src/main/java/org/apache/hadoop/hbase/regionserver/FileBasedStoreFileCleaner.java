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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Chore, every time it runs, will clear the unsused HFiles in the data
 * folder.
 */
@InterfaceAudience.Private public class FileBasedStoreFileCleaner extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedStoreFileCleaner.class);
  public static final String FILEBASED_STOREFILE_CLEANER_ENABLED =
      "hbase.region.filebased.storefilecleaner.enabled";
  public static final boolean DEFAULT_FILEBASED_STOREFILE_CLEANER_ENABLED = false;
  public static final String FILEBASED_STOREFILE_CLEANER_TTL =
      "hbase.region.filebased.storefilecleaner.ttl";
  public static final long DEFAULT_FILEBASED_STOREFILE_CLEANER_TTL = 1000 * 60 * 60 * 12; //12h
  public static final String FILEBASED_STOREFILE_CLEANER_DELAY =
      "hbase.region.filebased.storefilecleaner.delay";
  public static final int DEFAULT_FILEBASED_STOREFILE_CLEANER_DELAY = 1000 * 60 * 60 * 2; //2h
  public static final String FILEBASED_STOREFILE_CLEANER_DELAY_JITTER =
      "hbase.region.filebased.storefilecleaner.delay.jitter";
  public static final double DEFAULT_FILEBASED_STOREFILE_CLEANER_DELAY_JITTER = 0.25D;
  public static final String FILEBASED_STOREFILE_CLEANER_PERIOD =
      "hbase.region.filebased.storefilecleaner.period";
  public static final int DEFAULT_FILEBASED_STOREFILE_CLEANER_PERIOD = 1000 * 60 * 60 * 6; //6h

  private HRegionServer regionServer;
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private long ttl;

  public FileBasedStoreFileCleaner(final int delay, final int period, final Stoppable stopper, Configuration conf,
      HRegionServer regionServer) {
    super("FileBasedStoreFileCleaner", stopper, period, delay);
    this.regionServer = regionServer;
    setEnabled(conf.getBoolean(FILEBASED_STOREFILE_CLEANER_ENABLED, DEFAULT_FILEBASED_STOREFILE_CLEANER_ENABLED));
    ttl = conf.getLong(FILEBASED_STOREFILE_CLEANER_TTL, DEFAULT_FILEBASED_STOREFILE_CLEANER_TTL);
  }

  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  public boolean getEnabled() {
    return this.enabled.get();
  }

  @InterfaceAudience.Private
  @Override public void chore() {
    if (getEnabled()) {
      long start = EnvironmentEdgeManager.currentTime();
      AtomicLong deletedFiles = new AtomicLong(0);
      AtomicLong failedDeletes = new AtomicLong(0);
      for (HRegion region : regionServer.getRegions()) {
        for (HStore store : region.getStores()) {
          //only clean do cleanup in store using file based storefile tracking
          if (store.getStoreEngine().requireWritingToTmpDirFirst()) {
            continue;
          }
          Path storePath =
              new Path(region.getRegionFileSystem().getRegionDir(), store.getColumnFamilyName());
          List<FileStatus> fsStoreFiles = new ArrayList<>();
          try {
            fsStoreFiles = Arrays.asList(region.getRegionFileSystem().fs.listStatus(storePath));
          } catch (IOException e) {
            LOG.warn("Failed to list files in {}, cleanup is skipped there",storePath);
            continue;
          }

          fsStoreFiles.forEach(file -> cleanFileIfNeeded(file, store, deletedFiles, failedDeletes));

        }
      }
      logCleanupMetrics(EnvironmentEdgeManager.currentTime() - start, deletedFiles.get(), failedDeletes.get());
    } else {
      LOG.trace("File based storefile Cleaner chore disabled! Not cleaning.");
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

    if(isCompactedFile(file, store)){
      LOG.trace("Cleanup is done by a different chore for file {}, skip cleanup", file.getPath());
      return;
    }

    if(isCompactingFile(file, store)){
      LOG.trace("The file is the result of an ongoing compaction {}, skip cleanup", file.getPath());
      return;
    }

    deleteFile(file, store, deletedFiles, failedDeletes);
  }

  private boolean isCompactingFile(FileStatus file, HStore store) {
    return store.getStoreEngine().getCompactor().getCompactionTargets().contains(file.getPath());
  }

  private boolean isCompactedFile(FileStatus file, HStore store) {
    return store.getStoreEngine().getStoreFileManager().getCompactedfiles().stream().anyMatch(sf -> sf.getPath().equals(file.getPath()));
  }

  private boolean isActiveStorefile(FileStatus file, HStore store) {
    return store.getStoreEngine().getStoreFileManager().getStorefiles().stream().anyMatch(sf -> sf.getPath().equals(file.getPath()));
  }

  boolean validate(Path file) {
    if (HFileLink.isBackReferencesDir(file) || HFileLink.isBackReferencesDir(file.getParent())) {
      return true;
    }
    return StoreFileInfo.validateStoreFileName(file.getName());
  }

  boolean isOldEnough(FileStatus file){
    return file.getModificationTime() + ttl < System.currentTimeMillis();
  }

  private void deleteFile(FileStatus file, HStore store, AtomicLong deletedFiles, AtomicLong failedDeletes) {
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

  private void logCleanupMetrics(long runtime, long deletedFiles, long failedDeletes){
    regionServer.getMetrics()
      .updateFileBasedStoreFileCleanerTimer(runtime);
    regionServer.getMetrics().incrementFileBasedStoreFileCleanerDeletes(deletedFiles);
    regionServer.getMetrics().incrementFileBasedStoreFileCleanerFailedDeletes(failedDeletes);
    regionServer.getMetrics().incrementFileBasedStoreFileCleanerRuns();

    regionServer.reportFileBasedStoreFileCleanerUsage(runtime, deletedFiles, failedDeletes, true);
  }

}
