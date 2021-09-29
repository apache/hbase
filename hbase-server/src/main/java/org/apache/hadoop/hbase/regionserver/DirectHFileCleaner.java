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
import org.apache.hadoop.hbase.regionserver.compactions.DirectStoreCompactor;
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
 * This Chore, every time it runs, will clear the unsused  direct HFiles in the data
 * folder that are deletable for each HFile cleaner in the chain.
 */
@InterfaceAudience.Private public class DirectHFileCleaner extends ScheduledChore {
  private static final Logger LOG = LoggerFactory.getLogger(DirectHFileCleaner.class);
  public static final String DIRECT_HFILE_CLEANER_ENABLED =
      "hbase.region.direct.hfilecleaner.enabled";
  public static final boolean DEFAULT_DIRECT_HFILE_CLEANER_ENABLED = false;
  public static final String DIRECT_HFILE_CLEANER_TTL =
      "hbase.region.direct.hfilecleaner.ttl";
  public static final long DEFAULT_DIRECT_HFILE_CLEANER_TTL = 1000 * 60 * 60 * 12; //12h
  public static final String DIRECT_HFILE_CLEANER_DELAY =
      "hbase.region.direct.hfilecleaner.delay";
  public static final int DEFAULT_DIRECT_HFILE_CLEANER_DELAY = 1000 * 60 * 60 * 2; //2h
  public static final String DIRECT_HFILE_CLEANER_DELAY_JITTER =
      "hbase.region.direct.hfilecleaner.delay.jitter";
  public static final double DEFAULT_DIRECT_HFILE_CLEANER_DELAY_JITTER = 0.25D;
  public static final String DIRECT_HFILE_CLEANER_PERIOD =
      "hbase.region.direct.hfilecleaner.period";
  public static final int DEFAULT_DIRECT_HFILE_CLEANER_PERIOD = 1000 * 60 * 60 * 6; //6h

  private HRegionServer regionServer;
  private final AtomicBoolean enabled = new AtomicBoolean(true);
  private AtomicLong deletedFiles = new AtomicLong();
  private long ttl;

  public DirectHFileCleaner(final int delay, final int period, final Stoppable stopper, Configuration conf,
      HRegionServer regionServer) {
    super("DirectHFileCleaner", stopper, period, delay);
    this.regionServer = regionServer;
    setEnabled(conf.getBoolean(DIRECT_HFILE_CLEANER_ENABLED, DEFAULT_DIRECT_HFILE_CLEANER_ENABLED));
    ttl = conf.getLong(DIRECT_HFILE_CLEANER_TTL, DEFAULT_DIRECT_HFILE_CLEANER_TTL);
  }

  public boolean setEnabled(final boolean enabled) {
    return this.enabled.getAndSet(enabled);
  }

  public boolean getEnabled() {
    return this.enabled.get();
  }

  @Override protected void chore() {
    if (getEnabled()) {
      for (HRegion region : regionServer.getRegions()) {
        for (HStore store : region.getStores()) {
          if (!(store.getStoreEngine() instanceof PersistedStoreEngine)) {
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

          fsStoreFiles.forEach(file -> cleanFileIfNeeded(file, store));

        }
      }

    } else {
      LOG.trace("Direct Hfile Cleaner chore disabled! Not cleaning.");
    }
  }

  private void cleanFileIfNeeded(FileStatus file, HStore store) {
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

    deleteFile(file, store);
  }

  private boolean isCompactingFile(FileStatus file, HStore store) {
    if (!(store.getStoreEngine().getCompactor() instanceof DirectStoreCompactor)){
      return true; //skip examining current compaction targets if the data is not available
    }
    DirectStoreCompactor dsc = (DirectStoreCompactor) store.getStoreEngine().getCompactor();
    return dsc.getCompactionPaths().contains(file.getPath());
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

  private void deleteFile(FileStatus file, HStore store) {
    Path filePath = file.getPath();
    LOG.debug("Removing {} from store", filePath);
    try {
      boolean success = store.getFileSystem().delete(filePath, false);
      if (!success) {
        LOG.warn("Attempted to delete:" + filePath
            + ", but couldn't. Attempt to delete on next pass.");
      }
    } catch (IOException e) {
      e = e instanceof RemoteException ?
          ((RemoteException)e).unwrapRemoteException() : e;
      LOG.warn("Error while deleting: " + filePath, e);
    }
  }

}
