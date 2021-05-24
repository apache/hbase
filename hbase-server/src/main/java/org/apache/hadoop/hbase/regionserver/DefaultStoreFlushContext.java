/*
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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.monitoring.MonitoredTask;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.util.StringUtils;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Default implementation of StoreFlushContext, that assumes hfiles are flushed to temp files
 * first, so that upon commit phase, these hfiles need to be renamed into the final family dir.
 */
@InterfaceAudience.Private
public class DefaultStoreFlushContext extends StoreFlushContext {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultStoreFlushContext.class);

  private MemStoreSnapshot snapshot;
  private List<Path> tempFiles;
  private List<Path> committedFiles;
  private long cacheFlushCount;
  private long cacheFlushSize;
  private long outputFileSize;

  public DefaultStoreFlushContext(HStore store, Long cacheFlushSeqNum, FlushLifeCycleTracker tracker) {
    super(store, cacheFlushSeqNum, tracker);
  }

  /**
   * This is not thread safe. The caller should have a lock on the region or the store.
   * If necessary, the lock can be added with the patch provided in HBASE-10087
   */
  @Override
  public MemStoreSize prepare() {
    // passing the current sequence number of the wal - to allow bookkeeping in the memstore
    this.snapshot = store.memstore.snapshot();
    this.cacheFlushCount = snapshot.getCellsCount();
    this.cacheFlushSize = snapshot.getDataSize();
    committedFiles = new ArrayList<>(1);
    return snapshot.getMemStoreSize();
  }

  @Override
  public void flushCache(MonitoredTask status) throws IOException {
    RegionServerServices rsService = store.getHRegion().getRegionServerServices();
    ThroughputController throughputController =
      rsService == null ? null : rsService.getFlushThroughputController();
    tempFiles =
      store.flushCache(cacheFlushSeqNum, snapshot, status, throughputController, tracker);
  }

  @Override
  public boolean commit(MonitoredTask status) throws IOException {
    if (CollectionUtils.isEmpty(this.tempFiles)) {
      return false;
    }
    List<HStoreFile> storeFiles = new ArrayList<>(this.tempFiles.size());
    for (Path storeFilePath : tempFiles) {
      try {
        HStoreFile sf = store.commitFile(storeFilePath, cacheFlushSeqNum, status);
        outputFileSize += sf.getReader().length();
        storeFiles.add(sf);
      } catch (IOException ex) {
        LOG.error("Failed to commit store file {}", storeFilePath, ex);
        // Try to delete the files we have committed before.
        for (HStoreFile sf : storeFiles) {
          Path pathToDelete = sf.getPath();
          try {
            sf.deleteStoreFile();
          } catch (IOException deleteEx) {
            LOG.error(HBaseMarkers.FATAL, "Failed to delete store file we committed, "
              + "halting {}", pathToDelete, ex);
            Runtime.getRuntime().halt(1);
          }
        }
        throw new IOException("Failed to commit the flush", ex);
      }
    }

    for (HStoreFile sf : storeFiles) {
      if (store.getCoprocessorHost() != null) {
        store.getCoprocessorHost().postFlush(store, sf, tracker);
      }
      committedFiles.add(sf.getPath());
    }

    store.flushedCellsCount.addAndGet(cacheFlushCount);
    store.flushedCellsSize.addAndGet(cacheFlushSize);
    store.flushedOutputFileSize.addAndGet(outputFileSize);

    // Add new file to store files.  Clear snapshot too while we have the Store write lock.
    return store.updateStorefiles(storeFiles, snapshot.getId());
  }

  @Override
  public long getOutputFileSize() {
    return outputFileSize;
  }

  @Override
  public List<Path> getCommittedFiles() {
    return committedFiles;
  }

  /**
   * Similar to commit, but called in secondary region replicas for replaying the
   * flush cache from primary region. Adds the new files to the store, and drops the
   * snapshot depending on dropMemstoreSnapshot argument.
   * @param fileNames names of the flushed files
   * @param dropMemstoreSnapshot whether to drop the prepared memstore snapshot
   * @throws IOException
   */
  @Override
  public void replayFlush(List<String> fileNames, boolean dropMemstoreSnapshot)
    throws IOException {
    List<HStoreFile> storeFiles = new ArrayList<>(fileNames.size());
    for (String file : fileNames) {
      // open the file as a store file (hfile link, etc)
      StoreFileInfo storeFileInfo = store.getRegionFileSystem().getStoreFileInfo(store.getColumnFamilyName(), file);
      HStoreFile storeFile = store.createStoreFileAndReader(storeFileInfo);
      storeFiles.add(storeFile);
      store.storeSize.addAndGet(storeFile.getReader().length());
      store.totalUncompressedBytes
        .addAndGet(storeFile.getReader().getTotalUncompressedBytes());
      if (LOG.isInfoEnabled()) {
        LOG.info("Region: " + store.getRegionInfo().getEncodedName() +
          " added " + storeFile + ", entries=" + storeFile.getReader().getEntries() +
          ", sequenceid=" + storeFile.getReader().getSequenceID() + ", filesize="
          + StringUtils.TraditionalBinaryPrefix
          .long2String(storeFile.getReader().length(), "", 1));
      }
    }

    long snapshotId = -1; // -1 means do not drop
    if (dropMemstoreSnapshot && snapshot != null) {
      snapshotId = snapshot.getId();
      snapshot.close();
    }
    store.updateStorefiles(storeFiles, snapshotId);
  }

  /**
   * Abort the snapshot preparation. Drops the snapshot if any.
   * @throws IOException
   */
  @Override
  public void abort() throws IOException {
    if (snapshot != null) {
      //We need to close the snapshot when aborting, otherwise, the segment scanner
      //won't be closed. If we are using MSLAB, the chunk referenced by those scanners
      //can't be released, thus memory leak
      snapshot.close();
      store.updateStorefiles(Collections.emptyList(), snapshot.getId());
    }
  }
}
