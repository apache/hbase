/*
 * Copyright 2013 The Apache Software Foundation
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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.InjectionEvent;
import org.apache.hadoop.hbase.util.InjectionHandler;

/**
 * Class derives from the Store and acts as a read only store by reading the
 * snapshot of a store. The constructor snapshots the store by creating
 * hardlinks of the files and initializes scanners reading them.
 * Provides support only for the read only operations.
 *
 */
public class ReadOnlyStore extends Store {
  /**
   * Constructor to create a ReadOnlyStore.
   * Set the "hbase.store.readonly.hardlinks.folder" property to the
   * appropriate location in case ReadOnlyStore is used.
   *
   * @param basedir qualified path under which the region directory lives;
   * generally the table subdirectory
   * @param family HColumnDescriptor for this column
   * @param fs file system object
   * @param conf configuration object
   * failed.  Can be null.
   * @param regionInfo
   * @throws IOException
   */
  public ReadOnlyStore(Path basedir, HRegionInfo regionInfo, HColumnDescriptor family,
      FileSystem fs, Configuration confParam, final boolean createNewHardlinks)
  throws IOException {
    super(basedir, family, fs, confParam, null, regionInfo);
    if (createNewHardlinks) {
      this.storefiles = sortAndClone(loadStoreFilesWithHardLinks());
    } else {
      this.storefiles = sortAndClone(loadStoreFiles(this.fs.listStatus(this.homedir)));
    }
  }

  private List<StoreFile> loadStoreFilesWithHardLinks() throws IOException{
    String hardLinkFolder = conf.get(HConstants.READ_ONLY_HARDLINKS_FOLDER,
        HConstants.READ_ONLY_HARDLINKS_FOLDER_DEFAULT);
    Path folder = new Path(hardLinkFolder);
    if (!fs.exists(folder)) {
      fs.mkdirs(folder);
    }
    FileStatus[] files, hls = null;

    // this variable being true means that the hardlink creation should be repeated
    boolean repeat = true;

    /*
     * There is a race condition here:
     * New files created(during compaction which finished exactly after
     * the list of store files is populated and before entering the
     * inner for loop). But if we ensure that all the hard-links get created
     * correctly, then it is sufficient.
     */
    while (repeat) {
      repeat = true;
      files = this.fs.listStatus(this.getHomedir());
      // Using injection handling to test the case when compaction happens
      // during snapshotting was in progress.
      InjectionHandler.processEvent(
          InjectionEvent.READONLYSTORE_COMPACTION_WHILE_SNAPSHOTTING,
          (Object[])null);
      LOG.debug("Home Dir : " + this.getHomedir().toString());
      hls = new FileStatus[files.length];
      int i = 0;
      try {
        for (FileStatus file : files) {
          Path p = file.getPath();
          Path hl = StoreFile.getRandomFilename(fs, folder);
          LOG.info("Creating Hardlink. src : " +  file.getPath().toString() + " dest : " + hl.toString());
          if (fs.hardLink(p, hl)) {
            hls[i++] = fs.getFileStatus(hl);
          } else {
            repeat = false;
            break;
          }
        }
        boolean success = repeat;
        if (success) {
          // Successfully created list of hardlinks
          for (int j = 0; j < i; j++) {
            LOG.info("Hardlink for file : " + files[j].getPath().toString() +
                " Hardlink : " +  hls[j].getPath().toString());
          }
          break;
        } else {
          // Remove the hardlinks that were created.
          LOG.info("Creating Hardlinks failed (Compaction?)." +
          "Cleaning up the existing hardlinks and trying again.");
          for (int j = 0; j < i; j++) {
            fs.delete(hls[j].getPath(), false);
          }
          repeat = true;
        }
      } catch (Exception e) {
        LOG.error("Exception while creating the hardlinks, deleting the" +
        " hardlinks already created");
        for (int j = 0; j < i; j++) {
          fs.delete(hls[j].getPath(), false);
        }
        throw new IOException(e);
      }
    }
    return loadStoreFiles(hls);
  }

  @Override
  protected long add(final KeyValue kv, long seqNum) {
    throw new UnsupportedOperationException("Cannot add KeyValues to ReadOnlyStore");
  }

  @Override
  protected long delete(final KeyValue kv, long seqNum) {
    throw new UnsupportedOperationException("Cannot delete KeyValues from ReadOnlyStore");
  }

  @Override
  public void bulkLoadHFile(String srcPathStr, long sequenceId) throws IOException {
    throw new UnsupportedOperationException("Cannot BulkLoad into ReadOnlyStore");
  }

  @Override
  public CompactionRequest requestCompaction() {
    throw new UnsupportedOperationException("Cannot request compaction on ReadOnlyStore");
  }

  @Override
  public void finishRequest(CompactionRequest cr) {
    throw new UnsupportedOperationException("Cannot perform compaction on ReadOnlyStore");
  }

  @Override
  public byte[] checkSplit() {
    throw new UnsupportedOperationException("Not supported on ReadOnlyStore");
  }

  @Override
  public long updateColumnValue(byte [] row, byte [] f,
      byte [] qualifier, long newValue, long seqNum) {
    throw new UnsupportedOperationException("Not supported on ReadOnlyStore");
  }

  @Override
  public StoreFlusher getStoreFlusher(long cacheFlushId) {
    throw new UnsupportedOperationException("Not supported on ReadOnlyStore");
  }
}
