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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.io.ByteStreams;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

/**
 * To fully avoid listing, here we use two files for tracking. When loading, we will try to read
 * both the two files, if only one exists, we will trust this one, if both exist, we will compare
 * the timestamp to see which one is newer and trust that one. And we will record in memory that
 * which one is trusted by us, and when we need to update the store file list, we will write to the
 * other file.
 * <p/>
 * So in this way, we could avoid listing when we want to load the store file list file.
 */
@InterfaceAudience.Private
class StoreFileListFile {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileListFile.class);

  private static final String TRACK_FILE_DIR = ".filelist";

  private static final String TRACK_FILE = "f1";

  private static final String TRACK_FILE_ROTATE = "f2";

  private final StoreContext ctx;

  private final Path trackFileDir;

  private final Path[] trackFiles = new Path[2];

  // this is used to make sure that we do not go backwards
  private long prevTimestamp = -1;

  private int nextTrackFile = -1;

  StoreFileListFile(StoreContext ctx) {
    this.ctx = ctx;
    trackFileDir = new Path(ctx.getFamilyStoreDirectoryPath(), TRACK_FILE_DIR);
    trackFiles[0] = new Path(trackFileDir, TRACK_FILE);
    trackFiles[1] = new Path(trackFileDir, TRACK_FILE_ROTATE);
  }

  private StoreFileList load(Path path) throws IOException {
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    byte[] bytes;
    try (FSDataInputStream in = fs.open(path)) {
      bytes = ByteStreams.toByteArray(in);
    }
    // Read all the bytes and then parse it, so we will only throw InvalidProtocolBufferException
    // here. This is very important for upper layer to determine whether this is the normal case,
    // where the file does not exist or is incomplete. If there is another type of exception, the
    // upper layer should throw it out instead of just ignoring it, otherwise it will lead to data
    // loss.
    return StoreFileList.parseFrom(bytes);
  }

  private int select(StoreFileList[] lists) {
    if (lists[0] == null) {
      return 1;
    }
    if (lists[1] == null) {
      return 0;
    }
    return lists[0].getTimestamp() >= lists[1].getTimestamp() ? 0 : 1;
  }

  StoreFileList load() throws IOException {
    StoreFileList[] lists = new StoreFileList[2];
    for (int i = 0; i < 2; i++) {
      try {
        lists[i] = load(trackFiles[i]);
      } catch (FileNotFoundException | InvalidProtocolBufferException e) {
        // this is normal case, so use info and do not log stacktrace
        LOG.info("Failed to load track file {}: {}", trackFiles[i], e);
      }
    }
    int winnerIndex = select(lists);
    if (lists[winnerIndex] != null) {
      nextTrackFile = 1 - winnerIndex;
      prevTimestamp = lists[winnerIndex].getTimestamp();
    } else {
      nextTrackFile = 0;
    }
    return lists[winnerIndex];
  }

  /**
   * We will set the timestamp in this method so just pass the builder in
   */
  void update(StoreFileList.Builder builder) throws IOException {
    if (nextTrackFile < 0) {
      // we need to call load first to load the prevTimestamp and also the next file
      load();
    }
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    long timestamp = Math.max(prevTimestamp + 1, EnvironmentEdgeManager.currentTime());
    try (FSDataOutputStream out = fs.create(trackFiles[nextTrackFile], true)) {
      builder.setTimestamp(timestamp).build().writeTo(out);
    }
    // record timestamp
    prevTimestamp = timestamp;
    // rotate the file
    nextTrackFile = 1 - nextTrackFile;
    try {
      fs.delete(trackFiles[nextTrackFile], false);
    } catch (IOException e) {
      // we will create new file with overwrite = true, so not a big deal here, only for speed up
      // loading as we do not need to read this file when loading(we will hit FileNotFoundException)
      LOG.debug("failed to delete old track file {}, not a big deal, just ignore", e);
    }
  }
}
