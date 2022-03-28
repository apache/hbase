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

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.CRC32;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.StoreFileTrackerProtos.StoreFileList;

/**
 * To fully avoid listing, here we use two files for tracking. When loading, we will try to read
 * both the two files, if only one exists, we will trust this one, if both exist, we will compare
 * the timestamp to see which one is newer and trust that one. And we will record in memory that
 * which one is trusted by us, and when we need to update the store file list, we will write to the
 * other file.
 * <p/>
 * So in this way, we could avoid listing when we want to load the store file list file.
 * <p/>
 * To prevent loading partial file, we use the first 4 bytes as file length, and also append a 4
 * bytes crc32 checksum at the end. This is because the protobuf message parser sometimes can return
 * without error on partial bytes if you stop at some special points, but the return message will
 * have incorrect field value. We should try our best to prevent this happens because loading an
 * incorrect store file list file usually leads to data loss.
 */
@InterfaceAudience.Private
class StoreFileListFile {

  private static final Logger LOG = LoggerFactory.getLogger(StoreFileListFile.class);

  static final String TRACK_FILE_DIR = ".filelist";

  private static final String TRACK_FILE = "f1";

  private static final String TRACK_FILE_ROTATE = "f2";

  // 16 MB, which is big enough for a tracker file
  private static final int MAX_FILE_SIZE = 16 * 1024 * 1024;

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
    byte[] data;
    int expectedChecksum;
    try (FSDataInputStream in = fs.open(path)) {
      int length = in.readInt();
      if (length <= 0 || length > MAX_FILE_SIZE) {
        throw new IOException("Invalid file length " + length +
          ", either less than 0 or greater then max allowed size " + MAX_FILE_SIZE);
      }
      data = new byte[length];
      in.readFully(data);
      expectedChecksum = in.readInt();
    }
    CRC32 crc32 = new CRC32();
    crc32.update(data);
    int calculatedChecksum = (int) crc32.getValue();
    if (expectedChecksum != calculatedChecksum) {
      throw new IOException(
        "Checksum mismatch, expected " + expectedChecksum + ", actual " + calculatedChecksum);
    }
    return StoreFileList.parseFrom(data);
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
      } catch (FileNotFoundException | EOFException e) {
        // this is normal case, so use info and do not log stacktrace
        LOG.info("Failed to load track file {}: {}", trackFiles[i], e.toString());
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
    long timestamp = Math.max(prevTimestamp + 1, EnvironmentEdgeManager.currentTime());
    byte[] actualData = builder.setTimestamp(timestamp).build().toByteArray();
    CRC32 crc32 = new CRC32();
    crc32.update(actualData);
    int checksum = (int) crc32.getValue();
    // 4 bytes length at the beginning, plus 4 bytes checksum
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    try (FSDataOutputStream out = fs.create(trackFiles[nextTrackFile], true)) {
      out.writeInt(actualData.length);
      out.write(actualData);
      out.writeInt(checksum);
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
