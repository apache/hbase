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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.regionserver.StoreContext;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;

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

  private static final String TRACK_FILE_PREFIX = "f1";

  private static final String TRACK_FILE_ROTATE_PREFIX = "f2";

  private static final char TRACK_FILE_SEPARATOR = '.';

  private static final Pattern TRACK_FILE_PATTERN = Pattern.compile("^f(1|2)\\.\\d+$");

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

  // file sequence id to path
  private NavigableMap<Long, List<Path>> listFiles() throws IOException {
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    FileStatus[] statuses;
    try {
      statuses = fs.listStatus(trackFileDir);
    } catch (FileNotFoundException e) {
      LOG.debug("Track file directory {} does not exist", trackFileDir, e);
      return Collections.emptyNavigableMap();
    }
    if (statuses == null || statuses.length == 0) {
      return Collections.emptyNavigableMap();
    }
    TreeMap<Long, List<Path>> map = new TreeMap<>((l1, l2) -> l2.compareTo(l1));
    for (FileStatus status : statuses) {
      Path file = status.getPath();
      if (!status.isFile()) {
        LOG.warn("Found invalid track file {}, which is not a file", file);
        continue;
      }
      if (!TRACK_FILE_PATTERN.matcher(file.getName()).matches()) {
        LOG.warn("Found invalid track file {}, skip", file);
        continue;
      }
      List<String> parts = Splitter.on(TRACK_FILE_SEPARATOR).splitToList(file.getName());
      map.computeIfAbsent(Long.parseLong(parts.get(1)), k -> new ArrayList<>()).add(file);
    }
    return map;
  }

  private void initializeTrackFiles(long seqId) {
    trackFiles[0] = new Path(trackFileDir, TRACK_FILE_PREFIX + TRACK_FILE_SEPARATOR + seqId);
    trackFiles[1] = new Path(trackFileDir, TRACK_FILE_ROTATE_PREFIX + TRACK_FILE_SEPARATOR + seqId);
    LOG.info("Initialized track files: {}, {}", trackFiles[0], trackFiles[1]);
  }

  private void cleanUpTrackFiles(long loadedSeqId,
    NavigableMap<Long, List<Path>> seqId2TrackFiles) {
    LOG.info("Cleanup track file with sequence id < {}", loadedSeqId);
    FileSystem fs = ctx.getRegionFileSystem().getFileSystem();
    NavigableMap<Long, List<Path>> toDelete =
      loadedSeqId >= 0 ? seqId2TrackFiles.tailMap(loadedSeqId, false) : seqId2TrackFiles;
    toDelete.values().stream().flatMap(l -> l.stream()).forEach(file -> {
      ForkJoinPool.commonPool().execute(() -> {
        LOG.info("Deleting track file {}", file);
        try {
          fs.delete(file, false);
        } catch (IOException e) {
          LOG.warn("failed to delete unused track file {}", file, e);
        }
      });
    });
  }

  StoreFileList load(boolean readOnly) throws IOException {
    NavigableMap<Long, List<Path>> seqId2TrackFiles = listFiles();
    long seqId = -1L;
    StoreFileList[] lists = new StoreFileList[2];
    for (Map.Entry<Long, List<Path>> entry : seqId2TrackFiles.entrySet()) {
      List<Path> files = entry.getValue();
      // should not have more than 2 files, if not, it means that the track files are broken, just
      // throw exception out and fail the region open.
      if (files.size() > 2) {
        throw new DoNotRetryIOException("Should only have at most 2 track files for sequence id " +
          entry.getKey() + ", but got " + files.size() + " files: " + files);
      }
      boolean loaded = false;
      for (int i = 0; i < files.size(); i++) {
        try {
          lists[i] = load(files.get(i));
          loaded = true;
        } catch (EOFException e) {
          // this is normal case, so use info and do not log stacktrace
          LOG.info("Failed to load track file {}: {}", trackFiles[i], e.toString());
        }
      }
      if (loaded) {
        seqId = entry.getKey();
        break;
      }
    }
    if (readOnly) {
      return lists[select(lists)];
    }

    cleanUpTrackFiles(seqId, seqId2TrackFiles);

    if (seqId < 0) {
      initializeTrackFiles(System.currentTimeMillis());
      nextTrackFile = 0;
      return null;
    }

    initializeTrackFiles(Math.max(System.currentTimeMillis(), seqId + 1));
    int winnerIndex = select(lists);
    nextTrackFile = 1 - winnerIndex;
    prevTimestamp = lists[winnerIndex].getTimestamp();
    return lists[winnerIndex];
  }

  /**
   * We will set the timestamp in this method so just pass the builder in
   */
  void update(StoreFileList.Builder builder) throws IOException {
    if (nextTrackFile < 0) {
      // we need to call load first to load the prevTimestamp and also the next file
      // we are already in the update method, which is not read only, so pass false
      load(false);
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
      // loading as we do not need to read this file when loading
      LOG.debug("failed to delete old track file {}, not a big deal, just ignore", e);
    }
  }
}
