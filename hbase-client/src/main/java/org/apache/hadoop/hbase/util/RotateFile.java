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
package org.apache.hadoop.hbase.util;

import com.google.errorprone.annotations.RestrictedApi;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.zip.CRC32;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * A file storage which supports atomic update through two files, i.e, rotating. The implementation
 * does not require atomic rename.
 */
@InterfaceAudience.Private
public class RotateFile {

  private static final Logger LOG = LoggerFactory.getLogger(RotateFile.class);

  private final FileSystem fs;

  private final long maxFileSize;

  private final Path[] files = new Path[2];

  // this is used to make sure that we do not go backwards
  private long prevTimestamp = -1;

  private int nextFile = -1;

  /**
   * Constructs a new RotateFile object with the given parameters.
   * @param fs          the file system to use.
   * @param dir         the directory where the files will be created.
   * @param name        the base name for the files.
   * @param maxFileSize the maximum size of each file.
   */
  public RotateFile(FileSystem fs, Path dir, String name, long maxFileSize) {
    this.fs = fs;
    this.maxFileSize = maxFileSize;
    this.files[0] = new Path(dir, name + "-0");
    this.files[1] = new Path(dir, name + "-1");
  }

  private HBaseProtos.RotateFileData read(Path path) throws IOException {
    byte[] data;
    int expectedChecksum;
    try (FSDataInputStream in = fs.open(path)) {
      int length = in.readInt();
      if (length <= 0 || length > maxFileSize) {
        throw new IOException("Invalid file length " + length
          + ", either less than 0 or greater then max allowed size " + maxFileSize);
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
    return HBaseProtos.RotateFileData.parseFrom(data);
  }

  private int select(HBaseProtos.RotateFileData[] datas) {
    if (datas[0] == null) {
      return 1;
    }
    if (datas[1] == null) {
      return 0;
    }
    return datas[0].getTimestamp() >= datas[1].getTimestamp() ? 0 : 1;
  }

  /**
   * Reads the content of the rotate file by selecting the winner file based on the timestamp of the
   * data inside the files. It reads the content of both files and selects the one with the latest
   * timestamp as the winner. If a file is incomplete or does not exist, it logs the error and moves
   * on to the next file. It returns the content of the winner file as a byte array. If none of the
   * files have valid data, it returns null.
   * @return a byte array containing the data from the winner file, or null if no valid data is
   *         found.
   * @throws IOException if an error occurs while reading the files.
   */
  public byte[] read() throws IOException {
    HBaseProtos.RotateFileData[] datas = new HBaseProtos.RotateFileData[2];
    for (int i = 0; i < 2; i++) {
      try {
        datas[i] = read(files[i]);
      } catch (FileNotFoundException e) {
        LOG.debug("file {} does not exist", files[i], e);
      } catch (EOFException e) {
        LOG.debug("file {} is incomplete", files[i], e);
      }
    }
    int winnerIndex = select(datas);
    nextFile = 1 - winnerIndex;
    if (datas[winnerIndex] != null) {
      prevTimestamp = datas[winnerIndex].getTimestamp();
      return datas[winnerIndex].getData().toByteArray();
    } else {
      return null;
    }
  }

  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/RotateFile.java|.*/src/test/.*")
  static void write(FileSystem fs, Path file, long timestamp, byte[] data) throws IOException {
    HBaseProtos.RotateFileData proto = HBaseProtos.RotateFileData.newBuilder()
      .setTimestamp(timestamp).setData(ByteString.copyFrom(data)).build();
    byte[] protoData = proto.toByteArray();
    CRC32 crc32 = new CRC32();
    crc32.update(protoData);
    int checksum = (int) crc32.getValue();
    // 4 bytes length, 8 bytes timestamp, 4 bytes checksum at the end
    try (FSDataOutputStream out = fs.create(file, true)) {
      out.writeInt(protoData.length);
      out.write(protoData);
      out.writeInt(checksum);
    }
  }

  /**
   * Writes the given data to the next file in the rotation, with a timestamp calculated based on
   * the previous timestamp and the current time to make sure it is greater than the previous
   * timestamp. The method also deletes the previous file, which is no longer needed.
   * <p/>
   * Notice that, for a newly created {@link RotateFile} instance, you need to call {@link #read()}
   * first to initialize the nextFile index, before calling this method.
   * @param data the data to be written to the file
   * @throws IOException if an I/O error occurs while writing the data to the file
   */
  public void write(byte[] data) throws IOException {
    if (data.length > maxFileSize) {
      throw new IOException(
        "Data size " + data.length + " is greater than max allowed size " + maxFileSize);
    }
    long timestamp = Math.max(prevTimestamp + 1, EnvironmentEdgeManager.currentTime());
    write(fs, files[nextFile], timestamp, data);
    prevTimestamp = timestamp;
    nextFile = 1 - nextFile;
    try {
      fs.delete(files[nextFile], false);
    } catch (IOException e) {
      // we will create new file with overwrite = true, so not a big deal here, only for speed up
      // loading as we do not need to read this file when loading
      LOG.debug("Failed to delete old file {}, ignoring the exception", files[nextFile], e);
    }
  }

  /**
   * Deletes the two files used for rotating data. If any of the files cannot be deleted, an
   * IOException is thrown.
   * @throws IOException if there is an error deleting either file
   */
  public void delete() throws IOException {
    Path next = files[nextFile];
    // delete next file first, and then the current file, so when failing to delete, we can still
    // read the correct data
    if (fs.exists(next) && !fs.delete(next, false)) {
      throw new IOException("Can not delete " + next);
    }
    Path current = files[1 - nextFile];
    if (fs.exists(current) && !fs.delete(current, false)) {
      throw new IOException("Can not delete " + current);
    }
  }
}
