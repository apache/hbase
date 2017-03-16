/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.math.LongMath;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.Cacheable;
import org.apache.hadoop.hbase.io.hfile.CacheableDeserializer;
import org.apache.hadoop.hbase.io.hfile.Cacheable.MemoryType;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.nio.SingleByteBuff;
import org.apache.hadoop.util.StringUtils;

/**
 * IO engine that stores data as files to one or more local filesystems
 */
@InterfaceAudience.Private
public class FileIOEngine implements IOEngine {
  private static final Log LOG = LogFactory.getLog(FileIOEngine.class);

  private final List<SegmentFile> segments;
  private final long fileSize;
  private final long totalSize;

  class SegmentFile implements Closeable {
    final RandomAccessFile raf;
    final FileChannel channel;
    final String path;
    final long endOffset;

    SegmentFile(String path, long endOffset) throws IOException {
      this.path = path;
      this.endOffset = endOffset;
      try {
        this.raf = new RandomAccessFile(path, "rw");
      } catch (FileNotFoundException fex) {
        LOG.error("Cannot create bucket cache file " + path, fex);
        throw fex;
      }
      try {
        LOG.info("Allocating " + StringUtils.byteDesc(fileSize) + ", to the file: " + path);
        this.raf.setLength(fileSize);
      } catch (IOException ex) {
        LOG.error("Cannot extend bucket cache file " + path + "; insufficient space for "
            + StringUtils.byteDesc(fileSize), ex);
        closeRandomAccessFile();
        throw ex;
      }
      this.channel = this.raf.getChannel();
    }

    @Override
    public void close() throws IOException {
      IOException lastException = null;
      try {
        closeChannel();
      } catch (IOException ex) {
        LOG.error("Failed to close the FileChannel for " + path, ex);
        lastException = ex;
      }
      try {
        closeRandomAccessFile();
      } catch (IOException ex) {
        LOG.error("Failed to close the RandomAccessFile for " + path, ex);
        lastException = ex;
      }
      if (lastException != null) {
        throw lastException;
      }
    }

    @Override
    public String toString() {
      return "path=" + path + ", endOffset=" + endOffset;
    }

    void closeChannel() throws IOException {
      getChannel().close();
    }

    void closeRandomAccessFile() throws IOException {
      getRandomAccessFile().close();
    }

    void fsync() throws IOException {
      getChannel().force(true);
    }

    FileChannel getChannel() {
      return channel;
    }

    RandomAccessFile getRandomAccessFile() {
      return raf;
    }
  }

  public FileIOEngine(String filePath, long totalSize) throws IOException {
    this(new String[] {filePath}, totalSize);
  }

  /**
   * Initializes a FileIOEngine which can be used by the HBase bucket cache
   *
   * If the 'totalSize' does not evenly divide into the the number of files given, this will
   * allocate extra (unused) space, this is done to ensure the segments are of equal size
   *
   * @param filePaths paths to the files this FileIOEngine will read and write to
   * @param totalSize the total aggregate size for all of the files
   * @throws IOException if initializing the IOEngine fails for any reason.
   */
  public FileIOEngine(String[] filePaths, long totalSize) throws IOException {
    int numFiles = filePaths.length;
    this.segments = new ArrayList<>(numFiles);

    this.fileSize = LongMath.divide(totalSize, numFiles, RoundingMode.CEILING);
    this.totalSize = fileSize * numFiles;
    if (this.totalSize > totalSize) {
      LOG.warn("Initializing with a total size of " + this.totalSize + " when " + totalSize +
          " was requested.");
    }

    for (int i = 0; i < numFiles; ++i) {
      long endOffset = this.fileSize * (i + 1);
      String path = filePaths[i].trim();
      try {
        SegmentFile segmentFile = new SegmentFile(path, endOffset);
        this.segments.add(i, segmentFile);
      } catch (IOException ex) {
        LOG.error("Failed to initialize a file for this IOEngine " + path + ".  Shutting down", ex);
        shutdown();
        throw ex;
      }
    }
  }
  
  @Override
  public String toString() {
    return String.format("ioengine=%s, segments=[%s], size=%,d", getClass().getSimpleName(),
        Joiner.on(", ").join(getSegments()), totalSize);
  }

  /**
   * File IO engine is always able to support persistent storage for the cache
   *
   * @return true
   */
  @Override
  public boolean isPersistent() {
    return true;
  }

  /**
   * @return true if more than 1 file is used, false otherwise
   */
  @Override
  public boolean isSegmented() {
    return getSegments().size() > 1;
  }

  /**
   * Determines whether or not a successful allocation crossed a segment.
   *
   * @param offset the offset of the allocation
   * @param len the length of the allocation.
   * @return true if the allocation crosses a segment boundary, or crosses beyond the totalSize
   * @throws IllegalArgumentException if offset or the offset at length is less than 0 or greater
   * than or equal to the total size.
   */
  @Override
  public boolean allocationCrossedSegments(long offset, long len) {
    long offsetAtLength = offset + len;
    checkOffset(offset);
    SegmentFile segment = getSegment(offset);
    return segment.endOffset < offsetAtLength;
  }

  @Override
  public Cacheable read(long offset, int length, CacheableDeserializer<Cacheable> deserializer)
      throws IOException {
    ByteBuffer dstBuffer = ByteBuffer.allocate(length);

    // The buffer created out of the fileChannel is formed by copying the data from the file
    // Hence in this case there is no shared memory that we point to. Even if the BucketCache evicts
    // this buffer from the file the data is already copied and there is no need to ensure that
    // the results are not corrupted before consuming them.
    if (dstBuffer.limit() != length) {
      throw new RuntimeException(String.format("Only %d bytes read, %d expected",
          dstBuffer.limit(), length));
    }
    if (allocationCrossedSegments(offset, dstBuffer.remaining())) {
      throw new IllegalArgumentException(String.format("Read operation, with offset %d and " +
          "length %d, is crossing a segment", offset, dstBuffer.remaining()));
    }
    getSegment(offset).getChannel().read(dstBuffer, getFileOffset(offset));
    return deserializer.deserialize(new SingleByteBuff(dstBuffer), true, MemoryType.EXCLUSIVE);
  }

  /**
   * Transfers data from the given byte buffer to file
   *
   * @param srcByteBuff the given byte buffer from which bytes are to be read
   * @param offset The offset in the total size of IOEngine where the first byte to be written
   * @throws IOException if the write fails
   * @throws IllegalArgumentException if offset or the offset at length is less than 0 or greater
   * than or equal to the total size, or if the destination buffer is large enough to accommodate
   * for a read which reads beyond a segment.
   */
  @Override
  public void write(ByteBuff srcByteBuff, long offset) throws IOException {
    // When caching block into BucketCache there will be single buffer backing for this HFileBlock.
    assert srcByteBuff.hasArray();
    ByteBuffer srcByteBuffer = ByteBuffer.wrap(srcByteBuff.array(), srcByteBuff.arrayOffset(),
        srcByteBuff.remaining());
    write(srcByteBuffer, offset);
  }

  /**
   * Transfers data from the given byte buffer to file
   *
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the total size of IOEngine where the first byte to be written
   * @throws IOException if the write fails
   * @throws IllegalArgumentException if offset or the offset at length is less than 0 or greater
   * than or equal to the total size, or if the destination buffer is large enough to accommodate
   * for a read which reads beyond a segment.
   */
  @Override
  public void write(ByteBuffer srcBuffer, long offset) throws IOException {
    assert srcBuffer.hasArray();
    if (allocationCrossedSegments(offset, srcBuffer.remaining())) {
      throw new IllegalArgumentException(String.format("Write operation, with offset %d and " +
          "length %d, is crossing a segment", offset, srcBuffer.remaining()));
    }
    getSegment(offset).getChannel().write(srcBuffer, getFileOffset(offset));
  }

  /**
   * Sync the data to file after writing
   *
   * @throws IOException if one segment fails to sync.
   */
  @Override
  public void sync() throws IOException {
    for (SegmentFile segment : getSegments()) {
      segment.fsync();
    }
  }

  /**
   * Close the files
   */
  @Override
  public void shutdown() {
    for (SegmentFile segment : getSegments()) {
      IOUtils.closeQuietly(segment);
    }
  }

  @VisibleForTesting
  long getTotalSize() {
    return totalSize;
  }

  @VisibleForTesting
  List<SegmentFile> getSegments() {
    return segments;
  }

  /**
   * Obtain the segment corresponding to the given offset.
   *
   * @param offset the offset
   * @return The segment containing the offset
   */
  private SegmentFile getSegment(long offset) {
    int segmentIdx = (int) (offset / fileSize);
    return getSegments().get(segmentIdx);
  }

  /**
   * Obtain the file offset within a segment
   *
   * @param offset the offset
   * @return the real offset within the segment
   */
  private long getFileOffset(long offset) {
    return offset % fileSize;
  }

  private void checkOffset(long offset) {
    if (offset < 0L) {
      throw new IllegalArgumentException("offset is negative");
    } else if (offset >= totalSize) {
      throw new IllegalArgumentException(String.format("offset %d must not be greater than or " +
          "equal to size %d", offset, totalSize));
    }
  }

}
