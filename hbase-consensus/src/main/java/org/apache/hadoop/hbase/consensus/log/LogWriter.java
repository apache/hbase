package org.apache.hadoop.hbase.consensus.log;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import org.apache.hadoop.hbase.HConstants;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.hadoop.util.PureJavaCrc32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * LogWriter provides interfaces to perform writer operations against a log file.
 * This class is not thread safe. And there shall be ONLY one logWriter for each log file.
 *
 * It assumes the client will write the header of the file first, and then it can append
 * commits or truncate the commits, finally the caller will close the writer.
 *
 *  Here is the log file format:
 * ------------------
 * | File Header:   |
 * |  Version    4B |
 * |  Term       8B |
 * |  Index      8B |
 * ------------------
 * | Entry # N      |
 * |  Entry Header  |
 * |   Size     4B  |
 * |   CRC      8B  |
 * |   Index    8B  |
 * |  Entry PayLoad |
 * ------------------
 * | Entry # N      |
 * |  Entry Header  |
 * |   Size     4B  |
 * |   CRC      8B  |
 * |   Index    8B  |
 * |  Entry PayLoad |
 * ----------------
 *     .....
 * ------------------
 * | Entry # N      |
 * |  Entry Header  |
 * |   Size     4B  |
 * |   CRC      8B  |
 * |   Index    8B  |
 * |  Entry PayLoad |
 * ------------------
 */
@NotThreadSafe
public class LogWriter {

  private final Logger
    LOG = LoggerFactory.getLogger(LogWriter.class);

  private static final int CONSENSUS_LOG_DEFAULT_PAYLOAD_SIZE = 16 * 1024;
  // The outside application should not have direct access to this object as
  // the offset is maintained by the LogWriter
  private final RandomAccessFile raf;
  private boolean isSync = false;
  private long currOffset;
  private ByteBuffer buffer = ByteBuffer.allocateDirect(
    CONSENSUS_LOG_DEFAULT_PAYLOAD_SIZE);

  /** The CRC instance to compute CRC-32 of a log entry payload */
  private PureJavaCrc32 crc32 = new PureJavaCrc32();

  public LogWriter(RandomAccessFile raf, boolean isSync) {
    this.raf = raf;
    this.isSync = isSync;
    this.currOffset = 0;
  }


  public RandomAccessFile getRandomAccessFile() {
    return raf;
  }

  /**
   * Write the header data to the log file
   *
   * @param term The term of the log file. Each log file only contains transactions
   *             for  this term
   * @param index The initial index for this log file.
   * @throws IOException
   */
  public void writeFileHeader(long term, long index) throws IOException {
    buffer.clear();

    buffer.putInt(HConstants.RAFT_LOG_VERSION);

    // Write the index to the buffer
    buffer.putLong(term);
    buffer.putLong(index);
    buffer.flip();
    currOffset += raf.getChannel().write(buffer);
  }

  public static ByteBuffer generateFileHeader(long term, long index) {
    ByteBuffer bbuf = ByteBuffer.allocate(HConstants.RAFT_FILE_HEADER_SIZE);
    bbuf.putInt(HConstants.RAFT_LOG_VERSION);
    bbuf.putLong(term);
    bbuf.putLong(index);
    bbuf.flip();
    return bbuf;
  }

  /**
   * Append an specific commit (index with its transactions) to the log file
   *
   * @param index The index of the transactions
   * @param data  The transaction list
   * @return offset The file offset where this commit starts.
   * @throws IOException
   */
  public long append(long index, ByteBuffer data) throws IOException {

    // Get the current file offset right before this entry
    long offset = currOffset;

    ByteBuffer buffer = getBuffer(data.remaining() +
      HConstants.RAFT_TXN_HEADER_SIZE);

    // Clear the buffer
    buffer.clear();

    buffer.putInt(data.remaining());

    // Update the CRC for the entry payload
    this.crc32.reset();
    this.crc32.update(data.array(), data.arrayOffset() + data.position(),
      data.remaining());

    // Write CRC value
    buffer.putLong(crc32.getValue());

    // Write the index to the buffer
    buffer.putLong(index);

    // Write the payload
    buffer.put(data.array(), data.position() + data.arrayOffset(),
      data.remaining());

    // Reset the position
    buffer.flip();

    // Write the header
    currOffset += raf.getChannel().write(buffer, currOffset);

    buffer.clear();
    // Sync the file if enabled
    if (this.isSync) {
      raf.getChannel().force(true);
      raf.getFD().sync();
    }

    // Return the starting file offset before this entry
    return offset;
  }

  /**
   * Truncate the log from a specific offset
   * @param offset
   * @throws IOException
   */
  public void truncate(long offset) throws IOException {
    this.raf.getChannel().truncate(offset);

    // Need to always sync the data to the log file
    this.raf.getChannel().force(true);
    currOffset = offset;
  }

  /**
   * Close the writer; No more writer operation is allowed after the log is closed.
   * @throws IOException
   */
  public void close() throws IOException {
    this.raf.close();
  }

  public long getCurrentPosition() {
    return currOffset;
  }

  private ByteBuffer getBuffer(int payloadSize) {
    if (buffer.capacity() >= payloadSize) {
      return buffer;
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Allocating a new byte buffer of size " + payloadSize);
    }
    return ByteBuffer.allocate(payloadSize);
  }
}
