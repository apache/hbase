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
package org.apache.hadoop.hbase.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * When deal with OutputStream which is not ByteBufferWriter type, wrap it with this class. We will
 * have to write offheap ByteBuffer (DBB) data into the OS. This class is having a temp byte array
 * to which we can copy the DBB data for writing to the OS.
 * <br>
 * This is used while writing Cell data to WAL. In case of AsyncWAL, the OS created there is
 * ByteBufferWriter. But in case of FSHLog, the OS passed by DFS client, is not of type
 * ByteBufferWriter. We will need this temp solution until DFS client supports writing ByteBuffer
 * directly to the OS it creates.
 * <br>
 * Note: This class is not thread safe.
 */
@InterfaceAudience.Private
public class ByteBufferWriterOutputStream extends OutputStream
    implements ByteBufferWriter {

  private static final int DEFAULT_BUFFER_SIZE = 4096;

  private final OutputStream os;
  private final int bufSize;
  private byte[] buf;

  public ByteBufferWriterOutputStream(OutputStream os) {
    this(os, DEFAULT_BUFFER_SIZE);
  }

  public ByteBufferWriterOutputStream(OutputStream os, int size) {
    this.os = os;
    this.bufSize = size;
    this.buf = null;
  }

  /**
   * Writes len bytes from the specified ByteBuffer starting at offset off to
   * this OutputStream. If b is null, a NullPointerException is thrown. If off
   * is negative or larger than the ByteBuffer then an ArrayIndexOutOfBoundsException
   * is thrown. If len is greater than the length of the ByteBuffer, then an
   * ArrayIndexOutOfBoundsException is thrown. This method does not change the
   * position of the ByteBuffer.
   *
   * @param b    the ByteBuffer
   * @param off  the start offset in the data
   * @param len  the number of bytes to write
   * @throws IOException
   *             if an I/O error occurs. In particular, an IOException is thrown
   *             if the output stream is closed.
   */
  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    // Lazily load in the event that this version of 'write' is not invoked
    if (this.buf == null) {
      this.buf = new byte[this.bufSize];
    }
    int totalCopied = 0;
    while (totalCopied < len) {
      int bytesToCopy = Math.min((len - totalCopied), this.bufSize);
      ByteBufferUtils.copyFromBufferToArray(this.buf, b, off + totalCopied, 0,
          bytesToCopy);
      this.os.write(this.buf, 0, bytesToCopy);
      totalCopied += bytesToCopy;
    }
  }

  @Override
  public void writeInt(int i) throws IOException {
    StreamUtils.writeInt(this.os, i);
  }

  @Override
  public void write(int b) throws IOException {
    this.os.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    this.os.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    this.os.flush();
  }

  @Override
  public void close() throws IOException {
    this.os.close();
  }
}
