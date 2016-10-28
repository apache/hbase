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
package org.apache.hadoop.hbase.io.asyncfs;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

/**
 * Interface for asynchronous filesystem output stream.
 */
@InterfaceAudience.Private
public interface AsyncFSOutput extends Closeable {

  /**
   * Just call write(b, 0, b.length).
   * @see #write(byte[], int, int)
   */
  void write(byte[] b);

  /**
   * Copy the data into the buffer. Note that you need to call
   * {@link #flush(Object, CompletionHandler, boolean)} to flush the buffer manually.
   */
  void write(byte[] b, int off, int len);

  /**
   * Write an int to the buffer.
   */
  void writeInt(int i);

  /**
   * Copy the data in the given {@code bb} into the buffer.
   */
  void write(ByteBuffer bb);

  /**
   * Return the current size of buffered data.
   */
  int buffered();

  /**
   * Return current pipeline. Empty array if no pipeline.
   */
  DatanodeInfo[] getPipeline();

  /**
   * Flush the buffer out.
   * @param attachment will be passed to handler when completed.
   * @param handler will set the acked length as result when completed.
   * @param sync persistent the data to device
   */
  <A> void flush(A attachment, final CompletionHandler<Long, ? super A> handler, boolean sync);

  /**
   * The close method when error occurred.
   */
  void recoverAndClose(CancelableProgressable reporter) throws IOException;

  /**
   * Close the file. You should call {@link #recoverAndClose(CancelableProgressable)} if this method
   * throws an exception.
   */
  @Override
  void close() throws IOException;
}
