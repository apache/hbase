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
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Interface adds support for writing {@link ByteBuffer} into OutputStream.
 */
@InterfaceAudience.Private
public interface ByteBufferSupportOutputStream {

  /**
   * Writes <code>len</code> bytes from the specified ByteBuffer starting at offset <code>off</code>
   * to this output stream.
   *
   * @param b the data.
   * @param off the start offset in the data.
   * @param len the number of bytes to write.
   * @exception IOException
   *              if an I/O error occurs. In particular, an <code>IOException</code> is thrown if
   *              the output stream is closed.
   */
  void write(ByteBuffer b, int off, int len) throws IOException;

  /**
   * Writes an <code>int</code> to the underlying output stream as four
   * bytes, high byte first.
   * @param i the <code>int</code> to write
   * @throws IOException if an I/O error occurs.
   */
  void writeInt(int i) throws IOException;
}