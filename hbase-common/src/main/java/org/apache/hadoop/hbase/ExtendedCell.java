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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.HeapSize;

/**
 * Extension to {@link Cell} with server side required functions. Server side Cell implementations
 * must implement this.
 * @see SettableSequenceId
 * @see SettableTimestamp
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public interface ExtendedCell extends Cell, SettableSequenceId, SettableTimestamp, HeapSize,
    Cloneable {

  /**
   * Write this cell to an OutputStream in a {@link KeyValue} format.
   * <br> KeyValue format <br>
   * <code>&lt;4 bytes keylength&gt; &lt;4 bytes valuelength&gt; &lt;2 bytes rowlength&gt;
   * &lt;row&gt; &lt;1 byte columnfamilylength&gt; &lt;columnfamily&gt; &lt;columnqualifier&gt;
   * &lt;8 bytes timestamp&gt; &lt;1 byte keytype&gt; &lt;value&gt; &lt;2 bytes tagslength&gt;
   * &lt;tags&gt;</code>
   * @param out Stream to which cell has to be written
   * @param withTags Whether to write tags.
   * @return how many bytes are written.
   * @throws IOException
   */
  // TODO remove the boolean param once HBASE-16706 is done.
  int write(OutputStream out, boolean withTags) throws IOException;

  /**
   * @param withTags Whether to write tags.
   * @return Bytes count required to serialize this Cell in a {@link KeyValue} format.
   * <br> KeyValue format <br>
   * <code>&lt;4 bytes keylength&gt; &lt;4 bytes valuelength&gt; &lt;2 bytes rowlength&gt;
   * &lt;row&gt; &lt;1 byte columnfamilylength&gt; &lt;columnfamily&gt; &lt;columnqualifier&gt;
   * &lt;8 bytes timestamp&gt; &lt;1 byte keytype&gt; &lt;value&gt; &lt;2 bytes tagslength&gt;
   * &lt;tags&gt;</code>
   */
  // TODO remove the boolean param once HBASE-16706 is done.
  int getSerializedSize(boolean withTags);

  /**
   * Write the given Cell into the given buf's offset.
   * @param buf The buffer where to write the Cell.
   * @param offset The offset within buffer, to write the Cell.
   */
  void write(byte[] buf, int offset);
}