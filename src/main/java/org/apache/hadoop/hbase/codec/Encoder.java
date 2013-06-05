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

package org.apache.hadoop.hbase.codec;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;

/**
 * Accepts a stream of KeyValues. This can be used to anywhere KeyValues need to be written out, but
 * currently it is only used for serializing WALEdits. This could be backed by a List<KeyValue>, but
 * more efficient implementations will append results to a byte[] to eliminate overhead, and
 * possibly encode the underlying data further.
 * <p>
 * To read the data back, use a corresponding {@link Decoder}
 * @see Decoder
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface Encoder {
  /**
   * Implementation must copy the entire state of the cell. If the written cell is modified
   * immediately after the write method returns, the modifications must have absolutely no effect on
   * the copy of the cell that was added in the write.
   * @param cell cell to serialize
   * @throws IOException
   */
  void write(KeyValue cell) throws IOException;

  /**
   * Let the implementation decide what to do.  Usually means writing accumulated data into a
   * byte[] that can then be read from the implementation to be sent to disk, put in the block
   * cache, or sent over the network.
   * @throws IOException
   */
  void flush() throws IOException;
}