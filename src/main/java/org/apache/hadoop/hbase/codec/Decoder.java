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
package org.apache.hadoop.hbase.codec;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue;

/**
 * An interface for iterating through a sequence of KeyValues. Similar to Java's Iterator, but
 * without the hasNext() or remove() methods. The hasNext() method is problematic because it may
 * require actually loading the next object, which in turn requires storing the previous object
 * somewhere.
 * <p>
 * The core data block decoder should be as fast as possible, so we push the complexity and
 * performance expense of concurrently tracking multiple cells to layers above the {@link Decoder}.
 * <p>
 * The {@link #current()} method will return a reference to a the decodable type.
 * <p/>
 * Typical usage:
 *
 * <pre>
 * while (scanner.next()) {
 *   KeyValue kv = scanner.get();
 *   // do something
 * }
 * </pre>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface Decoder {
  /**
   * @return the current object which may be mutable
   */
  KeyValue current();

  /**
   * Advance the scanner 1 object
   * @return true if the next cell is found and {@link #current()} will return a valid object
   * @throws IOException if there is an error reading the next entry
   */
  boolean advance() throws IOException;
}