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

/**
 * This marks a Cell as streamable to a given OutputStream.
 */
@InterfaceAudience.Private
public interface Streamable {

  /**
   * Write this cell to an OutputStream.
   * @param out Stream to which cell has to be written
   * @return how many bytes are written.
   * @throws IOException
   */
  int write(OutputStream out) throws IOException;

  /**
   * Write this cell to an OutputStream.
   * @param out Stream to which cell has to be written
   * @param withTags Whether to write tags.
   * @return how many bytes are written.
   * @throws IOException
   */
  int write(OutputStream out, boolean withTags) throws IOException;
}