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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A class implementing IOEngine interface could support data services for
 * {@link BucketCache}.
 */
@InterfaceAudience.Private
public interface IOEngine {

  /**
   * @return true if persistent storage is supported for the cache when shutdown
   */
  boolean isPersistent();

  /**
   * Transfers data from IOEngine to the given byte buffer
   * @param dstBuffer the given byte buffer into which bytes are to be written
   * @param offset The offset in the IO engine where the first byte to be read
   * @return number of bytes read
   * @throws IOException
   */
  int read(ByteBuffer dstBuffer, long offset) throws IOException;

  /**
   * Transfers data from the given byte buffer to IOEngine
   * @param srcBuffer the given byte buffer from which bytes are to be read
   * @param offset The offset in the IO engine where the first byte to be
   *          written
   * @throws IOException
   */
  void write(ByteBuffer srcBuffer, long offset) throws IOException;

  /**
   * Sync the data to IOEngine after writing
   * @throws IOException
   */
  void sync() throws IOException;

  /**
   * Shutdown the IOEngine
   */
  void shutdown();
}
