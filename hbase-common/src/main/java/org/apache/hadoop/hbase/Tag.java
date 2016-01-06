/**
 * Copyright The Apache Software Foundation
 *
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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Tags are part of cells and helps to add metadata about them.
 * Metadata could be ACLs, visibility labels, etc.
 * <p>
 * Each Tag is having a type (one byte) and value part. The max value length for a Tag is 65533.
 * <p>
 * See {@link TagType} for reserved tag types.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface Tag {

  public final static int TYPE_LENGTH_SIZE = Bytes.SIZEOF_BYTE;
  public final static int TAG_LENGTH_SIZE = Bytes.SIZEOF_SHORT;
  public final static int INFRASTRUCTURE_SIZE = TYPE_LENGTH_SIZE + TAG_LENGTH_SIZE;
  public static final int MAX_TAG_LENGTH = (2 * Short.MAX_VALUE) + 1 - TAG_LENGTH_SIZE;

  /**
   * @return the tag type
   */
  byte getType();

  /**
   * @return Offset of tag value within the backed buffer
   */
  int getValueOffset();

  /**
   * @return Length of tag value within the backed buffer
   */
  int getValueLength();

  /**
   * Tells whether or not this Tag is backed by a byte array.
   * @return true when this Tag is backed by byte array
   */
  boolean hasArray();

  /**
   * @return The array containing the value bytes.
   * @throws UnsupportedOperationException
   *           when {@link #hasArray()} return false. Use {@link #getValueByteBuffer()} in such
   *           situation
   */
  byte[] getValueArray();

  /**
   * @return The {@link java.nio.ByteBuffer} containing the value bytes.
   */
  ByteBuffer getValueByteBuffer();
}
