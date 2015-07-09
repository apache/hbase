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

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This class is a server side extension to the Cell interface. This is used when the actual Cell
 * implementation is backed by {@link ByteBuffer}. This class contain ByteBuffer backed getters for
 * row, cf, qualifier, value and tags. Also getters of the position where these field bytes begin. A
 * cell object can be of this type only in server side. When the object is of this type, use the
 * getXXXByteBuffer() method along with getXXXPositionInByteBuffer(). If cell is backed by off heap
 * ByteBuffer the call to getXXXArray() will result is temporary byte array creation and bytes copy
 * resulting in lot of garbage.
 */
@InterfaceAudience.Private
public abstract class ByteBufferedCell implements Cell {

  /**
   * @return The {@link ByteBuffer} containing the row bytes.
   */
  abstract ByteBuffer getRowByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where row bytes start
   */
  abstract int getRowPositionInByteBuffer();

  /**
   * @return The {@link ByteBuffer} containing the column family bytes.
   */
  abstract ByteBuffer getFamilyByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where column family bytes start
   */
  abstract int getFamilyPositionInByteBuffer();

  /**
   * @return The {@link ByteBuffer} containing the column qualifier bytes.
   */
  abstract ByteBuffer getQualifierByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where column qualifier bytes start
   */
  abstract int getQualifierPositionInByteBuffer();

  /**
   * @return The {@link ByteBuffer} containing the value bytes.
   */
  abstract ByteBuffer getValueByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where value bytes start
   */
  abstract int getValuePositionInByteBuffer();

  /**
   * @return The {@link ByteBuffer} containing the tag bytes.
   */
  abstract ByteBuffer getTagsByteBuffer();

  /**
   * @return Position in the {@link ByteBuffer} where tag bytes start
   */
  abstract int getTagsPositionInByteBuffer();
}