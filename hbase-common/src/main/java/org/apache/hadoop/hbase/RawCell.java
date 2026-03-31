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
package org.apache.hadoop.hbase;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * An extended version of Cell that allows CPs manipulate Tags.
 */
// Added by HBASE-19092 to expose Tags to CPs (history server) w/o exposing ExtendedCell.
// Why is this in hbase-common and not in hbase-server where it is used?
// RawCell is an odd name for a class that is only for CPs that want to manipulate Tags on
// server-side only w/o exposing ExtendedCell -- super rare, super exotic.
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
public interface RawCell extends Cell {
  static final int MAX_TAGS_LENGTH = (2 * Short.MAX_VALUE) + 1;

  /**
   * Contiguous raw bytes representing tags that may start at any index in the containing array.
   * @return the tags byte array
   */
  byte[] getTagsArray();

  /**
   * Return the first offset where the tags start in the Cell
   */
  int getTagsOffset();

  /**
   * HBase internally uses 2 bytes to store tags length in Cell. As the tags length is always a
   * non-negative number, to make good use of the sign bit, the max of tags length is defined 2 *
   * Short.MAX_VALUE + 1 = 65535. As a result, the return type is int, because a short is not
   * capable of handling that. Please note that even if the return type is int, the max tags length
   * is far less than Integer.MAX_VALUE.
   * @return the total length of the tags in the Cell.
   */
  int getTagsLength();

  /**
   * Allows cloning the tags in the cell to a new byte[]
   * @return the byte[] having the tags
   */
  default byte[] cloneTags() {
    return PrivateCellUtil.cloneTags((ExtendedCell) this);
  }

  /**
   * Creates a list of tags in the current cell
   * @return a list of tags
   */
  default Iterator<Tag> getTags() {
    return PrivateCellUtil.tagsIterator((ExtendedCell) this);
  }

  /**
   * Returns the specific tag of the given type
   * @param type the type of the tag
   * @return the specific tag if available or null
   */
  default Optional<Tag> getTag(byte type) {
    return PrivateCellUtil.getTag((ExtendedCell) this, type);
  }

  /**
   * Check the length of tags. If it is invalid, throw IllegalArgumentException
   * @param tagsLength the given length of tags
   * @throws IllegalArgumentException if tagslength is invalid
   */
  public static void checkForTagsLength(int tagsLength) {
    if (tagsLength > MAX_TAGS_LENGTH) {
      throw new IllegalArgumentException("tagslength " + tagsLength + " > " + MAX_TAGS_LENGTH);
    }
  }

  /** Returns A new cell which is having the extra tags also added to it. */
  public static Cell createCell(Cell cell, List<Tag> tags) {
    return PrivateCellUtil.createCell((ExtendedCell) cell, tags);
  }
}
