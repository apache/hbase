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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Tags are part of cells and helps to add metadata about the KVs.
 * Metadata could be ACLs per cells, visibility labels, etc.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class Tag {
  public final static int TYPE_LENGTH_SIZE = Bytes.SIZEOF_BYTE;
  public final static int TAG_LENGTH_SIZE = Bytes.SIZEOF_SHORT;
  public final static int INFRASTRUCTURE_SIZE = TYPE_LENGTH_SIZE + TAG_LENGTH_SIZE;
  public static final int MAX_TAG_LENGTH = (2 * Short.MAX_VALUE) + 1 - TAG_LENGTH_SIZE;

  private final byte type;
  private final byte[] bytes;
  private int offset = 0;
  private int length = 0;

  // The special tag will write the length of each tag and that will be
  // followed by the type and then the actual tag.
  // So every time the length part is parsed we need to add + 1 byte to it to
  // get the type and then get the actual tag.
  public Tag(byte tagType, String tag) {
    this(tagType, Bytes.toBytes(tag));
  }

  /**
   * @param tagType
   * @param tag
   */
  public Tag(byte tagType, byte[] tag) {
    /**
     * Format for a tag : <length of tag - 2 bytes><type code - 1 byte><tag> taglength is serialized
     * using 2 bytes only but as this will be unsigned, we can have max taglength of
     * (Short.MAX_SIZE * 2) +1. It includes 1 byte type length and actual tag bytes length.
     */
    int tagLength = tag.length + TYPE_LENGTH_SIZE;
    if (tagLength > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException(
          "Invalid tag data being passed. Its length can not exceed " + MAX_TAG_LENGTH);
    }
    length = TAG_LENGTH_SIZE + tagLength;
    bytes = new byte[length];
    int pos = Bytes.putAsShort(bytes, 0, tagLength);
    pos = Bytes.putByte(bytes, pos, tagType);
    Bytes.putBytes(bytes, pos, tag, 0, tag.length);
    this.type = tagType;
  }

  /**
   * Creates a Tag from the specified byte array and offset. Presumes
   * <code>bytes</code> content starting at <code>offset</code> is formatted as
   * a Tag blob.
   * The bytes to include the tag type, tag length and actual tag bytes.
   * @param bytes
   *          byte array
   * @param offset
   *          offset to start of Tag
   */
  public Tag(byte[] bytes, int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  private static int getLength(byte[] bytes, int offset) {
    return TAG_LENGTH_SIZE + Bytes.readAsInt(bytes, offset, TAG_LENGTH_SIZE);
  }

  /**
   * Creates a Tag from the specified byte array, starting at offset, and for length
   * <code>length</code>. Presumes <code>bytes</code> content starting at <code>offset</code> is
   * formatted as a Tag blob.
   * @param bytes
   *          byte array
   * @param offset
   *          offset to start of the Tag
   * @param length
   *          length of the Tag
   */
  public Tag(byte[] bytes, int offset, int length) {
    if (length > MAX_TAG_LENGTH) {
      throw new IllegalArgumentException(
          "Invalid tag data being passed. Its length can not exceed " + MAX_TAG_LENGTH);
    }
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
    this.type = bytes[offset + TAG_LENGTH_SIZE];
  }

  /**
   * @return The byte array backing this Tag.
   */
  public byte[] getBuffer() {
    return this.bytes;
  }

  /**
   * @return the tag type
   */
  public byte getType() {
    return this.type;
  }

  /**
   * @return Length of actual tag bytes within the backed buffer
   */
  public int getTagLength() {
    return this.length - INFRASTRUCTURE_SIZE;
  }

  /**
   * @return Offset of actual tag bytes within the backed buffer
   */
  public int getTagOffset() {
    return this.offset + INFRASTRUCTURE_SIZE;
  }

  /**
   * Returns tag value in a new byte array.
   * Primarily for use client-side. If server-side, use
   * {@link #getBuffer()} with appropriate {@link #getTagOffset()} and {@link #getTagLength()}
   * instead to save on allocations.
   * @return tag value in a new byte array.
   */
  public byte[] getValue() {
    int tagLength = getTagLength();
    byte[] tag = new byte[tagLength];
    Bytes.putBytes(tag, 0, bytes, getTagOffset(), tagLength);
    return tag;
  }

  /**
   * Creates the list of tags from the byte array b. Expected that b is in the
   * expected tag format
   * @param b
   * @param offset
   * @param length
   * @return List of tags
   */
  public static List<Tag> asList(byte[] b, int offset, int length) {
    List<Tag> tags = new ArrayList<Tag>();
    int pos = offset;
    while (pos < offset + length) {
      int tagLen = Bytes.readAsInt(b, pos, TAG_LENGTH_SIZE);
      tags.add(new Tag(b, pos, tagLen + TAG_LENGTH_SIZE));
      pos += TAG_LENGTH_SIZE + tagLen;
    }
    return tags;
  }

  /**
   * Write a list of tags into a byte array
   * @param tags
   * @return the serialized tag data as bytes
   */
  public static byte[] fromList(List<Tag> tags) {
    if (tags == null || tags.size() <= 0) return null;
    int length = 0;
    for (Tag tag: tags) {
      length += tag.length;
    }
    byte[] b = new byte[length];
    int pos = 0;
    for (Tag tag: tags) {
      System.arraycopy(tag.bytes, tag.offset, b, pos, tag.length);
      pos += tag.length;
    }
    return b;
  }

  /**
   * Retrieve the first tag from the tags byte array matching the passed in tag type
   * @param b
   * @param offset
   * @param length
   * @param type
   * @return null if there is no tag of the passed in tag type
   */
  public static Tag getTag(byte[] b, int offset, int length, byte type) {
    int pos = offset;
    while (pos < offset + length) {
      int tagLen = Bytes.readAsInt(b, pos, TAG_LENGTH_SIZE);
      if(b[pos + TAG_LENGTH_SIZE] == type) {
        return new Tag(b, pos, tagLen + TAG_LENGTH_SIZE);
      }
      pos += TAG_LENGTH_SIZE + tagLen;
    }
    return null;
  }

  /**
   * Returns the total length of the entire tag entity
   */
  int getLength() {
    return this.length;
  }

  /**
   * Returns the offset of the entire tag entity
   */
  int getOffset() {
    return this.offset;
  }


  /**
   * @return A List<Tag> of any Tags found in <code>cell</code> else null.
   */
  public static List<Tag> carryForwardTags(final Cell cell) {
    return carryForwardTags(null, cell);
  }

  /**
   * @return Add to <code>tagsOrNull</code> any Tags <code>cell</code> is carrying or null if
   * it is carrying no Tags AND the passed in <code>tagsOrNull</code> is null (else we return new
   * List<Tag> with Tags found).
   */
  public static List<Tag> carryForwardTags(final List<Tag> tagsOrNull, final Cell cell) {
    List<Tag> tags = tagsOrNull;
    if (cell.getTagsLength() <= 0) return tags;
    Iterator<Tag> itr =
        CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(), cell.getTagsLength());
    if (tags == null) tags = new ArrayList<Tag>();
    while (itr.hasNext()) {
      tags.add(itr.next());
    }
    return tags;
  }
}