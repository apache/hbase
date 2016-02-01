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

import static org.apache.hadoop.hbase.Tag.TAG_LENGTH_SIZE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.util.StreamUtils;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

@InterfaceAudience.Private
public final class TagUtil {

  /**
   * Private constructor to keep this class from being instantiated.
   */
  private TagUtil(){}

  /**
   * Returns tag value in a new byte array.
   * Primarily for use client-side. If server-side, use
   * {@link Tag#getValueArray()} with appropriate {@link Tag#getValueOffset()}
   * and {@link Tag#getValueLength()} instead to save on allocations.
   *
   * @param tag The Tag whose value to be returned
   * @return tag value in a new byte array.
   */
  public static byte[] cloneValue(Tag tag) {
    int tagLength = tag.getValueLength();
    byte[] tagArr = new byte[tagLength];
    if (tag.hasArray()) {
      Bytes.putBytes(tagArr, 0, tag.getValueArray(), tag.getValueOffset(), tagLength);
    } else {
      ByteBufferUtils.copyFromBufferToArray(tagArr, tag.getValueByteBuffer(), tag.getValueOffset(),
          0, tagLength);
    }
    return tagArr;
  }

  /**
   * Creates list of tags from given byte array, expected that it is in the expected tag format.
   *
   * @param b The byte array
   * @param offset The offset in array where tag bytes begin
   * @param length Total length of all tags bytes
   * @return List of tags
   */
  public static List<Tag> asList(byte[] b, int offset, int length) {
    List<Tag> tags = new ArrayList<Tag>();
    int pos = offset;
    while (pos < offset + length) {
      int tagLen = Bytes.readAsInt(b, pos, TAG_LENGTH_SIZE);
      tags.add(new ArrayBackedTag(b, pos, tagLen + TAG_LENGTH_SIZE));
      pos += TAG_LENGTH_SIZE + tagLen;
    }
    return tags;
  }

  /**
   * Creates list of tags from given ByteBuffer, expected that it is in the expected tag format.
   *
   * @param b The ByteBuffer
   * @param offset The offset in ByteBuffer where tag bytes begin
   * @param length Total length of all tags bytes
   * @return List of tags
   */
  public static List<Tag> asList(ByteBuffer b, int offset, int length) {
    List<Tag> tags = new ArrayList<Tag>();
    int pos = offset;
    while (pos < offset + length) {
      int tagLen = ByteBufferUtils.readAsInt(b, pos, TAG_LENGTH_SIZE);
      tags.add(new OffheapTag(b, pos, tagLen + TAG_LENGTH_SIZE));
      pos += TAG_LENGTH_SIZE + tagLen;
    }
    return tags;
  }

  /**
   * Write a list of tags into a byte array
   *
   * @param tags The list of tags
   * @return the serialized tag data as bytes
   */
  public static byte[] fromList(List<Tag> tags) {
    if (tags == null || tags.isEmpty()) {
      return HConstants.EMPTY_BYTE_ARRAY;
    }
    int length = 0;
    for (Tag tag : tags) {
      length += tag.getValueLength() + Tag.INFRASTRUCTURE_SIZE;
    }
    byte[] b = new byte[length];
    int pos = 0;
    int tlen;
    for (Tag tag : tags) {
      tlen = tag.getValueLength();
      pos = Bytes.putAsShort(b, pos, tlen + Tag.TYPE_LENGTH_SIZE);
      pos = Bytes.putByte(b, pos, tag.getType());
      if (tag.hasArray()) {
        pos = Bytes.putBytes(b, pos, tag.getValueArray(), tag.getValueOffset(), tlen);
      } else {
        ByteBufferUtils.copyFromBufferToArray(b, tag.getValueByteBuffer(), tag.getValueOffset(),
            pos, tlen);
        pos += tlen;
      }
    }
    return b;
  }

  /**
   * Converts the value bytes of the given tag into a long value
   * @param tag The Tag
   * @return value as long
   */
  public static long getValueAsLong(Tag tag) {
    if (tag.hasArray()) {
      return Bytes.toLong(tag.getValueArray(), tag.getValueOffset(), tag.getValueLength());
    }
    return ByteBufferUtils.toLong(tag.getValueByteBuffer(), tag.getValueOffset());
  }

  /**
   * Converts the value bytes of the given tag into a byte value
   * @param tag The Tag
   * @return value as byte
   */
  public static byte getValueAsByte(Tag tag) {
    if (tag.hasArray()) {
      return tag.getValueArray()[tag.getValueOffset()];
    }
    return ByteBufferUtils.toByte(tag.getValueByteBuffer(), tag.getValueOffset());
  }

  /**
   * Converts the value bytes of the given tag into a String value
   * @param tag The Tag
   * @return value as String
   */
  public static String getValueAsString(Tag tag){
    if(tag.hasArray()){
      return Bytes.toString(tag.getValueArray(), tag.getValueOffset(), tag.getValueLength());
    }
    return Bytes.toString(cloneValue(tag));
  }

  /**
   * Matches the value part of given tags
   * @param t1 Tag to match the value
   * @param t2 Tag to match the value
   * @return True if values of both tags are same.
   */
  public static boolean matchingValue(Tag t1, Tag t2) {
    if (t1.hasArray() && t2.hasArray()) {
      return Bytes.equals(t1.getValueArray(), t1.getValueOffset(), t1.getValueLength(),
          t2.getValueArray(), t2.getValueOffset(), t2.getValueLength());
    }
    if (t1.hasArray()) {
      return ByteBufferUtils.equals(t2.getValueByteBuffer(), t2.getValueOffset(),
          t2.getValueLength(), t1.getValueArray(), t1.getValueOffset(), t1.getValueLength());
    }
    if (t2.hasArray()) {
      return ByteBufferUtils.equals(t1.getValueByteBuffer(), t1.getValueOffset(),
          t1.getValueLength(), t2.getValueArray(), t2.getValueOffset(), t2.getValueLength());
    }
    return ByteBufferUtils.equals(t1.getValueByteBuffer(), t1.getValueOffset(), t1.getValueLength(),
        t2.getValueByteBuffer(), t2.getValueOffset(), t2.getValueLength());
  }

  /**
   * Copies the tag's value bytes to the given byte array
   * @param tag The Tag
   * @param out The byte array where to copy the Tag value.
   * @param offset The offset within 'out' array where to copy the Tag value.
   */
  public static void copyValueTo(Tag tag, byte[] out, int offset) {
    if (tag.hasArray()) {
      Bytes.putBytes(out, offset, tag.getValueArray(), tag.getValueOffset(), tag.getValueLength());
    } else {
      ByteBufferUtils.copyFromBufferToArray(out, tag.getValueByteBuffer(), tag.getValueOffset(),
          offset, tag.getValueLength());
    }
  }

  /**
   * Reads an int value stored as a VInt at tag's given offset.
   * @param tag The Tag
   * @param offset The offset where VInt bytes begin
   * @return A pair of the int value and number of bytes taken to store VInt
   * @throws IOException When varint is malformed and not able to be read correctly
   */
  public static Pair<Integer, Integer> readVIntValuePart(Tag tag, int offset) throws IOException {
    if (tag.hasArray()) {
      return StreamUtils.readRawVarint32(tag.getValueArray(), offset);
    }
    return StreamUtils.readRawVarint32(tag.getValueByteBuffer(), offset);
  }

  /**
   * @return A List&lt;Tag&gt; of any Tags found in <code>cell</code> else null.
   */
  public static List<Tag> carryForwardTags(final Cell cell) {
    return carryForwardTags(null, cell);
  }

  /**
   * Add to <code>tagsOrNull</code> any Tags <code>cell</code> is carrying or null if none.
   */
  public static List<Tag> carryForwardTags(final List<Tag> tagsOrNull, final Cell cell) {
    Iterator<Tag> itr = CellUtil.tagsIterator(cell);
    if (itr == EMPTY_TAGS_ITR) {
      // If no Tags, return early.
      return tagsOrNull;
    }
    List<Tag> tags = tagsOrNull;
    if (tags == null) {
      tags = new ArrayList<Tag>();
    }
    while (itr.hasNext()) {
      tags.add(itr.next());
    }
    return tags;
  }

  /**
   * @return Carry forward the TTL tag.
   */
  public static List<Tag> carryForwardTTLTag(final List<Tag> tagsOrNull, final long ttl) {
    if (ttl == Long.MAX_VALUE) {
      return tagsOrNull;
    }
    List<Tag> tags = tagsOrNull;
    // If we are making the array in here, given we are the last thing checked, we'll be only thing
    // in the array so set its size to '1' (I saw this being done in earlier version of
    // tag-handling).
    if (tags == null) {
      tags = new ArrayList<Tag>(1);
    }
    tags.add(new ArrayBackedTag(TagType.TTL_TAG_TYPE, Bytes.toBytes(ttl)));
    return tags;
  }

  /**
   * Iterator returned when no Tags. Used by CellUtil too.
   */
  static final Iterator<Tag> EMPTY_TAGS_ITR = new Iterator<Tag>() {
    @Override
    public boolean hasNext() {
      return false;
    }

    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value="IT_NO_SUCH_ELEMENT",
      justification="Intentional")
    public Tag next() {
      return null;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  };
}