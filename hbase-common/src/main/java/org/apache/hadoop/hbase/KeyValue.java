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

import static org.apache.hadoop.hbase.util.Bytes.len;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An HBase Key/Value. This is the fundamental HBase Type.
 * <p>
 * HBase applications and users should use the Cell interface and avoid directly using KeyValue and
 * member functions not defined in Cell.
 * <p>
 * If being used client-side, the primary methods to access individual fields are
 * {@link #getRowArray()}, {@link #getFamilyArray()}, {@link #getQualifierArray()},
 * {@link #getTimestamp()}, and {@link #getValueArray()}. These methods allocate new byte arrays and
 * return copies. Avoid their use server-side.
 * <p>
 * Instances of this class are immutable. They do not implement Comparable but Comparators are
 * provided. Comparators change with context, whether user table or a catalog table comparison. Its
 * critical you use the appropriate comparator. There are Comparators for normal HFiles, Meta's
 * Hfiles, and bloom filter keys.
 * <p>
 * KeyValue wraps a byte array and takes offsets and lengths into passed array at where to start
 * interpreting the content as KeyValue. The KeyValue format inside a byte array is:
 * <code>&lt;keylength&gt; &lt;valuelength&gt; &lt;key&gt; &lt;value&gt;</code> Key is further
 * decomposed as: <code>&lt;rowlength&gt; &lt;row&gt; &lt;columnfamilylength&gt;
 * &lt;columnfamily&gt; &lt;columnqualifier&gt;
 * &lt;timestamp&gt; &lt;keytype&gt;</code> The <code>rowlength</code> maximum is
 * <code>Short.MAX_SIZE</code>, column family length maximum is <code>Byte.MAX_SIZE</code>, and
 * column qualifier + key length must be &lt; <code>Integer.MAX_SIZE</code>. The column does not
 * contain the family/qualifier delimiter, {@link #COLUMN_FAMILY_DELIMITER}<br>
 * KeyValue can optionally contain Tags. When it contains tags, it is added in the byte array after
 * the value part. The format for this part is: <code>&lt;tagslength&gt;&lt;tagsbytes&gt;</code>.
 * <code>tagslength</code> maximum is <code>Short.MAX_SIZE</code>. The <code>tagsbytes</code>
 * contain one or more tags where as each tag is of the form
 * <code>&lt;taglength&gt;&lt;tagtype&gt;&lt;tagbytes&gt;</code>. <code>tagtype</code> is one byte
 * and <code>taglength</code> maximum is <code>Short.MAX_SIZE</code> and it includes 1 byte type
 * length and actual tag bytes length.
 */
@InterfaceAudience.Private
public class KeyValue implements ExtendedCell, Cloneable {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValue.class);

  public static final int FIXED_OVERHEAD = ClassSize.OBJECT + // the KeyValue object itself
    ClassSize.REFERENCE + // pointer to "bytes"
    2 * Bytes.SIZEOF_INT + // offset, length
    Bytes.SIZEOF_LONG;// memstoreTS

  /**
   * Colon character in UTF-8
   */
  public static final char COLUMN_FAMILY_DELIMITER = ':';

  public static final byte[] COLUMN_FAMILY_DELIM_ARRAY = new byte[] { COLUMN_FAMILY_DELIMITER };

  /** Size of the key length field in bytes */
  public static final int KEY_LENGTH_SIZE = Bytes.SIZEOF_INT;

  /** Size of the key type field in bytes */
  public static final int TYPE_SIZE = Bytes.SIZEOF_BYTE;

  /** Size of the row length field in bytes */
  public static final int ROW_LENGTH_SIZE = Bytes.SIZEOF_SHORT;

  /** Size of the family length field in bytes */
  public static final int FAMILY_LENGTH_SIZE = Bytes.SIZEOF_BYTE;

  /** Size of the timestamp field in bytes */
  public static final int TIMESTAMP_SIZE = Bytes.SIZEOF_LONG;

  // Size of the timestamp and type byte on end of a key -- a long + a byte.
  public static final int TIMESTAMP_TYPE_SIZE = TIMESTAMP_SIZE + TYPE_SIZE;

  // Size of the length shorts and bytes in key.
  public static final int KEY_INFRASTRUCTURE_SIZE =
    ROW_LENGTH_SIZE + FAMILY_LENGTH_SIZE + TIMESTAMP_TYPE_SIZE;

  // How far into the key the row starts at. First thing to read is the short
  // that says how long the row is.
  public static final int ROW_OFFSET =
    Bytes.SIZEOF_INT /* keylength */ + Bytes.SIZEOF_INT /* valuelength */;

  public static final int ROW_KEY_OFFSET = ROW_OFFSET + ROW_LENGTH_SIZE;

  // Size of the length ints in a KeyValue datastructure.
  public static final int KEYVALUE_INFRASTRUCTURE_SIZE = ROW_OFFSET;

  /** Size of the tags length field in bytes */
  public static final int TAGS_LENGTH_SIZE = Bytes.SIZEOF_SHORT;

  public static final int KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE = ROW_OFFSET + TAGS_LENGTH_SIZE;

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up for its underlying data structure.
   * @param rlength row length
   * @param flength family length
   * @param qlength qualifier length
   * @param vlength value length
   * @return the <code>KeyValue</code> data structure length
   */
  public static long getKeyValueDataStructureSize(int rlength, int flength, int qlength,
    int vlength) {
    return KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE
      + getKeyDataStructureSize(rlength, flength, qlength) + vlength;
  }

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up for its underlying data structure.
   * @param rlength    row length
   * @param flength    family length
   * @param qlength    qualifier length
   * @param vlength    value length
   * @param tagsLength total length of the tags
   * @return the <code>KeyValue</code> data structure length
   */
  public static long getKeyValueDataStructureSize(int rlength, int flength, int qlength,
    int vlength, int tagsLength) {
    if (tagsLength == 0) {
      return getKeyValueDataStructureSize(rlength, flength, qlength, vlength);
    }
    return KeyValue.KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE
      + getKeyDataStructureSize(rlength, flength, qlength) + vlength + tagsLength;
  }

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up for its underlying data structure.
   * @param klength    key length
   * @param vlength    value length
   * @param tagsLength total length of the tags
   * @return the <code>KeyValue</code> data structure length
   */
  public static long getKeyValueDataStructureSize(int klength, int vlength, int tagsLength) {
    if (tagsLength == 0) {
      return (long) KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + klength + vlength;
    }
    return (long) KeyValue.KEYVALUE_WITH_TAGS_INFRASTRUCTURE_SIZE + klength + vlength + tagsLength;
  }

  /**
   * Computes the number of bytes that a <code>KeyValue</code> instance with the provided
   * characteristics would take up in its underlying data structure for the key.
   * @param rlength row length
   * @param flength family length
   * @param qlength qualifier length
   * @return the key data structure length
   */
  public static long getKeyDataStructureSize(int rlength, int flength, int qlength) {
    return (long) KeyValue.KEY_INFRASTRUCTURE_SIZE + rlength + flength + qlength;
  }

  /**
   * Key type. Has space for other key types to be added later. Cannot rely on enum ordinals . They
   * change if item is removed or moved. Do our own codes.
   */
  public static enum Type {
    Minimum((byte) 0),
    Put((byte) 4),

    Delete((byte) 8),
    DeleteFamilyVersion((byte) 10),
    DeleteColumn((byte) 12),
    DeleteFamily((byte) 14),

    // Maximum is used when searching; you look from maximum on down.
    Maximum((byte) 255);

    private final byte code;

    Type(final byte c) {
      this.code = c;
    }

    public byte getCode() {
      return this.code;
    }

    private static Type[] codeArray = new Type[256];

    static {
      for (Type t : Type.values()) {
        codeArray[t.code & 0xff] = t;
      }
    }

    /**
     * True to indicate that the byte b is a valid type.
     * @param b byte to check
     * @return true or false
     */
    static boolean isValidType(byte b) {
      return codeArray[b & 0xff] != null;
    }

    /**
     * Cannot rely on enum ordinals . They change if item is removed or moved. Do our own codes.
     * @param b the kv serialized byte[] to process
     * @return Type associated with passed code.
     */
    public static Type codeToType(final byte b) {
      Type t = codeArray[b & 0xff];
      if (t != null) {
        return t;
      }
      throw new RuntimeException("Unknown code " + b);
    }
  }

  /**
   * Lowest possible key. Makes a Key with highest possible Timestamp, empty row and column. No key
   * can be equal or lower than this one in memstore or in store file.
   */
  public static final KeyValue LOWESTKEY =
    new KeyValue(HConstants.EMPTY_BYTE_ARRAY, HConstants.LATEST_TIMESTAMP);

  ////
  // KeyValue core instance fields.
  protected byte[] bytes = null; // an immutable byte array that contains the KV
  protected int offset = 0; // offset into bytes buffer KV starts at
  protected int length = 0; // length of the KV starting from offset.

  /** Here be dragons **/

  /**
   * used to achieve atomic operations in the memstore.
   */
  @Override
  public long getSequenceId() {
    return seqId;
  }

  @Override
  public void setSequenceId(long seqId) {
    this.seqId = seqId;
  }

  // multi-version concurrency control version. default value is 0, aka do not care.
  private long seqId = 0;

  /** Dragon time over, return to normal business */

  /** Writable Constructor -- DO NOT USE */
  public KeyValue() {
  }

  /**
   * Creates a KeyValue from the start of the specified byte array. Presumes <code>bytes</code>
   * content is formatted as a KeyValue blob.
   * @param bytes byte array
   */
  public KeyValue(final byte[] bytes) {
    this(bytes, 0);
  }

  /**
   * Creates a KeyValue from the specified byte array and offset. Presumes <code>bytes</code>
   * content starting at <code>offset</code> is formatted as a KeyValue blob.
   * @param bytes  byte array
   * @param offset offset to start of KeyValue
   */
  public KeyValue(final byte[] bytes, final int offset) {
    this(bytes, offset, getLength(bytes, offset));
  }

  /**
   * Creates a KeyValue from the specified byte array, starting at offset, and for length
   * <code>length</code>.
   * @param bytes  byte array
   * @param offset offset to start of the KeyValue
   * @param length length of the KeyValue
   */
  public KeyValue(final byte[] bytes, final int offset, final int length) {
    KeyValueUtil.checkKeyValueBytes(bytes, offset, length, true);
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Creates a KeyValue from the specified byte array, starting at offset, and for length
   * <code>length</code>.
   * @param bytes  byte array
   * @param offset offset to start of the KeyValue
   * @param length length of the KeyValue
   * @param ts     timestamp
   */
  public KeyValue(final byte[] bytes, final int offset, final int length, long ts) {
    this(bytes, offset, length, null, 0, 0, null, 0, 0, ts, Type.Maximum, null, 0, 0, null);
  }

  /** Constructors that build a new backing byte array from fields */

  /**
   * Constructs KeyValue structure filled with null value. Sets type to
   * {@link KeyValue.Type#Maximum}
   * @param row       - row key (arbitrary byte array)
   * @param timestamp version timestamp
   */
  public KeyValue(final byte[] row, final long timestamp) {
    this(row, null, null, timestamp, Type.Maximum, null);
  }

  /**
   * Constructs KeyValue structure filled with null value.
   * @param row       - row key (arbitrary byte array)
   * @param timestamp version timestamp
   */
  public KeyValue(final byte[] row, final long timestamp, Type type) {
    this(row, null, null, timestamp, type, null);
  }

  /**
   * Constructs KeyValue structure filled with null value. Sets type to
   * {@link KeyValue.Type#Maximum}
   * @param row       - row key (arbitrary byte array)
   * @param family    family name
   * @param qualifier column qualifier
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier) {
    this(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Maximum);
  }

  /**
   * Constructs KeyValue structure as a put filled with specified values and LATEST_TIMESTAMP.
   * @param row       - row key (arbitrary byte array)
   * @param family    family name
   * @param qualifier column qualifier
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final byte[] value) {
    this(row, family, qualifier, HConstants.LATEST_TIMESTAMP, Type.Put, value);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type      key type
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final long timestamp, Type type) {
    this(row, family, qualifier, timestamp, type, null);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value     column value
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final long timestamp, final byte[] value) {
    this(row, family, qualifier, timestamp, Type.Put, value);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value     column value
   * @param tags      tags
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final long timestamp, final byte[] value, final Tag[] tags) {
    this(row, family, qualifier, timestamp, value, tags != null ? Arrays.asList(tags) : null);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param value     column value
   * @param tags      tags non-empty list of tags or null
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final long timestamp, final byte[] value, final List<Tag> tags) {
    this(row, 0, row == null ? 0 : row.length, family, 0, family == null ? 0 : family.length,
      qualifier, 0, qualifier == null ? 0 : qualifier.length, timestamp, Type.Put, value, 0,
      value == null ? 0 : value.length, tags);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final long timestamp, Type type, final byte[] value) {
    this(row, 0, len(row), family, 0, len(family), qualifier, 0, len(qualifier), timestamp, type,
      value, 0, len(value));
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final long timestamp, Type type, final byte[] value, final List<Tag> tags) {
    this(row, family, qualifier, 0, qualifier == null ? 0 : qualifier.length, timestamp, type,
      value, 0, value == null ? 0 : value.length, tags);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final byte[] family, final byte[] qualifier,
    final long timestamp, Type type, final byte[] value, final byte[] tags) {
    this(row, family, qualifier, 0, qualifier == null ? 0 : qualifier.length, timestamp, type,
      value, 0, value == null ? 0 : value.length, tags);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * @param row       row key
   * @param family    family name
   * @param qualifier column qualifier
   * @param qoffset   qualifier offset
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @param voffset   value offset
   * @param vlength   value length
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(byte[] row, byte[] family, byte[] qualifier, int qoffset, int qlength,
    long timestamp, Type type, byte[] value, int voffset, int vlength, List<Tag> tags) {
    this(row, 0, row == null ? 0 : row.length, family, 0, family == null ? 0 : family.length,
      qualifier, qoffset, qlength, timestamp, type, value, voffset, vlength, tags);
  }

  /**
   * @param row       row key
   * @param family    family name
   * @param qualifier qualifier name
   * @param qoffset   qualifier offset
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @param voffset   value offset
   * @param vlength   value length
   * @param tags      tags
   */
  public KeyValue(byte[] row, byte[] family, byte[] qualifier, int qoffset, int qlength,
    long timestamp, Type type, byte[] value, int voffset, int vlength, byte[] tags) {
    this(row, 0, row == null ? 0 : row.length, family, 0, family == null ? 0 : family.length,
      qualifier, qoffset, qlength, timestamp, type, value, voffset, vlength, tags, 0,
      tags == null ? 0 : tags.length);
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param row row key
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final int roffset, final int rlength, final byte[] family,
    final int foffset, final int flength, final byte[] qualifier, final int qoffset,
    final int qlength, final long timestamp, final Type type, final byte[] value, final int voffset,
    final int vlength) {
    this(row, roffset, rlength, family, foffset, flength, qualifier, qoffset, qlength, timestamp,
      type, value, voffset, vlength, null);
  }

  /**
   * Constructs KeyValue structure filled with specified values. Uses the provided buffer as the
   * data buffer.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param buffer    the bytes buffer to use
   * @param boffset   buffer offset
   * @param row       row key
   * @param roffset   row offset
   * @param rlength   row length
   * @param family    family name
   * @param foffset   family offset
   * @param flength   family length
   * @param qualifier column qualifier
   * @param qoffset   qualifier offset
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @param voffset   value offset
   * @param vlength   value length
   * @param tags      non-empty list of tags or null
   * @throws IllegalArgumentException an illegal value was passed or there is insufficient space
   *                                  remaining in the buffer
   */
  public KeyValue(byte[] buffer, final int boffset, final byte[] row, final int roffset,
    final int rlength, final byte[] family, final int foffset, final int flength,
    final byte[] qualifier, final int qoffset, final int qlength, final long timestamp,
    final Type type, final byte[] value, final int voffset, final int vlength, final Tag[] tags) {
    this.bytes = buffer;
    this.length = writeByteArray(buffer, boffset, row, roffset, rlength, family, foffset, flength,
      qualifier, qoffset, qlength, timestamp, type, value, voffset, vlength, tags);
    this.offset = boffset;
  }

  /**
   * Constructs KeyValue structure filled with specified values.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param row       row key
   * @param roffset   row offset
   * @param rlength   row length
   * @param family    family name
   * @param foffset   family offset
   * @param flength   family length
   * @param qualifier column qualifier
   * @param qoffset   qualifier offset
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @param voffset   value offset
   * @param vlength   value length
   * @param tags      tags
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final byte[] row, final int roffset, final int rlength, final byte[] family,
    final int foffset, final int flength, final byte[] qualifier, final int qoffset,
    final int qlength, final long timestamp, final Type type, final byte[] value, final int voffset,
    final int vlength, final List<Tag> tags) {
    this.bytes = createByteArray(row, roffset, rlength, family, foffset, flength, qualifier,
      qoffset, qlength, timestamp, type, value, voffset, vlength, tags);
    this.length = bytes.length;
    this.offset = 0;
  }

  /**
   * @param row       row key
   * @param roffset   row offset
   * @param rlength   row length
   * @param family    family name
   * @param foffset   fammily offset
   * @param flength   family length
   * @param qualifier column qualifier
   * @param qoffset   qualifier offset
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @param voffset   value offset
   * @param vlength   value length
   * @param tags      input tags
   */
  public KeyValue(final byte[] row, final int roffset, final int rlength, final byte[] family,
    final int foffset, final int flength, final byte[] qualifier, final int qoffset,
    final int qlength, final long timestamp, final Type type, final byte[] value, final int voffset,
    final int vlength, final byte[] tags, final int tagsOffset, final int tagsLength) {
    this.bytes = createByteArray(row, roffset, rlength, family, foffset, flength, qualifier,
      qoffset, qlength, timestamp, type, value, voffset, vlength, tags, tagsOffset, tagsLength);
    this.length = bytes.length;
    this.offset = 0;
  }

  /**
   * Constructs an empty KeyValue structure, with specified sizes. This can be used to partially
   * fill up KeyValues.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param rlength   row length
   * @param flength   family length
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param vlength   value length
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final int rlength, final int flength, final int qlength, final long timestamp,
    final Type type, final int vlength) {
    this(rlength, flength, qlength, timestamp, type, vlength, 0);
  }

  /**
   * Constructs an empty KeyValue structure, with specified sizes. This can be used to partially
   * fill up KeyValues.
   * <p>
   * Column is split into two fields, family and qualifier.
   * @param rlength    row length
   * @param flength    family length
   * @param qlength    qualifier length
   * @param timestamp  version timestamp
   * @param type       key type
   * @param vlength    value length
   * @param tagsLength length of the tags
   * @throws IllegalArgumentException an illegal value was passed
   */
  public KeyValue(final int rlength, final int flength, final int qlength, final long timestamp,
    final Type type, final int vlength, final int tagsLength) {
    this.bytes =
      createEmptyByteArray(rlength, flength, qlength, timestamp, type, vlength, tagsLength);
    this.length = bytes.length;
    this.offset = 0;
  }

  public KeyValue(byte[] row, int roffset, int rlength, byte[] family, int foffset, int flength,
    ByteBuffer qualifier, long ts, Type type, ByteBuffer value, List<Tag> tags) {
    this.bytes = createByteArray(row, roffset, rlength, family, foffset, flength, qualifier, 0,
      qualifier == null ? 0 : qualifier.remaining(), ts, type, value, 0,
      value == null ? 0 : value.remaining(), tags);
    this.length = bytes.length;
    this.offset = 0;
  }

  public KeyValue(ExtendedCell c) {
    this(c.getRowArray(), c.getRowOffset(), c.getRowLength(), c.getFamilyArray(),
      c.getFamilyOffset(), c.getFamilyLength(), c.getQualifierArray(), c.getQualifierOffset(),
      c.getQualifierLength(), c.getTimestamp(), Type.codeToType(c.getTypeByte()), c.getValueArray(),
      c.getValueOffset(), c.getValueLength(), c.getTagsArray(), c.getTagsOffset(),
      c.getTagsLength());
    this.seqId = c.getSequenceId();
  }

  /**
   * Create an empty byte[] representing a KeyValue All lengths are preset and can be filled in
   * later.
   * @param rlength   row length
   * @param flength   family length
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param vlength   value length
   * @return The newly created byte array.
   */
  private static byte[] createEmptyByteArray(final int rlength, int flength, int qlength,
    final long timestamp, final Type type, int vlength, int tagsLength) {
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }
    if (flength > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Family > " + Byte.MAX_VALUE);
    }
    // Qualifier length
    if (qlength > Integer.MAX_VALUE - rlength - flength) {
      throw new IllegalArgumentException("Qualifier > " + Integer.MAX_VALUE);
    }
    RawCell.checkForTagsLength(tagsLength);
    // Key length
    long longkeylength = getKeyDataStructureSize(rlength, flength, qlength);
    if (longkeylength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longkeylength + " > " + Integer.MAX_VALUE);
    }
    int keylength = (int) longkeylength;
    // Value length
    if (vlength > HConstants.MAXIMUM_VALUE_LENGTH) { // FindBugs INT_VACUOUS_COMPARISON
      throw new IllegalArgumentException("Valuer > " + HConstants.MAXIMUM_VALUE_LENGTH);
    }

    // Allocate right-sized byte array.
    byte[] bytes =
      new byte[(int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength, tagsLength)];
    // Write the correct size markers
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keylength);
    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short) (rlength & 0x0000ffff));
    pos += rlength;
    pos = Bytes.putByte(bytes, pos, (byte) (flength & 0x0000ff));
    pos += flength + qlength;
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    pos += vlength;
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(bytes, pos, tagsLength);
    }
    return bytes;
  }

  /**
   * Checks the parameters passed to a constructor.
   * @param row     row key
   * @param rlength row length
   * @param family  family name
   * @param flength family length
   * @param qlength qualifier length
   * @param vlength value length
   * @throws IllegalArgumentException an illegal value was passed
   */
  static void checkParameters(final byte[] row, final int rlength, final byte[] family, int flength,
    int qlength, int vlength) throws IllegalArgumentException {
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }
    if (row == null) {
      throw new IllegalArgumentException("Row is null");
    }
    // Family length
    flength = family == null ? 0 : flength;
    if (flength > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Family > " + Byte.MAX_VALUE);
    }
    // Qualifier length
    if (qlength > Integer.MAX_VALUE - rlength - flength) {
      throw new IllegalArgumentException("Qualifier > " + Integer.MAX_VALUE);
    }
    // Key length
    long longKeyLength = getKeyDataStructureSize(rlength, flength, qlength);
    if (longKeyLength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longKeyLength + " > " + Integer.MAX_VALUE);
    }
    // Value length
    if (vlength > HConstants.MAXIMUM_VALUE_LENGTH) { // FindBugs INT_VACUOUS_COMPARISON
      throw new IllegalArgumentException(
        "Value length " + vlength + " > " + HConstants.MAXIMUM_VALUE_LENGTH);
    }
  }

  /**
   * Write KeyValue format into the provided byte array.
   * @param buffer    the bytes buffer to use
   * @param boffset   buffer offset
   * @param row       row key
   * @param roffset   row offset
   * @param rlength   row length
   * @param family    family name
   * @param foffset   family offset
   * @param flength   family length
   * @param qualifier column qualifier
   * @param qoffset   qualifier offset
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @param voffset   value offset
   * @param vlength   value length
   * @return The number of useful bytes in the buffer.
   * @throws IllegalArgumentException an illegal value was passed or there is insufficient space
   *                                  remaining in the buffer
   */
  public static int writeByteArray(byte[] buffer, final int boffset, final byte[] row,
    final int roffset, final int rlength, final byte[] family, final int foffset, int flength,
    final byte[] qualifier, final int qoffset, int qlength, final long timestamp, final Type type,
    final byte[] value, final int voffset, int vlength, Tag[] tags) {

    checkParameters(row, rlength, family, flength, qlength, vlength);

    // Calculate length of tags area
    int tagsLength = 0;
    if (tags != null && tags.length > 0) {
      for (Tag t : tags) {
        tagsLength += t.getValueLength() + Tag.INFRASTRUCTURE_SIZE;
      }
    }
    RawCell.checkForTagsLength(tagsLength);
    int keyLength = (int) getKeyDataStructureSize(rlength, flength, qlength);
    int keyValueLength =
      (int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength, tagsLength);
    if (keyValueLength > buffer.length - boffset) {
      throw new IllegalArgumentException(
        "Buffer size " + (buffer.length - boffset) + " < " + keyValueLength);
    }

    // Write key, value and key row length.
    int pos = boffset;
    pos = Bytes.putInt(buffer, pos, keyLength);
    pos = Bytes.putInt(buffer, pos, vlength);
    pos = Bytes.putShort(buffer, pos, (short) (rlength & 0x0000ffff));
    pos = Bytes.putBytes(buffer, pos, row, roffset, rlength);
    pos = Bytes.putByte(buffer, pos, (byte) (flength & 0x0000ff));
    if (flength != 0) {
      pos = Bytes.putBytes(buffer, pos, family, foffset, flength);
    }
    if (qlength != 0) {
      pos = Bytes.putBytes(buffer, pos, qualifier, qoffset, qlength);
    }
    pos = Bytes.putLong(buffer, pos, timestamp);
    pos = Bytes.putByte(buffer, pos, type.getCode());
    if (value != null && value.length > 0) {
      pos = Bytes.putBytes(buffer, pos, value, voffset, vlength);
    }
    // Write the number of tags. If it is 0 then it means there are no tags.
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(buffer, pos, tagsLength);
      for (Tag t : tags) {
        int tlen = t.getValueLength();
        pos = Bytes.putAsShort(buffer, pos, tlen + Tag.TYPE_LENGTH_SIZE);
        pos = Bytes.putByte(buffer, pos, t.getType());
        Tag.copyValueTo(t, buffer, pos);
        pos += tlen;
      }
    }
    return keyValueLength;
  }

  /**
   * Write KeyValue format into a byte array.
   * @param row       row key
   * @param roffset   row offset
   * @param rlength   row length
   * @param family    family name
   * @param foffset   family offset
   * @param flength   family length
   * @param qualifier column qualifier
   * @param qoffset   qualifier offset
   * @param qlength   qualifier length
   * @param timestamp version timestamp
   * @param type      key type
   * @param value     column value
   * @param voffset   value offset
   * @param vlength   value length
   * @return The newly created byte array.
   */
  private static byte[] createByteArray(final byte[] row, final int roffset, final int rlength,
    final byte[] family, final int foffset, int flength, final byte[] qualifier, final int qoffset,
    int qlength, final long timestamp, final Type type, final byte[] value, final int voffset,
    int vlength, byte[] tags, int tagsOffset, int tagsLength) {

    checkParameters(row, rlength, family, flength, qlength, vlength);
    RawCell.checkForTagsLength(tagsLength);
    // Allocate right-sized byte array.
    int keyLength = (int) getKeyDataStructureSize(rlength, flength, qlength);
    byte[] bytes =
      new byte[(int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength, tagsLength)];
    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keyLength);
    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short) (rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    pos = Bytes.putByte(bytes, pos, (byte) (flength & 0x0000ff));
    if (flength != 0) {
      pos = Bytes.putBytes(bytes, pos, family, foffset, flength);
    }
    if (qlength != 0) {
      pos = Bytes.putBytes(bytes, pos, qualifier, qoffset, qlength);
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (value != null && value.length > 0) {
      pos = Bytes.putBytes(bytes, pos, value, voffset, vlength);
    }
    // Add the tags after the value part
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(bytes, pos, tagsLength);
      pos = Bytes.putBytes(bytes, pos, tags, tagsOffset, tagsLength);
    }
    return bytes;
  }

  /**
   * @param qualifier can be a ByteBuffer or a byte[], or null.
   * @param value     can be a ByteBuffer or a byte[], or null.
   */
  private static byte[] createByteArray(final byte[] row, final int roffset, final int rlength,
    final byte[] family, final int foffset, int flength, final Object qualifier, final int qoffset,
    int qlength, final long timestamp, final Type type, final Object value, final int voffset,
    int vlength, List<Tag> tags) {

    checkParameters(row, rlength, family, flength, qlength, vlength);

    // Calculate length of tags area
    int tagsLength = 0;
    if (tags != null && !tags.isEmpty()) {
      for (Tag t : tags) {
        tagsLength += t.getValueLength() + Tag.INFRASTRUCTURE_SIZE;
      }
    }
    RawCell.checkForTagsLength(tagsLength);
    // Allocate right-sized byte array.
    int keyLength = (int) getKeyDataStructureSize(rlength, flength, qlength);
    byte[] bytes =
      new byte[(int) getKeyValueDataStructureSize(rlength, flength, qlength, vlength, tagsLength)];

    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keyLength);

    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short) (rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    pos = Bytes.putByte(bytes, pos, (byte) (flength & 0x0000ff));
    if (flength != 0) {
      pos = Bytes.putBytes(bytes, pos, family, foffset, flength);
    }
    if (qlength > 0) {
      if (qualifier instanceof ByteBuffer) {
        pos = Bytes.putByteBuffer(bytes, pos, (ByteBuffer) qualifier);
      } else {
        pos = Bytes.putBytes(bytes, pos, (byte[]) qualifier, qoffset, qlength);
      }
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (vlength > 0) {
      if (value instanceof ByteBuffer) {
        pos = Bytes.putByteBuffer(bytes, pos, (ByteBuffer) value);
      } else {
        pos = Bytes.putBytes(bytes, pos, (byte[]) value, voffset, vlength);
      }
    }
    // Add the tags after the value part
    if (tagsLength > 0) {
      pos = Bytes.putAsShort(bytes, pos, tagsLength);
      for (Tag t : tags) {
        int tlen = t.getValueLength();
        pos = Bytes.putAsShort(bytes, pos, tlen + Tag.TYPE_LENGTH_SIZE);
        pos = Bytes.putByte(bytes, pos, t.getType());
        Tag.copyValueTo(t, bytes, pos);
        pos += tlen;
      }
    }
    return bytes;
  }

  /**
   * Needed doing 'contains' on List. Only compares the key portion, not the value.
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ExtendedCell)) {
      return false;
    }
    return PrivateCellUtil.equals(this, (ExtendedCell) other);
  }

  /**
   * In line with {@link #equals(Object)}, only uses the key portion, not the value.
   */
  @Override
  public int hashCode() {
    return calculateHashForKey(this);
  }

  private int calculateHashForKey(ExtendedCell cell) {
    // pre-calculate the 3 hashes made of byte ranges
    int rowHash = Bytes.hashCode(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
    int familyHash =
      Bytes.hashCode(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
    int qualifierHash = Bytes.hashCode(cell.getQualifierArray(), cell.getQualifierOffset(),
      cell.getQualifierLength());

    // combine the 6 sub-hashes
    int hash = 31 * rowHash + familyHash;
    hash = 31 * hash + qualifierHash;
    hash = 31 * hash + (int) cell.getTimestamp();
    hash = 31 * hash + cell.getTypeByte();
    return hash;
  }

  // ---------------------------------------------------------------------------
  //
  // KeyValue cloning
  //
  // ---------------------------------------------------------------------------

  /**
   * Clones a KeyValue. This creates a copy, re-allocating the buffer.
   * @return Fully copied clone of this KeyValue
   * @throws CloneNotSupportedException if cloning of keyValue is not supported
   */
  @Override
  public KeyValue clone() throws CloneNotSupportedException {
    KeyValue ret = (KeyValue) super.clone();
    ret.bytes = Arrays.copyOf(this.bytes, this.bytes.length);
    ret.offset = 0;
    ret.length = ret.bytes.length;
    // Important to clone the memstoreTS as well - otherwise memstore's
    // update-in-place methods (eg increment) will end up creating
    // new entries
    ret.setSequenceId(seqId);
    return ret;
  }

  /**
   * Creates a shallow copy of this KeyValue, reusing the data byte buffer.
   * http://en.wikipedia.org/wiki/Object_copy
   * @return Shallow copy of this KeyValue
   */
  public KeyValue shallowCopy() {
    KeyValue shallowCopy = new KeyValue(this.bytes, this.offset, this.length);
    shallowCopy.setSequenceId(this.seqId);
    return shallowCopy;
  }

  // ---------------------------------------------------------------------------
  //
  // String representation
  //
  // ---------------------------------------------------------------------------

  @Override
  public String toString() {
    if (this.bytes == null || this.bytes.length == 0) {
      return "empty";
    }
    return keyToString(this.bytes, this.offset + ROW_OFFSET, getKeyLength()) + "/vlen="
      + getValueLength() + "/seqid=" + seqId;
  }

  /** Return key as a String, empty string if k is null. */
  public static String keyToString(final byte[] k) {
    if (k == null) {
      return "";
    }
    return keyToString(k, 0, k.length);
  }

  /**
   * Produces a string map for this key/value pair. Useful for programmatic use and manipulation of
   * the data stored in an WALKey, for example, printing as JSON. Values are left out due to their
   * tendency to be large. If needed, they can be added manually.
   * @return the Map&lt;String,?&gt; containing data from this key
   */
  public Map<String, Object> toStringMap() {
    Map<String, Object> stringMap = new HashMap<>();
    stringMap.put("row", Bytes.toStringBinary(getRowArray(), getRowOffset(), getRowLength()));
    stringMap.put("family",
      Bytes.toStringBinary(getFamilyArray(), getFamilyOffset(), getFamilyLength()));
    stringMap.put("qualifier",
      Bytes.toStringBinary(getQualifierArray(), getQualifierOffset(), getQualifierLength()));
    stringMap.put("timestamp", getTimestamp());
    stringMap.put("vlen", getValueLength());
    Iterator<Tag> tags = getTags();
    if (tags != null) {
      List<String> tagsString = new ArrayList<String>();
      while (tags.hasNext()) {
        tagsString.add(tags.next().toString());
      }
      stringMap.put("tag", tagsString);
    }
    return stringMap;
  }

  /**
   * Use for logging.
   * @param b Key portion of a KeyValue.
   * @param o Offset to start of key
   * @param l Length of key.
   * @return Key as a String.
   */
  public static String keyToString(final byte[] b, final int o, final int l) {
    if (b == null) {
      return "";
    }
    int rowlength = Bytes.toShort(b, o);
    String row = Bytes.toStringBinary(b, o + Bytes.SIZEOF_SHORT, rowlength);
    int columnoffset = o + Bytes.SIZEOF_SHORT + 1 + rowlength;
    int familylength = b[columnoffset - 1];
    int columnlength = l - ((columnoffset - o) + TIMESTAMP_TYPE_SIZE);
    String family = familylength == 0 ? "" : Bytes.toStringBinary(b, columnoffset, familylength);
    String qualifier = columnlength == 0
      ? ""
      : Bytes.toStringBinary(b, columnoffset + familylength, columnlength - familylength);
    long timestamp = Bytes.toLong(b, o + (l - TIMESTAMP_TYPE_SIZE));
    String timestampStr = humanReadableTimestamp(timestamp);
    byte type = b[o + l - 1];
    return row + "/" + family + (family != null && family.length() > 0 ? ":" : "") + qualifier + "/"
      + timestampStr + "/" + Type.codeToType(type);
  }

  public static String humanReadableTimestamp(final long timestamp) {
    if (timestamp == HConstants.LATEST_TIMESTAMP) {
      return "LATEST_TIMESTAMP";
    }
    if (timestamp == PrivateConstants.OLDEST_TIMESTAMP) {
      return "OLDEST_TIMESTAMP";
    }
    return String.valueOf(timestamp);
  }

  // ---------------------------------------------------------------------------
  //
  // Public Member Accessors
  //
  // ---------------------------------------------------------------------------

  /**
   * To be used only in tests where the Cells are clearly assumed to be of type KeyValue and that we
   * need access to the backing array to do some test case related assertions.
   * @return The byte array backing this KeyValue.
   */
  public byte[] getBuffer() {
    return this.bytes;
  }

  /** Returns Offset into {@link #getBuffer()} at which this KeyValue starts. */
  public int getOffset() {
    return this.offset;
  }

  /** Returns Length of bytes this KeyValue occupies in {@link #getBuffer()}. */
  public int getLength() {
    return length;
  }

  // ---------------------------------------------------------------------------
  //
  // Length and Offset Calculators
  //
  // ---------------------------------------------------------------------------

  /**
   * Determines the total length of the KeyValue stored in the specified byte array and offset.
   * Includes all headers.
   * @param bytes  byte array
   * @param offset offset to start of the KeyValue
   * @return length of entire KeyValue, in bytes
   */
  private static int getLength(byte[] bytes, int offset) {
    int klength = ROW_OFFSET + Bytes.toInt(bytes, offset);
    int vlength = Bytes.toInt(bytes, offset + Bytes.SIZEOF_INT);
    return klength + vlength;
  }

  /** Returns Key offset in backing buffer.. */
  public int getKeyOffset() {
    return this.offset + ROW_OFFSET;
  }

  public String getKeyString() {
    return Bytes.toStringBinary(getBuffer(), getKeyOffset(), getKeyLength());
  }

  /** Returns Length of key portion. */
  public int getKeyLength() {
    return Bytes.toInt(this.bytes, this.offset);
  }

  /**
   * Returns the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getValueArray() {
    return bytes;
  }

  /** Returns the value offset */
  @Override
  public int getValueOffset() {
    int voffset = getKeyOffset() + getKeyLength();
    return voffset;
  }

  /** Returns Value length */
  @Override
  public int getValueLength() {
    int vlength = Bytes.toInt(this.bytes, this.offset + Bytes.SIZEOF_INT);
    return vlength;
  }

  /**
   * Returns the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getRowArray() {
    return bytes;
  }

  /** Returns Row offset */
  @Override
  public int getRowOffset() {
    return this.offset + ROW_KEY_OFFSET;
  }

  /** Returns Row length */
  @Override
  public short getRowLength() {
    return Bytes.toShort(this.bytes, getKeyOffset());
  }

  /**
   * Returns the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getFamilyArray() {
    return bytes;
  }

  /** Returns Family offset */
  @Override
  public int getFamilyOffset() {
    return getFamilyOffset(getFamilyLengthPosition(getRowLength()));
  }

  /** Returns Family offset */
  int getFamilyOffset(int familyLenPosition) {
    return familyLenPosition + Bytes.SIZEOF_BYTE;
  }

  /** Returns Family length */
  @Override
  public byte getFamilyLength() {
    return getFamilyLength(getFamilyLengthPosition(getRowLength()));
  }

  /** Returns Family length */
  public byte getFamilyLength(int famLenPos) {
    return this.bytes[famLenPos];
  }

  int getFamilyLengthPosition(int rowLength) {
    return this.offset + KeyValue.ROW_KEY_OFFSET + rowLength;
  }

  /**
   * Returns the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getQualifierArray() {
    return bytes;
  }

  /** Returns Qualifier offset */
  @Override
  public int getQualifierOffset() {
    return getQualifierOffset(getFamilyOffset());
  }

  /** Returns Qualifier offset */
  private int getQualifierOffset(int foffset) {
    return getQualifierOffset(foffset, getFamilyLength());
  }

  /** Returns Qualifier offset */
  int getQualifierOffset(int foffset, int flength) {
    return foffset + flength;
  }

  /** Returns Qualifier length */
  @Override
  public int getQualifierLength() {
    return getQualifierLength(getRowLength(), getFamilyLength());
  }

  /** Returns Qualifier length */
  private int getQualifierLength(int rlength, int flength) {
    return getQualifierLength(getKeyLength(), rlength, flength);
  }

  /** Returns Qualifier length */
  int getQualifierLength(int keyLength, int rlength, int flength) {
    return keyLength - (int) getKeyDataStructureSize(rlength, flength, 0);
  }

  /** Returns Timestamp offset */
  public int getTimestampOffset() {
    return getTimestampOffset(getKeyLength());
  }

  /** Return the timestamp offset */
  private int getTimestampOffset(final int keylength) {
    return getKeyOffset() + keylength - TIMESTAMP_TYPE_SIZE;
  }

  /** Returns True if this KeyValue has a LATEST_TIMESTAMP timestamp. */
  public boolean isLatestTimestamp() {
    return Bytes.equals(getBuffer(), getTimestampOffset(), Bytes.SIZEOF_LONG,
      HConstants.LATEST_TIMESTAMP_BYTES, 0, Bytes.SIZEOF_LONG);
  }

  /**
   * Update the timestamp.
   * @param now Time to set into <code>this</code> IFF timestamp ==
   *            {@link HConstants#LATEST_TIMESTAMP} (else, its a noop).
   * @return True is we modified this.
   */
  public boolean updateLatestStamp(final byte[] now) {
    if (this.isLatestTimestamp()) {
      int tsOffset = getTimestampOffset();
      System.arraycopy(now, 0, this.bytes, tsOffset, Bytes.SIZEOF_LONG);
      // clear cache or else getTimestamp() possibly returns an old value
      return true;
    }
    return false;
  }

  @Override
  public void setTimestamp(long ts) {
    Bytes.putBytes(this.bytes, this.getTimestampOffset(), Bytes.toBytes(ts), 0, Bytes.SIZEOF_LONG);
  }

  @Override
  public void setTimestamp(byte[] ts) {
    Bytes.putBytes(this.bytes, this.getTimestampOffset(), ts, 0, Bytes.SIZEOF_LONG);
  }

  // ---------------------------------------------------------------------------
  //
  // Methods that return copies of fields
  //
  // ---------------------------------------------------------------------------

  /**
   * Do not use unless you have to. Used internally for compacting and testing. Use
   * {@link #getRowArray()}, {@link #getFamilyArray()}, {@link #getQualifierArray()}, and
   * {@link #getValueArray()} if accessing a KeyValue client-side.
   * @return Copy of the key portion only.
   */
  public byte[] getKey() {
    int keylength = getKeyLength();
    byte[] key = new byte[keylength];
    System.arraycopy(getBuffer(), getKeyOffset(), key, 0, keylength);
    return key;
  }

  /** Return the timestamp. */
  @Override
  public long getTimestamp() {
    return getTimestamp(getKeyLength());
  }

  /** Return the timestamp. */
  long getTimestamp(final int keylength) {
    int tsOffset = getTimestampOffset(keylength);
    return Bytes.toLong(this.bytes, tsOffset);
  }

  /** Returns KeyValue.TYPE byte representation */
  @Override
  public byte getTypeByte() {
    return getTypeByte(getKeyLength());
  }

  /** Return the KeyValue.TYPE byte representation */
  byte getTypeByte(int keyLength) {
    return this.bytes[this.offset + keyLength - 1 + ROW_OFFSET];
  }

  /** Return the offset where the tag data starts. */
  @Override
  public int getTagsOffset() {
    int tagsLen = getTagsLength();
    if (tagsLen == 0) {
      return this.offset + this.length;
    }
    return this.offset + this.length - tagsLen;
  }

  /** Return the total length of the tag bytes */
  @Override
  public int getTagsLength() {
    int tagsLen = this.length - (getKeyLength() + getValueLength() + KEYVALUE_INFRASTRUCTURE_SIZE);
    if (tagsLen > 0) {
      // There are some Tag bytes in the byte[]. So reduce 2 bytes which is added to denote the tags
      // length
      tagsLen -= TAGS_LENGTH_SIZE;
    }
    return tagsLen;
  }

  /**
   * Returns the backing array of the entire KeyValue (all KeyValue fields are in a single array)
   */
  @Override
  public byte[] getTagsArray() {
    return bytes;
  }

  /**
   * Creates a new KeyValue that only contains the key portion (the value is set to be null). TODO
   * only used by KeyOnlyFilter -- move there.
   * @param lenAsVal replace value with the actual value length (false=empty)
   */
  public KeyValue createKeyOnly(boolean lenAsVal) {
    // KV format: <keylen:4><valuelen:4><key:keylen><value:valuelen>
    // Rebuild as: <keylen:4><0:4><key:keylen>
    int dataLen = lenAsVal ? Bytes.SIZEOF_INT : 0;
    byte[] newBuffer = new byte[getKeyLength() + ROW_OFFSET + dataLen];
    System.arraycopy(this.bytes, this.offset, newBuffer, 0,
      Math.min(newBuffer.length, this.length));
    Bytes.putInt(newBuffer, Bytes.SIZEOF_INT, dataLen);
    if (lenAsVal) {
      Bytes.putInt(newBuffer, newBuffer.length - dataLen, this.getValueLength());
    }
    return new KeyValue(newBuffer);
  }

  /**
   * Find index of passed delimiter walking from start of buffer forwards.
   * @param b         the kv serialized byte[] to process
   * @param delimiter input delimeter to fetch index from start
   * @return Index of delimiter having started from start of <code>b</code> moving rightward.
   */
  public static int getDelimiter(final byte[] b, int offset, final int length,
    final int delimiter) {
    if (b == null) {
      throw new IllegalArgumentException("Passed buffer is null");
    }
    int result = -1;
    for (int i = offset; i < length + offset; i++) {
      if (b[i] == delimiter) {
        result = i;
        break;
      }
    }
    return result;
  }

  /**
   * Find index of passed delimiter walking from end of buffer backwards.
   * @param b         the kv serialized byte[] to process
   * @param offset    the offset in the byte[]
   * @param length    the length in the byte[]
   * @param delimiter input delimeter to fetch index from end
   * @return Index of delimiter
   */
  public static int getDelimiterInReverse(final byte[] b, final int offset, final int length,
    final int delimiter) {
    if (b == null) {
      throw new IllegalArgumentException("Passed buffer is null");
    }
    int result = -1;
    for (int i = (offset + length) - 1; i >= offset; i--) {
      if (b[i] == delimiter) {
        result = i;
        break;
      }
    }
    return result;
  }

  /**
   * Create a KeyValue reading from <code>in</code>
   * @param in Where to read bytes from. Creates a byte array to hold the KeyValue backing bytes
   *           copied from the steam.
   * @return KeyValue created by deserializing from <code>in</code> OR if we find a length of zero,
   *         we will return null which can be useful marking a stream as done.
   * @throws IOException if any IO error happen
   */
  public static KeyValue create(final DataInput in) throws IOException {
    return create(in.readInt(), in);
  }

  /**
   * Create a KeyValue reading <code>length</code> from <code>in</code>
   * @param length length of the Key
   * @param in     Input to read from
   * @return Created KeyValue OR if we find a length of zero, we will return null which can be
   *         useful marking a stream as done.
   * @throws IOException if any IO error happen
   */
  public static KeyValue create(int length, final DataInput in) throws IOException {

    if (length <= 0) {
      if (length == 0) {
        return null;
      }
      throw new IOException("Failed read " + length + " bytes, stream corrupt?");
    }

    // This is how the old Writables.readFrom used to deserialize. Didn't even vint.
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return new KeyValue(bytes, 0, length);
  }

  /**
   * Write out a KeyValue in the manner in which we used to when KeyValue was a Writable.
   * @param kv  the KeyValue on which write is being requested
   * @param out OutputStream to write keyValue to
   * @return Length written on stream
   * @throws IOException if any IO error happen
   * @see #create(DataInput) for the inverse function
   */
  public static long write(final KeyValue kv, final DataOutput out) throws IOException {
    // This is how the old Writables write used to serialize KVs. Need to figure way to make it
    // work for all implementations.
    int length = kv.getLength();
    out.writeInt(length);
    out.write(kv.getBuffer(), kv.getOffset(), length);
    return (long) length + Bytes.SIZEOF_INT;
  }

  @Override
  public int write(OutputStream out, boolean withTags) throws IOException {
    int len = getSerializedSize(withTags);
    out.write(this.bytes, this.offset, len);
    return len;
  }

  @Override
  public int getSerializedSize(boolean withTags) {
    if (withTags) {
      return this.length;
    }
    return this.getKeyLength() + this.getValueLength() + KEYVALUE_INFRASTRUCTURE_SIZE;
  }

  @Override
  public int getSerializedSize() {
    return this.length;
  }

  @Override
  public void write(ByteBuffer buf, int offset) {
    ByteBufferUtils.copyFromArrayToBuffer(buf, offset, this.bytes, this.offset, this.length);
  }

  /**
   * Avoids redundant comparisons for better performance. TODO get rid of this wart
   */
  public interface SamePrefixComparator<T> {
    /**
     * Compare two keys assuming that the first n bytes are the same.
     * @param commonPrefix How many bytes are the same.
     */
    int compareIgnoringPrefix(int commonPrefix, byte[] left, int loffset, int llength, byte[] right,
      int roffset, int rlength);
  }

  /**
   * HeapSize implementation
   * <p/>
   * We do not count the bytes in the rowCache because it should be empty for a KeyValue in the
   * MemStore.
   */
  @Override
  public long heapSize() {
    // Deep object overhead for this KV consists of two parts. The first part is the KV object
    // itself, while the second part is the backing byte[]. We will only count the array overhead
    // from the byte[] only if this is the first KV in there.
    int fixed = ClassSize.align(FIXED_OVERHEAD);
    if (offset == 0) {
      // count both length and object overhead
      return fixed + ClassSize.sizeOfByteArray(length);
    } else {
      // only count the number of bytes
      return (long) fixed + length;
    }
  }

  /**
   * A simple form of KeyValue that creates a keyvalue with only the key part of the byte[] Mainly
   * used in places where we need to compare two cells. Avoids copying of bytes In places like block
   * index keys, we need to compare the key byte[] with a cell. Hence create a Keyvalue(aka Cell)
   * that would help in comparing as two cells
   */
  public static class KeyOnlyKeyValue extends KeyValue {
    private short rowLen = -1;

    public KeyOnlyKeyValue() {

    }

    public KeyOnlyKeyValue(byte[] b) {
      this(b, 0, b.length);
    }

    public KeyOnlyKeyValue(byte[] b, int offset, int length) {
      this.bytes = b;
      this.length = length;
      this.offset = offset;
      this.rowLen = Bytes.toShort(this.bytes, this.offset);
    }

    public void set(KeyOnlyKeyValue keyOnlyKeyValue) {
      this.bytes = keyOnlyKeyValue.bytes;
      this.length = keyOnlyKeyValue.length;
      this.offset = keyOnlyKeyValue.offset;
      this.rowLen = keyOnlyKeyValue.rowLen;
    }

    public void clear() {
      rowLen = -1;
      bytes = null;
      offset = 0;
      length = 0;
    }

    @Override
    public int getKeyOffset() {
      return this.offset;
    }

    /**
     * A setter that helps to avoid object creation every time and whenever there is a need to
     * create new KeyOnlyKeyValue.
     * @param key    Key to set
     * @param offset Offset of the Key
     * @param length length of the Key
     */
    public void setKey(byte[] key, int offset, int length) {
      this.bytes = key;
      this.offset = offset;
      this.length = length;
      this.rowLen = Bytes.toShort(this.bytes, this.offset);
    }

    @Override
    public byte[] getKey() {
      int keylength = getKeyLength();
      byte[] key = new byte[keylength];
      System.arraycopy(this.bytes, getKeyOffset(), key, 0, keylength);
      return key;
    }

    @Override
    public byte[] getRowArray() {
      return bytes;
    }

    @Override
    public int getRowOffset() {
      return getKeyOffset() + Bytes.SIZEOF_SHORT;
    }

    @Override
    public byte[] getFamilyArray() {
      return bytes;
    }

    @Override
    public byte getFamilyLength() {
      return this.bytes[getFamilyOffset() - 1];
    }

    @Override
    int getFamilyLengthPosition(int rowLength) {
      return this.offset + Bytes.SIZEOF_SHORT + rowLength;
    }

    @Override
    public int getFamilyOffset() {
      return this.offset + Bytes.SIZEOF_SHORT + getRowLength() + Bytes.SIZEOF_BYTE;
    }

    @Override
    public byte[] getQualifierArray() {
      return bytes;
    }

    @Override
    public int getQualifierLength() {
      return getQualifierLength(getRowLength(), getFamilyLength());
    }

    @Override
    public int getQualifierOffset() {
      return getFamilyOffset() + getFamilyLength();
    }

    @Override
    public int getKeyLength() {
      return length;
    }

    @Override
    public short getRowLength() {
      return rowLen;
    }

    @Override
    public byte getTypeByte() {
      return getTypeByte(getKeyLength());
    }

    @Override
    byte getTypeByte(int keyLength) {
      return this.bytes[this.offset + keyLength - 1];
    }

    private int getQualifierLength(int rlength, int flength) {
      return getKeyLength() - (int) getKeyDataStructureSize(rlength, flength, 0);
    }

    @Override
    public long getTimestamp() {
      int tsOffset = getTimestampOffset();
      return Bytes.toLong(this.bytes, tsOffset);
    }

    @Override
    public int getTimestampOffset() {
      return getKeyOffset() + getKeyLength() - TIMESTAMP_TYPE_SIZE;
    }

    @Override
    public byte[] getTagsArray() {
      return HConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public int getTagsOffset() {
      return 0;
    }

    @Override
    public byte[] getValueArray() {
      throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
    }

    @Override
    public int getValueOffset() {
      throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
    }

    @Override
    public int getValueLength() {
      throw new IllegalArgumentException("KeyOnlyKeyValue does not work with values.");
    }

    @Override
    public int getTagsLength() {
      return 0;
    }

    @Override
    public String toString() {
      if (this.bytes == null || this.bytes.length == 0) {
        return "empty";
      }
      return keyToString(this.bytes, this.offset, getKeyLength()) + "/vlen=0/mvcc=0";
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object other) {
      return super.equals(other);
    }

    @Override
    public long heapSize() {
      return super.heapSize() + Bytes.SIZEOF_SHORT;
    }

    @Override
    public int write(OutputStream out, boolean withTags) throws IOException {
      // This type of Cell is used only to maintain some internal states. We never allow this type
      // of Cell to be returned back over the RPC
      throw new IllegalStateException("A reader should never return this type of a Cell");
    }
  }

  @Override
  public ExtendedCell deepClone() {
    byte[] copy = Bytes.copy(this.bytes, this.offset, this.length);
    KeyValue kv = new KeyValue(copy, 0, copy.length);
    kv.setSequenceId(this.getSequenceId());
    return kv;
  }
}
