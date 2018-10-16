/**
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
package org.apache.hadoop.hbase.client;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.ByteArrayHashKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashKey;
import org.apache.hadoop.hbase.util.JenkinsHash;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

/**
 * Information about a region. A region is a range of keys in the whole keyspace
 * of a table, an identifier (a timestamp) for differentiating between subset
 * ranges (after region split) and a replicaId for differentiating the instance
 * for the same range and some status information about the region.
 *
 * The region has a unique name which consists of the following fields:
 * <ul>
 * <li> tableName   : The name of the table </li>
 * <li> startKey    : The startKey for the region. </li>
 * <li> regionId    : A timestamp when the region is created. </li>
 * <li> replicaId   : An id starting from 0 to differentiate replicas of the
 * same region range but hosted in separated servers. The same region range can
 * be hosted in multiple locations.</li>
 * <li> encodedName : An MD5 encoded string for the region name.</li>
 * </ul>
 *
 * <br> Other than the fields in the region name, region info contains:
 * <ul>
 * <li> endKey      : the endKey for the region (exclusive) </li>
 * <li> split       : Whether the region is split </li>
 * <li> offline     : Whether the region is offline </li>
 * </ul>
 *
 */
@InterfaceAudience.Public
public interface RegionInfo {
  /**
   * Separator used to demarcate the encodedName in a region name
   * in the new format. See description on new format above.
   */
  @InterfaceAudience.Private
  int ENC_SEPARATOR = '.';

  @InterfaceAudience.Private
  int MD5_HEX_LENGTH = 32;

  @InterfaceAudience.Private
  int DEFAULT_REPLICA_ID = 0;

  /**
   * to keep appended int's sorted in string format. Only allows 2 bytes
   * to be sorted for replicaId.
   */
  @InterfaceAudience.Private
  String REPLICA_ID_FORMAT = "%04X";

  @InterfaceAudience.Private
  byte REPLICA_ID_DELIMITER = (byte)'_';

  @InterfaceAudience.Private
  String INVALID_REGION_NAME_FORMAT_MESSAGE = "Invalid regionName format";

  @InterfaceAudience.Private
  Comparator<RegionInfo> COMPARATOR
    = (RegionInfo lhs, RegionInfo rhs) -> {
      if (rhs == null) {
        return 1;
      }

      // Are regions of same table?
      int result = lhs.getTable().compareTo(rhs.getTable());
      if (result != 0) {
        return result;
      }

      // Compare start keys.
      result = Bytes.compareTo(lhs.getStartKey(), rhs.getStartKey());
      if (result != 0) {
        return result;
      }

      // Compare end keys.
      result = Bytes.compareTo(lhs.getEndKey(), rhs.getEndKey());

      if (result != 0) {
        if (lhs.getStartKey().length != 0
                && lhs.getEndKey().length == 0) {
            return 1; // this is last region
        }
        if (rhs.getStartKey().length != 0
                && rhs.getEndKey().length == 0) {
            return -1; // o is the last region
        }
        return result;
      }

      // regionId is usually milli timestamp -- this defines older stamps
      // to be "smaller" than newer stamps in sort order.
      if (lhs.getRegionId() > rhs.getRegionId()) {
        return 1;
      } else if (lhs.getRegionId() < rhs.getRegionId()) {
        return -1;
      }

      int replicaDiff = lhs.getReplicaId() - rhs.getReplicaId();
      if (replicaDiff != 0) return replicaDiff;

      if (lhs.isOffline() == rhs.isOffline())
        return 0;
      if (lhs.isOffline() == true) return -1;

      return 1;
  };


  /**
   * @return Return a short, printable name for this region
   * (usually encoded name) for us logging.
   */
  String getShortNameToLog();

  /**
   * @return the regionId.
   */
  long getRegionId();

  /**
   * @return the regionName as an array of bytes.
   * @see #getRegionNameAsString()
   */
  byte [] getRegionName();

  /**
   * @return Region name as a String for use in logging, etc.
   */
  String getRegionNameAsString();

  /**
   * @return the encoded region name.
   */
  String getEncodedName();

  /**
   * @return the encoded region name as an array of bytes.
   */
  byte [] getEncodedNameAsBytes();

  /**
   * @return the startKey.
   */
  byte [] getStartKey();

  /**
   * @return the endKey.
   */
  byte [] getEndKey();

  /**
   * @return current table name of the region
   */
  TableName getTable();

  /**
   * @return returns region replica id
   */
  int getReplicaId();

  /**
   * @return True if has been split and has daughters.
   */
  boolean isSplit();

  /**
   * @return True if this region is offline.
   */
  boolean isOffline();

  /**
   * @return True if this is a split parent region.
   */
  boolean isSplitParent();

  /**
   * @return true if this region is a meta region.
   */
  boolean isMetaRegion();

  /**
   * @param rangeStartKey
   * @param rangeEndKey
   * @return true if the given inclusive range of rows is fully contained
   * by this region. For example, if the region is foo,a,g and this is
   * passed ["b","c"] or ["a","c"] it will return true, but if this is passed
   * ["b","z"] it will return false.
   * @throws IllegalArgumentException if the range passed is invalid (ie. end &lt; start)
   */
  boolean containsRange(byte[] rangeStartKey, byte[] rangeEndKey);

  /**
   * @param row
   * @return true if the given row falls in this region.
   */
  boolean containsRow(byte[] row);

  /**
   * Does region name contain its encoded name?
   * @param regionName region name
   * @return boolean indicating if this a new format region
   *         name which contains its encoded name.
   */
  @InterfaceAudience.Private
  static boolean hasEncodedName(final byte[] regionName) {
    // check if region name ends in ENC_SEPARATOR
    return (regionName.length >= 1) &&
      (regionName[regionName.length - 1] == RegionInfo.ENC_SEPARATOR);
  }

  /**
   * @return the encodedName
   */
  @InterfaceAudience.Private
  static String encodeRegionName(final byte [] regionName) {
    String encodedName;
    if (hasEncodedName(regionName)) {
      // region is in new format:
      // <tableName>,<startKey>,<regionIdTimeStamp>/encodedName/
      encodedName = Bytes.toString(regionName,
      regionName.length - MD5_HEX_LENGTH - 1,
      MD5_HEX_LENGTH);
    } else {
      // old format region name. First hbase:meta region also
      // use this format.EncodedName is the JenkinsHash value.
      HashKey<byte[]> key = new ByteArrayHashKey(regionName, 0, regionName.length);
      int hashVal = Math.abs(JenkinsHash.getInstance().hash(key, 0));
      encodedName = String.valueOf(hashVal);
    }
    return encodedName;
  }

  /**
   * @return Return a String of short, printable names for <code>hris</code>
   * (usually encoded name) for us logging.
   */
  static String getShortNameToLog(RegionInfo...hris) {
    return getShortNameToLog(Arrays.asList(hris));
  }

  /**
   * @return Return a String of short, printable names for <code>hris</code>
   * (usually encoded name) for us logging.
   */
  static String getShortNameToLog(final List<RegionInfo> ris) {
    return ris.stream().map(ri -> ri.getShortNameToLog()).
    collect(Collectors.toList()).toString();
  }

  /**
   * Gets the table name from the specified region name.
   * @param regionName to extract the table name from
   * @return Table name
   */
  @InterfaceAudience.Private
  // This method should never be used. Its awful doing parse from bytes.
  // It is fallback in case we can't get the tablename any other way. Could try removing it.
  // Keeping it Audience Private so can remove at later date.
  static TableName getTable(final byte [] regionName) {
    int offset = -1;
    for (int i = 0; i < regionName.length; i++) {
      if (regionName[i] == HConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    if (offset <= 0) {
      throw new IllegalArgumentException("offset=" + offset);
    }
    byte[] buff  = new byte[offset];
    System.arraycopy(regionName, 0, buff, 0, offset);
    return TableName.valueOf(buff);
  }

  /**
   * Gets the start key from the specified region name.
   * @param regionName
   * @return Start key.
   * @throws java.io.IOException
   */
  static byte[] getStartKey(final byte[] regionName) throws IOException {
    return parseRegionName(regionName)[1];
  }

  @InterfaceAudience.Private
  static boolean isEncodedRegionName(byte[] regionName) throws IOException {
    try {
      parseRegionName(regionName);
      return false;
    } catch (IOException e) {
      if (StringUtils.stringifyException(e)
      .contains(INVALID_REGION_NAME_FORMAT_MESSAGE)) {
        return true;
      }
      throw e;
    }
  }

  /**
   * @param bytes
   * @return A deserialized {@link RegionInfo}
   * or null if we failed deserialize or passed bytes null
   */
  @InterfaceAudience.Private
  static RegionInfo parseFromOrNull(final byte [] bytes) {
    if (bytes == null) return null;
    return parseFromOrNull(bytes, 0, bytes.length);
  }

  /**
   * @param bytes
   * @param offset
   * @param len
   * @return A deserialized {@link RegionInfo} or null
   *  if we failed deserialize or passed bytes null
   */
  @InterfaceAudience.Private
  static RegionInfo parseFromOrNull(final byte [] bytes, int offset, int len) {
    if (bytes == null || len <= 0) return null;
    try {
      return parseFrom(bytes, offset, len);
    } catch (DeserializationException e) {
      return null;
    }
  }

  /**
   * @param bytes A pb RegionInfo serialized with a pb magic prefix.
   * @return A deserialized {@link RegionInfo}
   * @throws DeserializationException
   */
  @InterfaceAudience.Private
  static RegionInfo parseFrom(final byte [] bytes) throws DeserializationException {
    if (bytes == null) return null;
    return parseFrom(bytes, 0, bytes.length);
  }

  /**
   * @param bytes A pb RegionInfo serialized with a pb magic prefix.
   * @param offset starting point in the byte array
   * @param len length to read on the byte array
   * @return A deserialized {@link RegionInfo}
   * @throws DeserializationException
   */
  @InterfaceAudience.Private
  static RegionInfo parseFrom(final byte [] bytes, int offset, int len)
  throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes, offset, len)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        HBaseProtos.RegionInfo.Builder builder = HBaseProtos.RegionInfo.newBuilder();
        ProtobufUtil.mergeFrom(builder, bytes, pblen + offset, len - pblen);
        HBaseProtos.RegionInfo ri = builder.build();
        return ProtobufUtil.toRegionInfo(ri);
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    } else {
      throw new DeserializationException("PB encoded RegionInfo expected");
    }
  }

  /**
   * Check whether two regions are adjacent
   * @param regionA
   * @param regionB
   * @return true if two regions are adjacent
   */
  static boolean areAdjacent(RegionInfo regionA, RegionInfo regionB) {
    if (regionA == null || regionB == null) {
      throw new IllegalArgumentException(
      "Can't check whether adjacent for null region");
    }
    RegionInfo a = regionA;
    RegionInfo b = regionB;
    if (Bytes.compareTo(a.getStartKey(), b.getStartKey()) > 0) {
      a = regionB;
      b = regionA;
    }
    if (Bytes.compareTo(a.getEndKey(), b.getStartKey()) == 0) {
      return true;
    }
    return false;
  }

  /**
   * @param ri
   * @return This instance serialized as protobuf w/ a magic pb prefix.
   * @see #parseFrom(byte[])
   */
  static byte [] toByteArray(RegionInfo ri) {
    byte [] bytes = ProtobufUtil.toRegionInfo(ri).toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  /**
   * Use logging.
   * @param encodedRegionName The encoded regionname.
   * @return <code>hbase:meta</code> if passed <code>1028785192</code> else returns
   * <code>encodedRegionName</code>
   */
  static String prettyPrint(final String encodedRegionName) {
    if (encodedRegionName.equals("1028785192")) {
      return encodedRegionName + "/hbase:meta";
    }
    return encodedRegionName;
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param regionid Region id (Usually timestamp from when region was created).
   * @param newFormat should we create the region name in the new format
   *                  (such that it contains its encoded name?).
   * @return Region name made of passed tableName, startKey and id
   */
  static byte [] createRegionName(final TableName tableName, final byte[] startKey,
                                  final long regionid, boolean newFormat) {
    return createRegionName(tableName, startKey, Long.toString(regionid), newFormat);
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id (Usually timestamp from when region was created).
   * @param newFormat should we create the region name in the new format
   *                  (such that it contains its encoded name?).
   * @return Region name made of passed tableName, startKey and id
   */
  static byte [] createRegionName(final TableName tableName,
                                  final byte[] startKey, final String id, boolean newFormat) {
    return createRegionName(tableName, startKey, Bytes.toBytes(id), newFormat);
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param regionid Region id (Usually timestamp from when region was created).
   * @param replicaId
   * @param newFormat should we create the region name in the new format
   *                  (such that it contains its encoded name?).
   * @return Region name made of passed tableName, startKey, id and replicaId
   */
  static byte [] createRegionName(final TableName tableName,
      final byte[] startKey, final long regionid, int replicaId, boolean newFormat) {
    return createRegionName(tableName, startKey, Bytes.toBytes(Long.toString(regionid)),
      replicaId, newFormat);
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id (Usually timestamp from when region was created).
   * @param newFormat should we create the region name in the new format
   *                  (such that it contains its encoded name?).
   * @return Region name made of passed tableName, startKey and id
   */
  static byte [] createRegionName(final TableName tableName,
      final byte[] startKey, final byte[] id, boolean newFormat) {
    return createRegionName(tableName, startKey, id, DEFAULT_REPLICA_ID, newFormat);
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id (Usually timestamp from when region was created).
   * @param replicaId
   * @param newFormat should we create the region name in the new format
   * @return Region name made of passed tableName, startKey, id and replicaId
   */
  static byte [] createRegionName(final TableName tableName,
      final byte[] startKey, final byte[] id, final int replicaId, boolean newFormat) {
    int len = tableName.getName().length + 2 + id.length + (startKey == null? 0: startKey.length);
    if (newFormat) {
      len += MD5_HEX_LENGTH + 2;
    }
    byte[] replicaIdBytes = null;
    // Special casing: replicaId is only appended if replicaId is greater than
    // 0. This is because all regions in meta would have to be migrated to the new
    // name otherwise
    if (replicaId > 0) {
      // use string representation for replica id
      replicaIdBytes = Bytes.toBytes(String.format(REPLICA_ID_FORMAT, replicaId));
      len += 1 + replicaIdBytes.length;
    }

    byte [] b = new byte [len];

    int offset = tableName.getName().length;
    System.arraycopy(tableName.getName(), 0, b, 0, offset);
    b[offset++] = HConstants.DELIMITER;
    if (startKey != null && startKey.length > 0) {
      System.arraycopy(startKey, 0, b, offset, startKey.length);
      offset += startKey.length;
    }
    b[offset++] = HConstants.DELIMITER;
    System.arraycopy(id, 0, b, offset, id.length);
    offset += id.length;

    if (replicaIdBytes != null) {
      b[offset++] = REPLICA_ID_DELIMITER;
      System.arraycopy(replicaIdBytes, 0, b, offset, replicaIdBytes.length);
      offset += replicaIdBytes.length;
    }

    if (newFormat) {
      //
      // Encoded name should be built into the region name.
      //
      // Use the region name thus far (namely, <tablename>,<startKey>,<id>_<replicaId>)
      // to compute a MD5 hash to be used as the encoded name, and append
      // it to the byte buffer.
      //
      String md5Hash = MD5Hash.getMD5AsHex(b, 0, offset);
      byte [] md5HashBytes = Bytes.toBytes(md5Hash);

      if (md5HashBytes.length != MD5_HEX_LENGTH) {
        System.out.println("MD5-hash length mismatch: Expected=" + MD5_HEX_LENGTH +
        "; Got=" + md5HashBytes.length);
      }

      // now append the bytes '.<encodedName>.' to the end
      b[offset++] = ENC_SEPARATOR;
      System.arraycopy(md5HashBytes, 0, b, offset, MD5_HEX_LENGTH);
      offset += MD5_HEX_LENGTH;
      b[offset++] = ENC_SEPARATOR;
    }

    return b;
  }

  /**
   * Creates a RegionInfo object for MOB data.
   *
   * @param tableName the name of the table
   * @return the MOB {@link RegionInfo}.
   */
  static RegionInfo createMobRegionInfo(TableName tableName) {
    return RegionInfoBuilder.newBuilder(tableName)
        .setStartKey(Bytes.toBytes(".mob")).setRegionId(0).build();
  }

  /**
   * Separate elements of a regionName.
   * @param regionName
   * @return Array of byte[] containing tableName, startKey and id
   * @throws IOException
   */
  static byte [][] parseRegionName(final byte[] regionName)
  throws IOException {
    // Region name is of the format:
    // tablename,startkey,regionIdTimestamp[_replicaId][.encodedName.]
    // startkey can contain the delimiter (',') so we parse from the start and end

    // parse from start
    int offset = -1;
    for (int i = 0; i < regionName.length; i++) {
      if (regionName[i] == HConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    if (offset == -1) {
      throw new IOException(INVALID_REGION_NAME_FORMAT_MESSAGE
      + ": " + Bytes.toStringBinary(regionName));
    }
    byte[] tableName = new byte[offset];
    System.arraycopy(regionName, 0, tableName, 0, offset);
    offset = -1;

    int endOffset = regionName.length;
    // check whether regionName contains encodedName
    if (regionName.length > MD5_HEX_LENGTH + 2
    && regionName[regionName.length-1] == ENC_SEPARATOR
    && regionName[regionName.length-MD5_HEX_LENGTH-2] == ENC_SEPARATOR) {
      endOffset = endOffset - MD5_HEX_LENGTH - 2;
    }

    // parse from end
    byte[] replicaId = null;
    int idEndOffset = endOffset;
    for (int i = endOffset - 1; i > 0; i--) {
      if (regionName[i] == REPLICA_ID_DELIMITER) { //replicaId may or may not be present
        replicaId = new byte[endOffset - i - 1];
        System.arraycopy(regionName, i + 1, replicaId, 0,
        endOffset - i - 1);
        idEndOffset = i;
        // do not break, continue to search for id
      }
      if (regionName[i] == HConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    if (offset == -1) {
      throw new IOException(INVALID_REGION_NAME_FORMAT_MESSAGE
      + ": " + Bytes.toStringBinary(regionName));
    }
    byte [] startKey = HConstants.EMPTY_BYTE_ARRAY;
    if(offset != tableName.length + 1) {
      startKey = new byte[offset - tableName.length - 1];
      System.arraycopy(regionName, tableName.length + 1, startKey, 0,
      offset - tableName.length - 1);
    }
    byte [] id = new byte[idEndOffset - offset - 1];
    System.arraycopy(regionName, offset + 1, id, 0,
    idEndOffset - offset - 1);
    byte [][] elements = new byte[replicaId == null ? 3 : 4][];
    elements[0] = tableName;
    elements[1] = startKey;
    elements[2] = id;
    if (replicaId != null) {
      elements[3] = replicaId;
    }
    return elements;
  }

  /**
   * Serializes given RegionInfo's as a byte array. Use this instead of
   * {@link RegionInfo#toByteArray(RegionInfo)} when
   * writing to a stream and you want to use the pb mergeDelimitedFrom (w/o the delimiter, pb reads
   * to EOF which may not be what you want). {@link #parseDelimitedFrom(byte[], int, int)} can
   * be used to read back the instances.
   * @param infos RegionInfo objects to serialize
   * @return This instance serialized as a delimited protobuf w/ a magic pb prefix.
   * @throws IOException
   */
  static byte[] toDelimitedByteArray(RegionInfo... infos) throws IOException {
    byte[][] bytes = new byte[infos.length][];
    int size = 0;
    for (int i = 0; i < infos.length; i++) {
      bytes[i] = toDelimitedByteArray(infos[i]);
      size += bytes[i].length;
    }

    byte[] result = new byte[size];
    int offset = 0;
    for (byte[] b : bytes) {
      System.arraycopy(b, 0, result, offset, b.length);
      offset += b.length;
    }
    return result;
  }

  /**
   * Use this instead of {@link RegionInfo#toByteArray(RegionInfo)} when writing to a stream and you want to use
   * the pb mergeDelimitedFrom (w/o the delimiter, pb reads to EOF which may not be what you want).
   * @param ri
   * @return This instance serialized as a delimied protobuf w/ a magic pb prefix.
   * @throws IOException
   */
  static byte [] toDelimitedByteArray(RegionInfo ri) throws IOException {
    return ProtobufUtil.toDelimitedByteArray(ProtobufUtil.toRegionInfo(ri));
  }

  /**
   * Parses an RegionInfo instance from the passed in stream.
   * Presumes the RegionInfo was serialized to the stream with
   * {@link #toDelimitedByteArray(RegionInfo)}.
   * @param in
   * @return An instance of RegionInfo.
   * @throws IOException
   */
  static RegionInfo parseFrom(final DataInputStream in) throws IOException {
    // I need to be able to move back in the stream if this is not a pb
    // serialization so I can do the Writable decoding instead.
    int pblen = ProtobufUtil.lengthOfPBMagic();
    byte [] pbuf = new byte[pblen];
    if (in.markSupported()) { //read it with mark()
      in.mark(pblen);
    }

    //assumption: if Writable serialization, it should be longer than pblen.
    int read = in.read(pbuf);
    if (read != pblen) throw new IOException("read=" + read + ", wanted=" + pblen);
    if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
      return ProtobufUtil.toRegionInfo(HBaseProtos.RegionInfo.parseDelimitedFrom(in));
    } else {
      throw new IOException("PB encoded RegionInfo expected");
    }
  }

  /**
   * Parses all the RegionInfo instances from the passed in stream until EOF. Presumes the
   * RegionInfo's were serialized to the stream with oDelimitedByteArray()
   * @param bytes serialized bytes
   * @param offset the start offset into the byte[] buffer
   * @param length how far we should read into the byte[] buffer
   * @return All the RegionInfos that are in the byte array. Keeps reading till we hit the end.
   * @throws IOException
   */
  static List<RegionInfo> parseDelimitedFrom(final byte[] bytes, final int offset,
                                             final int length) throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException("Can't build an object with empty bytes array");
    }
    DataInputBuffer in = new DataInputBuffer();
    List<RegionInfo> ris = new ArrayList<>();
    try {
      in.reset(bytes, offset, length);
      while (in.available() > 0) {
        RegionInfo ri = parseFrom(in);
        ris.add(ri);
      }
    } finally {
      in.close();
    }
    return ris;
  }
}
