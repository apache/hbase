/*
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

import edu.umd.cs.findbugs.annotations.CheckForNull;
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
import org.apache.hadoop.io.IOUtils;
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
public interface RegionInfo extends Comparable<RegionInfo> {
  /**
   * @deprecated since 2.3.2/3.0.0; to be removed in 4.0.0 with no replacement (for internal use).
   */
  @Deprecated
  @InterfaceAudience.Private
  // Not using RegionInfoBuilder intentionally to avoid a static loading deadlock: HBASE-24896
  RegionInfo UNDEFINED = new MutableRegionInfo(0, TableName.valueOf("__UNDEFINED__"),
    RegionInfo.DEFAULT_REPLICA_ID);

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
      if (replicaDiff != 0) {
        return replicaDiff;
      }

      if (lhs.isOffline() == rhs.isOffline()) {
        return 0;
      }
      if (lhs.isOffline()) {
        return -1;
      }

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
   * @deprecated since 3.0.0 and will be removed in 4.0.0
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-25210">HBASE-25210</a>
   */
  @Deprecated
  boolean isOffline();

  /**
   * @return True if this is a split parent region.
   * @deprecated since 3.0.0 and will be removed in 4.0.0, Use {@link #isSplit()} instead.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-25210">HBASE-25210</a>
   */
  @Deprecated
  boolean isSplitParent();

  /**
   * @return true if this region is a meta region.
   */
  boolean isMetaRegion();

  /**
   * @return true if the given inclusive range of rows is fully contained
   * by this region. For example, if the region is foo,a,g and this is
   * passed ["b","c"] or ["a","c"] it will return true, but if this is passed
   * ["b","z"] it will return false.
   * @throws IllegalArgumentException if the range passed is invalid (ie. end &lt; start)
   */
  boolean containsRange(byte[] rangeStartKey, byte[] rangeEndKey);

  /**
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

  @InterfaceAudience.Private
  static String getRegionNameAsString(byte[] regionName) {
    return getRegionNameAsString(null, regionName);
  }

  @InterfaceAudience.Private
  static String getRegionNameAsString(@CheckForNull RegionInfo ri, byte[] regionName) {
    if (RegionInfo.hasEncodedName(regionName)) {
      // new format region names already have their encoded name.
      return Bytes.toStringBinary(regionName);
    }

    // old format. regionNameStr doesn't have the region name.
    if (ri == null) {
      return Bytes.toStringBinary(regionName) + "." + RegionInfo.encodeRegionName(regionName);
    } else {
      return Bytes.toStringBinary(regionName) + "." + ri.getEncodedName();
    }
  }

  /**
   * @return Return a String of short, printable names for <code>hris</code>
   * (usually encoded name) for us logging.
   */
  static String getShortNameToLog(RegionInfo...hris) {
    return getShortNameToLog(Arrays.asList(hris));
  }

  /**
   * @return Return a String of short, printable names for <code>hris</code> (usually encoded name)
   *   for us logging.
   */
  static String getShortNameToLog(final List<RegionInfo> ris) {
    return ris.stream().map(RegionInfo::getEncodedName).collect(Collectors.toList()).toString();
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
   * @return Start key.
   */
  static byte[] getStartKey(final byte[] regionName) throws IOException {
    return parseRegionName(regionName)[1];
  }

  /**
   * Figure if the passed bytes represent an encoded region name or not.
   * @param regionName A Region name either encoded or not.
   * @return True if <code>regionName</code> represents an encoded name.
   */
  @InterfaceAudience.Private // For use by internals only.
  public static boolean isEncodedRegionName(byte[] regionName) {
    // If not parseable as region name, presume encoded. TODO: add stringency; e.g. if hex.
    return parseRegionNameOrReturnNull(regionName) == null && regionName.length <= MD5_HEX_LENGTH;
  }

  /**
   * @return A deserialized {@link RegionInfo}
   * or null if we failed deserialize or passed bytes null
   */
  @InterfaceAudience.Private
  static RegionInfo parseFromOrNull(final byte [] bytes) {
    if (bytes == null) return null;
    return parseFromOrNull(bytes, 0, bytes.length);
  }

  /**
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

  static boolean isMD5Hash(String encodedRegionName) {
    if (encodedRegionName.length() != MD5_HEX_LENGTH) {
      return false;
    }

    for (int i = 0; i < encodedRegionName.length(); i++) {
      char c = encodedRegionName.charAt(i);
      if (!((c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f') || (c >= '0' && c <= '9'))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check whether two regions are adjacent; i.e. lies just before or just
   * after in a table.
   * @return true if two regions are adjacent
   */
  static boolean areAdjacent(RegionInfo regionA, RegionInfo regionB) {
    if (regionA == null || regionB == null) {
      throw new IllegalArgumentException(
      "Can't check whether adjacent for null region");
    }
    if (!regionA.getTable().equals(regionB.getTable())) {
      return false;
    }
    RegionInfo a = regionA;
    RegionInfo b = regionB;
    if (Bytes.compareTo(a.getStartKey(), b.getStartKey()) > 0) {
      a = regionB;
      b = regionA;
    }
    return Bytes.equals(a.getEndKey(), b.getStartKey());
  }

  /**
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
   * @param startKey Can be null
   * @param regionid Region id (Usually timestamp from when region was created).
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
   * @param startKey Can be null
   * @param id Region id (Usually timestamp from when region was created).
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
      b[offset] = ENC_SEPARATOR;
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
    // Skipping reference to RegionInfoBuilder in this class.
    return new MutableRegionInfo(tableName, Bytes.toBytes(".mob"),
      HConstants.EMPTY_END_ROW, false, 0, DEFAULT_REPLICA_ID, false);
  }

  /**
   * Separate elements of a regionName.
   * @return Array of byte[] containing tableName, startKey and id OR null if
   *   not parseable as a region name.
   * @throws IOException if not parseable as regionName.
   */
  static byte [][] parseRegionName(final byte[] regionName) throws IOException {
    byte [][] result = parseRegionNameOrReturnNull(regionName);
    if (result == null) {
      throw new IOException(INVALID_REGION_NAME_FORMAT_MESSAGE + ": " + Bytes.toStringBinary(regionName));
    }
    return result;
  }

  /**
   * Separate elements of a regionName.
   * Region name is of the format:
   * <code>tablename,startkey,regionIdTimestamp[_replicaId][.encodedName.]</code>.
   * Startkey can contain the delimiter (',') so we parse from the start and then parse from
   * the end.
   * @return Array of byte[] containing tableName, startKey and id OR null if not parseable
   * as a region name.
   */
  static byte [][] parseRegionNameOrReturnNull(final byte[] regionName) {
    int offset = -1;
    for (int i = 0; i < regionName.length; i++) {
      if (regionName[i] == HConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    if (offset == -1) {
      return null;
    }
    byte[] tableName = new byte[offset];
    System.arraycopy(regionName, 0, tableName, 0, offset);
    offset = -1;

    int endOffset = regionName.length;
    // check whether regionName contains encodedName
    if (regionName.length > MD5_HEX_LENGTH + 2 &&
        regionName[regionName.length-1] == ENC_SEPARATOR &&
        regionName[regionName.length-MD5_HEX_LENGTH-2] == ENC_SEPARATOR) {
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
      return null;
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
   * @return This instance serialized as a delimied protobuf w/ a magic pb prefix.
   */
  static byte [] toDelimitedByteArray(RegionInfo ri) throws IOException {
    return ProtobufUtil.toDelimitedByteArray(ProtobufUtil.toRegionInfo(ri));
  }

  /**
   * Parses an RegionInfo instance from the passed in stream.
   * Presumes the RegionInfo was serialized to the stream with
   * {@link #toDelimitedByteArray(RegionInfo)}.
   * @return An instance of RegionInfo.
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
    IOUtils.readFully(in, pbuf, 0, pblen);
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
   */
  static List<RegionInfo> parseDelimitedFrom(final byte[] bytes, final int offset,
                                             final int length) throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException("Can't build an object with empty bytes array");
    }
    List<RegionInfo> ris = new ArrayList<>();
    try (DataInputBuffer in = new DataInputBuffer()) {
      in.reset(bytes, offset, length);
      while (in.available() > 0) {
        RegionInfo ri = parseFrom(in);
        ris.add(ri);
      }
    }
    return ris;
  }

  /**
   * @return True if this is first Region in Table
   */
  default boolean isFirst() {
    return Bytes.equals(getStartKey(), HConstants.EMPTY_START_ROW);
  }

  /**
   * @return True if this is last Region in Table
   */
  default boolean isLast() {
    return Bytes.equals(getEndKey(), HConstants.EMPTY_END_ROW);
  }

  /**
   * @return True if region is next, adjacent but 'after' this one.
   * @see #isAdjacent(RegionInfo)
   * @see #areAdjacent(RegionInfo, RegionInfo)
   */
  default boolean isNext(RegionInfo after) {
    return getTable().equals(after.getTable()) && Bytes.equals(getEndKey(), after.getStartKey());
  }

  /**
   * @return True if region is adjacent, either just before or just after this one.
   * @see #isNext(RegionInfo)
   */
  default boolean isAdjacent(RegionInfo other) {
    return getTable().equals(other.getTable()) && areAdjacent(this, other);
  }

  /**
   * @return True if RegionInfo is degenerate... if startKey > endKey.
   */
  default boolean isDegenerate() {
    return !isLast() && Bytes.compareTo(getStartKey(), getEndKey()) > 0;
  }

  /**
   * @return True if an overlap in region range.
   * @see #isDegenerate()
   */
  default boolean isOverlap(RegionInfo other) {
    if (other == null) {
      return false;
    }
    if (!getTable().equals(other.getTable())) {
      return false;
    }
    int startKeyCompare = Bytes.compareTo(getStartKey(), other.getStartKey());
    if (startKeyCompare == 0) {
      return true;
    }
    if (startKeyCompare < 0) {
      if (isLast()) {
        return true;
      }
      return Bytes.compareTo(getEndKey(), other.getStartKey()) > 0;
    }
    if (other.isLast()) {
      return true;
    }
    return Bytes.compareTo(getStartKey(), other.getEndKey()) < 0;
  }

  default int compareTo(RegionInfo other) {
    return RegionInfo.COMPARATOR.compare(this, other);
  }
}
