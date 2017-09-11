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
package org.apache.hadoop.hbase;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.util.ByteArrayHashKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashKey;
import org.apache.hadoop.hbase.util.JenkinsHash;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.StringUtils;

/**
 * Information about a region. A region is a range of keys in the whole keyspace of a table, an
 * identifier (a timestamp) for differentiating between subset ranges (after region split)
 * and a replicaId for differentiating the instance for the same range and some status information
 * about the region.
 *
 * The region has a unique name which consists of the following fields:
 * <ul>
 * <li> tableName   : The name of the table </li>
 * <li> startKey    : The startKey for the region. </li>
 * <li> regionId    : A timestamp when the region is created. </li>
 * <li> replicaId   : An id starting from 0 to differentiate replicas of the same region range
 * but hosted in separated servers. The same region range can be hosted in multiple locations.</li>
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
 * In 0.98 or before, a list of table's regions would fully cover the total keyspace, and at any
 * point in time, a row key always belongs to a single region, which is hosted in a single server.
 * In 0.99+, a region can have multiple instances (called replicas), and thus a range (or row) can
 * correspond to multiple HRegionInfo's. These HRI's share the same fields however except the
 * replicaId field. If the replicaId is not set, it defaults to 0, which is compatible with the
 * previous behavior of a range corresponding to 1 region.
 */
@InterfaceAudience.Public
public class HRegionInfo implements Comparable<HRegionInfo> {

  private static final Log LOG = LogFactory.getLog(HRegionInfo.class);

  /**
   * The new format for a region name contains its encodedName at the end.
   * The encoded name also serves as the directory name for the region
   * in the filesystem.
   *
   * New region name format:
   *    &lt;tablename>,,&lt;startkey>,&lt;regionIdTimestamp>.&lt;encodedName>.
   * where,
   *    &lt;encodedName> is a hex version of the MD5 hash of
   *    &lt;tablename>,&lt;startkey>,&lt;regionIdTimestamp>
   *
   * The old region name format:
   *    &lt;tablename>,&lt;startkey>,&lt;regionIdTimestamp>
   * For region names in the old format, the encoded name is a 32-bit
   * JenkinsHash integer value (in its decimal notation, string form).
   *<p>
   * **NOTE**
   *
   * The first hbase:meta region, and regions created by an older
   * version of HBase (0.20 or prior) will continue to use the
   * old region name format.
   */

  /** Separator used to demarcate the encodedName in a region name
   * in the new format. See description on new format above.
   */
  private static final int ENC_SEPARATOR = '.';
  public  static final int MD5_HEX_LENGTH   = 32;

  /** A non-capture group so that this can be embedded. */
  public static final String ENCODED_REGION_NAME_REGEX = "(?:[a-f0-9]+)";

  // to keep appended int's sorted in string format. Only allows 2 bytes to be
  // sorted for replicaId
  public static final String REPLICA_ID_FORMAT = "%04X";

  public static final byte REPLICA_ID_DELIMITER = (byte)'_';

  private static final int MAX_REPLICA_ID = 0xFFFF;
  public static final int DEFAULT_REPLICA_ID = 0;

  public static final String INVALID_REGION_NAME_FORMAT_MESSAGE = "Invalid regionName format";

  /**
   * Does region name contain its encoded name?
   * @param regionName region name
   * @return boolean indicating if this a new format region
   *         name which contains its encoded name.
   */
  private static boolean hasEncodedName(final byte[] regionName) {
    // check if region name ends in ENC_SEPARATOR
    if ((regionName.length >= 1)
        && (regionName[regionName.length - 1] == ENC_SEPARATOR)) {
      // region name is new format. it contains the encoded name.
      return true;
    }
    return false;
  }

  /**
   * @param regionName
   * @return the encodedName
   */
  public static String encodeRegionName(final byte [] regionName) {
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
   * @return Return a short, printable name for this region (usually encoded name) for us logging.
   */
  public String getShortNameToLog() {
    return prettyPrint(this.getEncodedName());
  }

  public static String getShortNameToLog(HRegionInfo...hris) {
    return getShortNameToLog(Arrays.asList(hris));
  }

  /**
   * @return Return a String of short, printable names for <code>hris</code>
   * (usually encoded name) for us logging.
   */
  public static String getShortNameToLog(final List<HRegionInfo> hris) {
    return hris.stream().map(hri -> hri.getShortNameToLog()).
        collect(Collectors.toList()).toString();
  }

  /**
   * Use logging.
   * @param encodedRegionName The encoded regionname.
   * @return <code>hbase:meta</code> if passed <code>1028785192</code> else returns
   * <code>encodedRegionName</code>
   */
  public static String prettyPrint(final String encodedRegionName) {
    if (encodedRegionName.equals("1028785192")) {
      return encodedRegionName + "/hbase:meta";
    }
    return encodedRegionName;
  }

  private byte [] endKey = HConstants.EMPTY_BYTE_ARRAY;
  // This flag is in the parent of a split while the parent is still referenced
  // by daughter regions.  We USED to set this flag when we disabled a table
  // but now table state is kept up in zookeeper as of 0.90.0 HBase.
  private boolean offLine = false;
  private long regionId = -1;
  private transient byte [] regionName = HConstants.EMPTY_BYTE_ARRAY;
  private boolean split = false;
  private byte [] startKey = HConstants.EMPTY_BYTE_ARRAY;
  private int hashCode = -1;
  //TODO: Move NO_HASH to HStoreFile which is really the only place it is used.
  public static final String NO_HASH = null;
  private String encodedName = null;
  private byte [] encodedNameAsBytes = null;
  private int replicaId = DEFAULT_REPLICA_ID;

  // Current TableName
  private TableName tableName = null;
  final static String DISPLAY_KEYS_KEY = "hbase.display.keys";
  public final static byte[] HIDDEN_END_KEY = Bytes.toBytes("hidden-end-key");
  public final static byte[] HIDDEN_START_KEY = Bytes.toBytes("hidden-start-key");

  /** HRegionInfo for first meta region */
  // TODO: How come Meta regions still do not have encoded region names? Fix.
  public static final HRegionInfo FIRST_META_REGIONINFO =
      new HRegionInfo(1L, TableName.META_TABLE_NAME);

  private void setHashCode() {
    int result = Arrays.hashCode(this.regionName);
    result ^= this.regionId;
    result ^= Arrays.hashCode(this.startKey);
    result ^= Arrays.hashCode(this.endKey);
    result ^= Boolean.valueOf(this.offLine).hashCode();
    result ^= Arrays.hashCode(this.tableName.getName());
    result ^= this.replicaId;
    this.hashCode = result;
  }


  /**
   * Private constructor used constructing HRegionInfo for the
   * first meta regions
   */
  private HRegionInfo(long regionId, TableName tableName) {
    this(regionId, tableName, DEFAULT_REPLICA_ID);
  }

  public HRegionInfo(long regionId, TableName tableName, int replicaId) {
    super();
    this.regionId = regionId;
    this.tableName = tableName;
    this.replicaId = replicaId;
    // Note: First Meta region replicas names are in old format
    this.regionName = createRegionName(tableName, null, regionId, replicaId, false);
    setHashCode();
  }

  public HRegionInfo(final TableName tableName) {
    this(tableName, null, null);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableName the table name
   * @param startKey first key in region
   * @param endKey end of key range
   * @throws IllegalArgumentException
   */
  public HRegionInfo(final TableName tableName, final byte[] startKey, final byte[] endKey)
  throws IllegalArgumentException {
    this(tableName, startKey, endKey, false);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableName the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(final TableName tableName, final byte[] startKey, final byte[] endKey,
      final boolean split)
  throws IllegalArgumentException {
    this(tableName, startKey, endKey, split, System.currentTimeMillis());
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableName the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @param regionid Region id to use.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(final TableName tableName, final byte[] startKey,
                     final byte[] endKey, final boolean split, final long regionid)
  throws IllegalArgumentException {
    this(tableName, startKey, endKey, split, regionid, DEFAULT_REPLICA_ID);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableName the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @param regionid Region id to use.
   * @param replicaId the replicaId to use
   * @throws IllegalArgumentException
   */
  public HRegionInfo(final TableName tableName, final byte[] startKey,
                     final byte[] endKey, final boolean split, final long regionid,
                     final int replicaId)
    throws IllegalArgumentException {
    super();
    if (tableName == null) {
      throw new IllegalArgumentException("TableName cannot be null");
    }
    this.tableName = tableName;
    this.offLine = false;
    this.regionId = regionid;
    this.replicaId = replicaId;
    if (this.replicaId > MAX_REPLICA_ID) {
      throw new IllegalArgumentException("ReplicaId cannot be greater than" + MAX_REPLICA_ID);
    }

    this.regionName = createRegionName(this.tableName, startKey, regionId, replicaId, true);

    this.split = split;
    this.endKey = endKey == null? HConstants.EMPTY_END_ROW: endKey.clone();
    this.startKey = startKey == null?
      HConstants.EMPTY_START_ROW: startKey.clone();
    this.tableName = tableName;
    setHashCode();
  }

  /**
   * Costruct a copy of another HRegionInfo
   *
   * @param other
   */
  public HRegionInfo(HRegionInfo other) {
    super();
    this.endKey = other.getEndKey();
    this.offLine = other.isOffline();
    this.regionId = other.getRegionId();
    this.regionName = other.getRegionName();
    this.split = other.isSplit();
    this.startKey = other.getStartKey();
    this.hashCode = other.hashCode();
    this.encodedName = other.getEncodedName();
    this.tableName = other.tableName;
    this.replicaId = other.replicaId;
  }

  public HRegionInfo(HRegionInfo other, int replicaId) {
    this(other);
    this.replicaId = replicaId;
    this.setHashCode();
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
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final long regionid, boolean newFormat) {
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
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final String id, boolean newFormat) {
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
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final long regionid, int replicaId, boolean newFormat) {
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
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final byte [] id, boolean newFormat) {
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
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final byte [] id, final int replicaId, boolean newFormat) {
    int len = tableName.getName().length + 2 + id.length +
        (startKey == null? 0: startKey.length);
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
        LOG.error("MD5-hash length mismatch: Expected=" + MD5_HEX_LENGTH +
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
   * Gets the table name from the specified region name.
   * @param regionName to extract the table name from
   * @return Table name
   */
  public static TableName getTable(final byte [] regionName) {
    int offset = -1;
    for (int i = 0; i < regionName.length; i++) {
      if (regionName[i] == HConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    byte[] buff  = new byte[offset];
    System.arraycopy(regionName, 0, buff, 0, offset);
    return TableName.valueOf(buff);
  }

  /**
   * Gets the start key from the specified region name.
   * @param regionName
   * @return Start key.
   */
  public static byte[] getStartKey(final byte[] regionName) throws IOException {
    return parseRegionName(regionName)[1];
  }

  /**
   * Separate elements of a regionName.
   * @param regionName
   * @return Array of byte[] containing tableName, startKey and id
   * @throws IOException
   */
  public static byte [][] parseRegionName(final byte [] regionName)
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

  public static boolean isEncodedRegionName(byte[] regionName) throws IOException {
    try {
      HRegionInfo.parseRegionName(regionName);
      return false;
    } catch (IOException e) {
      if (StringUtils.stringifyException(e)
          .contains(HRegionInfo.INVALID_REGION_NAME_FORMAT_MESSAGE)) {
        return true;
      }
      throw e;
    }
  }

  /** @return the regionId */
  public long getRegionId(){
    return regionId;
  }

  /**
   * @return the regionName as an array of bytes.
   * @see #getRegionNameAsString()
   */
  public byte [] getRegionName(){
    return regionName;
  }

  /**
   * @return Region name as a String for use in logging, etc.
   */
  public String getRegionNameAsString() {
    if (hasEncodedName(this.regionName)) {
      // new format region names already have their encoded name.
      return Bytes.toStringBinary(this.regionName);
    }

    // old format. regionNameStr doesn't have the region name.
    //
    //
    return Bytes.toStringBinary(this.regionName) + "." + this.getEncodedName();
  }

  /** @return the encoded region name */
  public synchronized String getEncodedName() {
    if (this.encodedName == null) {
      this.encodedName = encodeRegionName(this.regionName);
    }
    return this.encodedName;
  }

  public synchronized byte [] getEncodedNameAsBytes() {
    if (this.encodedNameAsBytes == null) {
      this.encodedNameAsBytes = Bytes.toBytes(getEncodedName());
    }
    return this.encodedNameAsBytes;
  }

  /** @return the startKey */
  public byte [] getStartKey(){
    return startKey;
  }

  /** @return the endKey */
  public byte [] getEndKey(){
    return endKey;
  }

  /**
   * Get current table name of the region
   * @return TableName
   */
  public TableName getTable() {
    // This method name should be getTableName but there was already a method getTableName
    // that returned a byte array.  It is unfortunate given everywhere else, getTableName returns
    // a TableName instance.
    if (tableName == null || tableName.getName().length == 0) {
      tableName = getTable(getRegionName());
    }
    return this.tableName;
  }

  /**
   * Returns true if the given inclusive range of rows is fully contained
   * by this region. For example, if the region is foo,a,g and this is
   * passed ["b","c"] or ["a","c"] it will return true, but if this is passed
   * ["b","z"] it will return false.
   * @throws IllegalArgumentException if the range passed is invalid (ie. end &lt; start)
   */
  public boolean containsRange(byte[] rangeStartKey, byte[] rangeEndKey) {
    if (Bytes.compareTo(rangeStartKey, rangeEndKey) > 0) {
      throw new IllegalArgumentException(
      "Invalid range: " + Bytes.toStringBinary(rangeStartKey) +
      " > " + Bytes.toStringBinary(rangeEndKey));
    }

    boolean firstKeyInRange = Bytes.compareTo(rangeStartKey, startKey) >= 0;
    boolean lastKeyInRange =
      Bytes.compareTo(rangeEndKey, endKey) < 0 ||
      Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY);
    return firstKeyInRange && lastKeyInRange;
  }

  /**
   * Return true if the given row falls in this region.
   */
  public boolean containsRow(byte[] row) {
    return Bytes.compareTo(row, startKey) >= 0 &&
      (Bytes.compareTo(row, endKey) < 0 ||
       Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY));
  }

  /**
   * @return true if this region is from hbase:meta
   */
  public boolean isMetaTable() {
    return isMetaRegion();
  }

  /** @return true if this region is a meta region */
  public boolean isMetaRegion() {
     return tableName.equals(HRegionInfo.FIRST_META_REGIONINFO.getTable());
  }

  /**
   * @return true if this region is from a system table
   */
  public boolean isSystemTable() {
    return tableName.isSystemTable();
  }

  /**
   * @return True if has been split and has daughters.
   */
  public boolean isSplit() {
    return this.split;
  }

  /**
   * @param split set split status
   */
  public void setSplit(boolean split) {
    this.split = split;
  }

  /**
   * @return True if this region is offline.
   */
  public boolean isOffline() {
    return this.offLine;
  }

  /**
   * The parent of a region split is offline while split daughters hold
   * references to the parent. Offlined regions are closed.
   * @param offLine Set online/offline status.
   */
  public void setOffline(boolean offLine) {
    this.offLine = offLine;
  }

  /**
   * @return True if this is a split parent region.
   */
  public boolean isSplitParent() {
    if (!isSplit()) return false;
    if (!isOffline()) {
      LOG.warn("Region is split but NOT offline: " + getRegionNameAsString());
    }
    return true;
  }

  /**
   * Returns the region replica id
   * @return returns region replica id
   */
  public int getReplicaId() {
    return replicaId;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "{ENCODED => " + getEncodedName() + ", " +
      HConstants.NAME + " => '" + Bytes.toStringBinary(this.regionName)
      + "', STARTKEY => '" +
      Bytes.toStringBinary(this.startKey) + "', ENDKEY => '" +
      Bytes.toStringBinary(this.endKey) + "'" +
      (isOffline()? ", OFFLINE => true": "") +
      (isSplit()? ", SPLIT => true": "") +
      ((replicaId > 0)? ", REPLICA_ID => " + replicaId : "") + "}";
  }

  /**
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (!(o instanceof HRegionInfo)) {
      return false;
    }
    return this.compareTo((HRegionInfo)o) == 0;
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.hashCode;
  }

  //
  // Comparable
  //

  @Override
  public int compareTo(HRegionInfo o) {
    if (o == null) {
      return 1;
    }

    // Are regions of same table?
    int result = this.tableName.compareTo(o.tableName);
    if (result != 0) {
      return result;
    }

    // Compare start keys.
    result = Bytes.compareTo(this.startKey, o.startKey);
    if (result != 0) {
      return result;
    }

    // Compare end keys.
    result = Bytes.compareTo(this.endKey, o.endKey);

    if (result != 0) {
      if (this.getStartKey().length != 0
              && this.getEndKey().length == 0) {
          return 1; // this is last region
      }
      if (o.getStartKey().length != 0
              && o.getEndKey().length == 0) {
          return -1; // o is the last region
      }
      return result;
    }

    // regionId is usually milli timestamp -- this defines older stamps
    // to be "smaller" than newer stamps in sort order.
    if (this.regionId > o.regionId) {
      return 1;
    } else if (this.regionId < o.regionId) {
      return -1;
    }

    int replicaDiff = this.getReplicaId() - o.getReplicaId();
    if (replicaDiff != 0) return replicaDiff;

    if (this.offLine == o.offLine)
      return 0;
    if (this.offLine == true) return -1;

    return 1;
  }

  /**
   * @return Comparator to use comparing {@link KeyValue}s.
   * @deprecated Use Region#getCellComparator().  deprecated for hbase 2.0, remove for hbase 3.0
   */
  @Deprecated
  public KVComparator getComparator() {
    return isMetaRegion()?
        KeyValue.META_COMPARATOR: KeyValue.COMPARATOR;
  }

  /**
   * Convert a HRegionInfo to the protobuf RegionInfo
   *
   * @return the converted RegionInfo
   */
  RegionInfo convert() {
    return convert(this);
  }

  /**
   * Convert a HRegionInfo to a RegionInfo
   *
   * @param info the HRegionInfo to convert
   * @return the converted RegionInfo
   */
  public static RegionInfo convert(final HRegionInfo info) {
    if (info == null) return null;
    RegionInfo.Builder builder = RegionInfo.newBuilder();
    builder.setTableName(ProtobufUtil.toProtoTableName(info.getTable()));
    builder.setRegionId(info.getRegionId());
    if (info.getStartKey() != null) {
      builder.setStartKey(UnsafeByteOperations.unsafeWrap(info.getStartKey()));
    }
    if (info.getEndKey() != null) {
      builder.setEndKey(UnsafeByteOperations.unsafeWrap(info.getEndKey()));
    }
    builder.setOffline(info.isOffline());
    builder.setSplit(info.isSplit());
    builder.setReplicaId(info.getReplicaId());
    return builder.build();
  }

  /**
   * Convert a RegionInfo to a HRegionInfo
   *
   * @param proto the RegionInfo to convert
   * @return the converted HRegionInfho
   */
  public static HRegionInfo convert(final RegionInfo proto) {
    if (proto == null) return null;
    TableName tableName =
        ProtobufUtil.toTableName(proto.getTableName());
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      return RegionReplicaUtil.getRegionInfoForReplica(FIRST_META_REGIONINFO,
          proto.getReplicaId());
    }
    long regionId = proto.getRegionId();
    int replicaId = proto.hasReplicaId() ? proto.getReplicaId() : DEFAULT_REPLICA_ID;
    byte[] startKey = null;
    byte[] endKey = null;
    if (proto.hasStartKey()) {
      startKey = proto.getStartKey().toByteArray();
    }
    if (proto.hasEndKey()) {
      endKey = proto.getEndKey().toByteArray();
    }
    boolean split = false;
    if (proto.hasSplit()) {
      split = proto.getSplit();
    }
    HRegionInfo hri = new HRegionInfo(
        tableName,
        startKey,
        endKey, split, regionId, replicaId);
    if (proto.hasOffline()) {
      hri.setOffline(proto.getOffline());
    }
    return hri;
  }

  /**
   * @return This instance serialized as protobuf w/ a magic pb prefix.
   * @see #parseFrom(byte[])
   */
  public byte [] toByteArray() {
    byte [] bytes = convert().toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  /**
   * @return A deserialized {@link HRegionInfo}
   * or null if we failed deserialize or passed bytes null
   * @see #toByteArray()
   */
  public static HRegionInfo parseFromOrNull(final byte [] bytes) {
    if (bytes == null) return null;
    return parseFromOrNull(bytes, 0, bytes.length);
  }

  /**
   * @return A deserialized {@link HRegionInfo} or null
   *  if we failed deserialize or passed bytes null
   * @see #toByteArray()
   */
  public static HRegionInfo parseFromOrNull(final byte [] bytes, int offset, int len) {
    if (bytes == null || len <= 0) return null;
    try {
      return parseFrom(bytes, offset, len);
    } catch (DeserializationException e) {
      return null;
    }
  }

  /**
   * @param bytes A pb RegionInfo serialized with a pb magic prefix.
   * @return A deserialized {@link HRegionInfo}
   * @throws DeserializationException
   * @see #toByteArray()
   */
  public static HRegionInfo parseFrom(final byte [] bytes) throws DeserializationException {
    if (bytes == null) return null;
    return parseFrom(bytes, 0, bytes.length);
  }

  /**
   * @param bytes A pb RegionInfo serialized with a pb magic prefix.
   * @param offset starting point in the byte array
   * @param len length to read on the byte array
   * @return A deserialized {@link HRegionInfo}
   * @throws DeserializationException
   * @see #toByteArray()
   */
  public static HRegionInfo parseFrom(final byte [] bytes, int offset, int len)
      throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(bytes, offset, len)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        HBaseProtos.RegionInfo.Builder builder = HBaseProtos.RegionInfo.newBuilder();
        ProtobufUtil.mergeFrom(builder, bytes, pblen + offset, len - pblen);
        HBaseProtos.RegionInfo ri = builder.build();
        return convert(ri);
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    } else {
      throw new DeserializationException("PB encoded HRegionInfo expected");
    }
  }

  /**
   * Use this instead of {@link #toByteArray()} when writing to a stream and you want to use
   * the pb mergeDelimitedFrom (w/o the delimiter, pb reads to EOF which may not be what you want).
   * @return This instance serialized as a delimited protobuf w/ a magic pb prefix.
   * @throws IOException
   * @see #toByteArray()
   */
  public byte [] toDelimitedByteArray() throws IOException {
    return ProtobufUtil.toDelimitedByteArray(convert());
  }

  /**
   * Get the descriptive name as {@link RegionState} does it but with hidden
   * startkey optionally
   * @param state
   * @param conf
   * @return descriptive string
   */
  public static String getDescriptiveNameFromRegionStateForDisplay(RegionState state,
      Configuration conf) {
    if (conf.getBoolean(DISPLAY_KEYS_KEY, true)) return state.toDescriptiveString();
    String descriptiveStringFromState = state.toDescriptiveString();
    int idx = descriptiveStringFromState.lastIndexOf(" state=");
    String regionName = getRegionNameAsStringForDisplay(state.getRegion(), conf);
    return regionName + descriptiveStringFromState.substring(idx);
  }

  /**
   * Get the end key for display. Optionally hide the real end key.
   * @param hri
   * @param conf
   * @return the endkey
   */
  public static byte[] getEndKeyForDisplay(HRegionInfo hri, Configuration conf) {
    boolean displayKey = conf.getBoolean(DISPLAY_KEYS_KEY, true);
    if (displayKey) return hri.getEndKey();
    return HIDDEN_END_KEY;
  }

  /**
   * Get the start key for display. Optionally hide the real start key.
   * @param hri
   * @param conf
   * @return the startkey
   */
  public static byte[] getStartKeyForDisplay(HRegionInfo hri, Configuration conf) {
    boolean displayKey = conf.getBoolean(DISPLAY_KEYS_KEY, true);
    if (displayKey) return hri.getStartKey();
    return HIDDEN_START_KEY;
  }

  /**
   * Get the region name for display. Optionally hide the start key.
   * @param hri
   * @param conf
   * @return region name as String
   */
  public static String getRegionNameAsStringForDisplay(HRegionInfo hri, Configuration conf) {
    return Bytes.toStringBinary(getRegionNameForDisplay(hri, conf));
  }

  /**
   * Get the region name for display. Optionally hide the start key.
   * @param hri
   * @param conf
   * @return region name bytes
   */
  public static byte[] getRegionNameForDisplay(HRegionInfo hri, Configuration conf) {
    boolean displayKey = conf.getBoolean(DISPLAY_KEYS_KEY, true);
    if (displayKey || hri.getTable().equals(TableName.META_TABLE_NAME)) {
      return hri.getRegionName();
    } else {
      // create a modified regionname with the startkey replaced but preserving
      // the other parts including the encodedname.
      try {
        byte[][]regionNameParts = parseRegionName(hri.getRegionName());
        regionNameParts[1] = HIDDEN_START_KEY; //replace the real startkey
        int len = 0;
        // get the total length
        for (byte[] b : regionNameParts) {
          len += b.length;
        }
        byte[] encodedRegionName =
            Bytes.toBytes(encodeRegionName(hri.getRegionName()));
        len += encodedRegionName.length;
        //allocate some extra bytes for the delimiters and the last '.'
        byte[] modifiedName = new byte[len + regionNameParts.length + 1];
        int lengthSoFar = 0;
        int loopCount = 0;
        for (byte[] b : regionNameParts) {
          System.arraycopy(b, 0, modifiedName, lengthSoFar, b.length);
          lengthSoFar += b.length;
          if (loopCount++ == 2) modifiedName[lengthSoFar++] = REPLICA_ID_DELIMITER;
          else  modifiedName[lengthSoFar++] = HConstants.DELIMITER;
        }
        // replace the last comma with '.'
        modifiedName[lengthSoFar - 1] = ENC_SEPARATOR;
        System.arraycopy(encodedRegionName, 0, modifiedName, lengthSoFar,
            encodedRegionName.length);
        lengthSoFar += encodedRegionName.length;
        modifiedName[lengthSoFar] = ENC_SEPARATOR;
        return modifiedName;
      } catch (IOException e) {
        //LOG.warn("Encountered exception " + e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Parses an HRegionInfo instance from the passed in stream.  Presumes the HRegionInfo was
   * serialized to the stream with {@link #toDelimitedByteArray()}
   * @param in
   * @return An instance of HRegionInfo.
   * @throws IOException
   */
  public static HRegionInfo parseFrom(final DataInputStream in) throws IOException {
    // I need to be able to move back in the stream if this is not a pb serialization so I can
    // do the Writable decoding instead.
    int pblen = ProtobufUtil.lengthOfPBMagic();
    byte [] pbuf = new byte[pblen];
    if (in.markSupported()) { //read it with mark()
      in.mark(pblen);
    }

    //assumption: if Writable serialization, it should be longer than pblen.
    int read = in.read(pbuf);
    if (read != pblen) throw new IOException("read=" + read + ", wanted=" + pblen);
    if (ProtobufUtil.isPBMagicPrefix(pbuf)) {
      return convert(HBaseProtos.RegionInfo.parseDelimitedFrom(in));
    } else {
      throw new IOException("PB encoded HRegionInfo expected");
    }
  }

  /**
   * Serializes given HRegionInfo's as a byte array. Use this instead of {@link #toByteArray()} when
   * writing to a stream and you want to use the pb mergeDelimitedFrom (w/o the delimiter, pb reads
   * to EOF which may not be what you want). {@link #parseDelimitedFrom(byte[], int, int)} can
   * be used to read back the instances.
   * @param infos HRegionInfo objects to serialize
   * @return This instance serialized as a delimited protobuf w/ a magic pb prefix.
   * @throws IOException
   * @see #toByteArray()
   */
  public static byte[] toDelimitedByteArray(HRegionInfo... infos) throws IOException {
    byte[][] bytes = new byte[infos.length][];
    int size = 0;
    for (int i = 0; i < infos.length; i++) {
      bytes[i] = infos[i].toDelimitedByteArray();
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
   * Parses all the HRegionInfo instances from the passed in stream until EOF. Presumes the
   * HRegionInfo's were serialized to the stream with {@link #toDelimitedByteArray()}
   * @param bytes serialized bytes
   * @param offset the start offset into the byte[] buffer
   * @param length how far we should read into the byte[] buffer
   * @return All the hregioninfos that are in the byte array. Keeps reading till we hit the end.
   */
  public static List<HRegionInfo> parseDelimitedFrom(final byte[] bytes, final int offset,
      final int length) throws IOException {
    if (bytes == null) {
      throw new IllegalArgumentException("Can't build an object with empty bytes array");
    }
    DataInputBuffer in = new DataInputBuffer();
    List<HRegionInfo> hris = new ArrayList<>();
    try {
      in.reset(bytes, offset, length);
      while (in.available() > 0) {
        HRegionInfo hri = parseFrom(in);
        hris.add(hri);
      }
    } finally {
      in.close();
    }
    return hris;
  }

  /**
   * Check whether two regions are adjacent
   * @param regionA
   * @param regionB
   * @return true if two regions are adjacent
   */
  public static boolean areAdjacent(HRegionInfo regionA, HRegionInfo regionB) {
    if (regionA == null || regionB == null) {
      throw new IllegalArgumentException(
          "Can't check whether adjacent for null region");
    }
    HRegionInfo a = regionA;
    HRegionInfo b = regionB;
    if (Bytes.compareTo(a.getStartKey(), b.getStartKey()) > 0) {
      a = regionB;
      b = regionA;
    }
    if (Bytes.compareTo(a.getEndKey(), b.getStartKey()) == 0) {
      return true;
    }
    return false;
  }
}
