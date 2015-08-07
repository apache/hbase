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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JenkinsHash;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.hadoop.io.DataInputBuffer;

/**
 * Information about a region. A region is a range of keys in the whole keyspace of a table, an
 * identifier (a timestamp) for differentiating between subset ranges (after region split)
 * and a replicaId for differentiating the instance for the same range and some status information
 * about the region.
 *
 * The region has a unique name which consists of the following fields:
 * <li> tableName   : The name of the table </li>
 * <li> startKey    : The startKey for the region. </li>
 * <li> regionId    : A timestamp when the region is created. </li>
 * <li> replicaId   : An id starting from 0 to differentiate replicas of the same region range
 * but hosted in separated servers. The same region range can be hosted in multiple locations.</li>
 * <li> encodedName : An MD5 encoded string for the region name.</li>
 *
 * <br> Other than the fields in the region name, region info contains:
 * <li> endKey      : the endKey for the region (exclusive) </li>
 * <li> split       : Whether the region is split </li>
 * <li> offline     : Whether the region is offline </li>
 *
 * In 0.98 or before, a list of table's regions would fully cover the total keyspace, and at any
 * point in time, a row key always belongs to a single region, which is hosted in a single server.
 * In 0.99+, a region can have multiple instances (called replicas), and thus a range (or row) can
 * correspond to multiple HRegionInfo's. These HRI's share the same fields however except the
 * replicaId field. If the replicaId is not set, it defaults to 0, which is compatible with the
 * previous behavior of a range corresponding to 1 region.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HRegionInfo implements Comparable<HRegionInfo> {
  /*
   * There are two versions associated with HRegionInfo: HRegionInfo.VERSION and
   * HConstants.META_VERSION. HRegionInfo.VERSION indicates the data structure's versioning
   * while HConstants.META_VERSION indicates the versioning of the serialized HRIs stored in
   * the hbase:meta table.
   *
   * Pre-0.92:
   *   HRI.VERSION == 0 and HConstants.META_VERSION does not exist
    *  (is not stored at hbase:meta table)
   *   HRegionInfo had an HTableDescriptor reference inside it.
   *   HRegionInfo is serialized as Writable to hbase:meta table.
   * For 0.92.x and 0.94.x:
   *   HRI.VERSION == 1 and HConstants.META_VERSION == 0
   *   HRI no longer has HTableDescriptor in it.
   *   HRI is serialized as Writable to hbase:meta table.
   * For 0.96.x:
   *   HRI.VERSION == 1 and HConstants.META_VERSION == 1
   *   HRI data structure is the same as 0.92 and 0.94
   *   HRI is serialized as PB to hbase:meta table.
   *
   * Versioning of HRegionInfo is deprecated. HRegionInfo does protobuf
   * serialization using RegionInfo class, which has it's own versioning.
   */
  @Deprecated
  public static final byte VERSION = 1;
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
      int hashVal = Math.abs(JenkinsHash.getInstance().hash(regionName,
        regionName.length, 0));
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

  /** HRegionInfo for first meta region */
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

  /** Default constructor - creates empty object
   * @deprecated As of release 0.96
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-5453">HBASE-5453</a>).
   *             This will be removed in HBase 2.0.0.
   *             Used by Writables and Writables are going away.
   */
  @Deprecated
  public HRegionInfo() {
    super();
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
   * @param regionName
   * @return Table name.
   * @deprecated As of release 0.96
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-9508">HBASE-9508</a>).
   *             This will be removed in HBase 2.0.0. Use {@link #getTable(byte[])}.
   */
  @Deprecated
  public static byte [] getTableName(byte[] regionName) {
    int offset = -1;
    for (int i = 0; i < regionName.length; i++) {
      if (regionName[i] == HConstants.DELIMITER) {
        offset = i;
        break;
      }
    }
    byte[] buff  = new byte[offset];
    System.arraycopy(regionName, 0, buff, 0, offset);
    return buff;
  }


  /**
   * Gets the table name from the specified region name.
   * Like {@link #getTableName(byte[])} only returns a {@link TableName} rather than a byte array.
   * @param regionName
   * @return Table name
   * @see #getTableName(byte[])
   */
  public static TableName getTable(final byte [] regionName) {
    return TableName.valueOf(getTableName(regionName));
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
      throw new IOException("Invalid regionName format: " + Bytes.toStringBinary(regionName));
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
      throw new IOException("Invalid regionName format: " + Bytes.toStringBinary(regionName));
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
   * @return byte array of table name
   * @deprecated As of release 0.96
   *             (<a href="https://issues.apache.org/jira/browse/HBASE-9508">HBASE-9508</a>).
   *             This will be removed in HBase 2.0.0. Use {@link #getTable()}.
   */
  @Deprecated
  public byte [] getTableName() {
    return getTable().toBytes();
  }

  /**
   * Get current table name of the region
   * @return TableName
   * @see #getTableName()
   */
  public TableName getTable() {
    // This method name should be getTableName but there was already a method getTableName
    // that returned a byte array.  It is unfortunate given everwhere else, getTableName returns
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
   * @throws IllegalArgumentException if the range passed is invalid (ie end < start)
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

  /** @return the object version number
   * @deprecated HRI is no longer a VersionedWritable */
  @Deprecated
  public byte getVersion() {
    return VERSION;
  }

  /**
   * @deprecated Use protobuf serialization instead.  See {@link #toByteArray()} and
   * {@link #toDelimitedByteArray()}
   */
  @Deprecated
  public void write(DataOutput out) throws IOException {
    out.writeByte(getVersion());
    Bytes.writeByteArray(out, endKey);
    out.writeBoolean(offLine);
    out.writeLong(regionId);
    Bytes.writeByteArray(out, regionName);
    out.writeBoolean(split);
    Bytes.writeByteArray(out, startKey);
    Bytes.writeByteArray(out, tableName.getName());
    out.writeInt(hashCode);
  }

  /**
   * @deprecated Use protobuf deserialization instead.
   * @see #parseFrom(byte[])
   */
  @Deprecated
  public void readFields(DataInput in) throws IOException {
    // Read the single version byte.  We don't ask the super class do it
    // because freaks out if its not the current classes' version.  This method
    // can deserialize version 0 and version 1 of HRI.
    byte version = in.readByte();
    if (version == 0) {
      // This is the old HRI that carried an HTD.  Migrate it.  The below
      // was copied from the old 0.90 HRI readFields.
      this.endKey = Bytes.readByteArray(in);
      this.offLine = in.readBoolean();
      this.regionId = in.readLong();
      this.regionName = Bytes.readByteArray(in);
      this.split = in.readBoolean();
      this.startKey = Bytes.readByteArray(in);
      try {
        HTableDescriptor htd = new HTableDescriptor();
        htd.readFields(in);
        this.tableName = htd.getTableName();
      } catch(EOFException eofe) {
         throw new IOException("HTD not found in input buffer", eofe);
      }
      this.hashCode = in.readInt();
    } else if (getVersion() == version) {
      this.endKey = Bytes.readByteArray(in);
      this.offLine = in.readBoolean();
      this.regionId = in.readLong();
      this.regionName = Bytes.readByteArray(in);
      this.split = in.readBoolean();
      this.startKey = Bytes.readByteArray(in);
      this.tableName = TableName.valueOf(Bytes.readByteArray(in));
      this.hashCode = in.readInt();
    } else {
      throw new IOException("Non-migratable/unknown version=" + getVersion());
    }
  }

  @Deprecated
  private void readFields(byte[] bytes, int offset, int len) throws IOException {
    if (bytes == null || len <= 0) {
      throw new IllegalArgumentException("Can't build a writable with empty " +
          "bytes array");
    }
    DataInputBuffer in = new DataInputBuffer();
    try {
      in.reset(bytes, offset, len);
      this.readFields(in);
    } finally {
      in.close();
    }
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
   */
  public KVComparator getComparator() {
    return isMetaRegion()?
      KeyValue.META_COMPARATOR: KeyValue.COMPARATOR;
  }

  /**
   * Convert a HRegionInfo to a RegionInfo
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
      builder.setStartKey(ByteStringer.wrap(info.getStartKey()));
    }
    if (info.getEndKey() != null) {
      builder.setEndKey(ByteStringer.wrap(info.getEndKey()));
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
      try {
        HRegionInfo hri = new HRegionInfo();
        hri.readFields(bytes, offset, len);
        return hri;
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
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
   * Extract a HRegionInfo and ServerName from catalog table {@link Result}.
   * @param r Result to pull from
   * @return A pair of the {@link HRegionInfo} and the {@link ServerName}
   * (or null for server address if no address set in hbase:meta).
   * @throws IOException
   * @deprecated use MetaTableAccessor methods for interacting with meta layouts
   */
  @Deprecated
  public static Pair<HRegionInfo, ServerName> getHRegionInfoAndServerName(final Result r) {
    HRegionInfo info =
      getHRegionInfo(r, HConstants.REGIONINFO_QUALIFIER);
    ServerName sn = getServerName(r);
    return new Pair<HRegionInfo, ServerName>(info, sn);
  }

  /**
   * Returns HRegionInfo object from the column
   * HConstants.CATALOG_FAMILY:HConstants.REGIONINFO_QUALIFIER of the catalog
   * table Result.
   * @param data a Result object from the catalog table scan
   * @return HRegionInfo or null
   * @deprecated use MetaTableAccessor methods for interacting with meta layouts
   */
  @Deprecated
  public static HRegionInfo getHRegionInfo(Result data) {
    return getHRegionInfo(data, HConstants.REGIONINFO_QUALIFIER);
  }

  /**
   * Returns the daughter regions by reading the corresponding columns of the catalog table
   * Result.
   * @param data a Result object from the catalog table scan
   * @return a pair of HRegionInfo or PairOfSameType(null, null) if the region is not a split
   * parent
   * @deprecated use MetaTableAccessor methods for interacting with meta layouts
   */
  @Deprecated
  public static PairOfSameType<HRegionInfo> getDaughterRegions(Result data) throws IOException {
    HRegionInfo splitA = getHRegionInfo(data, HConstants.SPLITA_QUALIFIER);
    HRegionInfo splitB = getHRegionInfo(data, HConstants.SPLITB_QUALIFIER);

    return new PairOfSameType<HRegionInfo>(splitA, splitB);
  }

  /**
   * Returns the merge regions by reading the corresponding columns of the catalog table
   * Result.
   * @param data a Result object from the catalog table scan
   * @return a pair of HRegionInfo or PairOfSameType(null, null) if the region is not a split
   * parent
   * @deprecated use MetaTableAccessor methods for interacting with meta layouts
   */
  @Deprecated
  public static PairOfSameType<HRegionInfo> getMergeRegions(Result data) throws IOException {
    HRegionInfo mergeA = getHRegionInfo(data, HConstants.MERGEA_QUALIFIER);
    HRegionInfo mergeB = getHRegionInfo(data, HConstants.MERGEB_QUALIFIER);

    return new PairOfSameType<HRegionInfo>(mergeA, mergeB);
  }

  /**
   * Returns the HRegionInfo object from the column {@link HConstants#CATALOG_FAMILY} and
   * <code>qualifier</code> of the catalog table result.
   * @param r a Result object from the catalog table scan
   * @param qualifier Column family qualifier -- either
   * {@link HConstants#SPLITA_QUALIFIER}, {@link HConstants#SPLITB_QUALIFIER} or
   * {@link HConstants#REGIONINFO_QUALIFIER}.
   * @return An HRegionInfo instance or null.
   * @deprecated use MetaTableAccessor methods for interacting with meta layouts
   */
  @Deprecated
  public static HRegionInfo getHRegionInfo(final Result r, byte [] qualifier) {
    Cell cell = r.getColumnLatestCell(
        HConstants.CATALOG_FAMILY, qualifier);
    if (cell == null) return null;
    return parseFromOrNull(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
  }

  /**
   * @deprecated use MetaTableAccessor methods for interacting with meta layouts
   */
  @Deprecated
  public static ServerName getServerName(final Result r) {
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER);
    if (cell == null || cell.getValueLength() == 0) return null;
    String hostAndPort = Bytes.toString(
        cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY,
      HConstants.STARTCODE_QUALIFIER);
    if (cell == null || cell.getValueLength() == 0) return null;
    try {
      return ServerName.valueOf(hostAndPort,
          Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
    } catch (IllegalArgumentException e) {
      LOG.error("Ignoring invalid region for server " + hostAndPort + "; cell=" + cell, e);
      return null;
    }
  }

  /**
   * The latest seqnum that the server writing to meta observed when opening the region.
   * E.g. the seqNum when the result of {@link #getServerName(Result)} was written.
   * @param r Result to pull the seqNum from
   * @return SeqNum, or HConstants.NO_SEQNUM if there's no value written.
   * @deprecated use MetaTableAccessor methods for interacting with meta layouts
   */
  @Deprecated
  public static long getSeqNumDuringOpen(final Result r) {
    Cell cell = r.getColumnLatestCell(HConstants.CATALOG_FAMILY, HConstants.SEQNUM_QUALIFIER);
    if (cell == null || cell.getValueLength() == 0) return HConstants.NO_SEQNUM;
    return Bytes.toLong(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
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
        // Presume Writables.  Need to reset the stream since it didn't start w/ pb.
      if (in.markSupported()) {
        in.reset();
        HRegionInfo hri = new HRegionInfo();
        hri.readFields(in);
        return hri;
      } else {
        //we cannot use BufferedInputStream, it consumes more than we read from the underlying IS
        ByteArrayInputStream bais = new ByteArrayInputStream(pbuf);
        SequenceInputStream sis = new SequenceInputStream(bais, in); //concatenate input streams
        HRegionInfo hri = new HRegionInfo();
        hri.readFields(new DataInputStream(sis));
        return hri;
      }
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
    List<HRegionInfo> hris = new ArrayList<HRegionInfo>();
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
