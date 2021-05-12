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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionInfoDisplay;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;

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
 * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0.
 *             use {@link RegionInfoBuilder} to build {@link RegionInfo}.
 */
@Deprecated
@InterfaceAudience.Public
public class HRegionInfo implements RegionInfo {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionInfo.class);

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

  /** A non-capture group so that this can be embedded. */
  public static final String ENCODED_REGION_NAME_REGEX = RegionInfoBuilder.ENCODED_REGION_NAME_REGEX;

  private static final int MAX_REPLICA_ID = 0xFFFF;

  /**
   * @param regionName
   * @return the encodedName
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#encodeRegionName(byte[])}.
   */
  @Deprecated
  public static String encodeRegionName(final byte [] regionName) {
    return RegionInfo.encodeRegionName(regionName);
  }

  /**
   * @return Return a short, printable name for this region (usually encoded name) for us logging.
   */
  @Override
  public String getShortNameToLog() {
    return prettyPrint(this.getEncodedName());
  }

  /**
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#getShortNameToLog(RegionInfo...)}.
   */
  @Deprecated
  public static String getShortNameToLog(HRegionInfo...hris) {
    return RegionInfo.getShortNameToLog(Arrays.asList(hris));
  }

  /**
   * @return Return a String of short, printable names for <code>hris</code>
   * (usually encoded name) for us logging.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#getShortNameToLog(List)})}.
   */
  @Deprecated
  public static String getShortNameToLog(final List<HRegionInfo> hris) {
    return RegionInfo.getShortNameToLog(hris.stream().collect(Collectors.toList()));
  }

  /**
   * Use logging.
   * @param encodedRegionName The encoded regionname.
   * @return <code>hbase:meta</code> if passed <code>1028785192</code> else returns
   * <code>encodedRegionName</code>
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#prettyPrint(String)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static String prettyPrint(final String encodedRegionName) {
    return RegionInfo.prettyPrint(encodedRegionName);
  }

  private byte [] endKey = HConstants.EMPTY_BYTE_ARRAY;
  // This flag is in the parent of a split while the parent is still referenced by daughter regions.
  // We USED to set this flag when we disabled a table but now table state is kept up in zookeeper
  // as of 0.90.0 HBase. And now in DisableTableProcedure, finally we will create bunch of
  // UnassignProcedures and at the last of the procedure we will set the region state to CLOSED, and
  // will not change the offLine flag.
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

  // Duplicated over in RegionInfoDisplay
  final static String DISPLAY_KEYS_KEY = RegionInfoDisplay.DISPLAY_KEYS_KEY;
  public final static byte[] HIDDEN_END_KEY = RegionInfoDisplay.HIDDEN_END_KEY;
  public final static byte[] HIDDEN_START_KEY = RegionInfoDisplay.HIDDEN_START_KEY;

  /** HRegionInfo for first meta region */
  // TODO: How come Meta regions still do not have encoded region names? Fix.
  public static final HRegionInfo FIRST_META_REGIONINFO =
      new HRegionInfo(1L, TableName.META_TABLE_NAME);

  private void setHashCode() {
    int result = Arrays.hashCode(this.regionName);
    result = (int) (result ^ this.regionId);
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
  public HRegionInfo(RegionInfo other) {
    super();
    this.endKey = other.getEndKey();
    this.offLine = other.isOffline();
    this.regionId = other.getRegionId();
    this.regionName = other.getRegionName();
    this.split = other.isSplit();
    this.startKey = other.getStartKey();
    this.hashCode = other.hashCode();
    this.encodedName = other.getEncodedName();
    this.tableName = other.getTable();
    this.replicaId = other.getReplicaId();
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#createRegionName(TableName, byte[], long, boolean)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final long regionid, boolean newFormat) {
    return RegionInfo.createRegionName(tableName, startKey, Long.toString(regionid), newFormat);
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id (Usually timestamp from when region was created).
   * @param newFormat should we create the region name in the new format
   *                  (such that it contains its encoded name?).
   * @return Region name made of passed tableName, startKey and id
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#createRegionName(TableName, byte[], String, boolean)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final String id, boolean newFormat) {
    return RegionInfo.createRegionName(tableName, startKey, Bytes.toBytes(id), newFormat);
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#createRegionName(TableName, byte[], long, int, boolean)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final long regionid, int replicaId, boolean newFormat) {
    return RegionInfo.createRegionName(tableName, startKey, Bytes.toBytes(Long.toString(regionid)),
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#createRegionName(TableName, byte[], byte[], boolean)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final byte [] id, boolean newFormat) {
    return RegionInfo.createRegionName(tableName, startKey, id, DEFAULT_REPLICA_ID, newFormat);
  }
  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id (Usually timestamp from when region was created).
   * @param replicaId
   * @param newFormat should we create the region name in the new format
   * @return Region name made of passed tableName, startKey, id and replicaId
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#createRegionName(TableName, byte[], byte[], int, boolean)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte [] createRegionName(final TableName tableName,
      final byte [] startKey, final byte [] id, final int replicaId, boolean newFormat) {
    return RegionInfo.createRegionName(tableName, startKey, id, replicaId, newFormat);
  }

  /**
   * Gets the table name from the specified region name.
   * @param regionName to extract the table name from
   * @return Table name
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#getTable(byte[])}.
   */
  @Deprecated
  public static TableName getTable(final byte [] regionName) {
    return RegionInfo.getTable(regionName);
  }

  /**
   * Gets the start key from the specified region name.
   * @param regionName
   * @return Start key.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#getStartKey(byte[])}.
   */
  @Deprecated
  public static byte[] getStartKey(final byte[] regionName) throws IOException {
    return RegionInfo.getStartKey(regionName);
  }

  /**
   * Separate elements of a regionName.
   * @param regionName
   * @return Array of byte[] containing tableName, startKey and id
   * @throws IOException
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#parseRegionName(byte[])}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte [][] parseRegionName(final byte [] regionName) throws IOException {
    return RegionInfo.parseRegionName(regionName);
  }

  /**
   *
   * @param regionName
   * @return if region name is encoded.
   * @throws IOException
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#isEncodedRegionName(byte[])}.
   */
  @Deprecated
  public static boolean isEncodedRegionName(byte[] regionName) throws IOException {
    return RegionInfo.isEncodedRegionName(regionName);
  }

  /** @return the regionId */
  @Override
  public long getRegionId(){
    return regionId;
  }

  /**
   * @return the regionName as an array of bytes.
   * @see #getRegionNameAsString()
   */
  @Override
  public byte [] getRegionName(){
    return regionName;
  }

  /**
   * @return Region name as a String for use in logging, etc.
   */
  @Override
  public String getRegionNameAsString() {
    if (RegionInfo.hasEncodedName(this.regionName)) {
      // new format region names already have their encoded name.
      return Bytes.toStringBinary(this.regionName);
    }

    // old format. regionNameStr doesn't have the region name.
    //
    //
    return Bytes.toStringBinary(this.regionName) + "." + this.getEncodedName();
  }

  /**
   * @return the encoded region name
   */
  @Override
  public synchronized String getEncodedName() {
    if (this.encodedName == null) {
      this.encodedName = RegionInfo.encodeRegionName(this.regionName);
    }
    return this.encodedName;
  }

  @Override
  public synchronized byte [] getEncodedNameAsBytes() {
    if (this.encodedNameAsBytes == null) {
      this.encodedNameAsBytes = Bytes.toBytes(getEncodedName());
    }
    return this.encodedNameAsBytes;
  }

  /**
   * @return the startKey
   */
  @Override
  public byte [] getStartKey(){
    return startKey;
  }

  /**
   * @return the endKey
   */
  @Override
  public byte [] getEndKey(){
    return endKey;
  }

  /**
   * Get current table name of the region
   * @return TableName
   */
  @Override
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
  @Override
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
   * @return true if the given row falls in this region.
   */
  @Override
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

  /**
   * @return true if this region is a meta region
   */
  @Override
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
   * @return true if has been split and has daughters.
   */
  @Override
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
   * @return true if this region is offline.
   */
  @Override
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
   * @return true if this is a split parent region.
   */
  @Override
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
  @Override
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
  HBaseProtos.RegionInfo convert() {
    return convert(this);
  }

  /**
   * Convert a HRegionInfo to a RegionInfo
   *
   * @param info the HRegionInfo to convert
   * @return the converted RegionInfo
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use toRegionInfo(org.apache.hadoop.hbase.client.RegionInfo)
   *             in org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static HBaseProtos.RegionInfo convert(final HRegionInfo info) {
    return ProtobufUtil.toRegionInfo(info);
  }

  /**
   * Convert a RegionInfo to a HRegionInfo
   *
   * @param proto the RegionInfo to convert
   * @return the converted HRegionInfo
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use toRegionInfo(HBaseProtos.RegionInfo)
   *             in org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static HRegionInfo convert(final HBaseProtos.RegionInfo proto) {
    RegionInfo ri = ProtobufUtil.toRegionInfo(proto);
    // This is hack of what is in RegionReplicaUtil but it is doing translation of
    // RegionInfo into HRegionInfo which is what is wanted here.
    HRegionInfo hri;
    if (ri.isMetaRegion()) {
      hri = ri.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID ?
      HRegionInfo.FIRST_META_REGIONINFO :
      new HRegionInfo(ri.getRegionId(), ri.getTable(), ri.getReplicaId());
    } else {
      hri = new HRegionInfo(
        ri.getTable(),
        ri.getStartKey(),
        ri.getEndKey(),
        ri.isSplit(),
        ri.getRegionId(),
        ri.getReplicaId());
      if (proto.hasOffline()) {
        hri.setOffline(proto.getOffline());
      }
    }
    return hri;
  }

  /**
   * @return This instance serialized as protobuf w/ a magic pb prefix.
   * @see #parseFrom(byte[])
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#toByteArray(RegionInfo)}.
   */
  @Deprecated
  public byte [] toByteArray() {
    return RegionInfo.toByteArray(this);
  }

  /**
   * @return A deserialized {@link HRegionInfo}
   * or null if we failed deserialize or passed bytes null
   * @see #toByteArray()
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#parseFromOrNull(byte[])}.
   */
  @Deprecated
  public static HRegionInfo parseFromOrNull(final byte [] bytes) {
    if (bytes == null) return null;
    return parseFromOrNull(bytes, 0, bytes.length);
  }

  /**
   * @return A deserialized {@link HRegionInfo} or null
   *  if we failed deserialize or passed bytes null
   * @see #toByteArray()
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#parseFromOrNull(byte[], int, int)}.
   */
  @Deprecated
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#parseFrom(byte[])}.
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#parseFrom(byte[], int, int)}.
   */
  @Deprecated
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#toDelimitedByteArray(RegionInfo)}.
   */
  @Deprecated
  public byte [] toDelimitedByteArray() throws IOException {
    return RegionInfo.toDelimitedByteArray(this);
  }

  /**
   * Get the descriptive name as {@link RegionState} does it but with hidden
   * startkey optionally
   * @param state
   * @param conf
   * @return descriptive string
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use RegionInfoDisplay#getDescriptiveNameFromRegionStateForDisplay(RegionState, Configuration)
   *             over in hbase-server module.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static String getDescriptiveNameFromRegionStateForDisplay(RegionState state,
      Configuration conf) {
    return RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(state, conf);
  }

  /**
   * Get the end key for display. Optionally hide the real end key.
   * @param hri
   * @param conf
   * @return the endkey
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use RegionInfoDisplay#getEndKeyForDisplay(RegionInfo, Configuration)
   *             over in hbase-server module.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte[] getEndKeyForDisplay(HRegionInfo hri, Configuration conf) {
    return RegionInfoDisplay.getEndKeyForDisplay(hri, conf);
  }

  /**
   * Get the start key for display. Optionally hide the real start key.
   * @param hri
   * @param conf
   * @return the startkey
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use RegionInfoDisplay#getStartKeyForDisplay(RegionInfo, Configuration)
   *             over in hbase-server module.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte[] getStartKeyForDisplay(HRegionInfo hri, Configuration conf) {
    return RegionInfoDisplay.getStartKeyForDisplay(hri, conf);
  }

  /**
   * Get the region name for display. Optionally hide the start key.
   * @param hri
   * @param conf
   * @return region name as String
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use RegionInfoDisplay#getRegionNameAsStringForDisplay(RegionInfo, Configuration)
   *             over in hbase-server module.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static String getRegionNameAsStringForDisplay(HRegionInfo hri, Configuration conf) {
    return RegionInfoDisplay.getRegionNameAsStringForDisplay(hri, conf);
  }

  /**
   * Get the region name for display. Optionally hide the start key.
   * @param hri
   * @param conf
   * @return region name bytes
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use RegionInfoDisplay#getRegionNameForDisplay(RegionInfo, Configuration)
   *             over in hbase-server module.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte[] getRegionNameForDisplay(HRegionInfo hri, Configuration conf) {
    return RegionInfoDisplay.getRegionNameForDisplay(hri, conf);
  }

  /**
   * Parses an HRegionInfo instance from the passed in stream.  Presumes the HRegionInfo was
   * serialized to the stream with {@link #toDelimitedByteArray()}
   * @param in
   * @return An instance of HRegionInfo.
   * @throws IOException
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#parseFrom(DataInputStream)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static HRegionInfo parseFrom(final DataInputStream in) throws IOException {
    // I need to be able to move back in the stream if this is not a pb serialization so I can
    // do the Writable decoding instead.
    int pblen = ProtobufUtil.lengthOfPBMagic();
    byte [] pbuf = new byte[pblen];
    if (in.markSupported()) { //read it with mark()
      in.mark(pblen);
    }

    //assumption: if Writable serialization, it should be longer than pblen.
    in.readFully(pbuf, 0, pblen);
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#toDelimitedByteArray(RegionInfo...)}.
   */
  @Deprecated
  @InterfaceAudience.Private
  public static byte[] toDelimitedByteArray(HRegionInfo... infos) throws IOException {
    return RegionInfo.toDelimitedByteArray(infos);
  }

  /**
   * Parses all the HRegionInfo instances from the passed in stream until EOF. Presumes the
   * HRegionInfo's were serialized to the stream with {@link #toDelimitedByteArray()}
   * @param bytes serialized bytes
   * @param offset the start offset into the byte[] buffer
   * @param length how far we should read into the byte[] buffer
   * @return All the hregioninfos that are in the byte array. Keeps reading till we hit the end.
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link RegionInfo#parseDelimitedFrom(byte[], int, int)}.
   */
  @Deprecated
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
   * @deprecated As of release 2.0.0, this will be removed in HBase 3.0.0
   *             Use {@link org.apache.hadoop.hbase.client.RegionInfo#areAdjacent(RegionInfo, RegionInfo)}.
   */
  @Deprecated
  public static boolean areAdjacent(HRegionInfo regionA, HRegionInfo regionB) {
    return RegionInfo.areAdjacent(regionA, regionB);
  }
}
