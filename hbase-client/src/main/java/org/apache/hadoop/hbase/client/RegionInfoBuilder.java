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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.Arrays;

@InterfaceAudience.Private
public class RegionInfoBuilder {
  private static final Log LOG = LogFactory.getLog(RegionInfoBuilder.class);

  /** A non-capture group so that this can be embedded. */
  public static final String ENCODED_REGION_NAME_REGEX = "(?:[a-f0-9]+)";

  private static final int MAX_REPLICA_ID = 0xFFFF;

  //TODO: Move NO_HASH to HStoreFile which is really the only place it is used.
  public static final String NO_HASH = null;

  /**
   * RegionInfo for first meta region
   * You cannot use this builder to make an instance of the {@link #FIRST_META_REGIONINFO}.
   * Just refer to this instance. Also, while the instance is actually a MutableRI, its type is
   * just RI so the mutable methods are not available (unless you go casting); it appears
   * as immutable (I tried adding Immutable type but it just makes a mess).
   */
  // TODO: How come Meta regions still do not have encoded region names? Fix.
  // hbase:meta,,1.1588230740 should be the hbase:meta first region name.
  public static final RegionInfo FIRST_META_REGIONINFO =
    new MutableRegionInfo(1L, TableName.META_TABLE_NAME, RegionInfo.DEFAULT_REPLICA_ID);

  private MutableRegionInfo content = null;

  public static RegionInfoBuilder newBuilder(TableName tableName) {
    return new RegionInfoBuilder(tableName);
  }

  public static RegionInfoBuilder newBuilder(RegionInfo regionInfo) {
    return new RegionInfoBuilder(regionInfo);
  }

  private RegionInfoBuilder(TableName tableName) {
    this.content = new MutableRegionInfo(tableName);
  }

  private RegionInfoBuilder(RegionInfo regionInfo) {
    this.content = new MutableRegionInfo(regionInfo);
  }

  public RegionInfoBuilder setStartKey(byte[] startKey) {
    content.setStartKey(startKey);
    return this;
  }

  public RegionInfoBuilder setEndKey(byte[] endKey) {
    content.setEndKey(endKey);
    return this;
  }

  public RegionInfoBuilder setRegionId(long regionId) {
    content.setRegionId(regionId);
    return this;
  }

  public RegionInfoBuilder setReplicaId(int replicaId) {
    content.setReplicaId(replicaId);
    return this;
  }

  public RegionInfoBuilder setSplit(boolean isSplit) {
    content.setSplit(isSplit);
    return this;
  }

  public RegionInfoBuilder setOffline(boolean isOffline) {
    content.setOffline(isOffline);
    return this;
  }

  public RegionInfo build() {
    RegionInfo ri = new MutableRegionInfo(content);
    // Run a late check that we are not creating default meta region.
    if (ri.getTable().equals(TableName.META_TABLE_NAME) &&
        ri.getReplicaId() == RegionInfo.DEFAULT_REPLICA_ID) {
      throw new IllegalArgumentException("Cannot create the default meta region; " +
        "use static define FIRST_META_REGIONINFO");
    }
    return new MutableRegionInfo(content);
  }

  /**
   * An implementation of RegionInfo that adds mutable methods so can build a RegionInfo instance.
   */
  @InterfaceAudience.Private
  static class MutableRegionInfo implements RegionInfo, Comparable<RegionInfo> {
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

    // This flag is in the parent of a split while the parent is still referenced
    // by daughter regions.  We USED to set this flag when we disabled a table
    // but now table state is kept up in zookeeper as of 0.90.0 HBase.
    private boolean offLine = false;
    private boolean split = false;
    private long regionId = -1;
    private int replicaId = RegionInfo.DEFAULT_REPLICA_ID;
    private transient byte [] regionName = HConstants.EMPTY_BYTE_ARRAY;
    private byte [] startKey = HConstants.EMPTY_BYTE_ARRAY;
    private byte [] endKey = HConstants.EMPTY_BYTE_ARRAY;
    private int hashCode = -1;
    private String encodedName;
    private byte [] encodedNameAsBytes;
    // Current TableName
    private TableName tableName;

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
     * Private constructor used constructing MutableRegionInfo for the
     * first meta regions
     */
    private MutableRegionInfo(long regionId, TableName tableName, int replicaId) {
      // This constructor is currently private for making hbase:meta region only.
      super();
      this.regionId = regionId;
      this.tableName = tableName;
      this.replicaId = replicaId;
      // Note: First Meta region replicas names are in old format so we pass false here.
      this.regionName =
        RegionInfo.createRegionName(tableName, null, regionId, replicaId, false);
      setHashCode();
    }

    MutableRegionInfo(final TableName tableName) {
      this(tableName, null, null);
    }

    /**
     * Construct MutableRegionInfo with explicit parameters
     *
     * @param tableName the table name
     * @param startKey first key in region
     * @param endKey end of key range
     * @throws IllegalArgumentException
     */
    MutableRegionInfo(final TableName tableName, final byte[] startKey, final byte[] endKey)
    throws IllegalArgumentException {
      this(tableName, startKey, endKey, false);
    }

    /**
     * Construct MutableRegionInfo with explicit parameters
     *
     * @param tableName the table descriptor
     * @param startKey first key in region
     * @param endKey end of key range
     * @param split true if this region has split and we have daughter regions
     * regions that may or may not hold references to this region.
     * @throws IllegalArgumentException
     */
    MutableRegionInfo(final TableName tableName, final byte[] startKey, final byte[] endKey,
        final boolean split)
    throws IllegalArgumentException {
      this(tableName, startKey, endKey, split, System.currentTimeMillis());
    }

    /**
     * Construct MutableRegionInfo with explicit parameters
     *
     * @param tableName the table descriptor
     * @param startKey first key in region
     * @param endKey end of key range
     * @param split true if this region has split and we have daughter regions
     * regions that may or may not hold references to this region.
     * @param regionid Region id to use.
     * @throws IllegalArgumentException
     */
    MutableRegionInfo(final TableName tableName, final byte[] startKey,
                       final byte[] endKey, final boolean split, final long regionid)
    throws IllegalArgumentException {
      this(tableName, startKey, endKey, split, regionid, RegionInfo.DEFAULT_REPLICA_ID);
    }

    /**
     * Construct MutableRegionInfo with explicit parameters
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
    MutableRegionInfo(final TableName tableName, final byte[] startKey,
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

      this.regionName = RegionInfo.createRegionName(this.tableName, startKey, regionId, replicaId,
        !this.tableName.equals(TableName.META_TABLE_NAME));

      this.split = split;
      this.endKey = endKey == null? HConstants.EMPTY_END_ROW: endKey.clone();
      this.startKey = startKey == null?
        HConstants.EMPTY_START_ROW: startKey.clone();
      this.tableName = tableName;
      setHashCode();
    }

    /**
     * Construct MutableRegionInfo.
     * Only for RegionInfoBuilder to use.
     * @param other
     */
    MutableRegionInfo(MutableRegionInfo other, boolean isMetaRegion) {
      super();
      if (other.getTable() == null) {
        throw new IllegalArgumentException("TableName cannot be null");
      }
      this.tableName = other.getTable();
      this.offLine = other.isOffline();
      this.regionId = other.getRegionId();
      this.replicaId = other.getReplicaId();
      if (this.replicaId > MAX_REPLICA_ID) {
        throw new IllegalArgumentException("ReplicaId cannot be greater than" + MAX_REPLICA_ID);
      }

      if(isMetaRegion) {
        // Note: First Meta region replicas names are in old format
        this.regionName = RegionInfo.createRegionName(
                other.getTable(), null, other.getRegionId(),
                other.getReplicaId(), false);
      } else {
        this.regionName = RegionInfo.createRegionName(
                other.getTable(), other.getStartKey(), other.getRegionId(),
                other.getReplicaId(), true);
      }

      this.split = other.isSplit();
      this.endKey = other.getEndKey() == null? HConstants.EMPTY_END_ROW: other.getEndKey().clone();
      this.startKey = other.getStartKey() == null?
        HConstants.EMPTY_START_ROW: other.getStartKey().clone();
      this.tableName = other.getTable();
      setHashCode();
    }

    /**
     * Construct a copy of RegionInfo as MutableRegionInfo.
     * Only for RegionInfoBuilder to use.
     * @param regionInfo
     */
    MutableRegionInfo(RegionInfo regionInfo) {
      super();
      this.endKey = regionInfo.getEndKey();
      this.offLine = regionInfo.isOffline();
      this.regionId = regionInfo.getRegionId();
      this.regionName = regionInfo.getRegionName();
      this.split = regionInfo.isSplit();
      this.startKey = regionInfo.getStartKey();
      this.hashCode = regionInfo.hashCode();
      this.encodedName = regionInfo.getEncodedName();
      this.tableName = regionInfo.getTable();
      this.replicaId = regionInfo.getReplicaId();
    }

    /**
     * @return Return a short, printable name for this region
     * (usually encoded name) for us logging.
     */
    @Override
    public String getShortNameToLog() {
      return RegionInfo.prettyPrint(this.getEncodedName());
    }

    /** @return the regionId */
    @Override
    public long getRegionId(){
      return regionId;
    }

    /**
     * set region id.
     * @param regionId
     * @return MutableRegionInfo
     */
    public MutableRegionInfo setRegionId(long regionId) {
      this.regionId = regionId;
      return this;
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
     * set region name.
     * @param regionName
     * @return MutableRegionInfo
     */
    public MutableRegionInfo setRegionName(byte[] regionName) {
      this.regionName = regionName;
      return this;
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

    /** @return the encoded region name */
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

    /** @return the startKey */
    @Override
    public byte [] getStartKey(){
      return startKey;
    }

    /**
     * @param startKey
     * @return MutableRegionInfo
     */
    public MutableRegionInfo setStartKey(byte[] startKey) {
      this.startKey = startKey;
      return this;
    }

    /** @return the endKey */
    @Override
    public byte [] getEndKey(){
      return endKey;
    }

    /**
     * @param endKey
     * @return MutableRegionInfo
     */
    public MutableRegionInfo setEndKey(byte[] endKey) {
      this.endKey = endKey;
      return this;
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
        tableName = RegionInfo.getTable(getRegionName());
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
     * Return true if the given row falls in this region.
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
    @Override
    public boolean isMetaTable() {
      return isMetaRegion();
    }

    /** @return true if this region is a meta region */
    @Override
    public boolean isMetaRegion() {
       return tableName.equals(FIRST_META_REGIONINFO.getTable());
    }

    /**
     * @return true if this region is from a system table
     */
    @Override
    public boolean isSystemTable() {
      return tableName.isSystemTable();
    }

    /**
     * @return True if has been split and has daughters.
     */
    @Override
    public boolean isSplit() {
      return this.split;
    }

    /**
     * @param split set split status
     * @return MutableRegionInfo
     */
    public MutableRegionInfo setSplit(boolean split) {
      this.split = split;
      return this;
    }

    /**
     * @return True if this region is offline.
     */
    @Override
    public boolean isOffline() {
      return this.offLine;
    }

    /**
     * The parent of a region split is offline while split daughters hold
     * references to the parent. Offlined regions are closed.
     * @param offLine Set online/offline status.
     * @return MutableRegionInfo
     */
    public MutableRegionInfo setOffline(boolean offLine) {
      this.offLine = offLine;
      return this;
    }

    /**
     * @return True if this is a split parent region.
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

    public MutableRegionInfo setReplicaId(int replicaId) {
      this.replicaId = replicaId;
      return this;
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
     * @param o
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
      if (!(o instanceof RegionInfo)) {
        return false;
      }
      return this.compareTo((RegionInfo)o) == 0;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      return this.hashCode;
    }

    @Override
    public int compareTo(RegionInfo other) {
      return RegionInfo.COMPARATOR.compare(this, other);
    }

    /**
     * @return Comparator to use comparing {@link KeyValue}s.
     * @deprecated Use Region#getCellComparator().  deprecated for hbase 2.0, remove for hbase 3.0
     */
    @Deprecated
    public KeyValue.KVComparator getComparator() {
      return isMetaRegion()?
          KeyValue.META_COMPARATOR: KeyValue.COMPARATOR;
    }
  }
}