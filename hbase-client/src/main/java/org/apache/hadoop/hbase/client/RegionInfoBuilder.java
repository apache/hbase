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
package org.apache.hadoop.hbase.client;

import java.util.Arrays;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class RegionInfoBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(RegionInfoBuilder.class);

  /** A non-capture group so that this can be embedded. */
  public static final String ENCODED_REGION_NAME_REGEX = "(?:[a-f0-9]+)";

  private static final int MAX_REPLICA_ID = 0xFFFF;

  //TODO: Move NO_HASH to HStoreFile which is really the only place it is used.
  public static final String NO_HASH = null;

  private final TableName tableName;
  private byte[] startKey = HConstants.EMPTY_START_ROW;
  private byte[] endKey = HConstants.EMPTY_END_ROW;
  private long regionId = System.currentTimeMillis();
  private int replicaId = RegionInfo.DEFAULT_REPLICA_ID;
  private boolean offLine = false;
  private boolean split = false;

  public static RegionInfoBuilder newBuilder(TableName tableName) {
    return new RegionInfoBuilder(tableName);
  }

  public static RegionInfoBuilder newBuilder(RegionInfo regionInfo) {
    return new RegionInfoBuilder(regionInfo);
  }

  private RegionInfoBuilder(TableName tableName) {
    this.tableName = tableName;
  }

  private RegionInfoBuilder(RegionInfo regionInfo) {
    this.tableName = regionInfo.getTable();
    this.startKey = regionInfo.getStartKey();
    this.endKey = regionInfo.getEndKey();
    this.offLine = regionInfo.isOffline();
    this.split = regionInfo.isSplit();
    this.regionId = regionInfo.getRegionId();
    this.replicaId = regionInfo.getReplicaId();
  }

  public RegionInfoBuilder setStartKey(byte[] startKey) {
    this.startKey = startKey;
    return this;
  }

  public RegionInfoBuilder setEndKey(byte[] endKey) {
    this.endKey = endKey;
    return this;
  }

  public RegionInfoBuilder setRegionId(long regionId) {
    this.regionId = regionId;
    return this;
  }

  public RegionInfoBuilder setReplicaId(int replicaId) {
    this.replicaId = replicaId;
    return this;
  }

  public RegionInfoBuilder setSplit(boolean split) {
    this.split = split;
    return this;
  }

  public RegionInfoBuilder setOffline(boolean offLine) {
    this.offLine = offLine;
    return this;
  }

  public RegionInfo build() {
    return new MutableRegionInfo(tableName, startKey, endKey, split,
        regionId, replicaId, offLine);
  }

  /**
   * An implementation of RegionInfo that adds mutable methods so can build a RegionInfo instance.
   */
  @InterfaceAudience.Private
  static class MutableRegionInfo implements RegionInfo {
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

    // This flag is in the parent of a split while the parent is still referenced by daughter
    // regions. We USED to set this flag when we disabled a table but now table state is kept up in
    // zookeeper as of 0.90.0 HBase. And now in DisableTableProcedure, finally we will create bunch
    // of UnassignProcedures and at the last of the procedure we will set the region state to
    // CLOSED, and will not change the offLine flag.
    private boolean offLine = false;
    private boolean split = false;
    private final long regionId;
    private final int replicaId;
    private final byte[] regionName;
    private final byte[] startKey;
    private final byte[] endKey;
    private final int hashCode;
    private final String encodedName;
    private final byte[] encodedNameAsBytes;
    private final TableName tableName;

    private static int generateHashCode(final TableName tableName, final byte[] startKey,
        final byte[] endKey, final long regionId,
        final int replicaId, boolean offLine, byte[] regionName) {
      int result = Arrays.hashCode(regionName);
      result = (int) (result ^ regionId);
      result ^= Arrays.hashCode(checkStartKey(startKey));
      result ^= Arrays.hashCode(checkEndKey(endKey));
      result ^= Boolean.valueOf(offLine).hashCode();
      result ^= Arrays.hashCode(tableName.getName());
      result ^= replicaId;
      return result;
    }

    private static byte[] checkStartKey(byte[] startKey) {
      return startKey == null? HConstants.EMPTY_START_ROW: startKey;
    }

    private static byte[] checkEndKey(byte[] endKey) {
      return endKey == null? HConstants.EMPTY_END_ROW: endKey;
    }

    private static TableName checkTableName(TableName tableName) {
      if (tableName == null) {
        throw new IllegalArgumentException("TableName cannot be null");
      }
      return tableName;
    }

    private static int checkReplicaId(int regionId) {
      if (regionId > MAX_REPLICA_ID) {
        throw new IllegalArgumentException("ReplicaId cannot be greater than" + MAX_REPLICA_ID);
      }
      return regionId;
    }

    MutableRegionInfo(final TableName tableName, final byte[] startKey, final byte[] endKey,
      final boolean split, final long regionId, final int replicaId, boolean offLine) {
      this.tableName = checkTableName(tableName);
      this.startKey = checkStartKey(startKey);
      this.endKey = checkEndKey(endKey);
      this.split = split;
      this.regionId = regionId;
      this.replicaId = checkReplicaId(replicaId);
      this.offLine = offLine;
      this.regionName = RegionInfo.createRegionName(this.tableName, this.startKey, this.regionId,
        this.replicaId, !this.tableName.equals(TableName.META_TABLE_NAME));
      this.encodedName = RegionInfo.encodeRegionName(this.regionName);
      this.hashCode = generateHashCode(this.tableName, this.startKey, this.endKey, this.regionId,
        this.replicaId, this.offLine, this.regionName);
      this.encodedNameAsBytes = Bytes.toBytes(this.encodedName);
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
     * @return the regionName as an array of bytes.
     * @see #getRegionNameAsString()
     */
    @Override
    public byte[] getRegionName() {
      return regionName;
    }

    /**
     * @return Region name as a String for use in logging, etc.
     */
    @Override
    public String getRegionNameAsString() {
      return RegionInfo.getRegionNameAsString(this, this.regionName);
    }

    /** @return the encoded region name */
    @Override
    public String getEncodedName() {
      return this.encodedName;
    }

    @Override
    public byte[] getEncodedNameAsBytes() {
      return this.encodedNameAsBytes;
    }

    /** @return the startKey */
    @Override
    public byte[] getStartKey() {
      return startKey;
    }

    /** @return the endKey */
    @Override
    public byte[] getEndKey() {
      return endKey;
    }

    /**
     * Get current table name of the region
     * @return TableName
     */
    @Override
    public TableName getTable() {
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

    /** @return true if this region is a meta region */
    @Override
    public boolean isMetaRegion() {
      return TableName.isMetaTableName(tableName);
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
      if (!isSplit()) {
        return false;
      }
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
      return compareTo((RegionInfo)o) == 0;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      return this.hashCode;
    }
  }
}
