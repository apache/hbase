/**
 * Copyright 2007 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.base.Joiner;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.consensus.quorum.QuorumInfo;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JenkinsHash;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.io.VersionedWritable;
import org.apache.hadoop.io.WritableComparable;

/**
 * HRegion information.
 * Contains HRegion id, start and end keys, a reference to this
 * HRegions' table descriptor, etc.
 */
public class HRegionInfo extends VersionedWritable implements WritableComparable<HRegionInfo>{
  private static final byte VERSION = 0;
  private static final Log LOG = LogFactory.getLog(HRegionInfo.class);
  protected Map<String, InetSocketAddress[]> favoredNodesMap = new HashMap<>();

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
   * ROOT, the first META region, and regions created by an older
   * version of HBase (0.20 or prior) will continue to use the
   * old region name format.
   */

  /** Separator used to demarcate the encodedName in a region name
   * in the new format. See description on new format above.
   */
  private static final int ENC_SEPARATOR = '.';
  public  static final int MD5_HEX_LENGTH   = 32;

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
      // old format region name. ROOT and first META region also
      // use this format.EncodedName is the JenkinsHash value.
      int hashVal = Math.abs(JenkinsHash.getInstance().hash(regionName,
                                                            regionName.length,
                                                            0));
      encodedName = String.valueOf(hashVal);
    }
    return encodedName;
  }

  /** delimiter used between portions of a region name */
  public static final int DELIMITER = ',';

  /** HRegionInfo for root region */
  public static final HRegionInfo ROOT_REGIONINFO =
    new HRegionInfo(0L, HTableDescriptor.ROOT_TABLEDESC);

  /** Encoded name for the root region. This is always the same. */
  public static final String ROOT_REGION_ENCODED_NAME_STR =
      HRegionInfo.ROOT_REGIONINFO.getEncodedName();

  /** HRegionInfo for first meta region */
  public static final HRegionInfo FIRST_META_REGIONINFO =
    new HRegionInfo(1L, HTableDescriptor.META_TABLEDESC);

  private byte [] endKey = HConstants.EMPTY_BYTE_ARRAY;
  private boolean offLine = false;
  private long regionId = -1;
  private transient byte [] regionName = HConstants.EMPTY_BYTE_ARRAY;
  private String regionNameStr = "";
  private boolean split = false;
  private byte [] splitPoint = null;
  private byte [] startKey = HConstants.EMPTY_BYTE_ARRAY;
  protected HTableDescriptor tableDesc = null;
  private int hashCode = -1;
  //TODO: Move NO_HASH to HStoreFile which is really the only place it is used.
  public static final String NO_HASH = null;
  private volatile String encodedName = NO_HASH;

  // Peers of the Consensus Quorum
  private QuorumInfo quorumInfo;
  // For compatability with non-hydrabase mode
  public static String LOCAL_DC_KEY = "LOCAL_DC_KEY_FOR_NON_HYDRABASE_MODE";

  private void setHashCode() {
    int result = Arrays.hashCode(this.regionName);
    result ^= this.regionId;
    result ^= Arrays.hashCode(this.startKey);
    result ^= Arrays.hashCode(this.endKey);
    result ^= Boolean.valueOf(this.offLine).hashCode();
    result ^= this.tableDesc.hashCode();
    this.hashCode = result;
  }

  /**
   * Private constructor used constructing HRegionInfo for the catalog root and
   * first meta regions
   */
  private HRegionInfo(long regionId, HTableDescriptor tableDesc) {
    super();
    this.regionId = regionId;
    this.tableDesc = tableDesc;

    // Note: Root & First Meta regions names are still in old format
    this.regionName = createRegionName(tableDesc.getName(), null,
                                       regionId, false);
    this.regionNameStr = Bytes.toStringBinary(this.regionName);
    setHashCode();
  }

  /** Default constructor - creates empty object */
  public HRegionInfo() {
    super();
    this.tableDesc = new HTableDescriptor();
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @throws IllegalArgumentException
   */
  public HRegionInfo(final HTableDescriptor tableDesc, final byte [] startKey,
      final byte [] endKey)
  throws IllegalArgumentException {
    this(tableDesc, startKey, endKey, false);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(HTableDescriptor tableDesc, final byte [] startKey,
      final byte [] endKey, final boolean split)
  throws IllegalArgumentException {
    this(tableDesc, startKey, endKey, split, System.currentTimeMillis());
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @param regionid Region id to use.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(HTableDescriptor tableDesc, final byte [] startKey,
    final byte [] endKey, final boolean split, final long regionid)
  throws IllegalArgumentException {
    this(tableDesc, startKey, endKey, split, regionid, null, null);
  }

  /**
   * Construct HRegionInfo with explicit parameters
   *
   * @param tableDesc the table descriptor
   * @param startKey first key in region
   * @param endKey end of key range
   * @param split true if this region has split and we have daughter regions
   * regions that may or may not hold references to this region.
   * @param regionid Region id to use.
   * @throws IllegalArgumentException
   */
  public HRegionInfo(HTableDescriptor tableDesc, final byte [] startKey,
    final byte [] endKey, final boolean split, final long regionid,
      final Map<String, Map<HServerAddress, Integer>> peers,
    final Map<String, InetSocketAddress[]> favoredNodesMap)
  throws IllegalArgumentException {
    super();
    if (tableDesc == null) {
      throw new IllegalArgumentException("tableDesc cannot be null");
    }
    this.offLine = false;
    this.regionId = regionid;
    this.regionName = createRegionName(tableDesc.getName(), startKey, regionId,
        !tableDesc.isMetaRegion());
    this.regionNameStr = Bytes.toStringBinary(this.regionName);
    this.split = split;
    this.endKey = endKey == null? HConstants.EMPTY_END_ROW: endKey.clone();
    this.startKey = startKey == null?
      HConstants.EMPTY_START_ROW: startKey.clone();
    this.tableDesc = tableDesc;
    this.quorumInfo = new QuorumInfo(peers, getEncodedName());
    this.favoredNodesMap = favoredNodesMap == null ?
            new HashMap<String, InetSocketAddress[]>() : favoredNodesMap;
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
    this.regionNameStr = Bytes.toStringBinary(this.regionName);
    this.split = other.isSplit();
    this.startKey = other.getStartKey();
    this.tableDesc = other.getTableDesc();
    this.hashCode = other.hashCode();
    this.encodedName = other.getEncodedName();
    this.quorumInfo = other.quorumInfo;
    this.favoredNodesMap = other.favoredNodesMap;
  }

  private static byte [] createRegionName(final byte [] tableName,
      final byte [] startKey, final long regionid, boolean newFormat) {
    return createRegionName(tableName, startKey, Long.toString(regionid), newFormat);
  }

  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id.
   * @param newFormat should we create the region name in the new format
   *                  (such that it contains its encoded name?).
   * @return Region name made of passed tableName, startKey and id
   */
  public static byte [] createRegionName(final byte [] tableName,
      final byte [] startKey, final String id, boolean newFormat) {
    return createRegionName(tableName, startKey, Bytes.toBytes(id), newFormat);
  }
  /**
   * Make a region name of passed parameters.
   * @param tableName
   * @param startKey Can be null
   * @param id Region id
   * @param newFormat should we create the region name in the new format
   *                  (such that it contains its encoded name?).
   * @return Region name made of passed tableName, startKey and id
   */
  public static byte [] createRegionName(final byte [] tableName,
      final byte [] startKey, final byte [] id, boolean newFormat) {
    byte [] b = new byte [tableName.length + 2 + id.length +
       (startKey == null? 0: startKey.length) +
       (newFormat ? (MD5_HEX_LENGTH + 2) : 0)];

    int offset = tableName.length;
    System.arraycopy(tableName, 0, b, 0, offset);
    b[offset++] = DELIMITER;
    if (startKey != null && startKey.length > 0) {
      System.arraycopy(startKey, 0, b, offset, startKey.length);
      offset += startKey.length;
    }
    b[offset++] = DELIMITER;
    System.arraycopy(id, 0, b, offset, id.length);
    offset += id.length;

    if (newFormat) {
      //
      // Encoded name should be built into the region name.
      //
      // Use the region name thus far (namely, <tablename>,<startKey>,<id>)
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
   * Separate elements of a regionName.
   * @param regionName
   * @return Array of byte[] containing tableName, startKey and id
   * @throws IOException
   */
  public static byte [][] parseRegionName(final byte [] regionName)
  throws IOException {
    int offset = -1;
    for (int i = 0; i < regionName.length; i++) {
      if (regionName[i] == DELIMITER) {
        offset = i;
        break;
      }
    }
    if(offset == -1) {
      throw new IOException("Invalid regionName format: " +
                            Bytes.toStringBinary(regionName));
    }
    byte [] tableName = new byte[offset];
    System.arraycopy(regionName, 0, tableName, 0, offset);
    offset = -1;
    for (int i = regionName.length - 1; i > 0; i--) {
      if(regionName[i] == DELIMITER) {
        offset = i;
        break;
      }
    }
    if(offset == -1) {
      throw new IOException("Invalid regionName format: " +
                            Bytes.toStringBinary(regionName));
    }
    byte [] startKey = HConstants.EMPTY_BYTE_ARRAY;
    if(offset != tableName.length + 1) {
      startKey = new byte[offset - tableName.length - 1];
      System.arraycopy(regionName, tableName.length + 1, startKey, 0,
          offset - tableName.length - 1);
    }
    byte [] id = new byte[regionName.length - offset - 1];
    System.arraycopy(regionName, offset + 1, id, 0,
        regionName.length - offset - 1);
    byte [][] elements = new byte[3][];
    elements[0] = tableName;
    elements[1] = startKey;
    elements[2] = id;
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
      return this.regionNameStr;
    }

    // old format. regionNameStr doesn't have the region name.
    //
    //
    return this.regionNameStr + "." + this.getEncodedName();
  }

  /** @return the encoded region name */
  public synchronized String getEncodedName() {
    if (this.encodedName == NO_HASH) {
      this.encodedName = encodeRegionName(this.regionName);
    }
    return this.encodedName;
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

  /** @return the tableDesc */
  public HTableDescriptor getTableDesc(){
    return tableDesc;
  }

  /**
   * @param newDesc new table descriptor to use
   */
  public void setTableDesc(HTableDescriptor newDesc) {
    this.tableDesc = newDesc;
  }

  /** @return true if this is the root region */
  public boolean isRootRegion() {
    return this.tableDesc.isRootRegion();
  }

  /** @return true if this is the meta table */
  public boolean isMetaTable() {
    return this.tableDesc.isMetaTable();
  }

  /** @return true if this region is a meta region */
  public boolean isMetaRegion() {
    return this.tableDesc.isMetaRegion();
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
   * @return point to explicitly split the region on
   */
  public byte[] getSplitPoint() {
    return (this.splitPoint != null && this.splitPoint.length > 0)
      ? this.splitPoint : null;
  }

  /**
   * @param splitPoint set split status & position to split on
   */
  public void setSplitPoint(byte[] splitPoint) {
    this.split = true;
    this.splitPoint = splitPoint;
  }

  /**
   * @return True if this region is offline.
   */
  public boolean isOffline() {
    return this.offLine;
  }

  /**
   * @param offLine set online - offline status
   */
  public void setOffline(boolean offLine) {
    this.offLine = offLine;
  }

  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return String.format("REGION => {%s => '%s', STARTKEY => '%s', " +
            "ENDKEY => '%s', ENCODED => %s, OFFLINE => %s, SPLIT => %s, " +
            "TABLE => {%s}, FAVORED_NODES_MAP => {%s}}",
            HConstants.NAME, regionNameStr, Bytes.toStringBinary(startKey),
            Bytes.toStringBinary(endKey), getEncodedName(), isOffline(),
            isSplit(), tableDesc.toString(),
            favoredNodesMap != null ? prettyPrintFavoredNodesMap() : "");
  }
  
  /**
   * @see java.lang.Object#equals(java.lang.Object)
   *
   * TODO (arjen): this does not consider split and split point!
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

    HRegionInfo that = (HRegionInfo)o;
    if (this.compareTo(that) != 0) {
      return false;
    }

    if (this.quorumInfo == null && that.quorumInfo != null) {
      return false;
    }
    if (this.quorumInfo != null && !this.quorumInfo.equals(that.quorumInfo)) {
      return false;
    }

    return hasSameFavoredNodesMap(that);
  }

  /**
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.hashCode;
  }

  /** @return the object version number */
  @Override
  public byte getVersion() {
    return VERSION;
  }

  //
  // Writable
  //

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Bytes.writeByteArray(out, endKey);
    out.writeBoolean(offLine);
    out.writeLong(regionId);
    Bytes.writeByteArray(out, regionName);
    out.writeBoolean(split);
    if (split) {
      Bytes.writeByteArray(out, splitPoint);
    }
    Bytes.writeByteArray(out, startKey);
    tableDesc.write(out);
    out.writeInt(hashCode);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.endKey = Bytes.readByteArray(in);
    this.offLine = in.readBoolean();
    this.regionId = in.readLong();
    this.regionName = Bytes.readByteArray(in);
    this.regionNameStr = Bytes.toStringBinary(this.regionName);
    this.split = in.readBoolean();
    if (this.split) {
      this.splitPoint = Bytes.readByteArray(in);
    }
    this.startKey = Bytes.readByteArray(in);
    this.tableDesc.readFields(in);
    this.hashCode = in.readInt();
    if (quorumInfo == null) {
      quorumInfo = new QuorumInfo(
        new HashMap<String, Map<HServerAddress, Integer>>(),
        HRegionInfo.encodeRegionName(regionName));
    }
  }

  //
  // Comparable
  //

  public int compareTo(HRegionInfo o) {
    if (o == null) {
      return 1;
    }

    // Are regions of same table?
    int result = this.tableDesc.compareTo(o.tableDesc);
    if (result != 0) {
      return result;
    }

    // Compare start keys.
    result = Bytes.compareTo(this.startKey, o.startKey);
    if (result != 0) {
      return result;
    }

    // Compare end keys.
    return Bytes.compareTo(this.endKey, o.endKey);
  }

  /**
   * @return Comparator to use comparing {@link KeyValue}s.
   */
  public KVComparator getComparator() {
    return isRootRegion()? KeyValue.ROOT_COMPARATOR: isMetaRegion()?
      KeyValue.META_COMPARATOR: KeyValue.COMPARATOR;
  }

  public Map<HServerAddress,Integer> getPeersWithRank() {
    return getQuorumInfo().getPeersWithRank();
  }

  public Map<HServerAddress, String> getPeersWithCluster() {
    return getQuorumInfo().getPeersWithCluster();
  }

  @Deprecated
  public InetSocketAddress[] getFavoredNodes() {
    return getFavoredNodes(LOCAL_DC_KEY);
  }

  public InetSocketAddress[] getFavoredNodes(String dcKey) {
    return this.favoredNodesMap != null?
              this.favoredNodesMap.get(dcKey):
                null;
  }

  @Deprecated
  public void setFavoredNodes(InetSocketAddress[] favoredNodes) {
    setFavoredNodes(LOCAL_DC_KEY, favoredNodes);
  }

  public void setFavoredNodes(String dcName, InetSocketAddress[] favoredNodes) {
    if (this.favoredNodesMap == null) {
      this.favoredNodesMap = new HashMap<>();
    }
    this.favoredNodesMap.put(dcName, favoredNodes);
    setHashCode();
  }

  public void setPeers(Map<String, Map<HServerAddress, Integer>> peers) {
    this.quorumInfo.setPeers(peers);
  }

  public Map<String, Map<HServerAddress, Integer>> getPeers() {
    QuorumInfo quorumInfo = getQuorumInfo();
    if (quorumInfo != null) {
      return quorumInfo.getPeers();
    }
    return null;
  }

  public Map<String, InetSocketAddress[]> getFavoredNodesMap() {
    return favoredNodesMap;
  }

  public void setFavoredNodesMap(
          final Map<String, InetSocketAddress[]> favoredNodesMap) {
    this.favoredNodesMap = favoredNodesMap;
  }

  public boolean hasSameFavoredNodesMap(final HRegionInfo that) {
    if (that == null) {
      return false;
    }

    if (!this.favoredNodesMap.keySet().equals(that.favoredNodesMap.keySet())) {
      return false;
    }

    for (String domain : this.favoredNodesMap.keySet()) {
      if (!Arrays.equals(this.favoredNodesMap.get(domain),
              that.favoredNodesMap.get(domain))) {
        return false;
      }
    }
    return true;
  }

  public QuorumInfo getQuorumInfo() {
    return quorumInfo;
  }

  public void setQuorumInfo(final QuorumInfo quorumInfo) {
    this.quorumInfo = quorumInfo;
  }

  public String prettyPrintFavoredNodesMap() {
    if (favoredNodesMap == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(128);
    Iterator<Entry<String, InetSocketAddress[]>> it
            = favoredNodesMap.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<String, InetSocketAddress[]> domain = it.next();
      InetSocketAddress[] favoredNodes = domain.getValue();
      sb.append(domain.getKey());
      sb.append(" => [");
      if (favoredNodes != null) {
        sb.append(Joiner.on(", ").join(favoredNodes));
      }
      sb.append(it.hasNext() ? "], " : "]");
    }
    return sb.toString();
  }

  public static class MultiDCHRegionInfo extends HRegionInfo {
    private Map<String, Map<HServerAddress, Integer>> combinedPeersMap;

    public MultiDCHRegionInfo(String dcsite, HRegionInfo regionInfo) {
      super(regionInfo);
      this.favoredNodesMap = new HashMap<>();
      this.favoredNodesMap.put(dcsite, regionInfo.getFavoredNodes());
      this.combinedPeersMap = regionInfo.getPeers();
    }

    public void merge(String otherDC, HRegionInfo other) {
      this.favoredNodesMap.put(otherDC, other.getFavoredNodes());
    }

    public void validate(int quorumSize, Map<String, Integer> maxPeersPerDC)
      throws IllegalArgument {
      if (favoredNodesMap.size() == 0) {
        return;
      }

      int rankNum = quorumSize;
      for (String cluster : maxPeersPerDC.keySet()) {
        int numPeerAssignedPerDC = maxPeersPerDC.get(cluster).intValue();
        if (combinedPeersMap.get(cluster) == null) {
          combinedPeersMap.put(cluster, new HashMap
            <HServerAddress, Integer>());
        }
        InetSocketAddress[] peerAddr = favoredNodesMap.get(cluster);
        for (InetSocketAddress addr : peerAddr) {
          this.combinedPeersMap.get(cluster).put(new HServerAddress(addr), rankNum--);
          if (--numPeerAssignedPerDC == 0) {
            break;
          }
        }
        if (rankNum <= 0) {
          break;
        }
      }

      if (rankNum > 0) {
        throw new IllegalArgument("Not enough nodes to complete the peer" +
          " the peer assignment.");
      }
    }

    @Override
    public Map<String, Map<HServerAddress, Integer>> getPeers() {
      return combinedPeersMap;
    }
  }
}
