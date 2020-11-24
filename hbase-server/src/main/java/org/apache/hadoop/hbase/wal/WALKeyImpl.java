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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.SequenceId;
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FamilyScope;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.ScopeType;

/**
 * Default implementation of Key for an Entry in the WAL.
 * For internal use only though Replication needs to have access.
 *
 * The log intermingles edits to many tables and rows, so each log entry
 * identifies the appropriate table and row.  Within a table and row, they're
 * also sorted.
 *
 * <p>Some Transactional edits (START, COMMIT, ABORT) will not have an associated row.
 *
 */
// TODO: Key and WALEdit are never used separately, or in one-to-many relation, for practical
//       purposes. They need to be merged into WALEntry.
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.REPLICATION})
public class WALKeyImpl implements WALKey {
  public static final WALKeyImpl EMPTY_WALKEYIMPL = new WALKeyImpl();

  public MultiVersionConcurrencyControl getMvcc() {
    return mvcc;
  }

  /**
   * Use it to complete mvcc transaction. This WALKeyImpl was part of
   * (the transaction is started when you call append; see the comment on FSHLog#append). To
   * complete call
   * {@link MultiVersionConcurrencyControl#complete(MultiVersionConcurrencyControl.WriteEntry)}
   * or {@link MultiVersionConcurrencyControl#complete(MultiVersionConcurrencyControl.WriteEntry)}
   * @return A WriteEntry gotten from local WAL subsystem.
   * @see #setWriteEntry(MultiVersionConcurrencyControl.WriteEntry)
   */
  public MultiVersionConcurrencyControl.WriteEntry getWriteEntry() {
    return this.writeEntry;
  }

  public void setWriteEntry(MultiVersionConcurrencyControl.WriteEntry writeEntry) {
    assert this.writeEntry == null;
    this.writeEntry = writeEntry;
    // Set our sequenceid now using WriteEntry.
    this.sequenceId = writeEntry.getWriteNumber();
  }

  private byte [] encodedRegionName;

  private TableName tablename;

  /**
   * SequenceId for this edit. Set post-construction at write-to-WAL time. Until then it is
   * NO_SEQUENCE_ID. Change it so multiple threads can read it -- e.g. access is synchronized.
   */
  private long sequenceId;

  /**
   * Used during WAL replay; the sequenceId of the edit when it came into the system.
   */
  private long origLogSeqNum = 0;

  /** Time at which this edit was written. */
  private long writeTime;

  /** The first element in the list is the cluster id on which the change has originated */
  private List<UUID> clusterIds;

  private NavigableMap<byte[], Integer> replicationScope;

  private long nonceGroup = HConstants.NO_NONCE;
  private long nonce = HConstants.NO_NONCE;
  private MultiVersionConcurrencyControl mvcc;

  /**
   * Set in a way visible to multiple threads; e.g. synchronized getter/setters.
   */
  private MultiVersionConcurrencyControl.WriteEntry writeEntry;

  private Map<String, byte[]> extendedAttributes;

  public WALKeyImpl() {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP,
        new ArrayList<>(), HConstants.NO_NONCE, HConstants.NO_NONCE, null, null, null);
  }

  public WALKeyImpl(final NavigableMap<byte[], Integer> replicationScope) {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP,
        new ArrayList<>(), HConstants.NO_NONCE, HConstants.NO_NONCE, null, replicationScope, null);
  }

  @InterfaceAudience.Private
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename,
                long logSeqNum,
      final long now, UUID clusterId) {
    List<UUID> clusterIds = new ArrayList<>(1);
    clusterIds.add(clusterId);
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, HConstants.NO_NONCE,
      HConstants.NO_NONCE, null, null, null);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now) {
    init(encodedRegionName,
        tablename,
        NO_SEQUENCE_ID,
        now,
        EMPTY_UUIDS,
        HConstants.NO_NONCE,
        HConstants.NO_NONCE,
        null, null, null);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
      final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, null, replicationScope, null);
  }

  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
      MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, mvcc, replicationScope, null);
  }

  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, final long now,
                    MultiVersionConcurrencyControl mvcc,
                    final NavigableMap<byte[], Integer> replicationScope,
                    Map<String, byte[]> extendedAttributes) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, mvcc, replicationScope, extendedAttributes);
  }

  public WALKeyImpl(final byte[] encodedRegionName,
                final TableName tablename,
                final long now,
                MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName,
        tablename,
        NO_SEQUENCE_ID,
        now,
        EMPTY_UUIDS,
        HConstants.NO_NONCE,
        HConstants.NO_NONCE,
        mvcc, null, null);
  }

  /**
   * Copy constructor that takes in an existing WALKeyImpl plus some extended attributes.
   * Intended for coprocessors to add annotations to a system-generated WALKey
   * for persistence to the WAL.
   * @param key Key to be copied into this new key
   * @param extendedAttributes Extra attributes to copy into the new key
   */
  public WALKeyImpl(WALKeyImpl key,
                    Map<String, byte[]> extendedAttributes){
    init(key.getEncodedRegionName(), key.getTableName(), key.getSequenceId(),
        key.getWriteTime(), key.getClusterIds(), key.getNonceGroup(), key.getNonce(),
        key.getMvcc(), key.getReplicationScopes(), extendedAttributes);

  }

  /**
   * Copy constructor that takes in an existing WALKey, the extra WALKeyImpl fields that the
   * parent interface is missing, plus some extended attributes. Intended
   * for coprocessors to add annotations to a system-generated WALKey for
   * persistence to the WAL.
   */
  public WALKeyImpl(WALKey key,
                    List<UUID> clusterIds,
                    MultiVersionConcurrencyControl mvcc,
                    final NavigableMap<byte[], Integer> replicationScopes,
                    Map<String, byte[]> extendedAttributes){
    init(key.getEncodedRegionName(), key.getTableName(), key.getSequenceId(),
        key.getWriteTime(), clusterIds, key.getNonceGroup(), key.getNonce(),
        mvcc, replicationScopes, extendedAttributes);

  }
  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   * <p>Used by log splitting and snapshots.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                         <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         - name of table
   * @param logSeqNum         - log sequence number
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup        the nonceGroup
   * @param nonce             the nonce
   * @param mvcc              the mvcc associate the WALKeyImpl
   * @param replicationScope  the non-default replication scope
   *                          associated with the region's column families
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
      final long now, List<UUID> clusterIds, long nonceGroup, long nonce,
      MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce, mvcc,
        replicationScope, null);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   * <p>Used by log splitting and snapshots.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         - name of table
   * @param logSeqNum         - log sequence number
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName,
                final TableName tablename,
                long logSeqNum,
                final long now,
                List<UUID> clusterIds,
                long nonceGroup,
                long nonce,
                MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup,
        nonce, mvcc, null, null);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename         the tablename
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup
   * @param nonce
   * @param mvcc mvcc control used to generate sequence numbers and control read/write points
   */
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename,
                final long now, List<UUID> clusterIds, long nonceGroup,
                final long nonce, final MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
        null, null);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename
   * @param now               Time at which this edit was written.
   * @param clusterIds        the clusters that have consumed the change(used in Replication)
   * @param nonceGroup        the nonceGroup
   * @param nonce             the nonce
   * @param mvcc mvcc control used to generate sequence numbers and control read/write points
   * @param replicationScope  the non-default replication scope of the column families
   */
  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename,
                final long now, List<UUID> clusterIds, long nonceGroup,
                final long nonce, final MultiVersionConcurrencyControl mvcc,
                NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
        replicationScope, null);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   *                          <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename
   * @param logSeqNum
   * @param nonceGroup
   * @param nonce
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKeyImpl(final byte[] encodedRegionName,
                final TableName tablename,
                long logSeqNum,
                long nonceGroup,
                long nonce,
                final MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName,
        tablename,
        logSeqNum,
        EnvironmentEdgeManager.currentTime(),
        EMPTY_UUIDS,
        nonceGroup,
        nonce,
        mvcc, null, null);
  }

  public WALKeyImpl(final byte[] encodedRegionName, final TableName tablename,
                    final long now, List<UUID> clusterIds, long nonceGroup,
                    final long nonce, final MultiVersionConcurrencyControl mvcc,
                    NavigableMap<byte[], Integer> replicationScope,
                    Map<String, byte[]> extendedAttributes){
    init(encodedRegionName,
        tablename,
        NO_SEQUENCE_ID,
        now,
        clusterIds,
        nonceGroup,
        nonce,
        mvcc, replicationScope, extendedAttributes);
  }

  @InterfaceAudience.Private
  protected void init(final byte[] encodedRegionName,
                      final TableName tablename,
                      long logSeqNum,
                      final long now,
                      List<UUID> clusterIds,
                      long nonceGroup,
                      long nonce,
                      MultiVersionConcurrencyControl mvcc,
                      NavigableMap<byte[], Integer> replicationScope,
                      Map<String, byte[]> extendedAttributes) {
    this.sequenceId = logSeqNum;
    this.writeTime = now;
    this.clusterIds = clusterIds;
    this.encodedRegionName = encodedRegionName;
    this.tablename = tablename;
    this.nonceGroup = nonceGroup;
    this.nonce = nonce;
    this.mvcc = mvcc;
    if (logSeqNum != NO_SEQUENCE_ID) {
      setSequenceId(logSeqNum);
    }
    this.replicationScope = replicationScope;
    this.extendedAttributes = extendedAttributes;
  }

  // For deserialization. DO NOT USE. See setWriteEntry below.
  @InterfaceAudience.Private
  protected void setSequenceId(long sequenceId) {
    this.sequenceId = sequenceId;
  }

  /**
   * @param compressionContext Compression context to use
   * @deprecated deparcated since hbase 2.1.0
   */
  @Deprecated
  public void setCompressionContext(CompressionContext compressionContext) {
    //do nothing
  }

  /** @return encoded region name */
  @Override
  public byte [] getEncodedRegionName() {
    return encodedRegionName;
  }

  /** @return table name */
  @Override
  public TableName getTableName() {
    return tablename;
  }

  /** @return log sequence number
   * @deprecated Use {@link #getSequenceId()}
   */
  @Deprecated
  public long getLogSeqNum() {
    return getSequenceId();
  }

  /**
   * Used to set original sequenceId for WALKeyImpl during WAL replay
   */
  public void setOrigLogSeqNum(final long sequenceId) {
    this.origLogSeqNum = sequenceId;
  }

  /**
   * Return a positive long if current WALKeyImpl is created from a replay edit; a replay edit is an
   * edit that came in when replaying WALs of a crashed server.
   * @return original sequence number of the WALEdit
   */
  @Override
  public long getOrigLogSeqNum() {
    return this.origLogSeqNum;
  }

  /**
   * SequenceId is only available post WAL-assign. Calls before this will get you a
   * {@link SequenceId#NO_SEQUENCE_ID}. See the comment on FSHLog#append and #getWriteNumber in this
   * method for more on when this sequenceId comes available.
   * @return long the new assigned sequence number
   */
  @Override
  public long getSequenceId() {
    return this.sequenceId;
  }

  /**
   * @return the write time
   */
  @Override
  public long getWriteTime() {
    return this.writeTime;
  }

  public NavigableMap<byte[], Integer> getReplicationScopes() {
    return replicationScope;
  }

  /** @return The nonce group */
  @Override
  public long getNonceGroup() {
    return nonceGroup;
  }

  /** @return The nonce */
  @Override
  public long getNonce() {
    return nonce;
  }

  private void setReplicationScope(NavigableMap<byte[], Integer> replicationScope) {
    this.replicationScope = replicationScope;
  }

  public void clearReplicationScope() {
    setReplicationScope(null);
  }

  /**
   * Marks that the cluster with the given clusterId has consumed the change
   */
  public void addClusterId(UUID clusterId) {
    if (!clusterIds.contains(clusterId)) {
      clusterIds.add(clusterId);
    }
  }

  /**
   * @return the set of cluster Ids that have consumed the change
   */
  public List<UUID> getClusterIds() {
    return clusterIds;
  }

  /**
   * @return the cluster id on which the change has originated. It there is no such cluster, it
   *         returns DEFAULT_CLUSTER_ID (cases where replication is not enabled)
   */
  @Override
  public UUID getOriginatingClusterId(){
    return clusterIds.isEmpty()? HConstants.DEFAULT_CLUSTER_ID: clusterIds.get(0);
  }

  @Override
  public void addExtendedAttribute(String attributeKey, byte[] attributeValue){
    if (extendedAttributes == null){
      extendedAttributes = new HashMap<String, byte[]>();
    }
    extendedAttributes.put(attributeKey, attributeValue);
  }

  @Override
  public byte[] getExtendedAttribute(String attributeKey){
    return extendedAttributes != null ? extendedAttributes.get(attributeKey) : null;
  }

  @Override
  public Map<String, byte[]> getExtendedAttributes(){
    return extendedAttributes != null ? new HashMap<String, byte[]>(extendedAttributes) :
        new HashMap<String, byte[]>();
  }

  @Override
  public String toString() {
    return tablename + "/" + Bytes.toString(encodedRegionName) + "/" + sequenceId;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    return compareTo((WALKey)obj) == 0;
  }

  @Override
  public int hashCode() {
    int result = Bytes.hashCode(this.encodedRegionName);
    result = (int) (result ^ getSequenceId());
    result = (int) (result ^ this.writeTime);
    return result;
  }

  @Override
  public int compareTo(WALKey o) {
    int result = Bytes.compareTo(this.encodedRegionName, o.getEncodedRegionName());
    if (result == 0) {
      long sid = getSequenceId();
      long otherSid = o.getSequenceId();
      if (sid < otherSid) {
        result = -1;
      } else if (sid  > otherSid) {
        result = 1;
      }
      if (result == 0) {
        if (this.writeTime < o.getWriteTime()) {
          result = -1;
        } else if (this.writeTime > o.getWriteTime()) {
          return 1;
        }
      }
    }
    // why isn't cluster id accounted for?
    return result;
  }

  /**
   * Drop this instance's tablename byte array and instead
   * hold a reference to the provided tablename. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internTableName(TableName tablename) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert tablename.equals(this.tablename);
    this.tablename = tablename;
  }

  /**
   * Drop this instance's region name byte array and instead
   * hold a reference to the provided region name. This is not
   * meant to be a general purpose setter - it's only used
   * to collapse references to conserve memory.
   */
  void internEncodedRegionName(byte []encodedRegionName) {
    // We should not use this as a setter - only to swap
    // in a new reference to the same table name.
    assert Bytes.equals(this.encodedRegionName, encodedRegionName);
    this.encodedRegionName = encodedRegionName;
  }

  public WALProtos.WALKey.Builder getBuilder(WALCellCodec.ByteStringCompressor compressor)
      throws IOException {
    WALProtos.WALKey.Builder builder = WALProtos.WALKey.newBuilder();
    builder.setEncodedRegionName(
      compressor.compress(this.encodedRegionName, CompressionContext.DictionaryIndex.REGION));
    builder.setTableName(
      compressor.compress(this.tablename.getName(), CompressionContext.DictionaryIndex.TABLE));
    builder.setLogSequenceNumber(getSequenceId());
    builder.setWriteTime(writeTime);
    if (this.origLogSeqNum > 0) {
      builder.setOrigSequenceNumber(this.origLogSeqNum);
    }
    if (this.nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    if (this.nonceGroup != HConstants.NO_NONCE) {
      builder.setNonceGroup(nonceGroup);
    }
    HBaseProtos.UUID.Builder uuidBuilder = HBaseProtos.UUID.newBuilder();
    for (UUID clusterId : clusterIds) {
      uuidBuilder.setLeastSigBits(clusterId.getLeastSignificantBits());
      uuidBuilder.setMostSigBits(clusterId.getMostSignificantBits());
      builder.addClusterIds(uuidBuilder.build());
    }
    if (replicationScope != null) {
      for (Map.Entry<byte[], Integer> e : replicationScope.entrySet()) {
        ByteString family =
            compressor.compress(e.getKey(), CompressionContext.DictionaryIndex.FAMILY);
        builder.addScopes(FamilyScope.newBuilder().setFamily(family)
            .setScopeType(ScopeType.forNumber(e.getValue())));
      }
    }
    if (extendedAttributes != null){
      for (Map.Entry<String, byte[]> e : extendedAttributes.entrySet()){
        WALProtos.Attribute attr = WALProtos.Attribute.newBuilder().
            setKey(e.getKey()).setValue(compressor.compress(e.getValue(),
            CompressionContext.DictionaryIndex.TABLE)).build();
        builder.addExtendedAttributes(attr);
      }
    }
    return builder;
  }

  public void readFieldsFromPb(WALProtos.WALKey walKey,
      WALCellCodec.ByteStringUncompressor uncompressor) throws IOException {
    this.encodedRegionName = uncompressor.uncompress(walKey.getEncodedRegionName(),
      CompressionContext.DictionaryIndex.REGION);
    byte[] tablenameBytes =
        uncompressor.uncompress(walKey.getTableName(), CompressionContext.DictionaryIndex.TABLE);
    this.tablename = TableName.valueOf(tablenameBytes);
    clusterIds.clear();
    for (HBaseProtos.UUID clusterId : walKey.getClusterIdsList()) {
      clusterIds.add(new UUID(clusterId.getMostSigBits(), clusterId.getLeastSigBits()));
    }
    if (walKey.hasNonceGroup()) {
      this.nonceGroup = walKey.getNonceGroup();
    }
    if (walKey.hasNonce()) {
      this.nonce = walKey.getNonce();
    }
    this.replicationScope = null;
    if (walKey.getScopesCount() > 0) {
      this.replicationScope = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      for (FamilyScope scope : walKey.getScopesList()) {
        byte[] family =
            uncompressor.uncompress(scope.getFamily(), CompressionContext.DictionaryIndex.FAMILY);
        this.replicationScope.put(family, scope.getScopeType().getNumber());
      }
    }
    setSequenceId(walKey.getLogSequenceNumber());
    this.writeTime = walKey.getWriteTime();
    if (walKey.hasOrigSequenceNumber()) {
      this.origLogSeqNum = walKey.getOrigSequenceNumber();
    }
    if (walKey.getExtendedAttributesCount() > 0){
      this.extendedAttributes = new HashMap<>(walKey.getExtendedAttributesCount());
      for (WALProtos.Attribute attr : walKey.getExtendedAttributesList()){
        byte[] value =
            uncompressor.uncompress(attr.getValue(), CompressionContext.DictionaryIndex.TABLE);
        extendedAttributes.put(attr.getKey(), value);
      }
    }
  }

  @Override
  public long estimatedSerializedSizeOf() {
    long size = encodedRegionName != null ? encodedRegionName.length : 0;
    size += tablename != null ? tablename.toBytes().length : 0;
    if (clusterIds != null) {
      size += 16 * clusterIds.size();
    }
    if (nonceGroup != HConstants.NO_NONCE) {
      size += Bytes.SIZEOF_LONG; // nonce group
    }
    if (nonce != HConstants.NO_NONCE) {
      size += Bytes.SIZEOF_LONG; // nonce
    }
    if (replicationScope != null) {
      for (Map.Entry<byte[], Integer> scope: replicationScope.entrySet()) {
        size += scope.getKey().length;
        size += Bytes.SIZEOF_INT;
      }
    }
    size += Bytes.SIZEOF_LONG; // sequence number
    size += Bytes.SIZEOF_LONG; // write time
    if (origLogSeqNum > 0) {
      size += Bytes.SIZEOF_LONG; // original sequence number
    }
    return size;
  }
}
