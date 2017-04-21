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
package org.apache.hadoop.hbase.wal;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.SequenceId;
// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.WALCellCodec;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.UnsafeByteOperations;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.FamilyScope;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.ScopeType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * A Key for an entry in the WAL.
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
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
public class WALKey implements SequenceId, Comparable<WALKey> {

  /**
   * Used to represent when a particular wal key doesn't know/care about the sequence ordering.
   */
  public static final long NO_SEQUENCE_ID = -1;

  @InterfaceAudience.Private // For internal use only.
  public MultiVersionConcurrencyControl getMvcc() {
    return mvcc;
  }

  /**
   * Use it to complete mvcc transaction. This WALKey was part of
   * (the transaction is started when you call append; see the comment on FSHLog#append). To
   * complete call
   * {@link MultiVersionConcurrencyControl#complete(MultiVersionConcurrencyControl.WriteEntry)}
   * or {@link MultiVersionConcurrencyControl#complete(MultiVersionConcurrencyControl.WriteEntry)}
   * @return A WriteEntry gotten from local WAL subsystem.
   * @see #setWriteEntry(MultiVersionConcurrencyControl.WriteEntry)
   */
  @InterfaceAudience.Private // For internal use only.
  public MultiVersionConcurrencyControl.WriteEntry getWriteEntry() throws InterruptedIOException {
    assert this.writeEntry != null;
    return this.writeEntry;
  }

  @InterfaceAudience.Private // For internal use only.
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
  public static final List<UUID> EMPTY_UUIDS = Collections.unmodifiableList(new ArrayList<UUID>());

  private CompressionContext compressionContext;

  public WALKey() {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP,
        new ArrayList<>(), HConstants.NO_NONCE, HConstants.NO_NONCE, null, null);
  }

  public WALKey(final NavigableMap<byte[], Integer> replicationScope) {
    init(null, null, 0L, HConstants.LATEST_TIMESTAMP,
        new ArrayList<>(), HConstants.NO_NONCE, HConstants.NO_NONCE, null, replicationScope);
  }

  @VisibleForTesting
  public WALKey(final byte[] encodedRegionName, final TableName tablename,
                long logSeqNum,
      final long now, UUID clusterId) {
    List<UUID> clusterIds = new ArrayList<>(1);
    clusterIds.add(clusterId);
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds,
        HConstants.NO_NONCE, HConstants.NO_NONCE, null, null);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName, final TableName tablename, final long now) {
    init(encodedRegionName,
        tablename,
        NO_SEQUENCE_ID,
        now,
        EMPTY_UUIDS,
        HConstants.NO_NONCE,
        HConstants.NO_NONCE,
        null, null);
  }

  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName, final TableName tablename, final long now,
      final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, null, replicationScope);
  }

  public WALKey(final byte[] encodedRegionName, final TableName tablename, final long now,
      MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, EMPTY_UUIDS, HConstants.NO_NONCE,
        HConstants.NO_NONCE, mvcc, replicationScope);
  }

  public WALKey(final byte[] encodedRegionName,
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
        mvcc, null);
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
   * @param mvcc              the mvcc associate the WALKey
   * @param replicationScope  the non-default replication scope
   *                          associated with the region's column families
   */
  // TODO: Fix being able to pass in sequenceid.
  public WALKey(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
      final long now, List<UUID> clusterIds, long nonceGroup, long nonce,
      MultiVersionConcurrencyControl mvcc, final NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce, mvcc,
        replicationScope);
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
  public WALKey(final byte[] encodedRegionName,
                final TableName tablename,
                long logSeqNum,
                final long now,
                List<UUID> clusterIds,
                long nonceGroup,
                long nonce,
                MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce, mvcc, null);
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
  public WALKey(final byte[] encodedRegionName, final TableName tablename,
                final long now, List<UUID> clusterIds, long nonceGroup,
                final long nonce, final MultiVersionConcurrencyControl mvcc) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
        null);
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
  public WALKey(final byte[] encodedRegionName, final TableName tablename,
                final long now, List<UUID> clusterIds, long nonceGroup,
                final long nonce, final MultiVersionConcurrencyControl mvcc,
                NavigableMap<byte[], Integer> replicationScope) {
    init(encodedRegionName, tablename, NO_SEQUENCE_ID, now, clusterIds, nonceGroup, nonce, mvcc,
        replicationScope);
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
  public WALKey(final byte[] encodedRegionName,
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
        mvcc, null);
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
                      NavigableMap<byte[], Integer> replicationScope) {
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
  }

  // For deserialization. DO NOT USE. See setWriteEntry below.
  @InterfaceAudience.Private
  protected void setSequenceId(long sequenceId) {
    this.sequenceId = sequenceId;
  }

  /**
   * @param compressionContext Compression context to use
   */
  public void setCompressionContext(CompressionContext compressionContext) {
    this.compressionContext = compressionContext;
  }

  /** @return encoded region name */
  public byte [] getEncodedRegionName() {
    return encodedRegionName;
  }

  /** @return table name */
  public TableName getTablename() {
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
   * Used to set original sequenceId for WALKey during WAL replay
   */
  public void setOrigLogSeqNum(final long sequenceId) {
    this.origLogSeqNum = sequenceId;
  }
  
  /**
   * Return a positive long if current WALKey is created from a replay edit; a replay edit is an
   * edit that came in when replaying WALs of a crashed server.
   * @return original sequence number of the WALEdit
   */
  public long getOrigLogSeqNum() {
    return this.origLogSeqNum;
  }
  
  /**
   * SequenceId is only available post WAL-assign. Calls before this will get you a
   * {@link #NO_SEQUENCE_ID}. See the comment on FSHLog#append and #getWriteNumber in this method
   * for more on when this sequenceId comes available.
   * @return long the new assigned sequence number
   */
  @Override
  public long getSequenceId() {
    return this.sequenceId;
  }

  /**
   * @return the write time
   */
  public long getWriteTime() {
    return this.writeTime;
  }

  public NavigableMap<byte[], Integer> getReplicationScopes() {
    return replicationScope;
  }

  /** @return The nonce group */
  public long getNonceGroup() {
    return nonceGroup;
  }

  /** @return The nonce */
  public long getNonce() {
    return nonce;
  }

  private void setReplicationScope(NavigableMap<byte[], Integer> replicationScope) {
    this.replicationScope = replicationScope;
  }

  public void serializeReplicationScope(boolean serialize) {
    if (!serialize) {
      setReplicationScope(null);
    }
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
  public UUID getOriginatingClusterId(){
    return clusterIds.isEmpty() ? HConstants.DEFAULT_CLUSTER_ID : clusterIds.get(0);
  }

  @Override
  public String toString() {
    return tablename + "/" + Bytes.toString(encodedRegionName) + "/" +
      sequenceId;
  }

  /**
   * Produces a string map for this key. Useful for programmatic use and
   * manipulation of the data stored in an WALKey, for example, printing
   * as JSON.
   *
   * @return a Map containing data from this key
   */
  public Map<String, Object> toStringMap() {
    Map<String, Object> stringMap = new HashMap<>();
    stringMap.put("table", tablename);
    stringMap.put("region", Bytes.toStringBinary(encodedRegionName));
    stringMap.put("sequence", getSequenceId());
    return stringMap;
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
    result ^= getSequenceId();
    result ^= this.writeTime;
    return result;
  }

  @Override
  public int compareTo(WALKey o) {
    int result = Bytes.compareTo(this.encodedRegionName, o.encodedRegionName);
    if (result == 0) {
      long sid = getSequenceId();
      long otherSid = o.getSequenceId();
      if (sid < otherSid) {
        result = -1;
      } else if (sid  > otherSid) {
        result = 1;
      }
      if (result == 0) {
        if (this.writeTime < o.writeTime) {
          result = -1;
        } else if (this.writeTime > o.writeTime) {
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

  public WALProtos.WALKey.Builder getBuilder(
      WALCellCodec.ByteStringCompressor compressor) throws IOException {
    WALProtos.WALKey.Builder builder = WALProtos.WALKey.newBuilder();
    if (compressionContext == null) {
      builder.setEncodedRegionName(UnsafeByteOperations.unsafeWrap(this.encodedRegionName));
      builder.setTableName(UnsafeByteOperations.unsafeWrap(this.tablename.getName()));
    } else {
      builder.setEncodedRegionName(compressor.compress(this.encodedRegionName,
          compressionContext.regionDict));
      builder.setTableName(compressor.compress(this.tablename.getName(),
          compressionContext.tableDict));
    }
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
        ByteString family = (compressionContext == null)
            ? UnsafeByteOperations.unsafeWrap(e.getKey())
            : compressor.compress(e.getKey(), compressionContext.familyDict);
        builder.addScopes(FamilyScope.newBuilder()
            .setFamily(family).setScopeType(ScopeType.forNumber(e.getValue())));
      }
    }
    return builder;
  }

  public void readFieldsFromPb(WALProtos.WALKey walKey,
                               WALCellCodec.ByteStringUncompressor uncompressor)
      throws IOException {
    if (this.compressionContext != null) {
      this.encodedRegionName = uncompressor.uncompress(
          walKey.getEncodedRegionName(), compressionContext.regionDict);
      byte[] tablenameBytes = uncompressor.uncompress(
          walKey.getTableName(), compressionContext.tableDict);
      this.tablename = TableName.valueOf(tablenameBytes);
    } else {
      this.encodedRegionName = walKey.getEncodedRegionName().toByteArray();
      this.tablename = TableName.valueOf(walKey.getTableName().toByteArray());
    }
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
        byte[] family = (compressionContext == null) ? scope.getFamily().toByteArray() :
          uncompressor.uncompress(scope.getFamily(), compressionContext.familyDict);
        this.replicationScope.put(family, scope.getScopeType().getNumber());
      }
    }
    setSequenceId(walKey.getLogSequenceNumber());
    this.writeTime = walKey.getWriteTime();
    if(walKey.hasOrigSequenceNumber()) {
      this.origLogSeqNum = walKey.getOrigSequenceNumber();
    }
  }
}