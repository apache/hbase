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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * A Key for an entry in the change log.
 *
 * The log intermingles edits to many tables and rows, so each log entry
 * identifies the appropriate table and row.  Within a table and row, they're
 * also sorted.
 *
 * <p>Some Transactional edits (START, COMMIT, ABORT) will not have an
 * associated row.
 * @deprecated use WALKey
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.REPLICATION)
@Deprecated
public class HLogKey extends WALKey implements Writable {
  private static final Log LOG = LogFactory.getLog(HLogKey.class);

  public HLogKey() {
    super();
  }

  @VisibleForTesting
  public HLogKey(final byte[] encodedRegionName, final TableName tablename, long logSeqNum,
      final long now, UUID clusterId) {
    super(encodedRegionName, tablename, logSeqNum, now, clusterId);
  }

  public HLogKey(final byte[] encodedRegionName, final TableName tablename) {
    super(encodedRegionName, tablename);
  }

  public HLogKey(final byte[] encodedRegionName, final TableName tablename, final long now) {
    super(encodedRegionName, tablename, now);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   * <p>Used by log splitting and snapshots.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   * <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename   - name of table
   * @param logSeqNum   - log sequence number
   * @param now Time at which this edit was written.
   * @param clusterIds the clusters that have consumed the change(used in Replication)
   */
  public HLogKey(final byte [] encodedRegionName, final TableName tablename,
      long logSeqNum, final long now, List<UUID> clusterIds, long nonceGroup, long nonce) {
    super(encodedRegionName, tablename, logSeqNum, now, clusterIds, nonceGroup, nonce);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   * <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename
   * @param now Time at which this edit was written.
   * @param clusterIds the clusters that have consumed the change(used in Replication)
   * @param nonceGroup
   * @param nonce
   */
  public HLogKey(final byte [] encodedRegionName, final TableName tablename,
      final long now, List<UUID> clusterIds, long nonceGroup, long nonce) {
    super(encodedRegionName, tablename, now, clusterIds, nonceGroup, nonce);
  }

  /**
   * Create the log key for writing to somewhere.
   * We maintain the tablename mainly for debugging purposes.
   * A regionName is always a sub-table object.
   *
   * @param encodedRegionName Encoded name of the region as returned by
   * <code>HRegionInfo#getEncodedNameAsBytes()</code>.
   * @param tablename
   * @param logSeqNum
   * @param nonceGroup
   * @param nonce
   */
  public HLogKey(final byte [] encodedRegionName, final TableName tablename, long logSeqNum,
      long nonceGroup, long nonce) {
    super(encodedRegionName, tablename, logSeqNum, nonceGroup, nonce);
  }

  @Override
  @Deprecated
  public void write(DataOutput out) throws IOException {
    LOG.warn("HLogKey is being serialized to writable - only expected in test code");
    WritableUtils.writeVInt(out, VERSION.code);
    if (compressionContext == null) {
      Bytes.writeByteArray(out, this.encodedRegionName);
      Bytes.writeByteArray(out, this.tablename.getName());
    } else {
      Compressor.writeCompressed(this.encodedRegionName, 0,
          this.encodedRegionName.length, out,
          compressionContext.regionDict);
      Compressor.writeCompressed(this.tablename.getName(), 0, this.tablename.getName().length, out,
          compressionContext.tableDict);
    }
    out.writeLong(this.logSeqNum);
    out.writeLong(this.writeTime);
    // Don't need to write the clusters information as we are using protobufs from 0.95
    // Writing only the first clusterId for testing the legacy read
    Iterator<UUID> iterator = clusterIds.iterator();
    if(iterator.hasNext()){
      out.writeBoolean(true);
      UUID clusterId = iterator.next();
      out.writeLong(clusterId.getMostSignificantBits());
      out.writeLong(clusterId.getLeastSignificantBits());
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Version version = Version.UNVERSIONED;
    // HLogKey was not versioned in the beginning.
    // In order to introduce it now, we make use of the fact
    // that encodedRegionName was written with Bytes.writeByteArray,
    // which encodes the array length as a vint which is >= 0.
    // Hence if the vint is >= 0 we have an old version and the vint
    // encodes the length of encodedRegionName.
    // If < 0 we just read the version and the next vint is the length.
    // @see Bytes#readByteArray(DataInput)
    setScopes(null); // writable HLogKey does not contain scopes
    int len = WritableUtils.readVInt(in);
    byte[] tablenameBytes = null;
    if (len < 0) {
      // what we just read was the version
      version = Version.fromCode(len);
      // We only compress V2 of WALkey.
      // If compression is on, the length is handled by the dictionary
      if (compressionContext == null || !version.atLeast(Version.COMPRESSED)) {
        len = WritableUtils.readVInt(in);
      }
    }
    if (compressionContext == null || !version.atLeast(Version.COMPRESSED)) {
      this.encodedRegionName = new byte[len];
      in.readFully(this.encodedRegionName);
      tablenameBytes = Bytes.readByteArray(in);
    } else {
      this.encodedRegionName = Compressor.readCompressed(in, compressionContext.regionDict);
      tablenameBytes = Compressor.readCompressed(in, compressionContext.tableDict);
    }

    this.logSeqNum = in.readLong();
    this.writeTime = in.readLong();

    this.clusterIds.clear();
    if (version.atLeast(Version.INITIAL)) {
      if (in.readBoolean()) {
        // read the older log
        // Definitely is the originating cluster
        clusterIds.add(new UUID(in.readLong(), in.readLong()));
      }
    } else {
      try {
        // dummy read (former byte cluster id)
        in.readByte();
      } catch(EOFException e) {
        // Means it's a very old key, just continue
      }
    }
    try {
      this.tablename = TableName.valueOf(tablenameBytes);
    } catch (IllegalArgumentException iae) {
      if (Bytes.toString(tablenameBytes).equals(TableName.OLD_META_STR)) {
        // It is a pre-namespace meta table edit, continue with new format.
        LOG.info("Got an old .META. edit, continuing with new format ");
        this.tablename = TableName.META_TABLE_NAME;
        this.encodedRegionName = HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes();
      } else if (Bytes.toString(tablenameBytes).equals(TableName.OLD_ROOT_STR)) {
        this.tablename = TableName.OLD_ROOT_TABLE_NAME;
         throw iae;
      } else throw iae;
    }
    // Do not need to read the clusters information as we are using protobufs from 0.95
  }

}
