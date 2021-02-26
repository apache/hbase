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
package org.apache.hadoop.hbase.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.replication.regionserver.WALFileLengthProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import static org.apache.commons.lang3.StringUtils.isNumeric;

/**
 * A Write Ahead Log (WAL) provides service for reading, writing waledits. This interface provides
 * APIs for WAL users (such as RegionServer) to use the WAL (do append, sync, etc).
 *
 * Note that some internals, such as log rolling and performance evaluation tools, will use
 * WAL.equals to determine if they have already seen a given WAL.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WAL extends Closeable, WALFileLengthProvider {

  /**
   * Registers WALActionsListener
   */
  void registerWALActionsListener(final WALActionsListener listener);

  /**
   * Unregisters WALActionsListener
   */
  boolean unregisterWALActionsListener(final WALActionsListener listener);

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * <p/>
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time.
   *
   * @return If lots of logs, flush the stores of returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link RegionInfo#getEncodedName()}
   */
  Map<byte[], List<byte[]>> rollWriter() throws FailedLogCloseException, IOException;

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * <p/>
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time.
   *
   * @param force
   *          If true, force creation of a new writer even if no entries have
   *          been written to the current writer
   * @return If lots of logs, flush the stores of returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link RegionInfo#getEncodedName()}
   */
  Map<byte[], List<byte[]>> rollWriter(boolean force) throws IOException;

  /**
   * Stop accepting new writes. If we have unsynced writes still in buffer, sync them.
   * Extant edits are left in place in backing storage to be replayed later.
   */
  void shutdown() throws IOException;

  /**
   * Caller no longer needs any edits from this WAL. Implementers are free to reclaim
   * underlying resources after this call; i.e. filesystem based WALs can archive or
   * delete files.
   */
  @Override
  void close() throws IOException;

  /**
   * Append a set of data edits to the WAL. 'Data' here means that the content in the edits will
   * also have transitioned through the memstore.
   * <p/>
   * The WAL is not flushed/sync'd after this transaction completes BUT on return this edit must
   * have its region edit/sequence id assigned else it messes up our unification of mvcc and
   * sequenceid. On return <code>key</code> will have the region edit/sequence id filled in.
   * @param info the regioninfo associated with append
   * @param key Modified by this call; we add to it this edits region edit/sequence id.
   * @param edits Edits to append. MAY CONTAIN NO EDITS for case where we want to get an edit
   *          sequence id that is after all currently appended edits.
   * @return Returns a 'transaction id' and <code>key</code> will have the region edit/sequence id
   *         in it.
   * @see #appendMarker(RegionInfo, WALKeyImpl, WALEdit)
   */
  long appendData(RegionInfo info, WALKeyImpl key, WALEdit edits) throws IOException;

  /**
   * Append an operational 'meta' event marker edit to the WAL. A marker meta edit could
   * be a FlushDescriptor, a compaction marker, or a region event marker; e.g. region open
   * or region close. The difference between a 'marker' append and a 'data' append as in
   * {@link #appendData(RegionInfo, WALKeyImpl, WALEdit)}is that a marker will not have
   * transitioned through the memstore.
   * <p/>
   * The WAL is not flushed/sync'd after this transaction completes BUT on return this edit must
   * have its region edit/sequence id assigned else it messes up our unification of mvcc and
   * sequenceid. On return <code>key</code> will have the region edit/sequence id filled in.
   * @param info the regioninfo associated with append
   * @param key Modified by this call; we add to it this edits region edit/sequence id.
   * @param edits Edits to append. MAY CONTAIN NO EDITS for case where we want to get an edit
   *          sequence id that is after all currently appended edits.
   * @return Returns a 'transaction id' and <code>key</code> will have the region edit/sequence id
   *         in it.
   * @see #appendData(RegionInfo, WALKeyImpl, WALEdit)
   */
  long appendMarker(RegionInfo info, WALKeyImpl key, WALEdit edits) throws IOException;

  /**
   * updates the seuence number of a specific store.
   * depending on the flag: replaces current seq number if the given seq id is bigger,
   * or even if it is lower than existing one
   */
  void updateStore(byte[] encodedRegionName, byte[] familyName, Long sequenceid,
      boolean onlyIfGreater);

  /**
   * Sync what we have in the WAL.
   */
  void sync() throws IOException;

  /**
   * Sync the WAL if the txId was not already sync'd.
   * @param txid Transaction id to sync to.
   */
  void sync(long txid) throws IOException;

  /**
   * @param forceSync Flag to force sync rather than flushing to the buffer. Example - Hadoop hflush
   *          vs hsync.
   */
  default void sync(boolean forceSync) throws IOException {
    sync();
  }

  /**
   * @param txid Transaction id to sync to.
   * @param forceSync Flag to force sync rather than flushing to the buffer. Example - Hadoop hflush
   *          vs hsync.
   */
  default void sync(long txid, boolean forceSync) throws IOException {
    sync(txid);
  }

  /**
   * WAL keeps track of the sequence numbers that are as yet not flushed im memstores
   * in order to be able to do accounting to figure which WALs can be let go. This method tells WAL
   * that some region is about to flush. The flush can be the whole region or for a column family
   * of the region only.
   *
   * <p>Currently, it is expected that the update lock is held for the region; i.e. no
   * concurrent appends while we set up cache flush.
   * @param families Families to flush. May be a subset of all families in the region.
   * @return Returns {@link HConstants#NO_SEQNUM} if we are flushing the whole region OR if
   * we are flushing a subset of all families but there are no edits in those families not
   * being flushed; in other words, this is effectively same as a flush of all of the region
   * though we were passed a subset of regions. Otherwise, it returns the sequence id of the
   * oldest/lowest outstanding edit.
   * @see #completeCacheFlush(byte[], long)
   * @see #abortCacheFlush(byte[])
   */
  Long startCacheFlush(final byte[] encodedRegionName, Set<byte[]> families);

  Long startCacheFlush(final byte[] encodedRegionName, Map<byte[], Long> familyToSeq);

  /**
   * Complete the cache flush.
   * @param encodedRegionName Encoded region name.
   * @param maxFlushedSeqId The maxFlushedSeqId for this flush. There is no edit in memory that is
   *          less that this sequence id.
   * @see #startCacheFlush(byte[], Set)
   * @see #abortCacheFlush(byte[])
   */
  void completeCacheFlush(final byte[] encodedRegionName, long maxFlushedSeqId);

  /**
   * Abort a cache flush. Call if the flush fails. Note that the only recovery
   * for an aborted flush currently is a restart of the regionserver so the
   * snapshot content dropped by the failure gets restored to the memstore.
   * @param encodedRegionName Encoded region name.
   */
  void abortCacheFlush(byte[] encodedRegionName);

  /**
   * @return Coprocessor host.
   */
  WALCoprocessorHost getCoprocessorHost();

  /**
   * Gets the earliest unflushed sequence id in the memstore for the region.
   * @param encodedRegionName The region to get the number for.
   * @return The earliest/lowest/oldest sequence id if present, HConstants.NO_SEQNUM if absent.
   * @deprecated Since version 1.2.0. Removing because not used and exposes subtle internal
   * workings. Use {@link #getEarliestMemStoreSeqNum(byte[], byte[])}
   */
  @Deprecated
  long getEarliestMemStoreSeqNum(byte[] encodedRegionName);

  /**
   * Gets the earliest unflushed sequence id in the memstore for the store.
   * @param encodedRegionName The region to get the number for.
   * @param familyName The family to get the number for.
   * @return The earliest/lowest/oldest sequence id if present, HConstants.NO_SEQNUM if absent.
   */
  long getEarliestMemStoreSeqNum(byte[] encodedRegionName, byte[] familyName);

  /**
   * Human readable identifying information about the state of this WAL.
   * Implementors are encouraged to include information appropriate for debugging.
   * Consumers are advised not to rely on the details of the returned String; it does
   * not have a defined structure.
   */
  @Override
  String toString();

  /**
   * When outside clients need to consume persisted WALs, they rely on a provided
   * Reader.
   */
  interface Reader extends Closeable {
    Entry next() throws IOException;
    Entry next(Entry reuse) throws IOException;
    void seek(long pos) throws IOException;
    long getPosition() throws IOException;
    void reset() throws IOException;
  }

  /**
   * Utility class that lets us keep track of the edit with it's key.
   */
  class Entry {
    private final WALEdit edit;
    private final WALKeyImpl key;

    public Entry() {
      this(new WALKeyImpl(), new WALEdit());
    }

    /**
     * Constructor for both params
     *
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(WALKeyImpl key, WALEdit edit) {
      this.key = key;
      this.edit = edit;
    }

    /**
     * Gets the edit
     *
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }

    /**
     * Gets the key
     *
     * @return key
     */
    public WALKeyImpl getKey() {
      return key;
    }

    /**
     * Set compression context for this entry.
     *
     * @param compressionContext
     *          Compression context
     * @deprecated deparcated since hbase 2.1.0
     */
    @Deprecated
    public void setCompressionContext(CompressionContext compressionContext) {
      key.setCompressionContext(compressionContext);
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }
  }

  /**
   * Split a WAL filename to get a start time. WALs usually have the time we start writing to them
   * as part of their name, usually the suffix. Sometimes there will be an extra suffix as when it
   * is a WAL for the meta table. For example, WALs might look like this
   * <code>10.20.20.171%3A60020.1277499063250</code> where <code>1277499063250</code> is the
   * timestamp. Could also be a meta WAL which adds a '.meta' suffix or a
   * synchronous replication WAL which adds a '.syncrep' suffix. Check for these. File also may have
   * no timestamp on it. For example the recovered.edits files are WALs but are named in ascending
   * order. Here is an example: 0000000000000016310. Allow for this.
   * @param name Name of the WAL file.
   * @return Timestamp or -1.
   */
  public static long getTimestamp(String name) {
    String [] splits = name.split("\\.");
    if (splits.length <= 1) {
      return -1;
    }
    String timestamp = splits[splits.length - 1];
    if (!isNumeric(timestamp)) {
      // Its a '.meta' or a '.syncrep' suffix.
      timestamp = splits[splits.length - 2];
      if (!isNumeric(timestamp)) {
        return -1;
      }
    }
    return Long.parseLong(timestamp);
  }
}
