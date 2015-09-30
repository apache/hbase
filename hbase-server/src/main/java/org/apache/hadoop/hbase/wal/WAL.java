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

import java.io.Closeable;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
// imports we use from yet-to-be-moved regionsever.wal
import org.apache.hadoop.hbase.regionserver.wal.CompressionContext;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALCoprocessorHost;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import com.google.common.annotations.VisibleForTesting;

/**
 * A Write Ahead Log (WAL) provides service for reading, writing waledits. This interface provides
 * APIs for WAL users (such as RegionServer) to use the WAL (do append, sync, etc).
 *
 * Note that some internals, such as log rolling and performance evaluation tools, will use
 * WAL.equals to determine if they have already seen a given WAL.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WAL {

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
   * <p>
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time.
   *
   * @return If lots of logs, flush the returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link HRegionInfo#getEncodedName()}
   */
  byte[][] rollWriter() throws FailedLogCloseException, IOException;

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * <p>
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time.
   *
   * @param force
   *          If true, force creation of a new writer even if no entries have
   *          been written to the current writer
   * @return If lots of logs, flush the returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link HRegionInfo#getEncodedName()}
   */
  byte[][] rollWriter(boolean force) throws FailedLogCloseException, IOException;

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
  void close() throws IOException;

  /**
   * Append a set of edits to the WAL. The WAL is not flushed/sync'd after this transaction
   * completes BUT on return this edit must have its region edit/sequence id assigned
   * else it messes up our unification of mvcc and sequenceid.  On return <code>key</code> will
   * have the region edit/sequence id filled in.
   * @param info
   * @param key Modified by this call; we add to it this edits region edit/sequence id.
   * @param edits Edits to append. MAY CONTAIN NO EDITS for case where we want to get an edit
   * sequence id that is after all currently appended edits.
   * @param htd used to give scope for replication TODO refactor out in favor of table name and
   * info
   * @param inMemstore Always true except for case where we are writing a compaction completion
   * record into the WAL; in this case the entry is just so we can finish an unfinished compaction
   * -- it is not an edit for memstore.
   * @return Returns a 'transaction id' and <code>key</code> will have the region edit/sequence id
   * in it.
   */
  long append(HTableDescriptor htd, HRegionInfo info, WALKey key, WALEdit edits,
    boolean inMemstore)
  throws IOException;

  /**
   * Sync what we have in the WAL.
   * @throws IOException
   */
  void sync() throws IOException;

  /**
   * Sync the WAL if the txId was not already sync'd.
   * @param txid Transaction id to sync to.
   * @throws IOException
   */
  void sync(long txid) throws IOException;

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
   * @see #completeCacheFlush(byte[])
   * @see #abortCacheFlush(byte[])
   */
  Long startCacheFlush(final byte[] encodedRegionName, Set<byte[]> families);

  /**
   * Complete the cache flush.
   * @param encodedRegionName Encoded region name.
   * @see #startCacheFlush(byte[], Set)
   * @see #abortCacheFlush(byte[])
   */
  void completeCacheFlush(final byte[] encodedRegionName);

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
   * workings. Use {@link #getEarliestMemstoreSeqNum(byte[], byte[])}
   */
  @VisibleForTesting
  @Deprecated
  long getEarliestMemstoreSeqNum(byte[] encodedRegionName);

  /**
   * Gets the earliest unflushed sequence id in the memstore for the store.
   * @param encodedRegionName The region to get the number for.
   * @param familyName The family to get the number for.
   * @return The earliest/lowest/oldest sequence id if present, HConstants.NO_SEQNUM if absent.
   */
  long getEarliestMemstoreSeqNum(byte[] encodedRegionName, byte[] familyName);

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
    private WALEdit edit;
    private WALKey key;

    public Entry() {
      edit = new WALEdit();
      // we use HLogKey here instead of WALKey directly to support legacy coprocessors.
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     *
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(WALKey key, WALEdit edit) {
      super();
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
    public WALKey getKey() {
      return key;
    }

    /**
     * Set compression context for this entry.
     *
     * @param compressionContext
     *          Compression context
     */
    public void setCompressionContext(CompressionContext compressionContext) {
      edit.setCompressionContext(compressionContext);
      key.setCompressionContext(compressionContext);
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

  }

}
