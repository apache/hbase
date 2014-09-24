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
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.io.Writable;

import com.google.common.annotations.VisibleForTesting;


@InterfaceAudience.Private
// TODO: Rename interface to WAL
public interface HLog {
  Log LOG = LogFactory.getLog(HLog.class);

  /** File Extension used while splitting an HLog into regions (HBASE-2312) */
  // TODO: this seems like an implementation detail that does not belong here.
  String SPLITTING_EXT = "-splitting";
  boolean SPLIT_SKIP_ERRORS_DEFAULT = false;
  /** The hbase:meta region's HLog filename extension */
  String META_HLOG_FILE_EXTN = ".meta";

  /**
   * Configuration name of HLog Trailer's warning size. If a waltrailer's size is greater than the
   * configured size, a warning is logged. This is used with Protobuf reader/writer.
   */
  // TODO: Implementation detail.  Why in here?
  String WAL_TRAILER_WARN_SIZE = "hbase.regionserver.waltrailer.warn.size";
  int DEFAULT_WAL_TRAILER_WARN_SIZE = 1024 * 1024; // 1MB

  // TODO: Implemenation detail.  Why in here?
  Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";

  interface Reader {

    /**
     * @param fs File system.
     * @param path Path.
     * @param c Configuration.
     * @param s Input stream that may have been pre-opened by the caller; may be null.
     */
    void init(FileSystem fs, Path path, Configuration c, FSDataInputStream s) throws IOException;

    void close() throws IOException;

    Entry next() throws IOException;

    Entry next(Entry reuse) throws IOException;

    void seek(long pos) throws IOException;

    long getPosition() throws IOException;
    void reset() throws IOException;

    /**
     * @return the WALTrailer of the current HLog. It may be null in case of legacy or corrupt WAL
     *         files.
     */
    // TODO: What we need a trailer on WAL for?
    WALTrailer getWALTrailer();
  }

  interface Writer {
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable) throws IOException;

    void close() throws IOException;

    void sync() throws IOException;

    void append(Entry entry) throws IOException;

    long getLength() throws IOException;

    /**
     * Sets HLog's WALTrailer. This trailer is appended at the end of WAL on closing.
     * @param walTrailer trailer to append to WAL.
     */
    void setWALTrailer(WALTrailer walTrailer);
  }

  /**
   * Utility class that lets us keep track of the edit with it's key.
   * Only used when splitting logs.
   */
  // TODO: Remove this Writable.
  class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     *
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
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
    public HLogKey getKey() {
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

    @Override
    @SuppressWarnings("deprecation")
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
  }

  /**
   * registers WALActionsListener
   *
   * @param listener
   */
  void registerWALActionsListener(final WALActionsListener listener);

  /**
   * unregisters WALActionsListener
   *
   * @param listener
   */
  boolean unregisterWALActionsListener(final WALActionsListener listener);

  /**
   * @return Current state of the monotonically increasing file id.
   */
  // TODO: Remove.  Implementation detail.
  long getFilenum();

  /**
   * @return the number of HLog files
   */
  int getNumLogFiles();
  
  /**
   * @return the size of HLog files
   */
  long getLogFileSize();
  
  // TODO: Log rolling should not be in this interface.
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
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
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
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  byte[][] rollWriter(boolean force) throws FailedLogCloseException,
      IOException;

  /**
   * Shut down the log.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Shut down the log and delete the log directory
   *
   * @throws IOException
   */
  void closeAndDelete() throws IOException;

  /**
   * Same as appendNoSync(HRegionInfo, TableName, WALEdit, List, long, HTableDescriptor),
   * except it causes a sync on the log
   * @param sequenceId of the region.
   */
  @VisibleForTesting
  public void append(HRegionInfo info, TableName tableName, WALEdit edits,
      final long now, HTableDescriptor htd, AtomicLong sequenceId) throws IOException;

  /**
   * For notification post append to the writer.
   * @param entries
   */
  void postAppend(final List<Entry> entries);

  /**
   * For notification post writer sync.
   */
  void postSync();

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded) regionName, rowname, and
   * log-sequence-id. The HLog is not flushed after this transaction is written to the log.
   * @param info
   * @param tableName
   * @param edits
   * @param clusterIds The clusters that have consumed the change (for replication)
   * @param now
   * @param htd
   * @param sequenceId of the region
   * @return txid of this transaction
   * @throws IOException
   */
  long appendNoSync(HRegionInfo info, TableName tableName, WALEdit edits,
      List<UUID> clusterIds, final long now, HTableDescriptor htd, AtomicLong sequenceId,
      boolean isInMemstore, long nonceGroup, long nonce) throws IOException;

  // TODO: Do we need all these versions of sync?
  void hsync() throws IOException;

  void hflush() throws IOException;

  void sync() throws IOException;

  void sync(long txid) throws IOException;

  /**
   * WAL keeps track of the sequence numbers that were not yet flushed from memstores
   * in order to be able to do cleanup. This method tells WAL that some region is about
   * to flush memstore.
   *
   * We stash the oldest seqNum for the region, and let the the next edit inserted in this
   * region be recorded in {@link #append(HRegionInfo, TableName, WALEdit, long, HTableDescriptor,
   * AtomicLong)} as new oldest seqnum.
   * In case of flush being aborted, we put the stashed value back; in case of flush succeeding,
   * the seqNum of that first edit after start becomes the valid oldest seqNum for this region.
   *
   * @return true if the flush can proceed, false in case wal is closing (ususally, when server is
   * closing) and flush couldn't be started.
   */
  boolean startCacheFlush(final byte[] encodedRegionName);

  /**
   * Complete the cache flush.
   * @param encodedRegionName Encoded region name.
   */
  void completeCacheFlush(final byte[] encodedRegionName);

  /**
   * Abort a cache flush. Call if the flush fails. Note that the only recovery
   * for an aborted flush currently is a restart of the regionserver so the
   * snapshot content dropped by the failure gets restored to the memstore.v
   * @param encodedRegionName Encoded region name.
   */
  void abortCacheFlush(byte[] encodedRegionName);

  /**
   * @return Coprocessor host.
   */
  WALCoprocessorHost getCoprocessorHost();

  /**
   * Get LowReplication-Roller status
   *
   * @return lowReplicationRollEnabled
   */
  // TODO: This is implementation detail?
  boolean isLowReplicationRollEnabled();

  /** Gets the earliest sequence number in the memstore for this particular region.
   * This can serve as best-effort "recent" WAL number for this region.
   * @param encodedRegionName The region to get the number for.
   * @return The number if present, HConstants.NO_SEQNUM if absent.
   */
  long getEarliestMemstoreSeqNum(byte[] encodedRegionName);
}
