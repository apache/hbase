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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.conf.PropagatingConfigurationObserver;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileDataBlockEncoder;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.security.User;

/**
 * Interface for objects that hold a column family in a Region. Its a memstore and a set of zero or
 * more StoreFiles, which stretch backwards over time.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Store extends HeapSize, StoreConfigInformation, PropagatingConfigurationObserver {

  /* The default priority for user-specified compaction requests.
   * The user gets top priority unless we have blocking compactions. (Pri <= 0)
   */ int PRIORITY_USER = 1;
  int NO_PRIORITY = Integer.MIN_VALUE;

  // General Accessors
  CellComparator getComparator();

  Collection<StoreFile> getStorefiles();

  /**
   * Close all the readers We don't need to worry about subsequent requests because the Region
   * holds a write lock that will prevent any more reads or writes.
   * @return the {@link StoreFile StoreFiles} that were previously being used.
   * @throws IOException on failure
   */
  Collection<StoreFile> close() throws IOException;

  /**
   * Return a scanner for both the memstore and the HStore files. Assumes we are not in a
   * compaction.
   * @param scan Scan to apply when scanning the stores
   * @param targetCols columns to scan
   * @return a scanner over the current key values
   * @throws IOException on failure
   */
  KeyValueScanner getScanner(Scan scan, final NavigableSet<byte[]> targetCols, long readPt)
      throws IOException;

  /**
   * Get all scanners with no filtering based on TTL (that happens further down
   * the line).
   * @param cacheBlocks
   * @param isGet
   * @param usePread
   * @param isCompaction
   * @param matcher
   * @param startRow
   * @param stopRow
   * @param readPt
   * @return all scanners for this store
   */
  List<KeyValueScanner> getScanners(
    boolean cacheBlocks,
    boolean isGet,
    boolean usePread,
    boolean isCompaction,
    ScanQueryMatcher matcher,
    byte[] startRow,
    byte[] stopRow,
    long readPt
  ) throws IOException;
  
  ScanInfo getScanInfo();

  /**
   * Adds or replaces the specified KeyValues.
   * <p>
   * For each KeyValue specified, if a cell with the same row, family, and qualifier exists in
   * MemStore, it will be replaced. Otherwise, it will just be inserted to MemStore.
   * <p>
   * This operation is atomic on each KeyValue (row/family/qualifier) but not necessarily atomic
   * across all of them.
   * @param cells
   * @param readpoint readpoint below which we can safely remove duplicate KVs
   * @return memstore size delta
   * @throws IOException
   */
  long upsert(Iterable<Cell> cells, long readpoint) throws IOException;

  /**
   * Adds a value to the memstore
   * @param cell
   * @return memstore size delta
   */
  long add(Cell cell);

  /**
   * When was the last edit done in the memstore
   */
  long timeOfOldestEdit();

  /**
   * Removes a Cell from the memstore. The Cell is removed only if its key
   * &amp; memstoreTS match the key &amp; memstoreTS value of the cell
   * parameter.
   * @param cell
   */
  void rollback(final Cell cell);

  FileSystem getFileSystem();


  /**
   * @param maxKeyCount
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @param includeMVCCReadpoint whether we should out the MVCC readpoint
   * @return Writer for a new StoreFile in the tmp dir.
   */
  StoreFile.Writer createWriterInTmp(
      long maxKeyCount,
      Compression.Algorithm compression,
      boolean isCompaction,
      boolean includeMVCCReadpoint,
      boolean includesTags
  ) throws IOException;

  /**
   * @param maxKeyCount
   * @param compression Compression algorithm to use
   * @param isCompaction whether we are creating a new file in a compaction
   * @param includeMVCCReadpoint whether we should out the MVCC readpoint
   * @param shouldDropBehind should the writer drop caches behind writes
   * @return Writer for a new StoreFile in the tmp dir.
   */
  StoreFile.Writer createWriterInTmp(
    long maxKeyCount,
    Compression.Algorithm compression,
    boolean isCompaction,
    boolean includeMVCCReadpoint,
    boolean includesTags,
    boolean shouldDropBehind
  ) throws IOException;




  // Compaction oriented methods

  boolean throttleCompaction(long compactionSize);

  /**
   * getter for CompactionProgress object
   * @return CompactionProgress object; can be null
   */
  CompactionProgress getCompactionProgress();

  CompactionContext requestCompaction() throws IOException;

  /**
   * @deprecated see requestCompaction(int, CompactionRequest, User)
   */
  @Deprecated
  CompactionContext requestCompaction(int priority, CompactionRequest baseRequest)
      throws IOException;

  CompactionContext requestCompaction(int priority, CompactionRequest baseRequest, User user)
      throws IOException;

  void cancelRequestedCompaction(CompactionContext compaction);

  /**
   * @deprecated see compact(CompactionContext, CompactionThroughputController, User)
   */
  @Deprecated
  List<StoreFile> compact(CompactionContext compaction,
      CompactionThroughputController throughputController) throws IOException;

  List<StoreFile> compact(CompactionContext compaction,
    CompactionThroughputController throughputController, User user) throws IOException;

  /**
   * @return true if we should run a major compaction.
   */
  boolean isMajorCompaction() throws IOException;

  void triggerMajorCompaction();

  /**
   * See if there's too much store files in this store
   * @return true if number of store files is greater than the number defined in minFilesToCompact
   */
  boolean needsCompaction();

  int getCompactPriority();

  StoreFlushContext createFlushContext(long cacheFlushId);

  /**
   * Call to complete a compaction. Its for the case where we find in the WAL a compaction
   * that was not finished.  We could find one recovering a WAL after a regionserver crash.
   * See HBASE-2331.
   * @param compaction the descriptor for compaction
   * @param pickCompactionFiles whether or not pick up the new compaction output files and
   * add it to the store
   * @param removeFiles whether to remove/archive files from filesystem
   */
  void replayCompactionMarker(CompactionDescriptor compaction, boolean pickCompactionFiles,
      boolean removeFiles)
      throws IOException;

  // Split oriented methods

  boolean canSplit();

  /**
   * Determines if Store should be split
   * @return byte[] if store should be split, null otherwise.
   */
  byte[] getSplitPoint();

  // Bulk Load methods

  /**
   * This throws a WrongRegionException if the HFile does not fit in this region, or an
   * InvalidHFileException if the HFile is not valid.
   */
  void assertBulkLoadHFileOk(Path srcPath) throws IOException;

  /**
   * This method should only be called from Region. It is assumed that the ranges of values in the
   * HFile fit within the stores assigned region. (assertBulkLoadHFileOk checks this)
   *
   * @param srcPathStr
   * @param sequenceId sequence Id associated with the HFile
   */
  Path bulkLoadHFile(String srcPathStr, long sequenceId) throws IOException;

  // General accessors into the state of the store
  // TODO abstract some of this out into a metrics class

  /**
   * @return <tt>true</tt> if the store has any underlying reference files to older HFiles
   */
  boolean hasReferences();

  /**
   * @return The size of this store's memstore, in bytes
   */
  long getMemStoreSize();

  /**
   * @return The amount of memory we could flush from this memstore; usually this is equal to
   * {@link #getMemStoreSize()} unless we are carrying snapshots and then it will be the size of
   * outstanding snapshots.
   */
  long getFlushableSize();

  /**
   * Returns the memstore snapshot size
   * @return size of the memstore snapshot
   */
  long getSnapshotSize();

  HColumnDescriptor getFamily();

  /**
   * @return The maximum sequence id in all store files.
   */
  long getMaxSequenceId();

  /**
   * @return The maximum memstoreTS in all store files.
   */
  long getMaxMemstoreTS();

  /**
   * @return the data block encoder
   */
  HFileDataBlockEncoder getDataBlockEncoder();

  /** @return aggregate size of all HStores used in the last compaction */
  long getLastCompactSize();

  /** @return aggregate size of HStore */
  long getSize();

  /**
   * @return Count of store files
   */
  int getStorefilesCount();

  /**
   * @return The size of the store files, in bytes, uncompressed.
   */
  long getStoreSizeUncompressed();

  /**
   * @return The size of the store files, in bytes.
   */
  long getStorefilesSize();

  /**
   * @return The size of the store file indexes, in bytes.
   */
  long getStorefilesIndexSize();

  /**
   * Returns the total size of all index blocks in the data block indexes, including the root level,
   * intermediate levels, and the leaf level for multi-level indexes, or just the root level for
   * single-level indexes.
   * @return the total size of block indexes in the store
   */
  long getTotalStaticIndexSize();

  /**
   * Returns the total byte size of all Bloom filter bit arrays. For compound Bloom filters even the
   * Bloom blocks currently not loaded into the block cache are counted.
   * @return the total size of all Bloom filters in the store
   */
  long getTotalStaticBloomSize();

  // Test-helper methods

  /**
   * Used for tests.
   * @return cache configuration for this Store.
   */
  CacheConfig getCacheConfig();

  /**
   * @return the parent region info hosting this store
   */
  HRegionInfo getRegionInfo();

  RegionCoprocessorHost getCoprocessorHost();

  boolean areWritesEnabled();

  /**
   * @return The smallest mvcc readPoint across all the scanners in this
   * region. Writes older than this readPoint, are included  in every
   * read operation.
   */
  long getSmallestReadPoint();

  String getColumnFamilyName();

  TableName getTableName();

  /**
   * @return The number of cells flushed to disk
   */
  long getFlushedCellsCount();

  /**
   * @return The total size of data flushed to disk, in bytes
   */
  long getFlushedCellsSize();

  /**
   * @return The number of cells processed during minor compactions
   */
  long getCompactedCellsCount();

  /**
   * @return The total amount of data processed during minor compactions, in bytes
   */
  long getCompactedCellsSize();

  /**
   * @return The number of cells processed during major compactions
   */
  long getMajorCompactedCellsCount();

  /**
   * @return The total amount of data processed during major compactions, in bytes
   */
  long getMajorCompactedCellsSize();

  /*
   * @param o Observer who wants to know about changes in set of Readers
   */
  void addChangedReaderObserver(ChangedReadersObserver o);

  /*
   * @param o Observer no longer interested in changes in set of Readers.
   */
  void deleteChangedReaderObserver(ChangedReadersObserver o);

  /**
   * @return Whether this store has too many store files.
   */
  boolean hasTooManyStoreFiles();

  /**
   * Checks the underlying store files, and opens the files that  have not
   * been opened, and removes the store file readers for store files no longer
   * available. Mainly used by secondary region replicas to keep up to date with
   * the primary region files.
   * @throws IOException
   */
  void refreshStoreFiles() throws IOException;

  /**
   * This value can represent the degree of emergency of compaction for this store. It should be
   * greater than or equal to 0.0, any value greater than 1.0 means we have too many store files.
   * <ul>
   * <li>if getStorefilesCount &lt;= getMinFilesToCompact, return 0.0</li>
   * <li>return (getStorefilesCount - getMinFilesToCompact) / (blockingFileCount -
   * getMinFilesToCompact)</li>
   * </ul>
   * <p>
   * And for striped stores, we should calculate this value by the files in each stripe separately
   * and return the maximum value.
   * <p>
   * It is similar to {@link #getCompactPriority()} except that it is more suitable to use in a
   * linear formula.
   */
  double getCompactionPressure();

   /**
    * Replaces the store files that the store has with the given files. Mainly used by
    * secondary region replicas to keep up to date with
    * the primary region files.
    * @throws IOException
    */
  void refreshStoreFiles(Collection<String> newFiles) throws IOException;

  void bulkLoadHFile(StoreFileInfo fileInfo) throws IOException;

  boolean isPrimaryReplicaStore();
}
