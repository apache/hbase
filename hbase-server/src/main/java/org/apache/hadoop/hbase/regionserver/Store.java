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
import java.util.OptionalDouble;
import java.util.OptionalLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Interface for objects that hold a column family in a Region. Its a memstore and a set of zero or
 * more StoreFiles, which stretch backwards over time.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface Store {

  /**
   * The default priority for user-specified compaction requests.
   * The user gets top priority unless we have blocking compactions. (Pri <= 0)
   */
  int PRIORITY_USER = 1;
  int NO_PRIORITY = Integer.MIN_VALUE;

  // General Accessors
  CellComparator getComparator();

  Collection<? extends StoreFile> getStorefiles();

  Collection<? extends StoreFile> getCompactedFiles();

  /**
   * When was the last edit done in the memstore
   */
  long timeOfOldestEdit();

  FileSystem getFileSystem();

  /**
   * Tests whether we should run a major compaction. For example, if the configured major compaction
   * interval is reached.
   * @return true if we should run a major compaction.
   */
  boolean shouldPerformMajorCompaction() throws IOException;

  /**
   * See if there's too much store files in this store
   * @return <code>true</code> if number of store files is greater than the number defined in
   *         minFilesToCompact
   */
  boolean needsCompaction();

  int getCompactPriority();

  /**
   * Returns whether this store is splittable, i.e., no reference file in this store.
   */
  boolean canSplit();

  /**
   * @return <code>true</code> if the store has any underlying reference files to older HFiles
   */
  boolean hasReferences();

  /**
   * @return The size of this store's memstore.
   */
  MemStoreSize getMemStoreSize();

  /**
   * @return The amount of memory we could flush from this memstore; usually this is equal to
   * {@link #getMemStoreSize()} unless we are carrying snapshots and then it will be the size of
   * outstanding snapshots.
   */
  MemStoreSize getFlushableSize();

  /**
   * @return size of the memstore snapshot
   */
  MemStoreSize getSnapshotSize();

  ColumnFamilyDescriptor getColumnFamilyDescriptor();

  /**
   * @return The maximum sequence id in all store files.
   */
  OptionalLong getMaxSequenceId();

  /**
   * @return The maximum memstoreTS in all store files.
   */
  OptionalLong getMaxMemStoreTS();

  /** @return aggregate size of all HStores used in the last compaction */
  long getLastCompactSize();

  /** @return aggregate size of HStore */
  long getSize();

  /**
   * @return Count of store files
   */
  int getStorefilesCount();

  /**
   * @return Count of compacted store files
   */
  int getCompactedFilesCount();

  /**
   * @return Max age of store files in this store
   */
  OptionalLong getMaxStoreFileAge();

  /**
   * @return Min age of store files in this store
   */
  OptionalLong getMinStoreFileAge();

  /**
   *  @return Average age of store files in this store
   */
  OptionalDouble getAvgStoreFileAge();

  /**
   *  @return Number of reference files in this store
   */
  long getNumReferenceFiles();

  /**
   *  @return Number of HFiles in this store
   */
  long getNumHFiles();

  /**
   * @return The size of the store files, in bytes, uncompressed.
   */
  long getStoreSizeUncompressed();

  /**
   * @return The size of the store files, in bytes.
   */
  long getStorefilesSize();

  /**
   * @return The size of only the store files which are HFiles, in bytes.
   */
  long getHFilesSize();

  /**
   * @return The size of the store file root-level indexes, in bytes.
   */
  long getStorefilesRootLevelIndexSize();

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

  /**
   * @return the parent region info hosting this store
   */
  RegionInfo getRegionInfo();

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
   * @return The total size of out output files on disk, in bytes
   */
  long getFlushedOutputFileSize();

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

  /**
   * @return Whether this store has too many store files.
   */
  boolean hasTooManyStoreFiles();

  /**
   * Checks the underlying store files, and opens the files that have not been opened, and removes
   * the store file readers for store files no longer available. Mainly used by secondary region
   * replicas to keep up to date with the primary region files.
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

  boolean isPrimaryReplicaStore();

  /**
   * @return true if the memstore may need some extra memory space
   */
  boolean isSloppyMemStore();

  int getCurrentParallelPutCount();

  /**
   * @return the number of read requests purely from the memstore.
   */
  long getMemstoreOnlyRowReadsCount();

  /**
   * @return the number of read requests from the files under this store.
   */
  long getMixedRowReadsCount();

  /**
   * @return a read only configuration of this store; throws {@link UnsupportedOperationException}
   *         if you try to set a configuration.
   */
  Configuration getReadOnlyConfiguration();
}
