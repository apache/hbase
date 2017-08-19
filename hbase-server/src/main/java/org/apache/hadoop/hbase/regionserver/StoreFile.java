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
import java.util.Comparator;
import java.util.OptionalLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * An interface to describe a store data file.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface StoreFile {

  static final String STORE_FILE_READER_NO_READAHEAD = "hbase.store.reader.no-readahead";

  // Keys for fileinfo values in HFile

  /** Max Sequence ID in FileInfo */
  static final byte[] MAX_SEQ_ID_KEY = Bytes.toBytes("MAX_SEQ_ID_KEY");

  /** Major compaction flag in FileInfo */
  static final byte[] MAJOR_COMPACTION_KEY = Bytes.toBytes("MAJOR_COMPACTION_KEY");

  /** Minor compaction flag in FileInfo */
  static final byte[] EXCLUDE_FROM_MINOR_COMPACTION_KEY =
      Bytes.toBytes("EXCLUDE_FROM_MINOR_COMPACTION");

  /** Bloom filter Type in FileInfo */
  static final byte[] BLOOM_FILTER_TYPE_KEY = Bytes.toBytes("BLOOM_FILTER_TYPE");

  /** Delete Family Count in FileInfo */
  static final byte[] DELETE_FAMILY_COUNT = Bytes.toBytes("DELETE_FAMILY_COUNT");

  /** Last Bloom filter key in FileInfo */
  static final byte[] LAST_BLOOM_KEY = Bytes.toBytes("LAST_BLOOM_KEY");

  /** Key for Timerange information in metadata */
  static final byte[] TIMERANGE_KEY = Bytes.toBytes("TIMERANGE");

  /** Key for timestamp of earliest-put in metadata */
  static final byte[] EARLIEST_PUT_TS = Bytes.toBytes("EARLIEST_PUT_TS");

  /** Key for the number of mob cells in metadata */
  static final byte[] MOB_CELLS_COUNT = Bytes.toBytes("MOB_CELLS_COUNT");

  /** Meta key set when store file is a result of a bulk load */
  static final byte[] BULKLOAD_TASK_KEY = Bytes.toBytes("BULKLOAD_SOURCE_TASK");
  static final byte[] BULKLOAD_TIME_KEY = Bytes.toBytes("BULKLOAD_TIMESTAMP");

  /**
   * Key for skipping resetting sequence id in metadata. For bulk loaded hfiles, the scanner resets
   * the cell seqId with the latest one, if this metadata is set as true, the reset is skipped.
   */
  static final byte[] SKIP_RESET_SEQ_ID = Bytes.toBytes("SKIP_RESET_SEQ_ID");

  CacheConfig getCacheConf();

  Cell getFirstKey();

  Cell getLastKey();

  Comparator<Cell> getComparator();

  long getMaxMemstoreTS();

  /**
   * @return the StoreFile object associated to this StoreFile. null if the StoreFile is not a
   *         reference.
   */
  StoreFileInfo getFileInfo();

  /**
   * @return Path or null if this StoreFile was made with a Stream.
   */
  Path getPath();

  /**
   * @return Returns the qualified path of this StoreFile
   */
  Path getQualifiedPath();

  /**
   * @return True if this is a StoreFile Reference.
   */
  boolean isReference();

  /**
   * @return True if this is HFile.
   */
  boolean isHFile();

  /**
   * @return True if this file was made by a major compaction.
   */
  boolean isMajorCompactionResult();

  /**
   * @return True if this file should not be part of a minor compaction.
   */
  boolean excludeFromMinorCompaction();

  /**
   * @return This files maximum edit sequence id.
   */
  long getMaxSequenceId();

  long getModificationTimeStamp() throws IOException;

  /**
   * Only used by the Striped Compaction Policy
   * @param key
   * @return value associated with the metadata key
   */
  byte[] getMetadataValue(byte[] key);

  /**
   * Check if this storefile was created by bulk load. When a hfile is bulk loaded into HBase, we
   * append {@code '_SeqId_<id-when-loaded>'} to the hfile name, unless
   * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is explicitly turned off. If
   * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is turned off, fall back to
   * BULKLOAD_TIME_KEY.
   * @return true if this storefile was created by bulk load.
   */
  boolean isBulkLoadResult();

  boolean isCompactedAway();

  /**
   * @return true if the file is still used in reads
   */
  boolean isReferencedInReads();

  /**
   * Return the timestamp at which this bulk load file was generated.
   */
  OptionalLong getBulkLoadTimestamp();

  /**
   * @return the cached value of HDFS blocks distribution. The cached value is calculated when store
   *         file is opened.
   */
  HDFSBlocksDistribution getHDFSBlockDistribution();

  /**
   * Initialize the reader used for pread.
   */
  void initReader() throws IOException;

  /**
   * Must be called after initReader.
   */
  StoreFileScanner getPreadScanner(boolean cacheBlocks, long readPt, long scannerOrder,
      boolean canOptimizeForNonNullColumn);

  StoreFileScanner getStreamScanner(boolean canUseDropBehind, boolean cacheBlocks,
      boolean isCompaction, long readPt, long scannerOrder, boolean canOptimizeForNonNullColumn)
      throws IOException;

  /**
   * @return Current reader. Must call initReader first else returns null.
   * @see #initReader()
   */
  StoreFileReader getReader();

  /**
   * @param evictOnClose whether to evict blocks belonging to this file
   * @throws IOException
   */
  void closeReader(boolean evictOnClose) throws IOException;

  /**
   * Marks the status of the file as compactedAway.
   */
  void markCompactedAway();

  /**
   * Delete this file
   * @throws IOException
   */
  void deleteReader() throws IOException;

  /**
   * @return a length description of this StoreFile, suitable for debug output
   */
  String toStringDetailed();

  OptionalLong getMinimumTimestamp();

  OptionalLong getMaximumTimestamp();
}
