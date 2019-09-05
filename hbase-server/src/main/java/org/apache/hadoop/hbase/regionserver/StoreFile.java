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
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * An interface to describe a store data file.
 * <p>
 * <strong>NOTICE: </strong>this interface is mainly designed for coprocessor, so it will not expose
 * all the internal APIs for a 'store file'. If you are implementing something inside HBase, i.e,
 * not a coprocessor hook, usually you should use {@link HStoreFile} directly as it is the only
 * implementation of this interface.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface StoreFile {

  /**
   * Get the first key in this store file.
   */
  Optional<Cell> getFirstKey();

  /**
   * Get the last key in this store file.
   */
  Optional<Cell> getLastKey();

  /**
   * Get the comparator for comparing two cells.
   */
  CellComparator getComparator();

  /**
   * Get max of the MemstoreTS in the KV's in this store file.
   */
  long getMaxMemStoreTS();

  /**
   * @return Path or null if this StoreFile was made with a Stream.
   */
  Path getPath();

  /**
   * @return Encoded Path if this StoreFile was made with a Stream.
   */
  Path getEncodedPath();

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

  /**
   * Get the modification time of this store file. Usually will access the file system so throws
   * IOException.
   * @deprecated Since 2.0.0. Will be removed in 3.0.0.
   * @see #getModificationTimestamp()
   */
  @Deprecated
  long getModificationTimeStamp() throws IOException;

  /**
   * Get the modification time of this store file. Usually will access the file system so throws
   * IOException.
   */
  long getModificationTimestamp() throws IOException;

  /**
   * Check if this storefile was created by bulk load. When a hfile is bulk loaded into HBase, we
   * append {@code '_SeqId_<id-when-loaded>'} to the hfile name, unless
   * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is explicitly turned off. If
   * "hbase.mapreduce.bulkload.assign.sequenceNumbers" is turned off, fall back to
   * BULKLOAD_TIME_KEY.
   * @return true if this storefile was created by bulk load.
   */
  boolean isBulkLoadResult();

  /**
   * Return the timestamp at which this bulk load file was generated.
   */
  OptionalLong getBulkLoadTimestamp();

  /**
   * @return a length description of this StoreFile, suitable for debug output
   */
  String toStringDetailed();

  /**
   * Get the min timestamp of all the cells in the store file.
   */
  OptionalLong getMinimumTimestamp();

  /**
   * Get the max timestamp of all the cells in the store file.
   */
  OptionalLong getMaximumTimestamp();
}
