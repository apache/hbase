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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.OptionalInt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility functions for region server storage layer.
 */
@InterfaceAudience.Private
public class StoreUtils {

  private static final Log LOG = LogFactory.getLog(StoreUtils.class);

  /**
   * Creates a deterministic hash code for store file collection.
   */
  public static OptionalInt getDeterministicRandomSeed(Collection<StoreFile> files) {
    return files.stream().mapToInt(f -> f.getPath().getName().hashCode()).findFirst();
  }

  /**
   * Determines whether any files in the collection are references.
   * @param files The files.
   */
  public static boolean hasReferences(final Collection<StoreFile> files) {
    if (files != null) {
      for (StoreFile hsf: files) {
        if (hsf.isReference()) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Gets lowest timestamp from candidate StoreFiles
   */
  public static long getLowestTimestamp(final Collection<StoreFile> candidates)
    throws IOException {
    long minTs = Long.MAX_VALUE;
    for (StoreFile storeFile : candidates) {
      minTs = Math.min(minTs, storeFile.getModificationTimeStamp());
    }
    return minTs;
  }

  /**
   * Gets the largest file (with reader) out of the list of files.
   * @param candidates The files to choose from.
   * @return The largest file; null if no file has a reader.
   */
  static Optional<StoreFile> getLargestFile(Collection<StoreFile> candidates) {
    return candidates.stream().filter(f -> f.getReader() != null)
        .max((f1, f2) -> Long.compare(f1.getReader().length(), f2.getReader().length()));
  }

  /**
   * Return the largest memstoreTS found across all storefiles in the given list. Store files that
   * were created by a mapreduce bulk load are ignored, as they do not correspond to any specific
   * put operation, and thus do not have a memstoreTS associated with them.
   * @return 0 if no non-bulk-load files are provided or, this is Store that does not yet have any
   *         store files.
   */
  public static long getMaxMemstoreTSInList(Collection<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      if (!sf.isBulkLoadResult()) {
        max = Math.max(max, sf.getMaxMemstoreTS());
      }
    }
    return max;
  }

  /**
   * Return the highest sequence ID found across all storefiles in
   * the given list.
   * @param sfs
   * @return 0 if no non-bulk-load files are provided or, this is Store that
   * does not yet have any store files.
   */
  public static long getMaxSequenceIdInList(Collection<StoreFile> sfs) {
    long max = 0;
    for (StoreFile sf : sfs) {
      max = Math.max(max, sf.getMaxSequenceId());
    }
    return max;
  }

  /**
   * Gets the approximate mid-point of the given file that is optimal for use in splitting it.
   * @param file the store file
   * @param comparator Comparator used to compare KVs.
   * @return The split point row, or null if splitting is not possible, or reader is null.
   */
  static Optional<byte[]> getFileSplitPoint(StoreFile file, CellComparator comparator)
      throws IOException {
    StoreFileReader reader = file.getReader();
    if (reader == null) {
      LOG.warn("Storefile " + file + " Reader is null; cannot get split point");
      return Optional.empty();
    }
    // Get first, last, and mid keys. Midkey is the key that starts block
    // in middle of hfile. Has column and timestamp. Need to return just
    // the row we want to split on as midkey.
    Cell midkey = reader.midkey();
    if (midkey != null) {
      Cell firstKey = reader.getFirstKey();
      Cell lastKey = reader.getLastKey();
      // if the midkey is the same as the first or last keys, we cannot (ever) split this region.
      if (comparator.compareRows(midkey, firstKey) == 0 ||
          comparator.compareRows(midkey, lastKey) == 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("cannot split because midkey is the same as first or last row");
        }
        return Optional.empty();
      }
      return Optional.of(CellUtil.cloneRow(midkey));
    }
    return Optional.empty();
  }
}
