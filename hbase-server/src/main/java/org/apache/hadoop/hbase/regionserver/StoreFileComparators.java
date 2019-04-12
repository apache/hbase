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

import java.util.Comparator;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Useful comparators for comparing store files.
 */
@InterfaceAudience.Private
final class StoreFileComparators {
  /**
   * Comparator that compares based on the Sequence Ids of the the store files. Bulk loads that did
   * not request a seq ID are given a seq id of -1; thus, they are placed before all non- bulk
   * loads, and bulk loads with sequence Id. Among these files, the size is used to determine the
   * ordering, then bulkLoadTime. If there are ties, the path name is used as a tie-breaker.
   */
  public static final Comparator<HStoreFile> SEQ_ID =
    Comparator.comparingLong(HStoreFile::getMaxSequenceId)
      .thenComparing(Comparator.comparingLong(new GetFileSize()).reversed())
      .thenComparingLong(new GetBulkTime()).thenComparing(new GetPathName());

  /**
   * Comparator for time-aware compaction. SeqId is still the first ordering criterion to maintain
   * MVCC.
   */
  public static final Comparator<HStoreFile> SEQ_ID_MAX_TIMESTAMP =
    Comparator.comparingLong(HStoreFile::getMaxSequenceId).thenComparingLong(new GetMaxTimestamp())
      .thenComparing(Comparator.comparingLong(new GetFileSize()).reversed())
      .thenComparingLong(new GetBulkTime()).thenComparing(new GetPathName());

  private static class GetFileSize implements ToLongFunction<HStoreFile> {

    @Override
    public long applyAsLong(HStoreFile sf) {
      if (sf.getReader() != null) {
        return sf.getReader().length();
      } else {
        // the reader may be null for the compacted files and if the archiving
        // had failed.
        return -1L;
      }
    }
  }

  private static class GetBulkTime implements ToLongFunction<HStoreFile> {

    @Override
    public long applyAsLong(HStoreFile sf) {
      return sf.getBulkLoadTimestamp().orElse(Long.MAX_VALUE);
    }
  }

  private static class GetPathName implements Function<HStoreFile, String> {

    @Override
    public String apply(HStoreFile sf) {
      return sf.getPath().getName();
    }
  }

  private static class GetMaxTimestamp implements ToLongFunction<HStoreFile> {

    @Override
    public long applyAsLong(HStoreFile sf) {
      return sf.getMaximumTimestamp().orElse(Long.MAX_VALUE);
    }
  }
}
