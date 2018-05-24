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

import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MemStoreCompactionStrategy is the root of a class hierarchy which defines the strategy for
 * choosing the next action to apply in an (in-memory) memstore compaction.
 * Possible action are:
 *  - No-op - do nothing
 *  - Flatten - to change the segment's index from CSLM to a flat representation
 *  - Merge - to merge the indices of the segments in the pipeline
 *  - Compact - to merge the indices while removing data redundancies
 *
 * In addition while applying flat/merge actions it is possible to count the number of unique
 * keys in the result segment.
 */
@InterfaceAudience.Private
public abstract class MemStoreCompactionStrategy {

  protected static final Logger LOG = LoggerFactory.getLogger(MemStoreCompactionStrategy.class);
  // The upper bound for the number of segments we store in the pipeline prior to merging.
  public static final String COMPACTING_MEMSTORE_THRESHOLD_KEY =
      "hbase.hregion.compacting.pipeline.segments.limit";
  public static final int COMPACTING_MEMSTORE_THRESHOLD_DEFAULT = 2;

  /**
   * Types of actions to be done on the pipeline upon MemStoreCompaction invocation.
   * Note that every value covers the previous ones, i.e. if MERGE is the action it implies
   * that the youngest segment is going to be flatten anyway.
   */
  public enum Action {
    NOOP,
    FLATTEN,  // flatten a segment in the pipeline
    FLATTEN_COUNT_UNIQUE_KEYS,  // flatten a segment in the pipeline and count its unique keys
    MERGE,    // merge all the segments in the pipeline into one
    MERGE_COUNT_UNIQUE_KEYS,    // merge all pipeline segments into one and count its unique keys
    COMPACT   // compact the data of all pipeline segments
  }

  protected final String cfName;
  // The limit on the number of the segments in the pipeline
  protected final int pipelineThreshold;


  public MemStoreCompactionStrategy(Configuration conf, String cfName) {
    this.cfName = cfName;
    if(conf == null) {
      pipelineThreshold = COMPACTING_MEMSTORE_THRESHOLD_DEFAULT;
    } else {
      pipelineThreshold =         // get the limit on the number of the segments in the pipeline
          conf.getInt(COMPACTING_MEMSTORE_THRESHOLD_KEY, COMPACTING_MEMSTORE_THRESHOLD_DEFAULT);
    }
  }

  @Override
  public String toString() {
    return getName() + ", pipelineThreshold=" + this.pipelineThreshold;
  }

  protected abstract String getName();

  // get next compaction action to apply on compaction pipeline
  public abstract Action getAction(VersionedSegmentsList versionedList);
  // update policy stats based on the segment that replaced previous versioned list (in
  // compaction pipeline)
  public void updateStats(Segment replacement) {}
  // resets policy stats
  public void resetStats() {}

  protected Action simpleMergeOrFlatten(VersionedSegmentsList versionedList, String strategy) {
    int numOfSegments = versionedList.getNumOfSegments();
    if (numOfSegments > pipelineThreshold) {
      // to avoid too many segments, merge now
      LOG.trace("Strategy={}, store={}; merging {} segments", strategy, cfName, numOfSegments);
      return getMergingAction();
    }

    // just flatten a segment
    LOG.trace("Strategy={}, store={}; flattening a segment", strategy, cfName);
    return getFlattenAction();
  }

  protected Action getMergingAction() {
    return Action.MERGE;
  }

  protected Action getFlattenAction() {
    return Action.FLATTEN;
  }

  protected Action compact(VersionedSegmentsList versionedList, String strategyInfo) {
    int numOfSegments = versionedList.getNumOfSegments();
    LOG.trace("{} in-memory compaction for store={} compacting {} segments", strategyInfo,
        cfName, numOfSegments);
    return Action.COMPACT;
  }
}
