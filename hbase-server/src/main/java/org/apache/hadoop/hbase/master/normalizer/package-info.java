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

/**
 * The Region Normalizer subsystem is responsible for coaxing all the regions in a table toward
 * a "normal" size, according to their storefile size. It does this by splitting regions that
 * are significantly larger than the norm, and merging regions that are significantly smaller than
 * the norm.
 * </p>
 * The public interface to the Region Normalizer subsystem is limited to the following classes:
 * <ul>
 *   <li>
 *     The {@link org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory} provides an
 *     entry point for creating an instance of the
 *     {@link org.apache.hadoop.hbase.master.normalizer.RegionNormalizerManager}.
 *   </li>
 *   <li>
 *     The {@link org.apache.hadoop.hbase.master.normalizer.RegionNormalizerManager} encapsulates
 *     the whole Region Normalizer subsystem. You'll find one of these hanging off of the
 *     {@link org.apache.hadoop.hbase.master.HMaster}, which uses it to delegate API calls. There
 *     is usually only a single instance of this class.
 *   </li>
 *   <li>
 *     Various configuration points that share the common prefix of {@code hbase.normalizer}.
 *     <ul>
 *       <li>Whether to split a region as part of normalization. Configuration:
 *         {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#SPLIT_ENABLED_KEY},
 *         default: {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#DEFAULT_SPLIT_ENABLED}.
 *       </li>
 *       <li>Whether to merge a region as part of normalization. Configuration:
 *         {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#MERGE_ENABLED_KEY},
 *         default: {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#DEFAULT_MERGE_ENABLED}.
 *       </li>
 *       <li>The minimum number of regions in a table to consider it for merge normalization.
 *         Configuration: {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#MIN_REGION_COUNT_KEY},
 *         default: {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#DEFAULT_MIN_REGION_COUNT}.
 *       </li>
 *       <li>The minimum age for a region to be considered for a merge, in days. Configuration:
 *         {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#MERGE_MIN_REGION_AGE_DAYS_KEY},
 *         default: {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#DEFAULT_MERGE_MIN_REGION_AGE_DAYS}.
 *       </li>
 *       <li>The minimum size for a region to be considered for a merge, in whole MBs. Configuration:
 *         {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#MERGE_MIN_REGION_SIZE_MB_KEY},
 *         default: {@value org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer#DEFAULT_MERGE_MIN_REGION_SIZE_MB}.
 *       </li>
 *       <li>The limit on total throughput of the Region Normalizer's actions, in whole MBs. Configuration:
 *         {@value org.apache.hadoop.hbase.master.normalizer.RegionNormalizerWorker#RATE_LIMIT_BYTES_PER_SEC_KEY},
 *         default: unlimited.
 *       </li>
 *     </ul>
 *     <p>
 *       To see detailed logging of the application of these configuration values, set the log
 *       level for this package to `TRACE`.
 *     </p>
 *   </li>
 * </ul>
 * The Region Normalizer subsystem is composed of a handful of related classes:
 * <ul>
 *   <li>
 *     The {@link org.apache.hadoop.hbase.zookeeper.RegionNormalizerTracker} provides a system by
 *     which the Normalizer can be disabled at runtime. It currently does this by managing a znode,
 *     but this is an implementation detail.
 *   </li>
 *   <li>
 *     The {@link org.apache.hadoop.hbase.master.normalizer.RegionNormalizerWorkQueue} is a
 *     {@link java.util.Set}-like {@link java.util.Queue} that permits a single copy of a given
 *     work item to exist in the queue at one time. It also provides a facility for a producer to
 *     add an item to the front of the line. Consumers are blocked waiting for new work.
 *   </li>
 *   <li>
 *     The {@link org.apache.hadoop.hbase.master.normalizer.RegionNormalizerChore} wakes up
 *     periodically and schedules new normalization work, adding targets to the queue.
 *   </li>
 *   <li>
 *     The {@link org.apache.hadoop.hbase.master.normalizer.RegionNormalizerWorker} runs in a
 *     daemon thread, grabbing work off the queue as is it becomes available.
 *   </li>
 *   <li>
 *     The {@link org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer} implements the
 *     logic for calculating target region sizes and emitting a list of corresponding
 *     {@link org.apache.hadoop.hbase.master.normalizer.NormalizationPlan} objects.
 *   </li>
 * </ul>
 */
package org.apache.hadoop.hbase.master.normalizer;
