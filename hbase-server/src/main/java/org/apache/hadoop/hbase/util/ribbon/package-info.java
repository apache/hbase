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
 * Ribbon Filter implementation for HBase.
 * <p>
 * Ribbon Filter is a space-efficient alternative to Bloom filters, based on the paper:
 * "Ribbon filter: practically smaller than Bloom and Xor" by Peter C. Dillinger and
 * Stefan Walzer (2021).
 * <p>
 * Key features:
 * <ul>
 *   <li><b>Space Efficiency</b>: ~30% smaller than Bloom for the same FPR</li>
 *   <li><b>Fast Queries</b>: O(w) XOR operations where w is bandwidth (typically 64)</li>
 *   <li><b>Batch Construction</b>: Requires all keys before building (vs Bloom's streaming)</li>
 *   <li><b>ICML Storage</b>: Interleaved Column-Major Layout for space-optimal storage</li>
 * </ul>
 * <p>
 * This implementation uses ICML (Interleaved Column-Major Layout) storage, which:
 * <ul>
 *   <li>Never fails construction</li>
 *   <li>Supports fractional bits per key for optimal space usage</li>
 * </ul>
 * <p>
 * Main classes:
 * <ul>
 *   <li>{@link org.apache.hadoop.hbase.util.ribbon.RibbonFilterChunk} - Main filter</li>
 *   <li>{@link org.apache.hadoop.hbase.util.ribbon.RibbonHasher} - Hash generation for keys</li>
 *   <li>{@link org.apache.hadoop.hbase.util.ribbon.RibbonBanding} - Gaussian elimination</li>
 *   <li>{@link org.apache.hadoop.hbase.util.ribbon.InterleavedRibbonSolution} - ICML solution</li>
 *   <li>{@link org.apache.hadoop.hbase.util.ribbon.RibbonFilterUtil} - Utility methods</li>
 * </ul>
 *
 * @see <a href="https://arxiv.org/abs/2103.02515">Ribbon Filter Paper</a>
 * @see <a href="https://github.com/FastFilter/fastfilter_cpp">Reference implementation by the paper
 *      authors (Apache 2.0 licensed)</a>
 */
@InterfaceAudience.Private
package org.apache.hadoop.hbase.util.ribbon;

import org.apache.yetus.audience.InterfaceAudience;
