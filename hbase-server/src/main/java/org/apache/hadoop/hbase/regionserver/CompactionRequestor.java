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
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;

@InterfaceAudience.Private
public interface CompactionRequestor {
  /**
   * @param r Region to compact
   * @param why Why compaction was requested -- used in debug messages
   * @return The created {@link CompactionRequest CompactionRequests} or an empty list if no
   *         compactions were started
   * @throws IOException
   */
  List<CompactionRequest> requestCompaction(final Region r, final String why)
      throws IOException;

  /**
   * @param r Region to compact
   * @param why Why compaction was requested -- used in debug messages
   * @param requests custom compaction requests. Each compaction must specify the store on which it
   *          is acting. Can be <tt>null</tt> in which case a compaction will be attempted on all
   *          stores for the region.
   * @return The created {@link CompactionRequest CompactionRequests} or an empty list if no
   *         compactions were started
   * @throws IOException
   */
  List<CompactionRequest> requestCompaction(
    final Region r, final String why, List<Pair<CompactionRequest, Store>> requests
  )
      throws IOException;

  /**
   * @param r Region to compact
   * @param s Store within region to compact
   * @param why Why compaction was requested -- used in debug messages
   * @param request custom compaction request for the {@link Region} and {@link Store}. Custom
   *          request must be <tt>null</tt> or be constructed with matching region and store.
   * @return The created {@link CompactionRequest} or <tt>null</tt> if no compaction was started.
   * @throws IOException
   */
  CompactionRequest requestCompaction(
    final Region r, final Store s, final String why, CompactionRequest request
  ) throws IOException;

  /**
   * @param r Region to compact
   * @param why Why compaction was requested -- used in debug messages
   * @param pri Priority of this compaction. minHeap. &lt;=0 is critical
   * @param requests custom compaction requests. Each compaction must specify the store on which it
   *          is acting. Can be <tt>null</tt> in which case a compaction will be attempted on all
   *          stores for the region.
   * @param user  the effective user
   * @return The created {@link CompactionRequest CompactionRequests} or an empty list if no
   *         compactions were started.
   * @throws IOException
   */
  List<CompactionRequest> requestCompaction(
    final Region r, final String why, int pri, List<Pair<CompactionRequest, Store>> requests,
    User user
  ) throws IOException;

  /**
   * @param r Region to compact
   * @param s Store within region to compact
   * @param why Why compaction was requested -- used in debug messages
   * @param pri Priority of this compaction. minHeap. &lt;=0 is critical
   * @param request custom compaction request to run. {@link Store} and {@link Region} for the
   *          request must match the region and store specified here.
   * @param user
   * @return The created {@link CompactionRequest} or <tt>null</tt> if no compaction was started
   * @throws IOException
   */
  CompactionRequest requestCompaction(
    final Region r, final Store s, final String why, int pri, CompactionRequest request, User user
  ) throws IOException;
}
