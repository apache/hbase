/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface allowing various implementations of tracking files that have recently been archived to
 * allow for the Master to notice changes to snapshot sizes for space quotas.
 *
 * This object needs to ensure that {@link #addArchivedFiles(Set)} and
 * {@link #computeAndStoreSnapshotSizes(Collection)} are mutually exclusive. If a "full" computation
 * is in progress, new changes being archived should be held.
 */
@InterfaceAudience.Private
public interface FileArchiverNotifier {

  /**
   * Records a file and its size in bytes being moved to the archive directory.
   *
   * @param fileSizes A collection of file name to size in bytes
   * @throws IOException If there was an IO-related error persisting the file size(s)
   */
  void addArchivedFiles(Set<Entry<String, Long>> fileSizes) throws IOException;

  /**
   * Computes the size of a table and all of its snapshots, recording new "full" sizes for each.
   *
   * @param currentSnapshots the current list of snapshots against this table
   * @return The total size of all snapshots against this table.
   * @throws IOException If there was an IO-related error computing or persisting the sizes.
   */
  long computeAndStoreSnapshotSizes(Collection<String> currentSnapshots) throws IOException;
}