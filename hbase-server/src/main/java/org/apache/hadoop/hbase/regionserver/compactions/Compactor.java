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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreFile;

/**
 * A compactor is a compaction algorithm associated a given policy.
 */
@InterfaceAudience.Private
public abstract class Compactor {

  protected CompactionProgress progress;
  protected Configuration conf;

  Compactor(final Configuration conf) {
    this.conf = conf;
  }

  /**
   * Do a minor/major compaction on an explicit set of storefiles from a Store.
   * @param request the requested compaction
   * @return Product of compaction or an empty list if all cells expired or deleted and nothing made
   *         it through the compaction.
   * @throws IOException
   */
  public abstract List<Path> compact(final CompactionRequest request) throws IOException;

  /**
   * Compact a list of files for testing. Creates a fake {@link CompactionRequest} to pass to
   * {@link #compact(CompactionRequest)};
   * @param filesToCompact the files to compact. These are used as the compactionSelection for the
   *          generated {@link CompactionRequest}.
   * @param isMajor true to major compact (prune all deletes, max versions, etc)
   * @return Product of compaction or an empty list if all cells expired or deleted and nothing made
   *         it through the compaction.
   * @throws IOException
   */
  public List<Path> compactForTesting(final Collection<StoreFile> filesToCompact, boolean isMajor)
      throws IOException {
    CompactionRequest cr = new CompactionRequest(filesToCompact);
    cr.setIsMajor(isMajor);
    return this.compact(cr);
  }

  public CompactionProgress getProgress() {
    return this.progress;
  }
}
