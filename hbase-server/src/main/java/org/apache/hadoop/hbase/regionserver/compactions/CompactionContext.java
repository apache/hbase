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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.security.User;


/**
 * This class holds all "physical" details necessary to run a compaction,
 * and abstracts away the details specific to a particular compaction.
 * It also has compaction request with all the logical details.
 * Hence, this class is basically the compaction.
 */
@InterfaceAudience.Private
public abstract class CompactionContext {
  protected CompactionRequest request = null;

  /**
   * Called before coprocessor preCompactSelection and should filter the candidates
   * for coprocessor; i.e. exclude the files that definitely cannot be compacted at this time.
   * @param filesCompacting files currently compacting
   * @return the list of files that can theoretically be compacted.
   */
  public abstract List<StoreFile> preSelect(final List<StoreFile> filesCompacting);

  /**
   * Called to select files for compaction. Must fill in the request field if successful.
   * @param filesCompacting Files currently being compacted by other compactions.
   * @param isUserCompaction Whether this is a user compaction.
   * @param mayUseOffPeak Whether the underlying policy may assume it's off-peak hours.
   * @param forceMajor Whether to force major compaction.
   * @return Whether the selection succeeded. Selection may be empty and lead to no compaction.
   */
  public abstract boolean select(
      final List<StoreFile> filesCompacting, final boolean isUserCompaction,
      final boolean mayUseOffPeak, final boolean forceMajor) throws IOException;

  /**
   * Forces external selection to be applied for this compaction.
   * @param request The pre-cooked request with selection and other settings.
   */
  public void forceSelect(CompactionRequest request) {
    this.request = request;
  }

  /**
   * Runs the compaction based on current selection. select/forceSelect must have been called.
   * @return The new file paths resulting from compaction.
   */
  public abstract List<Path> compact(CompactionThroughputController throughputController)
      throws IOException;

  public abstract List<Path> compact(CompactionThroughputController throughputController, User user)
      throws IOException;

  public CompactionRequest getRequest() {
    assert hasSelection();
    return this.request;
  }

  public boolean hasSelection() {
    return this.request != null;
  }
}
