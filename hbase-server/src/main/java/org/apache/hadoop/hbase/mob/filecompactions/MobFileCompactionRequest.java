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
package org.apache.hadoop.hbase.mob.filecompactions;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * The compaction request for mob files.
 */
@InterfaceAudience.Private
public abstract class MobFileCompactionRequest {

  protected long selectionTime;
  protected CompactionType type = CompactionType.PART_FILES;

  public void setCompactionType(CompactionType type) {
    this.type = type;
  }

  /**
   * Gets the selection time.
   * @return The selection time.
   */
  public long getSelectionTime() {
    return this.selectionTime;
  }

  /**
   * Gets the compaction type.
   * @return The compaction type.
   */
  public CompactionType getCompactionType() {
    return type;
  }

  protected enum CompactionType {

    /**
     * Part of mob files are selected.
     */
    PART_FILES,

    /**
     * All of mob files are selected.
     */
    ALL_FILES;
  }
}
