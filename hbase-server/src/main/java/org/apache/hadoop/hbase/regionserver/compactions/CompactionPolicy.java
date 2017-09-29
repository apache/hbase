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
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A compaction policy determines how to select files for compaction,
 * how to compact them, and how to generate the compacted files.
 */
@InterfaceAudience.Private
public abstract class CompactionPolicy {
  protected CompactionConfiguration comConf;
  protected StoreConfigInformation storeConfigInfo;

  public CompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
    this.storeConfigInfo = storeConfigInfo;
    this.comConf = new CompactionConfiguration(conf, this.storeConfigInfo);
  }

  /**
   * @param filesToCompact Files to compact. Can be null.
   * @return True if we should run a major compaction.
   */
  public abstract boolean shouldPerformMajorCompaction(Collection<HStoreFile> filesToCompact)
      throws IOException;

  /**
   * @param compactionSize Total size of some compaction
   * @return whether this should be a large or small compaction
   */
  public abstract boolean throttleCompaction(long compactionSize);

  /**
   * Inform the policy that some configuration has been change,
   * so cached value should be updated it any.
   */
  public void setConf(Configuration conf) {
    this.comConf = new CompactionConfiguration(conf, this.storeConfigInfo);
  }

  /**
   * @return The current compaction configuration settings.
   */
  public CompactionConfiguration getConf() {
    return this.comConf;
  }
}
