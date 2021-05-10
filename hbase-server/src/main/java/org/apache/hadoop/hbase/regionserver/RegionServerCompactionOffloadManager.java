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

import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionServerCompactionOffloadManager {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegionServerCompactionOffloadManager.class);

  private volatile boolean compactionOffloadEnabled;
  // Storage for compaction offload enable value
  private CompactionOffloadSwitchStorage compactionOffloadSwitchStorage;

  RegionServerCompactionOffloadManager(final RegionServerServices rsServices) {
    compactionOffloadSwitchStorage = new CompactionOffloadSwitchStorage(rsServices.getZooKeeper(),
        rsServices.getConfiguration());
    try {
      compactionOffloadEnabled = compactionOffloadSwitchStorage.isCompactionOffloadEnabled();
    } catch (IOException e) {
      compactionOffloadEnabled = false;
      LOG.error("Get initial compaction offload value failed ", e);
    }
  }

  boolean isCompactionOffloadEnabled() {
    return compactionOffloadEnabled;
  }

  public void switchCompactionOffload(boolean enable) throws IOException {
    if (compactionOffloadEnabled != enable) {
      boolean previousEnabled = compactionOffloadEnabled;
      compactionOffloadEnabled = compactionOffloadSwitchStorage.isCompactionOffloadEnabled();
      LOG.info("Switch compaction offload from {} to {}", previousEnabled,
        compactionOffloadEnabled);
    } else {
      LOG.warn(
        "Skip switch compaction offload enable value because previous value {} is the same as current value {}",
        compactionOffloadEnabled, enable);
    }
  }
}
