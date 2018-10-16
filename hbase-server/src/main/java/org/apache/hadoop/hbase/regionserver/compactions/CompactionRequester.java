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

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Request a compaction.
 */
@InterfaceAudience.Private
public interface CompactionRequester {

  /**
   * Request compaction on all the stores of the given region.
   */
  void requestCompaction(HRegion region, String why, int priority,
      CompactionLifeCycleTracker tracker, @Nullable User user) throws IOException;

  /**
   * Request compaction on the given store.
   */
  void requestCompaction(HRegion region, HStore store, String why, int priority,
      CompactionLifeCycleTracker tracker, @Nullable User user) throws IOException;

  /**
   * on/off compaction
   */
  void switchCompaction(boolean onOrOff);

}
