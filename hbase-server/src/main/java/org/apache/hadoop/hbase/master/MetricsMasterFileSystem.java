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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;

public class MetricsMasterFileSystem {

  private final MetricsMasterFileSystemSource source;

  public MetricsMasterFileSystem() {
    source = CompatibilitySingletonFactory.getInstance(MetricsMasterFileSystemSource.class);
  }

  /**
   * Record a single instance of a split
   * @param time time that the split took
   * @param size length of original WALs that were split
   */
  public synchronized void addSplit(long time, long size) {
    source.updateSplitTime(time);
    source.updateSplitSize(size);
  }

  /**
   * Record a single instance of a split
   * @param time time that the split took
   * @param size length of original WALs that were split
   */
  public synchronized void addMetaWALSplit(long time, long size) {
    source.updateMetaWALSplitTime(time);
    source.updateMetaWALSplitSize(size);
  }
}
