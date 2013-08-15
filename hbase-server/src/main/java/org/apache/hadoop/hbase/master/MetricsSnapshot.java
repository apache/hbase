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

public class MetricsSnapshot {

  private final MetricsSnapshotSource source;

  public MetricsSnapshot() {
    source = CompatibilitySingletonFactory.getInstance(MetricsSnapshotSource.class);
  }

  /**
   * Record a single instance of a snapshot
   * @param time time that the snapshot took
   */
  public void addSnapshot(long time) {
    source.updateSnapshotTime(time);
  }

  /**
   * Record a single instance of a snapshot
   * @param time time that the snapshot restore took
   */
  public void addSnapshotRestore(long time) {
    source.updateSnapshotRestoreTime(time);
  }

  /**
   * Record a single instance of a snapshot cloned table
   * @param time time that the snapshot clone took
   */
  public void addSnapshotClone(long time) {
    source.updateSnapshotCloneTime(time);
  }
}
