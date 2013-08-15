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

import org.apache.hadoop.hbase.metrics.BaseSource;

public interface MetricsSnapshotSource extends BaseSource {
  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Snapshots";

  /**
   * The context metrics will be under.
   */
  String METRICS_CONTEXT = "master";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "Master,sub=" + METRICS_NAME;

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase master server";

  String SNAPSHOT_TIME_NAME = "snapshotTime";
  String SNAPSHOT_RESTORE_TIME_NAME = "snapshotRestoreTime";
  String SNAPSHOT_CLONE_TIME_NAME = "snapshotCloneTime";
  String SNAPSHOT_TIME_DESC = "Time it takes to finish snapshot()";
  String SNAPSHOT_RESTORE_TIME_DESC = "Time it takes to finish restoreSnapshot()";
  String SNAPSHOT_CLONE_TIME_DESC = "Time it takes to finish cloneSnapshot()";

  void updateSnapshotTime(long time);

  void updateSnapshotCloneTime(long time);

  void updateSnapshotRestoreTime(long time);
}
