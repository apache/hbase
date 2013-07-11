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

package org.apache.hadoop.hbase.regionserver.wal;

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * Interface of the source that will export metrics about log replay statistics when recovering a
 * region server in distributedLogReplay mode
 */
public interface MetricsEditsReplaySource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "replay";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer HLog Edits Replay";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;


  String REPLAY_TIME_NAME = "replayTime";
  String REPLAY_TIME_DESC = "Time an replay operation took.";
  String REPLAY_BATCH_SIZE_NAME = "replayBatchSize";
  String REPLAY_BATCH_SIZE_DESC = "Number of changes in each replay batch.";
  String REPLAY_DATA_SIZE_NAME = "replayDataSize";
  String REPLAY_DATA_SIZE_DESC = "Size (in bytes) of the data of each replay.";

  /**
   * Add the time a replay command took
   */
  void updateReplayTime(long time);

  /**
   * Add the batch size of each replay
   */
  void updateReplayBatchSize(long size);

  /**
   * Add the payload data size of each replay
   */
  void updateReplayDataSize(long size);

}
