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
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsMasterFileSystemSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "FileSystem";

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
  String METRICS_DESCRIPTION = "Metrics about HBase master file system.";

  String META_SPLIT_TIME_NAME = "metaHlogSplitTime";
  String META_SPLIT_SIZE_NAME = "metaHlogSplitSize";
  String SPLIT_TIME_NAME = "hlogSplitTime";
  String SPLIT_SIZE_NAME = "hlogSplitSize";

  String META_SPLIT_TIME_DESC = "Time it takes to finish splitMetaLog()";
  String META_SPLIT_SIZE_DESC = "Size of hbase:meta WAL files being split";
  String SPLIT_TIME_DESC = "Time it takes to finish WAL.splitLog()";
  String SPLIT_SIZE_DESC = "Size of WAL files being split";


  void updateMetaWALSplitTime(long time);

  void updateMetaWALSplitSize(long size);

  void updateSplitTime(long time);

  void updateSplitSize(long size);

}
