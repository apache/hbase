/**
 * Copyright The Apache Software Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is used for exporting current state of load on a RegionServer.
 */
@InterfaceAudience.Public
public interface ServerMetrics {

  ServerName getServerName();

  /**
   * @return the version number of a regionserver.
   */
  default int getVersionNumber() {
    return 0;
  }

  /**
   * @return the string type version of a regionserver.
   */
  default String getVersion() {
    return "0.0.0";
  }

  /**
   * @return the number of requests per second.
   */
  long getRequestCountPerSecond();

  /**
   * @return total Number of requests from the start of the region server.
   */
  long getRequestCount();

  /**
   * @return the amount of used heap
   */
  Size getUsedHeapSize();

  /**
   * @return the maximum allowable size of the heap
   */
  Size getMaxHeapSize();

  int getInfoServerPort();

  /**
   * Call directly from client such as hbase shell
   * @return the list of ReplicationLoadSource
   */
  List<ReplicationLoadSource> getReplicationLoadSourceList();

  /**
   * Call directly from client such as hbase shell
   * @return a map of ReplicationLoadSource list per peer id
   */
  Map<String, List<ReplicationLoadSource>> getReplicationLoadSourceMap();

  /**
   * Call directly from client such as hbase shell
   * @return ReplicationLoadSink
   */
  @Nullable
  ReplicationLoadSink getReplicationLoadSink();

  /**
   * @return region load metrics
   */
  Map<byte[], RegionMetrics> getRegionMetrics();

  /**
   * @return metrics per user
   */
  Map<byte[], UserMetrics> getUserMetrics();

  /**
   * Return the RegionServer-level and Region-level coprocessors
   * @return string set of loaded RegionServer-level and Region-level coprocessors
   */
  Set<String> getCoprocessorNames();

  /**
   * @return the timestamp (server side) of generating this metrics
   */
  long getReportTimestamp();

  /**
   * @return the last timestamp (server side) of generating this metrics
   */
  long getLastReportTimestamp();

}
