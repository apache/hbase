/*
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

  /** Return the server name **/
  ServerName getServerName();

  /** Return the version number of the regionserver. */
  default int getVersionNumber() {
    return 0;
  }

  /** Return the string formatted version of the regionserver. */
  default String getVersion() {
    return "0.0.0";
  }

  /** Return the number of requests per second. */
  long getRequestCountPerSecond();

  /** Return the total number of requests over the lifetime of the region server. */
  long getRequestCount();

  /** Return the total number of read requests over the lifetime of the region server. */
  long getReadRequestsCount();

  /** Return the total number of write requests over the lifetime of the region server. */
  long getWriteRequestsCount();

  /** Return the amount of used heap */
  Size getUsedHeapSize();

  /** Return the maximum allowable size of the heap */
  Size getMaxHeapSize();

  /** Return the info server port. */
  int getInfoServerPort();

  /**
   * Return the list of replication load sources.
   * <p>
   * Used by clients such as the hbase shell
   */
  List<ReplicationLoadSource> getReplicationLoadSourceList();

  /**
   * Return the map of replication load sources as a list list per peer id.
   * <p>
   * Used by clients such as the hbase shell
   */
  Map<String, List<ReplicationLoadSource>> getReplicationLoadSourceMap();

  /**
   * return the replication load sink.
   * <p>
   * Used by clients such as the hbase shell
   */
  @Nullable
  ReplicationLoadSink getReplicationLoadSink();

  /** Return the region load metrics */
  Map<byte[], RegionMetrics> getRegionMetrics();

  /**
   * Return the region metrics per user
   */
  Map<byte[], UserMetrics> getUserMetrics();

  /** Return the combined list of RegionServer-level and Region-level coprocessors. */
  Set<String> getCoprocessorNames();

  /** Return the timestamp (server side) when metrics were last collected. */
  long getReportTimestamp();

  /** Return the last timestamp (server side) when metrics were last collected. */
  long getLastReportTimestamp();

  /**
   * Return the list of active monitored tasks.
   * <p>
   * Used by clients such as the hbase shell
   */
  @Nullable
  List<ServerTask> getTasks();
}
