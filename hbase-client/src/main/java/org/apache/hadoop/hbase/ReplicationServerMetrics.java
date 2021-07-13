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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.replication.ReplicationLoadSink;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * This class is used for exporting current state of load on a ReplicationServer.
 */
@InterfaceAudience.Public
public interface ReplicationServerMetrics {

  ServerName getServerName();

  /**
   * @return the version number of a replication server.
   */
  default int getVersionNumber() {
    return 0;
  }

  /**
   * @return the string type version of a replication server.
   */
  default String getVersion() {
    return "0.0.0";
  }

  /**
   * @return the number of requests per second.
   */
  long getRequestCountPerSecond();

  /**
   * @return total Number of requests from the start of the replication server.
   */
  long getRequestCount();

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
  ReplicationLoadSink getReplicationLoadSink();

  /**
   * @return the timestamp (server side) of generating this metrics
   */
  long getReportTimestamp();

  /**
   * @return the last timestamp (server side) of generating this metrics
   */
  long getLastReportTimestamp();

  List<String> getQueueNodes();

}
