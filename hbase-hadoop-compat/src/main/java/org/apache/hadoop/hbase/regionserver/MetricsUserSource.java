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

import java.util.Map;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public interface MetricsUserSource extends Comparable<MetricsUserSource> {

  //These client metrics will be reported through clusterStatus and hbtop only
  interface ClientMetrics {
    void incrementReadRequest();

    void incrementWriteRequest();

    String getHostName();

    long getReadRequestsCount();

    long getWriteRequestsCount();

    void incrementFilteredReadRequests();

    long getFilteredReadRequests();
  }

  String getUser();

  void register();

  void deregister();

  void updatePut(long t);

  void updateDelete(long t);

  void updateGet(long t);

  void updateIncrement(long t);

  void updateAppend(long t);

  void updateReplay(long t);

  void updateScanTime(long t);

  void getMetrics(MetricsCollector metricsCollector, boolean all);

  /**
   * Metrics collected at client level for a user(needed for reporting through clusterStatus
   * and  hbtop currently)
   * @return metrics per hostname
   */
  Map<String, ClientMetrics> getClientMetrics();

  /**
   * Create a instance of ClientMetrics if not present otherwise return the previous one
   *
   * @param hostName hostname of the client
   * @return Instance of ClientMetrics
   */
  ClientMetrics getOrCreateMetricsClient(String hostName);
}
