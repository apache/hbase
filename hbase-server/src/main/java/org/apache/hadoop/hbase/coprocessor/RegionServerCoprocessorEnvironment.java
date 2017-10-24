/*
 *
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
package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RegionServerCoprocessorEnvironment
    extends CoprocessorEnvironment<RegionServerCoprocessor> {
  /**
   * @return Hosting Server's ServerName
   */
  ServerName getServerName();

  /**
   * @return Interface to Map of regions online on this RegionServer {@link #getServerName()}}.
   */
  OnlineRegions getOnlineRegions();

  /**
   * Be careful RPC'ing from a Coprocessor context.
   * RPC's will fail, stall, retry, and/or crawl because the remote side is not online, is
   * struggling or it is on the other side of a network partition. Any use of Connection from
   * inside a Coprocessor must be able to handle all such hiccups.
   *
   * <p>Using a Connection to get at a local resource -- say a Region that is on the local
   * Server or using Admin Interface from a Coprocessor hosted on the Master -- will result in a
   * short-circuit of the RPC framework to make a direct invocation avoiding RPC (and
   * protobuf marshalling/unmarshalling).
   *
   * @return The host's Connection to the Cluster.
   */
  Connection getConnection();

  /**
   * Returns a MetricRegistry that can be used to track metrics at the region server level.
   *
   * <p>See ExampleMasterObserverWithMetrics class in the hbase-examples modules for examples
   * of how metrics can be instantiated and used.</p>
   * @return A MetricRegistry for the coprocessor class to track and export metrics.
   */
  MetricRegistry getMetricRegistryForRegionServer();
}
