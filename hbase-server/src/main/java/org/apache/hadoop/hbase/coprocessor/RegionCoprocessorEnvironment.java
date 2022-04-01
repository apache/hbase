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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.RawCellBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.OnlineRegions;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.User;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RegionCoprocessorEnvironment extends CoprocessorEnvironment<RegionCoprocessor> {
  /** @return the region associated with this coprocessor */
  Region getRegion();

  /** @return region information for the region this coprocessor is running on */
  RegionInfo getRegionInfo();

  /**
   * @return Interface to Map of regions online on this RegionServer {@link #getServerName()}}.
   */
  OnlineRegions getOnlineRegions();

  /** @return shared data between all instances of this coprocessor */
  ConcurrentMap<String, Object> getSharedData();

  /**
   * @return Hosting Server's ServerName
   */
  ServerName getServerName();

  /**
   * Returns the hosts' Connection to the Cluster. <b>Do not close! This is a shared connection with
   * the hosting server. Throws {@link UnsupportedOperationException} if you try to close or abort
   * it</b>. For light-weight usage only. Heavy-duty usage will pull down the hosting RegionServer
   * responsiveness as well as that of other Coprocessors making use of this Connection. Use to
   * create table on start or to do administrative operations. Coprocessors should create their own
   * Connections if heavy usage to avoid impinging on hosting Server operation. To create a
   * Connection or if a Coprocessor requires a region with a particular Configuration, use
   * {@link org.apache.hadoop.hbase.client.ConnectionFactory} or
   * {@link #createConnection(Configuration)}}.
   * <p>
   * Be aware that operations that make use of this Connection are executed as the RegionServer
   * User, the hbase super user that started this server process. Exercise caution running
   * operations as this User (See {@link #createConnection(Configuration)}} to run as other than the
   * RegionServer User).
   * <p>
   * Be careful RPC'ing from a Coprocessor context. RPC's will fail, stall, retry, and/or crawl
   * because the remote side is not online, is struggling or it is on the other side of a network
   * partition. Any use of Connection from inside a Coprocessor must be able to handle all such
   * hiccups.
   * <p>
   * NOTE: the returned Connection is created using {@link User#getCurrent},so the Connection is as
   * the System User who running the RegionServer, not the client User who initiate the RPC.
   * @see #createConnection(Configuration)
   * @return The host's Connection to the Cluster.
   */
  Connection getConnection();

  /**
   * Creates a cluster connection using the passed Configuration. Creating a Connection is a
   * heavy-weight operation. The resultant Connection's cache of region locations will be empty.
   * Therefore you should cache and reuse Connections rather than create a Connection on demand.
   * Create on start of your Coprocessor. You will have to cast the CoprocessorEnvironment
   * appropriately to get at this API at start time because Coprocessor start method is passed a
   * subclass of this CoprocessorEnvironment or fetch Connection using a synchronized accessor
   * initializing the Connection on first access. Close the returned Connection when done to free
   * resources. Using this API rather than
   * {@link org.apache.hadoop.hbase.client.ConnectionFactory#createConnection(Configuration)}
   * returns a Connection that will short-circuit RPC if the target is a local resource. Use
   * ConnectionFactory if you don't need this ability.
   * <p>
   * Be careful RPC'ing from a Coprocessor context. RPC's will fail, stall, retry, and/or crawl
   * because the remote side is not online, is struggling or it is on the other side of a network
   * partition. Any use of Connection from inside a Coprocessor must be able to handle all such
   * hiccups.
   * <p>
   * NOTE: the returned Connection is created using {@link User#getCurrent},so the Connection is as
   * the System User who running the RegionServer, not the client User who initiate the RPC.
   * @return Connection created using the passed conf.
   */
  Connection createConnection(Configuration conf) throws IOException;

  /**
   * Returns a MetricRegistry that can be used to track metrics at the region server level. All
   * metrics tracked at this level will be shared by all the coprocessor instances
   * of the same class in the same region server process. Note that there will be one
   * region coprocessor environment per region in the server, but all of these instances will share
   * the same MetricRegistry. The metric instances (like Counter, Timer, etc) will also be shared
   * among all of the region coprocessor instances.
   *
   * <p>See ExampleRegionObserverWithMetrics class in the hbase-examples modules to see examples of how
   * metrics can be instantiated and used.</p>
   * @return A MetricRegistry for the coprocessor class to track and export metrics.
   */
  // Note: we are not exposing getMetricRegistryForRegion(). per-region metrics are already costly
  // so we do not want to allow coprocessors to export metrics at the region level. We can allow
  // getMetricRegistryForTable() to allow coprocessors to track metrics per-table, per-regionserver.
  MetricRegistry getMetricRegistryForRegionServer();

  /**
   * Returns a CellBuilder so that coprocessors can build cells. These cells can also include tags.
   * Note that this builder does not support updating seqId of the cells
   * @return the RawCellBuilder
   */
  RawCellBuilder getCellBuilder();
}
