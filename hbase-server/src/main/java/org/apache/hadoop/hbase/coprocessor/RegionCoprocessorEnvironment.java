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

import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface RegionCoprocessorEnvironment extends CoprocessorEnvironment {
  /** @return the region associated with this coprocessor */
  Region getRegion();

  /** @return region information for the region this coprocessor is running on */
  HRegionInfo getRegionInfo();

  /** @return reference to the region server services */
  RegionServerServices getRegionServerServices();

  /** @return shared data between all instances of this coprocessor */
  ConcurrentMap<String, Object> getSharedData();

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


}
