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

import org.apache.hadoop.hbase.metrics.BaseSource;

/**
 * This interface will be implemented by a MetricsSource that will export metrics from
 * multiple regions into the hadoop metrics system.
 */
public interface MetricsRegionAggregateSource extends BaseSource {

  /**
   * The name of the metrics
   */
  String METRICS_NAME = "Regions";

  /**
   * The name of the metrics context that metrics will be under.
   */
  String METRICS_CONTEXT = "regionserver";

  /**
   * Description
   */
  String METRICS_DESCRIPTION = "Metrics about HBase RegionServer regions and tables";

  /**
   * The name of the metrics context that metrics will be under in jmx
   */
  String METRICS_JMX_CONTEXT = "RegionServer,sub=" + METRICS_NAME;

  String NUM_REGIONS = "numRegions";
  String NUMBER_OF_REGIONS_DESC = "Number of regions in the metrics system";

  /**
   * Register a MetricsRegionSource as being open.
   *
   * @param source the source for the region being opened.
   */
  void register(MetricsRegionSource source);

  /**
   * Remove a region's source. This is called when a region is closed.
   *
   * @param source The region to remove.
   */
  void deregister(MetricsRegionSource source);
}
