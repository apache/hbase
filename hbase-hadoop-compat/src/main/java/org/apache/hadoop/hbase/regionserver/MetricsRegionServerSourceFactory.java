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

import org.apache.hadoop.hbase.io.MetricsIOSource;
import org.apache.hadoop.hbase.io.MetricsIOWrapper;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Interface of a factory to create Metrics Sources used inside of regionservers.
 */
@InterfaceAudience.Private
public interface MetricsRegionServerSourceFactory {

  /**
   * Given a wrapper create a MetricsRegionServerSource.
   *
   * @param regionServerWrapper The wrapped region server
   * @return a Metrics Source.
   */
  MetricsRegionServerSource createServer(MetricsRegionServerWrapper regionServerWrapper);

  /**
   * Create a MetricsRegionSource from a MetricsRegionWrapper.
   *
   * @param wrapper The wrapped region
   * @return A metrics region source
   */
  MetricsRegionSource createRegion(MetricsRegionWrapper wrapper);

  /**
   * Create a MetricsUserSource from a user
   * @return A metrics user source
   */
  MetricsUserSource createUser(String shortUserName);

  /**
   * Return the singleton instance for MetricsUserAggregateSource
   * @return A metrics user aggregate source
   */
  MetricsUserAggregateSource getUserAggregate();

  /**
   * Create a MetricsTableSource from a MetricsTableWrapper.
   *
   * @param table The table name
   * @param wrapper The wrapped table aggregate
   * @return A metrics table source
   */
  MetricsTableSource createTable(String table, MetricsTableWrapperAggregate wrapper);

  /**
   * Get a MetricsTableAggregateSource
   *
   * @return A metrics table aggregate source
   */
  MetricsTableAggregateSource getTableAggregate();

  /**
   * Get a MetricsHeapMemoryManagerSource
   * @return A metrics heap memory manager source
   */
  MetricsHeapMemoryManagerSource getHeapMemoryManager();

  /**
   * Create a MetricsIOSource from a MetricsIOWrapper.
   *
   * @return A metrics IO source
   */
  MetricsIOSource createIO(MetricsIOWrapper wrapper);
}
