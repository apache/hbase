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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.metrics.MetricRegistry;
import org.apache.hadoop.hbase.wal.WAL;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface WALCoprocessorEnvironment extends CoprocessorEnvironment<WALCoprocessor> {
  /** @return reference to the region server's WAL */
  WAL getWAL();

  /**
   * Returns a MetricRegistry that can be used to track metrics at the region server level.
   *
   * <p>See ExampleRegionServerObserverWithMetrics class in the hbase-examples modules for examples
   * of how metrics can be instantiated and used.</p>
   * @return A MetricRegistry for the coprocessor class to track and export metrics.
   */
  MetricRegistry getMetricRegistryForRegionServer();
}
