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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.metrics.MetricRegistry;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.COPROC)
@InterfaceStability.Evolving
public interface MasterCoprocessorEnvironment extends CoprocessorEnvironment {
  /** @return reference to the HMaster services */
  MasterServices getMasterServices();

  /**
   * Returns a MetricRegistry that can be used to track metrics at the master level.
   *
   * <p>See ExampleMasterObserverWithMetrics class in the hbase-examples modules for examples
   * of how metrics can be instantiated and used.</p>
   * @return A MetricRegistry for the coprocessor class to track and export metrics.
   */
  MetricRegistry getMetricRegistryForMaster();
}
