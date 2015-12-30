/**
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
package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;

/**
 * Performs "normalization" of regions on the cluster, making sure that suboptimal
 * choice of split keys doesn't leave cluster in a situation when some regions are
 * substantially larger than others for considerable amount of time.
 *
 * Users who want to use this feature could either use default {@link SimpleRegionNormalizer}
 * or plug in their own implementation. Please note that overly aggressive normalization rules
 * (attempting to make all regions perfectly equal in size) could potentially lead to
 * "split/merge storms".
 */
@InterfaceAudience.Private
public interface RegionNormalizer {
  /**
   * Set the master service. Must be called before first call to
   * {@link #computePlanForTable(TableName)}.
   * @param masterServices master services to use
   */
  void setMasterServices(MasterServices masterServices);

  /**
   * Computes next optimal normalization plan.
   * @param table table to normalize
   * @return Next (perhaps most urgent) normalization action to perform
   */
  NormalizationPlan computePlanForTable(TableName table) throws HBaseIOException;

  /**
   * Notification for the case where plan couldn't be executed due to constraint violation, such as
   * namespace quota
   * @param hri the region which is involved in the plan
   * @param type type of plan
   */
  void planSkipped(HRegionInfo hri, PlanType type);
  
  /**
   * @param type type of plan for which skipped count is to be returned
   * @return the count of plans of specified type which were skipped
   */
  long getSkippedCount(NormalizationPlan.PlanType type);
}
