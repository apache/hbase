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
package org.apache.hadoop.hbase.master.normalizer;

import java.util.List;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Performs "normalization" of regions of a table, making sure that suboptimal
 * choice of split keys doesn't leave cluster in a situation when some regions are
 * substantially larger than others for considerable amount of time.
 *
 * Users who want to use this feature could either use default {@link SimpleRegionNormalizer}
 * or plug in their own implementation. Please note that overly aggressive normalization rules
 * (attempting to make all regions perfectly equal in size) could potentially lead to
 * "split/merge storms".
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface RegionNormalizer extends Configurable {
  /**
   * Set the master service. Must be called before first call to
   * {@link #computePlansForTable(TableName)}.
   * @param masterServices master services to use
   */
  void setMasterServices(MasterServices masterServices);

  /**
   * Computes a list of normalizer actions to perform on the target table. This is the primary
   * entry-point from the Master driving a normalization activity.
   * @param table table to normalize
   * @return A list of the normalization actions to perform, or an empty list
   *   if there's nothing to do.
   */
  List<NormalizationPlan> computePlansForTable(TableName table)
      throws HBaseIOException;

  /**
   * Notification for the case where plan couldn't be executed due to constraint violation, such as
   * namespace quota
   * @param hri the region which is involved in the plan
   * @param type type of plan
   */
  void planSkipped(RegionInfo hri, PlanType type);

  /**
   * @param type type of plan for which skipped count is to be returned
   * @return the count of plans of specified type which were skipped
   */
  long getSkippedCount(NormalizationPlan.PlanType type);
}
