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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan.PlanType;

/**
 * Plan which signifies that no normalization is required,
 * or normalization of this table isn't allowed, this is singleton.
 */
@InterfaceAudience.Private
public final class EmptyNormalizationPlan implements NormalizationPlan {
  private static final EmptyNormalizationPlan instance = new EmptyNormalizationPlan();

  private EmptyNormalizationPlan() {
  }

  /**
   * @return singleton instance
   */
  public static EmptyNormalizationPlan getInstance(){
    return instance;
  }

  /**
   * No-op for empty plan.
   */
  @Override
  public void execute(Admin admin) {
  }

  @Override
  public PlanType getType() {
    return PlanType.NONE;
  }
}
