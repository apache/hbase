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

import java.io.IOException;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Normalization plan to merge regions (smallest region in the table with its smallest neighbor).
 */
@InterfaceAudience.Private
public class MergeNormalizationPlan implements NormalizationPlan {
  private static final Logger LOG = LoggerFactory.getLogger(MergeNormalizationPlan.class.getName());

  private final RegionInfo firstRegion;
  private final RegionInfo secondRegion;

  public MergeNormalizationPlan(RegionInfo firstRegion, RegionInfo secondRegion) {
    this.firstRegion = firstRegion;
    this.secondRegion = secondRegion;
  }

  @Override
  public PlanType getType() {
    return PlanType.MERGE;
  }

  RegionInfo getFirstRegion() {
    return firstRegion;
  }

  RegionInfo getSecondRegion() {
    return secondRegion;
  }

  @Override
  public String toString() {
    return "MergeNormalizationPlan{" +
      "firstRegion=" + firstRegion +
      ", secondRegion=" + secondRegion +
      '}';
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void execute(Admin admin) {
    LOG.info("Executing merging normalization plan: " + this);
    try {
      admin.mergeRegionsAsync(firstRegion.getEncodedNameAsBytes(),
        secondRegion.getEncodedNameAsBytes(), true);
    } catch (IOException ex) {
      LOG.error("Error during region merge: ", ex);
    }
  }
}
