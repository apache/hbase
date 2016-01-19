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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;

import java.io.IOException;
import java.util.Arrays;

/**
 * Normalization plan to split region.
 */
@InterfaceAudience.Private
public class SplitNormalizationPlan implements NormalizationPlan {
  private static final Log LOG = LogFactory.getLog(SplitNormalizationPlan.class.getName());

  private HRegionInfo regionInfo;
  private byte[] splitPoint;

  public SplitNormalizationPlan(HRegionInfo regionInfo, byte[] splitPoint) {
    this.regionInfo = regionInfo;
    this.splitPoint = splitPoint;
  }

  @Override
  public PlanType getType() {
    return PlanType.SPLIT;
  }

  public HRegionInfo getRegionInfo() {
    return regionInfo;
  }

  public void setRegionInfo(HRegionInfo regionInfo) {
    this.regionInfo = regionInfo;
  }

  public byte[] getSplitPoint() {
    return splitPoint;
  }

  public void setSplitPoint(byte[] splitPoint) {
    this.splitPoint = splitPoint;
  }

  @Override
  public String toString() {
    return "SplitNormalizationPlan{" +
      "regionInfo=" + regionInfo +
      ", splitPoint=" + Arrays.toString(splitPoint) +
      '}';
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void execute(Admin admin) {
    LOG.info("Executing splitting normalization plan: " + this);
    try {
      admin.splitRegion(regionInfo.getRegionName());
    } catch (IOException ex) {
      LOG.error("Error during region split: ", ex);
    }
  }
}
