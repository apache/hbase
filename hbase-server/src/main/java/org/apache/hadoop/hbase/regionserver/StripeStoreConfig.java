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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;

/**
 * Configuration class for stripe store and compactions.
 * See {@link StripeStoreFileManager} for general documentation.
 * See getters for the description of each setting.
 */
@InterfaceAudience.Private
public class StripeStoreConfig {
  public static final String MAX_SPLIT_IMBALANCE = "hbase.store.stripe.split.max.imbalance";
  private float maxSplitImbalance;

  public StripeStoreConfig(Configuration config) {
    maxSplitImbalance = config.getFloat(MAX_SPLIT_IMBALANCE, 1.5f);
    if (maxSplitImbalance == 0) {
      maxSplitImbalance = 1.5f;
    }
    if (maxSplitImbalance < 1f) {
      maxSplitImbalance = 1f / maxSplitImbalance;
    }
  }

  /**
   * @return the maximum imbalance to tolerate between sides when splitting the region
   * at the stripe boundary. If the ratio of a larger to a smaller side of the split on
   * the stripe-boundary is bigger than this, then some stripe will be split.
   */
  public float getMaxSplitImbalance() {
    return this.maxSplitImbalance;
  }
}
