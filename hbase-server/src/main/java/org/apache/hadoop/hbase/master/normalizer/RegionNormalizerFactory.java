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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Factory to create instance of {@link RegionNormalizer} as configured.
 */
@InterfaceAudience.Private
public final class RegionNormalizerFactory {

  private RegionNormalizerFactory() {
  }

  /**
   * Create a region normalizer from the given conf.
   * @param conf configuration
   * @return {@link RegionNormalizer} implementation
   */
  public static RegionNormalizer getRegionNormalizer(Configuration conf) {

    // Create instance of Region Normalizer
    Class<? extends RegionNormalizer> balancerKlass =
      conf.getClass(HConstants.HBASE_MASTER_NORMALIZER_CLASS, SimpleRegionNormalizer.class,
        RegionNormalizer.class);
    return ReflectionUtils.newInstance(balancerKlass, conf);
  }
}
