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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The class that creates a flush policy from a conf and HTableDescriptor.
 * <p>
 * The default flush policy is {@link FlushLargeStoresPolicy}. And for 0.98, the default flush
 * policy is {@link FlushAllStoresPolicy}.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FlushPolicyFactory {

  private static final Log LOG = LogFactory.getLog(FlushPolicyFactory.class);

  public static final String HBASE_FLUSH_POLICY_KEY = "hbase.regionserver.flush.policy";

  private static final Class<? extends FlushPolicy> DEFAULT_FLUSH_POLICY_CLASS =
      FlushAllLargeStoresPolicy.class;

  /**
   * Create the FlushPolicy configured for the given table.
   */
  public static FlushPolicy create(HRegion region, Configuration conf) throws IOException {
    Class<? extends FlushPolicy> clazz = getFlushPolicyClass(region.getTableDesc(), conf);
    FlushPolicy policy = ReflectionUtils.newInstance(clazz, conf);
    policy.configureForRegion(region);
    return policy;
  }

  /**
   * Get FlushPolicy class for the given table.
   */
  public static Class<? extends FlushPolicy> getFlushPolicyClass(HTableDescriptor htd,
      Configuration conf) throws IOException {
    String className = htd.getFlushPolicyClassName();
    if (className == null) {
      className = conf.get(HBASE_FLUSH_POLICY_KEY, DEFAULT_FLUSH_POLICY_CLASS.getName());
    }
    try {
      Class<? extends FlushPolicy> clazz = Class.forName(className).asSubclass(FlushPolicy.class);
      return clazz;
    } catch (Exception e) {
      LOG.warn(
        "Unable to load configured flush policy '" + className + "' for table '"
            + htd.getTableName() + "', load default flush policy "
            + DEFAULT_FLUSH_POLICY_CLASS.getName() + " instead", e);
      return DEFAULT_FLUSH_POLICY_CLASS;
    }
  }
}
