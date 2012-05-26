/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver.metrics;

import javax.management.ObjectName;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.MetricsMBeanBase;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsRegistry;

/**
 * Exports dynamic region server metric recorded in
 * {@link RegionServerDynamicMetrics} as an MBean
 * for JMX monitoring.
 */
@InterfaceAudience.Private
public class RegionServerDynamicStatistics extends MetricsMBeanBase {
  private final ObjectName mbeanName;

  public RegionServerDynamicStatistics(MetricsRegistry registry) {
    super(registry, "RegionServerDynamicStatistics");
    mbeanName = MBeanUtil.registerMBean("RegionServer", "RegionServerDynamicStatistics", this);
  }

  public void shutdown() {
    if (mbeanName != null)
      MBeanUtil.unregisterMBean(mbeanName);
  }

}
