/*
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
package org.apache.hadoop.metrics2.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public final class MetricsExportHelper {
  private MetricsExportHelper() {
  }

  public static Collection<MetricsRecord> export() {
    MetricsSystemImpl instance = (MetricsSystemImpl) DefaultMetricsSystem.instance();
    MetricsBuffer metricsBuffer = instance.sampleMetrics();
    List<MetricsRecord> metrics = new ArrayList<>();
    for (MetricsBuffer.Entry entry : metricsBuffer) {
      entry.records().forEach(metrics::add);
    }
    return metrics;
  }

}
