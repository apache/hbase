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

package org.apache.hadoop.hbase.http.prometheus;

import org.apache.commons.lang3.StringUtils;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.yetus.audience.InterfaceAudience;
import java.util.regex.Pattern;

@InterfaceAudience.Private
class PrometheusUtils {

  /*
   * (not a javadoc, but ascii-art)
   *  This regex is used to split a camel case string to insert _ and make it snake_cased
   *
   *  Split rules
   *   (1) split between [a-z] and [A-Z]
   *   (2) split between [A-Z] and ([A-Z][a-z])
   *   (3) & (4) split when characters other than [A-Za-z0-9]
   *
   *   Rule numbers  (1)                 (2)                        (3) (4)
   */
  private static final Pattern SPLIT_PATTERN =
    Pattern.compile("(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=([A-Z][a-z]))|\\W|(_)+");

  /**
   * map a camel case metric name to snake_case metric name. Here are few examples
   * <p/>
   * <br/> aMetricName => a_metric_name
   * <br/> a_MetricName => a_metric_name
   * <br/> a_metric_name => a_metric_name
   * <br/> AMetricName => a_metric_name
   * <br/> SCMPipelineMetrics => scm_pipeline_metrics
   * <p/>
   * For more examples, refer {@link org.apache.hadoop.hbase.http.prometheus.TestPrometheusUtils}
   * <p/>
   *
   * @param metricRecordName : metric record name
   * @param metricName : metric name
   * @return snake_case metric name
   */
  @VisibleForTesting
  static String toPrometheusName(String metricRecordName, String metricName) {
    String baseName = metricRecordName + StringUtils.capitalize(metricName);
    String[] parts = SPLIT_PATTERN.split(baseName);
    return String
            .join("_", parts)
            .toLowerCase();
  }
}
