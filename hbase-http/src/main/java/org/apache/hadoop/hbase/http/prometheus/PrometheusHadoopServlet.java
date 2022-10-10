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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsExportHelper;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class PrometheusHadoopServlet extends HttpServlet {

  private static final Pattern SPLIT_PATTERN =
    Pattern.compile("(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=([A-Z][a-z]))|\\W|(_)+");

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    writeMetrics(resp.getWriter(), "true".equals(req.getParameter("description")));
  }

  static String toPrometheusName(String metricRecordName, String metricName) {
    String baseName = metricRecordName + StringUtils.capitalize(metricName);
    String[] parts = SPLIT_PATTERN.split(baseName);
    return String.join("_", parts).toLowerCase();
  }

  /*
   * SimpleClient for Prometheus is not used, because the format is very easy to implement and this
   * solution doesn't add any dependencies to the project. You can check the Prometheus format here:
   * https://prometheus.io/docs/instrumenting/exposition_formats/
   */
  @RestrictedApi(explanation = "Should only be called in tests or self", link = "",
      allowedOnPath = ".*/src/test/.*|.*/PrometheusHadoopServlet\\.java")
  void writeMetrics(Writer writer, boolean desc) throws IOException {
    Collection<MetricsRecord> metricRecords = MetricsExportHelper.export();
    for (MetricsRecord metricsRecord : metricRecords) {
      for (AbstractMetric metrics : metricsRecord.metrics()) {
        if (metrics.type() == MetricType.COUNTER || metrics.type() == MetricType.GAUGE) {

          String key = toPrometheusName(metricsRecord.name(), metrics.name());

          if (desc) {
            String description = metrics.description();
            if (!description.isEmpty()) writer.append("# HELP ").append(description).append('\n');
          }

          writer.append("# TYPE ").append(key).append(" ")
            .append(metrics.type().toString().toLowerCase()).append('\n').append(key).append("{");

          /* add tags */
          String sep = "";
          for (MetricsTag tag : metricsRecord.tags()) {
            String tagName = tag.name().toLowerCase();
            writer.append(sep).append(tagName).append("=\"").append(tag.value()).append("\"");
            sep = ",";
          }
          writer.append("} ");
          writer.append(metrics.value().toString()).append('\n');
        }
      }
    }
    writer.flush();
  }
}
