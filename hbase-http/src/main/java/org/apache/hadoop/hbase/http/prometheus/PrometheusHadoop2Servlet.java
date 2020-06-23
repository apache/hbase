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

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricType;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.impl.MetricsExportHelper;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public class PrometheusHadoop2Servlet extends HttpServlet {

  private Logger LOG = LoggerFactory.getLogger(PrometheusHadoop2Servlet.class);

  /**
   * These 2 structures will make sure that there is only one thread which
   * reads the metricRecords and produces a string which can be directly consumed
   * to write on socket / any writer.
   */
  private final AtomicInteger amIProducer = new AtomicInteger(0);
  private final Object waitingRoom = new Object();

  /* reference to generated metrics buffer */
  private final AtomicReference<String> pMetricsBuffer = new AtomicReference<>();

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws IOException {
    writeMetrics(resp.getWriter());
  }

  @VisibleForTesting
  void writeMetrics(Writer writer) throws IOException {
    if (amIProducer.compareAndSet(0, 1)) {
      try {
        renderMetrics(MetricsExportHelper.export());
      } finally {
        amIProducer.set(0);
        synchronized (waitingRoom) {
          waitingRoom.notifyAll();
        }
      }
    } else {
      synchronized (waitingRoom) {
        try {
          waitingRoom.wait(5000L);
        } catch (InterruptedException e) {
          LOG.error("Interrupted while waiting for metric collection", e);
        }
      }
    }
    writer.write(pMetricsBuffer.get());
    writer.flush();
  }

  private void renderMetrics(Collection<MetricsRecord> metricRecords) {
    StringBuilder builder = new StringBuilder();
    for (MetricsRecord metricsRecord : metricRecords) {
      for (AbstractMetric metrics : metricsRecord.metrics()) {
        if (metrics.type() == MetricType.COUNTER || metrics.type() == MetricType.GAUGE) {

          String key = PrometheusUtils.toPrometheusName(metricsRecord.name(), metrics.name());
          builder.append("# TYPE ").append(key).append(" ").append(metrics.type().toString().toLowerCase()).append("\n").append(key).append("{");

          /* add tags */
          String sep = "";
          for (MetricsTag tag : metricsRecord.tags()) {
            String tagName = tag.name().toLowerCase();

            //ignore specific tag which includes sub-hierarchy
            if (!tagName.equals("numopenconnectionsperuser")) {
              builder.append(sep).append(tagName).append("=\"").append(tag.value()).append("\"");
              sep = ",";
            }
          }
          builder.append("} ");
          builder.append(metrics.value().toString()).append('\n');
        }
      }
    }
    pMetricsBuffer.set(builder.toString());
  }

}
