/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.http.prom;

import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.yetus.audience.InterfaceAudience;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@InterfaceAudience.Private
public class PrometheusHadoop2Servlet extends HttpServlet {

  public PrometheusMetricsSink getPrometheusSink() {
    return (PrometheusMetricsSink) getServletContext().getAttribute(HttpServer.PROMETHEUS_SINK);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
    throws ServletException, IOException {
    PrometheusMetricsSink sink = getPrometheusSink();
    sink.writeMetrics(resp.getWriter());
    resp.getWriter().flush();
  }
}
