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
package org.apache.hadoop.hbase.regionserver.http;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RequestStatsCollector;
import org.apache.hadoop.hbase.regionserver.RequestStatsCollector.OrderBy;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class RSRequestServlet extends HttpServlet {
  private static final long serialVersionUID = 1L;

  public static final String PARAM_ORDER_BY = "orderBy";
  public static final String PARAM_TOP_N = "topN";
  public static final String PARAM_SQL = "sql";
  public static final String PARAM_LIMIT = "limit";
  public static final String PARAM_MAX_SIZE_PERCENT = "maxSizePercent";
  public static final String PARAM_SAMPLING_RATE = "samplingRate";

  public static final String ACTION = "action";
  public static final String ACTION_START = "start";
  public static final String ACTION_STOP = "stop";
  public static final String ACTION_CLEAR = "clear";

  public static final String N_A = "N/A";
  public static final String STATUS_RUNNING = "RUNNING";
  public static final String STATUS_STOPPED = "STOPPED";

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    if (!HttpServer.isInstrumentationAccessAllowed(getServletContext(), request, response)) {
      return;
    }

    String path = request.getPathInfo();
    if (path == null) {
      response.sendError(HttpServletResponse.SC_NOT_FOUND, "Path not found");
      return;
    }

    RequestStatsCollector collector =
      ((RpcServer) ((HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER))
        .getRpcServer()).getRequestStatsCollector();

    switch (path) {
      case "/info":
        response.setContentType("application/json; charset=UTF-8");
        try (PrintWriter writer = response.getWriter()) {
          writer.write(collector.buildInfoJson());
        }
        break;

      case "/query":
        String resultJson;
        String sql = StringEscapeUtils.unescapeXml(request.getParameter(PARAM_SQL));
        if (sql != null && !sql.isEmpty()) {
          Integer limit = parseInt(PARAM_LIMIT, RequestStatsCollector.TOP_N_MIN,
            RequestStatsCollector.TOP_N_MAX, request, response);
          if (limit == null) return;

          try {
            resultJson = collector.sql(sql, limit);
          } catch (SQLException | ClassNotFoundException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            break;
          }
        } else {
          Integer topN = parseInt(PARAM_TOP_N, RequestStatsCollector.TOP_N_MIN,
            RequestStatsCollector.TOP_N_MAX, request, response);
          if (topN == null) return;

          String orderByStr = request.getParameter(PARAM_ORDER_BY);
          OrderBy orderBy = OrderBy.fromValue(orderByStr);

          try {
            resultJson = collector.queryAndBuildResultJson(topN, orderBy);
          } catch (IllegalStateException e) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage());
            break;
          }
        }

        response.setContentType("application/json; charset=UTF-8");
        try (PrintWriter writer = response.getWriter()) {
          writer.write(resultJson);
        }
        break;

      default:
        response.sendError(HttpServletResponse.SC_NOT_FOUND, "Unknown path: " + path);
    }
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
    throws IOException {
    String action = request.getParameter(ACTION);
    if (action == null || action.isEmpty()) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "action is required");
      return;
    }

    RequestStatsCollector collector =
      ((RpcServer) ((HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER))
        .getRpcServer()).getRequestStatsCollector();

    switch (action) {
      case ACTION_START -> {
        Integer maxSizePercent =
          parseInt(PARAM_MAX_SIZE_PERCENT, RequestStatsCollector.COLLECTION_SIZE_PERCENT_MIN,
            RequestStatsCollector.COLLECTION_SIZE_PERCENT_MAX, request, response);
        if (maxSizePercent == null) return;

        Integer samplingRate =
          parseInt(PARAM_SAMPLING_RATE, RequestStatsCollector.SAMPLING_RATE_MIN,
            RequestStatsCollector.SAMPLING_RATE_MAX, request, response);
        if (samplingRate == null) return;

        collector.start(maxSizePercent, samplingRate);
      }
      case ACTION_STOP -> collector.stop();
      case ACTION_CLEAR -> collector.clear();
      default -> {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "unknown action");
        return;
      }
    }

    response.setContentType("application/json; charset=UTF-8");
    try (PrintWriter writer = response.getWriter()) {
      writer.write(collector.buildInfoJson());
    }
  }

  static Integer parseInt(String name, int min, int max, HttpServletRequest request,
    HttpServletResponse response) throws IOException {
    String maxSizePercentInput = request.getParameter(name);
    if (maxSizePercentInput == null || maxSizePercentInput.isEmpty()) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, name + " is required");
      return null;
    }

    try {
      int result = Integer.parseInt(maxSizePercentInput);
      if (result >= min && result <= max) {
        return result;
      }

      response.sendError(HttpServletResponse.SC_BAD_REQUEST, name + " is out of range");
    } catch (NumberFormatException e) {
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, name + " is not a int");
    }
    return null;
  }
}
