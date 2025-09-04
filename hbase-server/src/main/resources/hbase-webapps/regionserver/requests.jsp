<%--
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
--%>
<%@ page import="org.apache.hadoop.hbase.ipc.RpcServer" %>
<%@ page import="org.apache.hadoop.hbase.regionserver.HRegionServer" %>
<%@ page import="org.apache.hadoop.hbase.regionserver.RequestStatsCollector" %>
<%@ page import="static org.apache.hadoop.hbase.regionserver.RequestStatsCollector.*" %>
<%@ page import="static org.apache.hadoop.hbase.regionserver.http.RSRequestServlet.*" %>
<%@ page contentType="text/html;charset=UTF-8"%>
<%
  HRegionServer rs = (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  RequestStatsCollector collector = ((RpcServer)(rs.getRpcServer())).getRequestStatsCollector();
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="Real-time Requests"/>
</jsp:include>

<style>
  .fixed_width_header {
    width: 220px;
  }
</style>

<div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>Real-time Request Stats</h1>
    </div>
  </div>

  <section>
    <h2>Collection Configuration</h2>
    <form>
      <table class='table table-striped'>
        <thead>
          <tr>
            <th class="fixed_width_header">Name</th>
            <th class="fixed_width_header">Value</th>
            <th>Description</th>
          </tr>
        </thead>
        <tbody>
        <tr>
          <td>
            <label for="<%= PARAM_MAX_SIZE_PERCENT %>">Collection Size (%)</label>
          </td>
          <td>
            <input type="number" step="1"
                   class="form-control"
                   id="<%= PARAM_MAX_SIZE_PERCENT %>"
                   name="<%= PARAM_MAX_SIZE_PERCENT %>"
                   oninput="validateRange(this, <%= COLLECTION_SIZE_PERCENT_MIN %>, <%= COLLECTION_SIZE_PERCENT_MAX %>)"
                   value=<%= collector.getCollectionSizePercent() %> />
          </td>
          <td>
            Percentage of total heap size to use for request statistics (<%= COLLECTION_SIZE_PERCENT_MIN %> ~ <%= COLLECTION_SIZE_PERCENT_MAX %>)
          </td>
        </tr>
        <tr>
          <td>
            <label for="<%= PARAM_SAMPLING_RATE %>">Sampling Rate (%)</label>
          </td>
          <td>
            <input type="number" step="1"
                   class="form-control"
                   id="<%= PARAM_SAMPLING_RATE %>"
                   name="<%= PARAM_SAMPLING_RATE %>"
                   oninput="validateRange(this, <%= SAMPLING_RATE_MIN %>, <%= SAMPLING_RATE_MAX %>)"
                   value=<%= collector.getSamplingRate() %> />
          </td>
          <td>
            Percentage of requests to sample (<%= SAMPLING_RATE_MIN %> ~ <%= SAMPLING_RATE_MAX %>)
          </td>
        </tr>
        <tr>
          <td colspan="3">
            <button id="startBtn" type="button" class="btn btn-primary" onclick="doAction('<%= ACTION_START %>')">Start</button>
            <button id="stopBtn" type="button" class="btn btn-primary" onclick="doAction('<%= ACTION_STOP %>')">Stop</button>
            <button id="clearBtn" type="button" class="btn btn-primary" onclick="doAction('<%= ACTION_CLEAR %>')">Clear</button>
            <span id="warningMsg" style="margin-left:8px;"></span>
          </td>
        </tr>
        </tbody>
      </table>
    </form>

    <div id="errorArea"></div>
  </section>

  <section>
    <h2>Status</h2>
    <table class='table table-striped'>
      <thead>
        <tr>
          <th class="fixed_width_header">Status</th>
          <th class="fixed_width_header">Started At</th>
          <th class="fixed_width_header">Stopped At</th>
          <th>Max Size</th>
          <th>Current Size</th>
          <th>Current Entry Count</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <td><span id="status"></span></td>
          <td><span id="startTime"></span></td>
          <td><span id="stopTime"></span></td>
          <td><span id="maxSize"></span></td>
          <td><span id="size"></span></td>
          <td><span id="entryCount"></span></td>
        </tr>
      </tbody>
    </table>
  </section>

  <section>
    <h2>Query Stats</h2>
    <div class="tabbable">
      <ul class="nav nav-pills" role="tablist">
        <li class="nav-item"><a class="nav-link active" href="#tab_simple" data-bs-toggle="tab" role="tab">Simple</a></li>
        <li class="nav-item"><a class="nav-link" href="#tab_sql" data-bs-toggle="tab" role="tab">SQL</a></li>
      </ul>
      <div class="tab-content">
        <div class="tab-pane active" id="tab_simple" role="tabpanel">
          <form>
            <table class='table table-striped'>
              <thead>
                <tr>
                  <th class="fixed_width_header">Name</th>
                  <th class="fixed_width_header">Value</th>
                  <th>Description</th>
                </tr>
                </thead>
              <tbody>
                <tr>
                  <td>
                    <label for="<%= PARAM_ORDER_BY %>">Order by</label>
                  </td>
                  <td>
                    <select id="<%= PARAM_ORDER_BY %>" name="<%= PARAM_ORDER_BY %>" class="form-select">
                      <option value="counts">Counts</option>
                      <option value="requestSizeSum">Request Size Sum</option>
                      <option value="requestSizeAvg">Request Size Avg</option>
                      <option value="responseTimeSum" selected>Response Time Sum</option>
                      <option value="responseTimeAvg">Response Time Avg</option>
                      <option value="responseSizeSum">Response Size Sum</option>
                      <option value="responseSizeAvg">Response Size Avg</option>
                    </select>
                  </td>
                  <td>
                    Sort key for ordering results in descending order
                  </td>
                </tr>
                <tr>
                  <td>
                    <label for="<%= PARAM_TOP_N %>">Top N</label>
                  </td>
                  <td>
                    <input type="number" step="1"
                           class="form-control"
                           id="<%= PARAM_TOP_N %>"
                           name="<%= PARAM_TOP_N %>"
                           oninput="validateRange(this, <%= TOP_N_MIN %>, <%= TOP_N_MAX %>)"
                           value=100 />
                  </td>
                  <td>
                    Number of top requests to display (<%= TOP_N_MIN %> ~ <%= TOP_N_MAX %>)
                  </td>
                </tr>
                <tr>
                  <td colspan="3">
                    <button id="queryBtn" type="button" class="btn btn-primary" onclick="query(false)">Query</button>
                  </td>
                </tr>
              </tbody>
            </table>
          </form>
        </div>
        <div class="tab-pane" id="tab_sql" role="tabpanel">
          <form>
            <table class='table table-striped'>
              <thead>
                <tr>
                  <th>Schema</th>
                  <th><label for="<%= PARAM_SQL %>">SQL</label></th>
                  <th><label for="<%= PARAM_LIMIT %>">Limit (<%= TOP_N_MIN %> ~ <%= TOP_N_MAX %>)</label></th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>
                      <pre><code><%= """
                        CREATE TABLE stats (
                          rowKey                VARCHAR,
                          region                VARCHAR,
                          `table`               VARCHAR,
                          operation             VARCHAR,
                          client                VARCHAR,
                          counts                BIGINT,
                          requestSizeSumBytes   BIGINT,
                          responseTimeSumMs     BIGINT,
                          responseSizeSumBytes  BIGINT
                        );""".stripIndent()
                      %></code></pre>
                  </td>
                  <td>
                    <textarea class="form-control" id="<%= PARAM_SQL %>" name="<%= PARAM_SQL %>" rows="25" cols="80" style="font-family: monospace;"><%=
                        """
                          select
                            rowKey,
                            region,
                            `table`,
                            operation,
                            client,
                            sum(counts) counts,
                            sum(requestSizeSumBytes) as requestSizeSumBytes,
                            sum(cast(requestSizeSumBytes as double))/sum(counts) as requestSizeAvgBytes,
                            sum(responseTimeSumMs) as responseTimeSumMs,
                            sum(cast(responseTimeSumMs as double))/sum(counts) as responseTimeAvgMs,
                            sum(responseSizeSumBytes) as responseSizeSumBytes,
                            sum(cast(responseSizeSumBytes as double))/sum(counts) as responseSizeAvgBytes
                          from stats
                          group by
                            rowKey,
                            region,
                            `table`,
                            operation,
                            client
                          order by responseTimeSumMs desc""".stripIndent()
                      %></textarea>
                  </td>
                  <td>
                    <input type="number" step="1"
                           class="form-control"
                           id="<%= PARAM_LIMIT %>"
                           name="<%= PARAM_LIMIT %>"
                           oninput="validateRange(this, <%= TOP_N_MIN %>, <%= TOP_N_MAX %>)"
                           value=100 />
                  </td>
                </tr>
                <tr>
                  <td colspan="3">
                    <button id="querySqlBtn" type="button" class="btn btn-primary" onclick="query(true)">Query</button>
                  </td>
                </tr>
              </tbody>
            </table>
          </form>
        </div>
      </div>
    </div>

    <div id="executionTimeArea"></div>
    <div id="statsArea"></div>
  </section>
</div>

<script>
  document.addEventListener("DOMContentLoaded", () => {
    fetch("${pageContext.request.contextPath}/requests/info", { method: 'GET' })
      .then(response => response.json())
      .then(data => {
        updateElements(data)
      });
  });

  function updateElements(info) {
    document.getElementById("status").textContent = info.status;
    document.getElementById("startTime").textContent = info.startTime === 0 ? "<%= N_A %>" : new Date(info.startTime).toLocaleString();
    document.getElementById("stopTime").textContent = info.stopTime === 0 ? "<%= N_A %>" : new Date(info.stopTime).toLocaleString();
    document.getElementById("maxSize").textContent = info.maxSize;
    document.getElementById("size").textContent = info.size;
    document.getElementById("entryCount").textContent = info.entryCount === -1 ? "<%= N_A %>" : info.entryCount.toLocaleString();

    document.getElementById("startBtn").disabled = info.status === "<%= STATUS_RUNNING %>";
    document.getElementById("stopBtn").disabled = info.status !== "<%= STATUS_RUNNING %>";
    document.getElementById("clearBtn").disabled = info.status === "<%= STATUS_RUNNING %>" || info.size === "<%= N_A %>";
    document.getElementById("queryBtn").disabled = info.size === "<%= N_A %>";
    document.getElementById("querySqlBtn").disabled = info.size === "<%= N_A %>";
    document.getElementById("warningMsg").textContent = info.status === "<%= STATUS_RUNNING %>"
      ? "Warning: Statistics collection consumes CPU and memory. Stop it when not needed."
      : (info.entryCount === -1
          ? ""
          : "Warning: Collected statistics data consumes memory. Clear it when no longer needed.");
  }

  function doAction(action) {
    if (document.getElementById("<%= PARAM_MAX_SIZE_PERCENT %>").value === "") {
      document.getElementById("errorArea").innerHTML = "Please enter a valid Collection Size (%)";
      return;
    }

    if (document.getElementById("<%= PARAM_SAMPLING_RATE %>").value === "") {
      document.getElementById("errorArea").innerHTML = "Please enter a valid Sampling Rate (%)";
      return;
    }

    if (action === '<%= ACTION_START %>') {
      if (!confirm("Collecting statistics consumes CPU and memory. Use with caution. It may be better to start with low 'Collection Size' and 'Sampling Rate'.\n\nDo you want to proceed?")) {
        return;
      }
    }

    fetch("${pageContext.request.contextPath}/requests", {
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      body: new URLSearchParams({
        action: action,
        maxSizePercent: document.getElementById("<%= PARAM_MAX_SIZE_PERCENT %>").value,
        samplingRate: document.getElementById("<%= PARAM_SAMPLING_RATE %>").value
      })
    })
    .then(response => {
      if (!response.ok) {
        return response.text().then(text => {
          throw new Error(text);
        });
      }

      return response.json();
    })
    .then(json => {
      updateElements(json);
      document.getElementById("errorArea").innerHTML = "";
    })
    .catch(err => {
      document.getElementById("errorArea").innerHTML = err;
    });
  }

  function query(isSql) {
    const orderBy = document.getElementById("<%= PARAM_ORDER_BY %>").value;
    const topN = document.getElementById("<%= PARAM_TOP_N %>").value;
    const limit = document.getElementById("<%= PARAM_LIMIT %>").value;
    const sql = document.getElementById("<%= PARAM_SQL %>").value;
    const params = isSql
      ? new URLSearchParams({
          limit: limit,
          sql: sql
        })
      : new URLSearchParams({
          topN: topN,
          orderBy: orderBy
        });

    document.getElementById("queryBtn").disabled = true;
    document.getElementById("querySqlBtn").disabled = true;

    fetch("${pageContext.request.contextPath}/requests/query?" + params.toString())
      .then(response => {
        if (!response.ok) {
          return response.text().then(text => {
            throw new Error(text);
          });
        }

        return response.json();
      })
      .then(json => {
        if (json.length === 0) {
          document.getElementById("statsArea").innerHTML = "<p>No data</p>";
          return;
        }

        updateElements(json.info);

        const executionTimeMs = json.executionTimeMs;
        document.getElementById("executionTimeArea").innerHTML = "<p>Execution Time: " + (executionTimeMs/1000.0).toFixed(2) + " sec</p>";

        const stats = json.stats;
        if (!Array.isArray(stats) || stats.length === 0) {
          document.getElementById("statsArea").innerHTML = "<p>No data</p>";
          return;
        }

        const keys = Object.keys(stats[0]);
        let html = "<table class='table table-striped'><thead><tr>";
        keys.forEach(key => {
          html += "<th>" + formatHeader(key) + (!json.isSql && key.startsWith(orderBy) ? "▼" : "") + "</th>";
        });
        html += "</tr></thead><tbody>";

        stats.forEach(row => {
          html += "<tr>";
          keys.forEach(key => {
            const value = row[key];
            if (typeof value === 'number') {
              html += "<td style='text-align: right;'>" + value.toLocaleString() + "</td>";
            } else {
              html += "<td>" + value + "</td>";
            }
          });
          html += "</tr>";
        });

        html += "</tbody></table>";
        document.getElementById("statsArea").innerHTML = html;
      })
      .catch(err => {
        document.getElementById("statsArea").innerHTML = err;
      })
      .finally(() => {
        document.getElementById("queryBtn").disabled = false;
        document.getElementById("querySqlBtn").disabled = false;
      });
  }

  function formatHeader(str) {
    if (!str) return str;

    let result = str.charAt(0).toUpperCase() + str.slice(1);
    result = result.replace(/([A-Z])/g, ' $1');
    result = result.replace(/\(/g, ' (');
    return result;
  }

  function validateRange(e, min, max) {
    let val = parseInt(e.value, 10);
    if (isNaN(val)) {
      e.value = "";
    } else if (val < min) {
      e.value = min;
    } else if (val > max) {
      e.value = max;
    } else {
      e.value = val;
    }
  }
</script>

<jsp:include page="footer.jsp" />
