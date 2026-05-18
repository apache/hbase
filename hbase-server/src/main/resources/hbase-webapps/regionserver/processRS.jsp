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
<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.Date"
  import="java.util.List"
  import="javax.management.ObjectName"
  import="java.lang.management.ManagementFactory"
  import="java.lang.management.MemoryPoolMXBean"
  import="java.lang.management.RuntimeMXBean"
  import="java.lang.management.GarbageCollectorMXBean"
  import="org.apache.hadoop.hbase.util.JSONMetricUtil"
  import="org.apache.hadoop.hbase.procedure2.util.StringUtils"
  import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
%>
<%
RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
ObjectName jvmMetrics = new ObjectName("Hadoop:service=HBase,name=JvmMetrics");
ObjectName rsMetrics = new ObjectName("Hadoop:service=HBase,name=RegionServer,sub=Server");
Object pauseWarnThresholdExceeded = JSONMetricUtil.getValueFromMBean(rsMetrics, "pauseWarnThresholdExceeded");
Object pauseInfoThresholdExceeded = JSONMetricUtil.getValueFromMBean(rsMetrics, "pauseInfoThresholdExceeded");

// There is always two of GC collectors
List<GarbageCollectorMXBean> gcBeans = JSONMetricUtil.getGcCollectorBeans();
List<MemoryPoolMXBean> mPools = JSONMetricUtil.getMemoryPools();
pageContext.setAttribute("pageTitle", "Process info for PID: " + JSONMetricUtil.getProcessPID());
%>
<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <div class="row">
      <div class="page-header">
          <h1><%= JSONMetricUtil.getCommmand().split(" ")[0] %></h1>
      </div>
  </div>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>Started</th>
        <th>Uptime</th>
        <th>PID</th>
        <th>JvmPauseMonitor Count</th>
        <th>Owner</th>
    </tr>
    <tr>
      <tr>
        <td><%= new Date(runtimeBean.getStartTime()) %></td>
        <td><%= StringUtils.humanTimeDiff(runtimeBean.getUptime()) %></td>
        <td><%= JSONMetricUtil.getProcessPID() %></td>
        <td><%= pauseWarnThresholdExceeded != null && pauseInfoThresholdExceeded != null ?
                (long)pauseWarnThresholdExceeded + (long)pauseInfoThresholdExceeded : 0 %></td>
        <td><%= runtimeBean.getSystemProperties().get("user.name") %></td>
      </tr>
  </table>
</div>
<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
    <h2>Threads</h2>
    </div>
    </div>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>ThreadsNew</th>
        <th>ThreadsRunnable</th>
        <th>ThreadsBlocked</th>
        <th>ThreadsWaiting</th>
        <th>ThreadsTimedWaiting</th>
        <th>ThreadsTerminated</th>
    </tr>
    <tr>
      <tr>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsNew")  %></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsRunnable")%></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsBlocked")%></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsWaiting")%></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsTimedWaiting")%></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsTerminated")%></td>
      </tr>
  </table>
</div>
<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
    <h2>GC Collectors</h2>
    </div>
  </div>
    <% if (gcBeans != null && !gcBeans.isEmpty()) { %>
<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <% int idx = 0; for (GarbageCollectorMXBean gc : gcBeans) { %>
    <li class="nav-item">
      <a class="nav-link <%= idx == 0 ? "active" : "" %>" href="#tab_gc_<%= idx %>" data-bs-toggle="tab" role="tab">
        <%= gc.getName() %>
      </a>
    </li>
    <% idx++; } %>
  </ul>
    <div class="tab-content">
      <% idx = 0; long totalGcTime = 0; for (GarbageCollectorMXBean gc : gcBeans) { totalGcTime += gc.getCollectionTime(); %>
      <div class="tab-pane <%= idx == 0 ? "active" : "" %>" id="tab_gc_<%= idx %>" role="tabpanel">
          <table class="table table-striped">
            <tr>
              <th>Collection Count</th>
              <th>Collection Time</th>
              <th>Last duration</th>
            </tr>
            <tr>
              <td><%= gc.getCollectionCount() %></td>
              <td><%= StringUtils.humanTimeDiff(gc.getCollectionTime()) %></td>
              <td><%= StringUtils.humanTimeDiff(JSONMetricUtil.getLastGcDuration(
                gc.getObjectName())) %></td>
            </tr>
          </table>
      </div>
      <% idx++; } %>
      </div>
</div>
<p>Total GC Collection time: <%= StringUtils.humanTimeDiff(totalGcTime) %></p>
  <% } else { %>
  <p>Can not display GC Collector stats.</p>
  <% } %>
</div>
<% for(MemoryPoolMXBean mp:mPools) {
if(mp.getName().contains("Cache")) continue;%>
<div class="container-fluid content">
  <div class="row">
      <div class="page-header">
          <h2><%= mp.getName() %></h2>
      </div>
  </div>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>Commited</th>
        <th>Init</th>
        <th>Max</th>
        <th>Used</th>
        <th>Utilization [%]</th>
    </tr>
    <tr>
      <tr>
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getCommitted(), "B", 1) %></td>
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getInit(), "B", 1) %></td>
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getMax(), "B", 1) %></td>
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getUsed(), "B", 1) %></td>
        <td><%= JSONMetricUtil.calcPercentage(mp.getUsage().getUsed(),
          mp.getUsage().getCommitted()) %></td>
      </tr>
  </table>
</div>
<% } %>

<jsp:include page="footer.jsp" />
