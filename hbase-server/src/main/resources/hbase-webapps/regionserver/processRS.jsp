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

// There is always two of GC collectors
List<GarbageCollectorMXBean> gcBeans = JSONMetricUtil.getGcCollectorBeans();
GarbageCollectorMXBean collector1 = null;
GarbageCollectorMXBean collector2 = null;
try {
collector1 = gcBeans.get(0);
collector2 = gcBeans.get(1);
} catch(IndexOutOfBoundsException e) {}
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
        <th>JvmPauseMonitor Count </th>
        <th>Owner</th>
    </tr>
    <tr>
      <tr>
        <td><%= new Date(runtimeBean.getStartTime()) %></td>
        <td><%= StringUtils.humanTimeDiff(runtimeBean.getUptime()) %></td>
        <td><%= JSONMetricUtil.getProcessPID() %></td>
        <td><%= (long)JSONMetricUtil.getValueFromMBean(rsMetrics, "pauseWarnThresholdExceeded")
          + (long)JSONMetricUtil.getValueFromMBean(rsMetrics, "pauseInfoThresholdExceeded") %></td>
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
        <th>ThreadsRunable</th>
        <th>ThreadsBlocked</th>
        <th>ThreadsWaiting</th>
        <th>ThreadsTimeWaiting</th>
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
    <% if (gcBeans.size() == 2) { %>
<div class="tabbable">
  <ul class="nav nav-pills">
    <li class="active">
      <a href="#tab_gc1" data-toggle="tab"><%=collector1.getName() %></a>
    </li>
    <li class="">
      <a href="#tab_gc2" data-toggle="tab"><%=collector2.getName() %></a>
     </li>
  </ul>
    <div class="tab-content" style="padding-bottom: 9px; border-bottom: 1px solid #ddd;">
      <div class="tab-pane active" id="tab_gc1">
          <table class="table table-striped">
            <tr>
              <th>Collection Count</th>
              <th>Collection Time</th>
              <th>Last duration</th>
            </tr>
            <tr>
              <td> <%= collector1.getCollectionCount() %></td>
              <td> <%= StringUtils.humanTimeDiff(collector1.getCollectionTime()) %> </td>
              <td> <%= StringUtils.humanTimeDiff(JSONMetricUtil.getLastGcDuration(
                collector1.getObjectName())) %></td>
            </tr>
          </table>
      </div>
      <div class="tab-pane" id="tab_gc2">
        <table class="table table-striped">
          <tr>
            <th>Collection Count</th>
            <th>Collection Time</th>
             <th>Last duration</th>
          </tr>
          <tr>
            <td> <%= collector2.getCollectionCount()  %></td>
            <td> <%= StringUtils.humanTimeDiff(collector2.getCollectionTime()) %> </td>
            <td> <%= StringUtils.humanTimeDiff(JSONMetricUtil.getLastGcDuration(
              collector2.getObjectName())) %></td>
          </tr>
          </table>
      </div>
      </div>
  </div>
  <%} else { %>
  <p> Can not display GC Collector stats.</p>
  <%} %>
  Total GC Collection time: <%= StringUtils.humanTimeDiff(collector1.getCollectionTime() +
    collector2.getCollectionTime())%>
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
