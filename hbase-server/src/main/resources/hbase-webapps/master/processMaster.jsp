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
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="static org.apache.commons.lang3.StringEscapeUtils.escapeXml"
  import="javax.management.ObjectName"
  import="java.lang.management.ManagementFactory"
  import="java.lang.management.MemoryPoolMXBean"
  import="java.lang.management.RuntimeMXBean"
  import="java.lang.management.GarbageCollectorMXBean"
  import="org.apache.hadoop.hbase.util.JSONMetricUtil"
  import="org.apache.hadoop.hbase.procedure2.util.StringUtils"
  import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
  import="com.fasterxml.jackson.databind.JsonNode"
%>
<%
RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
ObjectName jvmMetrics = new ObjectName("Hadoop:service=HBase,name=JvmMetrics");

// There is always two of GC collectors
List<GarbageCollectorMXBean> gcBeans = JSONMetricUtil.getGcCollectorBeans();
GarbageCollectorMXBean collector1 = null;
GarbageCollectorMXBean collector2 = null;
try {
collector1 = gcBeans.get(0);
collector2 = gcBeans.get(1);
} catch(IndexOutOfBoundsException e) {}
List<MemoryPoolMXBean> mPools = JSONMetricUtil.getMemoryPools();
%>
<!DOCTYPE html>
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta charset="utf-8">
    <title>Process info for PID: <%= JSONMetricUtil.getProcessPID() %></title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">

    <link href="/static/css/bootstrap.min.css" rel="stylesheet">
    <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
    <link href="/static/css/hbase.css" rel="stylesheet">
</head>
<body>
<div class="navbar  navbar-fixed-top navbar-default">
    <div class="container-fluid">
        <div class="navbar-header">
            <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
            </button>
            <a class="navbar-brand" href="/rs-status"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
        </div>
        <div class="collapse navbar-collapse">
            <ul class="nav navbar-nav">
            <li><a href="/master-status">Home</a></li>
                <li><a href="/tablesDetailed.jsp">Table Details</a></li>
                <li><a href="/procedures.jsp">Procedures</a></li>
                <li><a href="/processMaster.jsp">Process Metrics</a></li>
                <li><a href="/logs/">Local Logs</a></li>
                <li><a href="/logLevel">Log Level</a></li>
                <li><a href="/dump">Debug Dump</a></li>
                <li><a href="/jmx">Metrics Dump</a></li>
                <% if (HBaseConfiguration.isShowConfInServlet()) { %>
                <li><a href="/conf">HBase Configuration</a></li>
                <% } %>
            </ul>
        </div><!--/.nav-collapse -->
    </div>
</div>
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
        <th>Owner</th>
    </tr>
    <tr>
      <tr>
        <td><%= new Date(runtimeBean.getStartTime()) %></a></td>
        <td><%= StringUtils.humanTimeDiff(runtimeBean.getUptime()) %></a></td>
        <td><%= JSONMetricUtil.getProcessPID() %></a></td>
        <td><%= runtimeBean.getSystemProperties().get("user.name") %></a></td>
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
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsNew")  %></a></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsRunnable")%></a></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsBlocked")%></a></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsWaiting")%></a></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsTimedWaiting")%></a></td>
        <td><%= JSONMetricUtil.getValueFromMBean(jvmMetrics, "ThreadsTerminated")%></a></td>
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
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getCommitted(), "B", 1) %></a></td>
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getInit(), "B", 1) %></a></td>
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getMax(), "B", 1) %></a></td>
        <td><%= TraditionalBinaryPrefix.long2String(mp.getUsage().getUsed(), "B", 1) %></a></td>
        <td><%= JSONMetricUtil.calcPercentage(mp.getUsage().getUsed(),
          mp.getUsage().getCommitted()) %></a></td>
      </tr>
  </table>
</div>
<% } %>

<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>
<script src="/static/js/tab.js" type="text/javascript"></script>

</body>
</html>
