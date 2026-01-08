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
         import="java.util.*"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.tool.CanaryTool"
         import="java.util.concurrent.atomic.LongAdder"
         import="org.apache.hadoop.hbase.util.JvmVersion"
         import="static org.apache.hadoop.hbase.util.CanaryStatusUtil.serverNameLink" %>
<%
  CanaryTool.RegionStdOutSink sink =
    (CanaryTool.RegionStdOutSink) getServletContext().getAttribute("sink");
  if (sink == null) {
    throw new ServletException(
      "RegionStdOutSink is null! The CanaryTool's InfoServer is not initialized correctly");
  }
%>
<!DOCTYPE html>
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
  <meta charset="utf-8">
  <title>Canary</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="">
  <meta name="author" content="">

  <link href="/static/css/bootstrap.min.css" rel="stylesheet">
  <link href="/static/css/hbase.css" rel="stylesheet">
  <link rel="shortcut icon" href="/static/favicon.ico">
</head>

<body>

  <nav class="navbar navbar-expand-md navbar-light fixed-top bg-light">
    <div class="container-fluid">
      <a class="navbar-brand" href="/canary.jsp"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
      <button type="button" class="navbar-toggler" data-bs-toggle="collapse" data-bs-target=".navbar-collapse">
        <span class="navbar-toggler-icon"></span>
      </button>
    </div>
  </nav>

  <div class="container">

    <section>
      <h2>Failed Servers</h2>
      <%
        Map<ServerName, LongAdder> perServerFailuresCount = sink.getPerServerFailuresCount();
        %>
      <table class="table table-striped">
        <tr>
          <th>Server</th>
          <th>Failures Count</th>
        </tr>
        <% if (perServerFailuresCount != null && perServerFailuresCount.size() > 0) {
          for (Map.Entry<ServerName, LongAdder> entry : perServerFailuresCount.entrySet()) { %>
            <tr>
              <td><%= serverNameLink(entry.getKey()) %></td>
              <td><%= entry.getValue() %></td>
            </tr>
        <%
          }
        } %>
        <tr><td>Total Failed Servers: <%= (perServerFailuresCount != null) ? perServerFailuresCount.size() : 0 %></td></tr>
      </table>
    </section>

    <section>
      <h2>Failed Tables</h2>
      <%
        Map<String, LongAdder> perTableFailuresCount = sink.getPerTableFailuresCount();
      %>
      <table class="table table-striped">
        <tr>
          <th>Table</th>
          <th>Failures Count</th>
        </tr>
        <% if (perTableFailuresCount != null && perTableFailuresCount.size() > 0) {
          for (Map.Entry<String, LongAdder> entry : perTableFailuresCount.entrySet()) { %>
            <tr>
              <td><%= entry.getKey() %></td>
              <td><%= entry.getValue() %></td>
            </tr>
          <% } %>
        <% } %>
        <tr><td>Total Failed Tables: <%= (perTableFailuresCount != null) ? perTableFailuresCount.size() : 0 %></td></tr>
      </table>
    </section>

    <section>
      <h2>Software Attributes</h2>
      <table id="attributes_table" class="table table-striped">
        <tr>
          <th>Attribute Name</th>
          <th>Value</th>
          <th>Description</th>
        </tr>
        <tr>
          <td>JVM Version</td>
          <td><%= JvmVersion.getVersion() %></td>
          <td>JVM vendor and version</td>
        </tr>
        <tr>
          <td>HBase Version</td>
          <td><%= org.apache.hadoop.hbase.util.VersionInfo.getVersion() %>, r<%= org.apache.hadoop.hbase.util.VersionInfo.getRevision() %></td><td>HBase version and revision</td>
        </tr>
        <tr>
          <td>HBase Compiled</td>
          <td><%= org.apache.hadoop.hbase.util.VersionInfo.getDate() %>, <%= org.apache.hadoop.hbase.util.VersionInfo.getUser() %></td>
          <td>When HBase version was compiled and by whom</td>
        </tr>
        <tr>
          <td>Hadoop Version</td>
          <td><%= org.apache.hadoop.util.VersionInfo.getVersion() %>, r<%= org.apache.hadoop.util.VersionInfo.getRevision() %></td>
          <td>Hadoop version and revision</td>
        </tr>
        <tr>
          <td>Hadoop Compiled</td>
          <td><%= org.apache.hadoop.util.VersionInfo.getDate() %>, <%= org.apache.hadoop.util.VersionInfo.getUser() %></td>
          <td>When Hadoop version was compiled and by whom</td>
        </tr>
      </table>
    </section>

  </div> <!-- /container -->

  <script src="/static/js/jquery.min.js" type="text/javascript"></script>
  <script src="/static/js/bootstrap.bundle.min.js" type="text/javascript"></script>
  <script src="/static/js/tab.js" type="text/javascript"></script>
</body>

</html>
