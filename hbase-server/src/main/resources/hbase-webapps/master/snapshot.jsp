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
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.snapshot.SnapshotInfo"
  import="org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.HBaseConfiguration" %>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  boolean readOnly = conf.getBoolean("hbase.master.ui.readonly", false);
  String snapshotName = request.getParameter("name");
  SnapshotDescription snapshot = null;
  SnapshotInfo.SnapshotStats stats = null;
  TableName snapshotTable = null;
  boolean tableExists = false;
  try (Admin admin = master.getConnection().getAdmin()) {
    for (SnapshotDescription snapshotDesc: admin.listSnapshots()) {
      if (snapshotName.equals(snapshotDesc.getName())) {
        snapshot = snapshotDesc;
        stats = SnapshotInfo.getSnapshotStats(conf, snapshot);
        snapshotTable = TableName.valueOf(snapshot.getTable());
        tableExists = admin.tableExists(snapshotTable);
        break;
      }
    }
  }

  String action = request.getParameter("action");
  String cloneName = request.getParameter("cloneName");
  boolean isActionResultPage = (!readOnly && action != null);
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta charset="utf-8">
    <% if (isActionResultPage) { %>
      <title>HBase Master: <%= master.getServerName() %></title>
    <% } else { %>
      <title>Snapshot: <%= snapshotName %></title>
    <% } %>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">

    <link href="/static/css/bootstrap.min.css" rel="stylesheet">
    <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
    <link href="/static/css/hbase.css" rel="stylesheet">
    <% if (isActionResultPage) { %>
    <script type="text/javascript">
    <!--
        setTimeout("history.back()",5000);
    -->
    </script>
    <% } %>
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
            <a class="navbar-brand" href="/master-status"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
        </div>
        <div class="collapse navbar-collapse">
            <ul class="nav navbar-nav">
                <li><a href="/master-status">Home</a></li>
                <li><a href="/tablesDetailed.jsp">Table Details</a></li>
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
<% if (snapshot == null) { %>
  <div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>Snapshot "<%= snapshotName %>" does not exists</h1>
    </div>
  </div>
  <p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
<% } else { %>
  <div class="container-fluid content">
  <div class="row">
      <div class="page-header">
          <h1>Snapshot: <%= snapshotName %></h1>
      </div>
  </div>
  <h2>Snapshot Attributes</h2>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>Table</th>
        <th>Creation Time</th>
        <th>Type</th>
        <th>Format Version</th>
        <th>State</th>
    </tr>
    <tr>

        <td>
          <% if (tableExists) { %>
            <a href="table.jsp?name=<%= snapshotTable.getNameAsString() %>">
              <%= snapshotTable.getNameAsString() %></a>
          <% } else { %>
            <%= snapshotTable.getNameAsString() %>
          <% } %>
        </td>
        <td><%= new Date(snapshot.getCreationTime()) %></td>
        <td><%= snapshot.getType() %></td>
        <td><%= snapshot.getVersion() %></td>
        <% if (stats.isSnapshotCorrupted()) { %>
          <td style="font-weight: bold; color: #dd0000;">CORRUPTED</td>
        <% } else { %>
          <td>ok</td>
        <% } %>
    </tr>
  </table>
  <div class="row">
    <div class="span12">
    <%= stats.getStoreFilesCount() %> HFiles (<%= stats.getArchivedStoreFilesCount() %> in archive),
    total size <%= StringUtils.humanReadableInt(stats.getStoreFilesSize()) %>
    (<%= stats.getSharedStoreFilePercentage() %>&#37;
    <%= StringUtils.humanReadableInt(stats.getSharedStoreFilesSize()) %> shared with the source
    table)
    </div>
    <div class="span12">
    <%= stats.getLogsCount() %> Logs, total size
    <%= StringUtils.humanReadableInt(stats.getLogsSize()) %>
    </div>
  </div>
  <% if (stats.isSnapshotCorrupted()) { %>
    <div class="row">
      <div class="span12">
          <h3>CORRUPTED Snapshot</h3>
      </div>
      <div class="span12">
        <%= stats.getMissingStoreFilesCount() %> hfile(s) and
        <%= stats.getMissingLogsCount() %> log(s) missing.
      </div>
    </div>
  <% } %>
<%
  } // end else
%>


<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
