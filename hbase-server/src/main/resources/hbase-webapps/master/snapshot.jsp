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
  import="java.util.HashMap"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.client.HConnectionManager"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.ServerName"
  import="org.apache.hadoop.hbase.ServerLoad"
  import="org.apache.hadoop.hbase.RegionLoad"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.snapshot.SnapshotInfo"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.util.FSUtils"
  import="org.apache.hadoop.hbase.protobuf.ProtobufUtil"
  import="org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription"
  import="org.apache.hadoop.util.StringUtils"
  import="java.util.List"
  import="java.util.Map"
  import="org.apache.hadoop.hbase.HConstants"%>
<%@ page import="org.apache.hadoop.hbase.TableName" %><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  HBaseAdmin hbadmin = new HBaseAdmin(conf);
  boolean readOnly = conf.getBoolean("hbase.master.ui.readonly", false);
  String snapshotName = request.getParameter("name");
  SnapshotDescription snapshot = null;
  SnapshotInfo.SnapshotStats stats = null;
  TableName snapshotTable = null;
  for (SnapshotDescription snapshotDesc: hbadmin.listSnapshots()) {
    if (snapshotName.equals(snapshotDesc.getName())) {
      snapshot = snapshotDesc;
      stats = SnapshotInfo.getSnapshotStats(conf, snapshot);
      snapshotTable = TableName.valueOf(snapshot.getTable());
      break;
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
      <meta http-equiv="refresh" content="5,javascript:history.back()" />
    <% } %>
  </head>
<body>
<div class="navbar  navbar-fixed-top navbar-default">
    <div class="container">
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
              <li><a href="/">Home</a></li>
              <li><a href="/tablesDetailed.jsp">Table Details</a></li>
              <li><a href="/logs/">Local logs</a></li>
              <li><a href="/logLevel">Log Level</a></li>
              <li><a href="/dump">Debug dump</a></li>
              <li><a href="/jmx">Metrics Dump</a></li>
          </ul>
        </div><!--/.nav-collapse -->
      </div>
    </div>
  </div>
<% if (isActionResultPage) { %>
  <div class="container">
    <div class="row inner_header">
        <div class="span8">
          <h1>Snapshot action request...</h1>
        </div>
        <div class="span4 logo">
            <img src="/static/hbase_logo.png" height="66" width="266" alt="HBase logo"/>
        </div>
    </div>
    <p><hr><p>
<%
  if (action.equals("restore")) {
    hbadmin.restoreSnapshot(snapshotName);
    %> Restore Snapshot request accepted. <%
  } else if (action.equals("clone")) {
    if (cloneName != null && cloneName.length() > 0) {
      hbadmin.cloneSnapshot(snapshotName, cloneName);
      %> Clone from Snapshot request accepted. <%
    } else {
      %> Clone from Snapshot request failed, No table name specified. <%
    }
  }
%>
<p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
</div>
<% } else if (snapshot == null) { %>
  <div class="container">
  <div class="row inner_header">
    <div class="span8">
      <h1>Snapshot "<%= snapshotName %>" does not exists</h1>
    </div>
    <div class="span4 logo">
        <img src="/static/hbase_logo.png" height="66" width="266" alt="HBase logo"/>
    </div>
  </div>
  <p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
<% } else { %>
  <div class="container">
  <div class="row">
      <div class="span8">
          <h1>Snapshot: <%= snapshotName %></h1>
      </div>
      <div class="span4">
          <img src="/static/hbase_logo.png" height="66" width="266" alt="HBase logo"/>
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
        <td><a href="table.jsp?name=<%= snapshotTable.getNameAsString() %>">
            <%= snapshotTable.getNameAsString() %></a></td>
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

HConnectionManager.deleteConnection(hbadmin.getConfiguration());
%>


<% if (!readOnly && action == null && snapshot != null) { %>
<p><hr><p>
Actions:
<p>
<center>
<table class="table table-striped" width="90%" >
<tr>
  <form method="get">
  <input type="hidden" name="action" value="clone">
  <input type="hidden" name="name" value="<%= snapshotName %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Clone" class="btn"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">New Table Name (clone):<input type="text" name="cloneName" size="40"></td>
  <td style="border-style: none">
    This action will create a new table by cloning the snapshot content.
    There are no copies of data involved.
    And writing on the newly created table will not influence the snapshot data.
  </td>
  </form>
</tr>
<tr><td style="border-style: none" colspan="4">&nbsp;</td></tr>
<tr>
  <form method="get">
  <input type="hidden" name="action" value="restore">
  <input type="hidden" name="name" value="<%= snapshotName %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Restore" class="btn"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">&nbsp;</td>
  <td style="border-style: none">Restore a specified snapshot.
  The restore will replace the content of the original table,
  bringing back the content to the snapshot state.
  The table must be disabled.</td>
  </form>
</tr>
</table>
</center>
<p>
</div>
<% } %>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
