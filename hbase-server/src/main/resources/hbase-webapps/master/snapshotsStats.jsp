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
  import="java.util.concurrent.atomic.AtomicLong"
  import="java.util.Date"
  import="java.util.List"
  import="java.util.Map"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.fs.Path"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos.SnapshotDescription"
  import="org.apache.hadoop.hbase.snapshot.SnapshotInfo"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.util.StringUtils" %>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  AtomicLong totalSharedSize = new AtomicLong();
  AtomicLong totalArchivedSize = new AtomicLong();
  AtomicLong totalMobSize = new AtomicLong();
  long totalSize = 0;
  long totalUnsharedArchivedSize = 0;

  Map<Path, Integer> filesMap = null;

  List<SnapshotDescription> snapshots = master.isInitialized() ?
    master.getSnapshotManager().getCompletedSnapshots() : null;

  if (snapshots != null && snapshots.size() > 0) {
    filesMap = SnapshotInfo.getSnapshotsFilesMap(master.getConfiguration(),
                   totalArchivedSize, totalSharedSize, totalMobSize);
    totalSize = totalSharedSize.get() + totalArchivedSize.get() + totalMobSize.get();
  }
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta charset="utf-8">
    <title>HBase Master Snapshots: <%= master.getServerName() %></title>
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
            <a class="navbar-brand" href="/master-status"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
        </div>
        <div class="collapse navbar-collapse">
            <ul class="nav navbar-nav">
                <li><a href="/master-status">Home</a></li>
                <li><a href="/tablesDetailed.jsp">Table Details</a></li>
                <li><a href="/procedures.jsp">Procedures</a></li>
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
          <h1>Snapshot Storefile Stats</h1>
      </div>
  </div>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>Snapshot Name</th>
        <th>Table</th>
        <th>Creation Time</th>
        <th>Shared Storefile Size</th>
        <th>Mob Storefile Size</th>
        <th>Archived Storefile Size</th>
    </tr>
    <%for (SnapshotDescription snapshotDesc : snapshots) { %>
    <tr>
      <td><a href="/snapshot.jsp?name=<%= snapshotDesc.getName() %>">
        <%= snapshotDesc.getName() %></a></td>
      <%
        TableName snapshotTable = TableName.valueOf(snapshotDesc.getTable());
        SnapshotInfo.SnapshotStats stats = SnapshotInfo.getSnapshotStats(master.getConfiguration(),
          snapshotDesc, filesMap);
        totalUnsharedArchivedSize += stats.getNonSharedArchivedStoreFilesSize();
      %>
      <td><a href="/table.jsp?name=<%= snapshotTable.getNameAsString() %>">
        <%= snapshotTable.getNameAsString() %></a></td>
      <td><%= new Date(snapshotDesc.getCreationTime()) %></td>
      <td><%= StringUtils.humanReadableInt(stats.getSharedStoreFilesSize()) %></td>
      <td><%= StringUtils.humanReadableInt(stats.getMobStoreFilesSize())  %></td>
      <td><%= StringUtils.humanReadableInt(stats.getArchivedStoreFileSize()) %>
        (<%= StringUtils.humanReadableInt(stats.getNonSharedArchivedStoreFilesSize()) %>)</td>
    </tr>
    <% } %>
    <p><%= snapshots.size() %> snapshot(s) in set.</p>
    <p>Total Storefile Size: <%= StringUtils.humanReadableInt(totalSize) %></p>
    <p>Total Shared Storefile Size: <%= StringUtils.humanReadableInt(totalSharedSize.get()) %>,
       Total Mob Storefile Size: <%= StringUtils.humanReadableInt(totalMobSize.get()) %>,
       Total Archived Storefile Size: <%= StringUtils.humanReadableInt(totalArchivedSize.get()) %>
       (<%= StringUtils.humanReadableInt(totalUnsharedArchivedSize) %>)</p>
    <p>Shared Storefile Size is the Storefile size shared between snapshots and active tables.
       Mob Storefile Size is the Mob Storefile size shared between snapshots and active tables.
       Archived Storefile Size is the Storefile size in Archive.
       The format of Archived Storefile Size is NNN(MMM). NNN is the total Storefile
       size in Archive, MMM is the total Storefile size in Archive that is specific
       to the snapshot (not shared with other snapshots and tables)</p>
  </table>
</div>

<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
