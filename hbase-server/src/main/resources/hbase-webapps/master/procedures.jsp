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
  import="static org.apache.commons.lang.StringEscapeUtils.escapeXml"
  import="java.util.Collections"
  import="java.util.Comparator"
  import="java.util.ArrayList"
  import="java.util.Date"
  import="java.util.List"
  import="java.util.Set"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.ProcedureInfo"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv"
  import="org.apache.hadoop.hbase.procedure2.ProcedureExecutor"
  import="org.apache.hadoop.hbase.procedure2.store.wal.ProcedureWALFile"
  import="org.apache.hadoop.hbase.procedure2.store.wal.WALProcedureStore"
  import="org.apache.hadoop.hbase.procedure2.util.StringUtils"

%>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  ProcedureExecutor<MasterProcedureEnv> procExecutor = master.getMasterProcedureExecutor();
  WALProcedureStore walStore = master.getWalProcedureStore();

  ArrayList<WALProcedureStore.SyncMetrics> syncMetricsBuff = walStore.getSyncMetrics();
  long millisToNextRoll = walStore.getMillisToNextPeriodicRoll();
  long millisFromLastRoll = walStore.getMillisFromLastRoll();
  ArrayList<ProcedureWALFile> procedureWALFiles = walStore.getActiveLogs();
  Set<ProcedureWALFile> corruptedWALFiles = walStore.getCorruptedLogs();
  List<ProcedureInfo> procedures = procExecutor.listProcedures();
  Collections.sort(procedures, new Comparator<ProcedureInfo>() {
    @Override
    public int compare(ProcedureInfo lhs, ProcedureInfo rhs) {
      long cmp = lhs.getParentId() - rhs.getParentId();
      cmp = cmp != 0 ? cmp : lhs.getProcId() - rhs.getProcId();
      return cmp < 0 ? -1 : cmp > 0 ? 1 : 0;
    }
  });
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta charset="utf-8">
    <title>HBase Master Procedures: <%= master.getServerName() %></title>
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
          <h1>Procedures</h1>
      </div>
  </div>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>Id</th>
        <th>Parent</th>
        <th>State</th>
        <th>Owner</th>
        <th>Type</th>
        <th>Start Time</th>
        <th>Last Update</th>
        <th>Errors</th>
    </tr>
    <tr>
      <% for (ProcedureInfo procInfo : procedures) { %>
      <tr>
        <td><%= procInfo.getProcId() %></a></td>
        <td><%= procInfo.hasParentId() ? procInfo.getParentId() : "" %></a></td>
        <td><%= escapeXml(procInfo.getProcState().toString()) %></a></td>
        <td><%= escapeXml(procInfo.getProcOwner()) %></a></td>
        <td><%= escapeXml(procInfo.getProcName()) %></a></td>
        <td><%= new Date(procInfo.getStartTime()) %></a></td>
        <td><%= new Date(procInfo.getLastUpdate()) %></a></td>
        <td><%= escapeXml(procInfo.isFailed() ? procInfo.getExceptionMessage() : "") %></a></td>
      </tr>
    <% } %>
  </table>
</div>
<br>
<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
      <h2>Procedure WAL State</h2>
    </div>
  </div>
<div class="tabbable">
  <ul class="nav nav-pills">
    <li class="active">
      <a href="#tab_WALFiles" data-toggle="tab">WAL files</a>
    </li>
    <li class="">
      <a href="#tab_WALFilesCorrupted" data-toggle="tab">Corrupted WAL files</a>
     </li>
    <li class="">
      <a href="#tab_WALRollTime" data-toggle="tab">WAL roll time</a>
     </li>
     <li class="">
       <a href="#tab_SyncStats" data-toggle="tab">Sync stats</a>
     </li>
  </ul>
    <div class="tab-content" style="padding-bottom: 9px; border-bottom: 1px solid #ddd;">
      <div class="tab-pane active" id="tab_WALFiles">
        <% if (procedureWALFiles != null && procedureWALFiles.size() > 0) { %>
          <table class="table table-striped">
            <tr>
              <th>LogID</th>
              <th>Size</th>
              <th>Timestamp</th>
              <th>Path</th>
            </tr>
            <% for (int i = procedureWALFiles.size() - 1; i >= 0; --i) { %>
            <%    ProcedureWALFile pwf = procedureWALFiles.get(i); %>
            <tr>
              <td> <%= pwf.getLogId() %></td>
              <td> <%= StringUtils.humanSize(pwf.getSize()) %> </td>
              <td> <%= new Date(pwf.getTimestamp()) %></a></td>
              <td> <%= escapeXml(pwf.toString()) %></t>
            </tr>
            <% } %>
          </table>
        <% } else {%>
          <p> No WAL files</p>
        <% } %>
      </div>
      <div class="tab-pane" id="tab_WALFilesCorrupted">
      <% if (corruptedWALFiles != null && corruptedWALFiles.size() > 0) { %>
        <table class="table table-striped">
          <tr>
            <th>LogID</th>
            <th>Size</th>
            <th>Timestamp</th>
            <th>Path</th>
          </tr>
          <% for (ProcedureWALFile cwf:corruptedWALFiles) { %>
          <tr>
            <td> <%= cwf.getLogId() %></td>
            <td> <%= StringUtils.humanSize(cwf.getSize()) %> </td>
            <td> <%= new Date(cwf.getTimestamp()) %></a></td>
            <td> <%= escapeXml(cwf.toString()) %></t>
          </tr>
          <% } %>
          </table>
      <% } else {%>
        <p> No corrupted WAL files</p>
      <% } %>
      </div>
      <div class="tab-pane" id="tab_WALRollTime">
        <table class="table table-striped">
          <tr>
            <th> Milliseconds to next roll</th>
            <th> Milliseconds from last roll</th>
          </tr>
          <tr>
            <td> <%=StringUtils.humanTimeDiff(millisToNextRoll)  %></td>
            <td> <%=StringUtils.humanTimeDiff(millisFromLastRoll) %></td>
          </tr>
        </table>
      </div>
      <div class="tab-pane" id="tab_SyncStats">
        <table class="table table-striped">
          <tr>
            <th> Time</th>
            <th> Sync Wait</th>
            <th> Last num of synced entries</th>
            <th> Total Synced</th>
            <th> Synced per second</th>
          </tr>
          <% for (int i = syncMetricsBuff.size() - 1; i >= 0; --i) { %>
          <%    WALProcedureStore.SyncMetrics syncMetrics = syncMetricsBuff.get(i); %>
          <tr>
            <td> <%= new Date(syncMetrics.getTimestamp()) %></a></td>
            <td> <%= StringUtils.humanTimeDiff(syncMetrics.getSyncWaitMs()) %></td>
            <td> <%= syncMetrics.getSyncedEntries() %></td>
            <td> <%= StringUtils.humanSize(syncMetrics.getTotalSyncedBytes()) %></td>
            <td> <%= StringUtils.humanSize(syncMetrics.getSyncedPerSec()) %></td>
          </tr>
          <%} %>
        </table>
        </div>
      </div>
  </div>
</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
