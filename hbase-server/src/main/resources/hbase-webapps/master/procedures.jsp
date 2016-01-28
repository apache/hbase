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
  import="java.util.Date"
  import="java.util.List"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.ProcedureInfo"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv"
  import="org.apache.hadoop.hbase.procedure2.ProcedureExecutor"
%>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  ProcedureExecutor<MasterProcedureEnv> procExecutor = master.getMasterProcedureExecutor();

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

<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
