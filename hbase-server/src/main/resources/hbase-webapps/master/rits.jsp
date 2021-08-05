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
         import="java.util.ArrayList"
         import="java.util.List"
         import="java.util.HashMap"
         import="java.util.HashSet"
         import="java.util.Map"
         import="java.util.Set"
         import="org.apache.hadoop.hbase.HBaseConfiguration"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.master.RegionState"
         import="org.apache.hadoop.hbase.util.GsonUtil"
         import="org.apache.hbase.thirdparty.com.google.gson.Gson"
%>
<%
    HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
    Set<RegionState> rit = master.getAssignmentManager().getRegionStates().getRegionsInTransition();
    String table = request.getParameter("table");
    String state = request.getParameter("state");
    if (table != null && state != null && !table.equals("null") && !state.equals("null")) {
        Set<RegionState> ritFiltered = new HashSet<>();
        for (RegionState regionState: rit) {
            if (regionState.getRegion().getTable().getNameAsString().equals(table) &&
              regionState.getState().name().equals(state)){
                ritFiltered.add(regionState);
            }
        }
        rit = ritFiltered;
    }

    String format = request.getParameter("format");
    if(format == null || format.isEmpty()){
        format = "html";
    }
    String filter = request.getParameter("filter");
%>


<% if (format.equals("html")) { %>
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
        <li><a href="/prof">Profiler</a></li>
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
            <h1>Regions in transition</h1>
        </div>
    </div>
    <div class="row">
        <div class="page-header">
            <a href="/rits.jsp?format=txt&filter=region&table=<%=table%>&state=<%=state%>" class="btn btn-primary">Regions in text format</a>
            <a href="/rits.jsp?format=json&table=<%=table%>&state=<%=state%>" class="btn btn-info">RIT info as JSON</a>
            <p>regions in text format can be copied and passed to command-line utils such as hbck2</p>
        </div>
    </div>

    <% if (rit != null && rit.size() > 0) { %>
        <table class="table table-striped">
            <tr>
                <th>Region</th>
                <th>Table</th>
                <th>RegionState</th>
                <th>Server</th>
                <th>Start Time</th>
                <th>Duration (ms)</th>
            </tr>
            <% for (RegionState regionState : rit) { %>
            <tr>
                <td><%= regionState.getRegion().getEncodedName() %></td>
                <td><%= regionState.getRegion().getTable() %></td>
                <td><%= regionState.getState() %></td>
                <td><%= regionState.getServerName() %></td>
                <td><%= regionState.getStamp() %></td>
                <td><%= regionState.getRitDuration() %></td>
            </tr>
            <% } %>
            <p><%= rit.size() %> region(s) in transition.</p>
        </table>
    <% } else { %>
        <p> no region in transition right now. </p>
    <% } %>
</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
<% } else if (format.equals("json")) { %>
    <%
        Gson GSON = GsonUtil.createGson().create();
        Map<String, List<Map<String, Object>>> map = new HashMap<>();
        List<Map<String, Object>> rits = new ArrayList<>();
        map.put("rits", rits);
        for (RegionState regionState : rit) {
            Map<String, Object> r = new HashMap<>();
            r.put("region", regionState.getRegion().getEncodedName());
            r.put("table", regionState.getRegion().getTable().getNameAsString());
            r.put("state", regionState.getState());
            r.put("server", regionState.getServerName());
            r.put("startTime", regionState.getStamp());
            r.put("duration", regionState.getRitDuration());
            rits.add(r);
        }
    %>
    <%= GSON.toJson(map) %>
<% } else { %>
<div class="container-fluid content">
  <div class="row">
    <p>
      <%
        if (filter.equals("region")) {
          for (RegionState regionState : rit) { %>
            <%= regionState.getRegion().getEncodedName() %><br>
      <%  }
      } else { %>
      "Not a valid filter"
      <% } %>
    </p>
  </div>
</div>
<% } %>
