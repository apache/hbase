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
  import="java.util.TreeMap"
  import="java.util.List"
  import="java.util.Map"
  import="java.util.Set"
  import="java.util.Collection"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.HTable"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.client.RegionLocator"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.HRegionLocation"
  import="org.apache.hadoop.hbase.ServerName"
  import="org.apache.hadoop.hbase.ServerLoad"
  import="org.apache.hadoop.hbase.RegionLoad"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.zookeeper.MetaTableLocator"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.util.FSUtils"
  import="org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.HColumnDescriptor"
  import="org.apache.hadoop.hbase.client.RegionReplicaUtil"
  import="org.apache.hadoop.hbase.HBaseConfiguration" %>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();

  MetaTableLocator metaTableLocator = new MetaTableLocator();
  String fqtn = request.getParameter("name");
  HTable table = null;
  String tableHeader;
  boolean withReplica = false;
  ServerName rl = metaTableLocator.getMetaRegionLocation(master.getZooKeeper());
  boolean showFragmentation = conf.getBoolean("hbase.master.ui.fragmentation.enabled", false);
  boolean readOnly = conf.getBoolean("hbase.master.ui.readonly", false);
  int numMetaReplicas = conf.getInt(HConstants.META_REPLICAS_NUM,
                        HConstants.DEFAULT_META_REPLICA_NUM);
  Map<String, Integer> frags = null;
  if (showFragmentation) {
      frags = FSUtils.getTableFragmentation(master);
  }
  String action = request.getParameter("action");
  String key = request.getParameter("key");
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta charset="utf-8">
    <% if ( !readOnly && action != null ) { %>
        <title>HBase Master: <%= master.getServerName() %></title>
    <% } else { %>
        <title>Table: <%= fqtn %></title>
    <% } %>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">


      <link href="/static/css/bootstrap.min.css" rel="stylesheet">
      <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
      <link href="/static/css/hbase.css" rel="stylesheet">
      <% if ( ( !readOnly && action != null ) || fqtn == null ) { %>
	  <script type="text/javascript">
      <!--
		  setTimeout("history.back()",5000);
	  -->
	  </script>
      <% } else { %>
      <!--[if lt IE 9]>
          <script src="/static/js/html5shiv.js"></script>
      <![endif]-->
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
<%
if ( fqtn != null ) {
  table = (HTable) master.getConnection().getTable(fqtn);
  if (table.getTableDescriptor().getRegionReplication() > 1) {
    tableHeader = "<h2>Table Regions</h2><table class=\"table table-striped\"><tr><th>Name</th><th>Region Server</th><th>Start Key</th><th>End Key</th><th>Locality</th><th>Requests</th><th>ReplicaID</th></tr>";
    withReplica = true;
  } else {
    tableHeader = "<h2>Table Regions</h2><table class=\"table table-striped\"><tr><th>Name</th><th>Region Server</th><th>Start Key</th><th>End Key</th><th>Locality</th><th>Requests</th></tr>";
  }
  if ( !readOnly && action != null ) {
%>
<div class="container">


        <div class="row inner_header">
            <div class="page-header">
                <h1>Table action request accepted</h1>
            </div>
        </div>
<p><hr><p>
<%
  try (Admin admin = master.getConnection().getAdmin()) {
    if (action.equals("split")) {
      if (key != null && key.length() > 0) {
        admin.splitRegion(Bytes.toBytes(key));
      } else {
        admin.split(TableName.valueOf(fqtn));
      }

    %> Split request accepted. <%
    } else if (action.equals("compact")) {
      if (key != null && key.length() > 0) {
        admin.compactRegion(Bytes.toBytes(key));
      } else {
        admin.compact(TableName.valueOf(fqtn));
      }
    %> Compact request accepted. <%
    }
  }
%>
<p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
</div>
<%
  } else {
%>
<div class="container">



    <div class="row inner_header">
        <div class="page-header">
            <h1>Table <small><%= fqtn %></small></h1>
        </div>
    </div>
    <div class="row">
<%
  if(fqtn.equals(TableName.META_TABLE_NAME.getNameAsString())) {
%>
<%= tableHeader %>
<%
  // NOTE: Presumes meta with one or more replicas
  for (int j = 0; j < numMetaReplicas; j++) {
    HRegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                            HRegionInfo.FIRST_META_REGIONINFO, j);
    ServerName metaLocation = metaTableLocator.waitMetaRegionLocation(master.getZooKeeper(), j, 1);
    for (int i = 0; i < 1; i++) {
      String url = "//" + metaLocation.getHostname() + ":" +
                   master.getRegionServerInfoPort(metaLocation) + "/";
%>
<tr>
  <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
    <td><a href="<%= url %>"><%= metaLocation.getHostname().toString() + ":" + master.getRegionServerInfoPort(metaLocation) %></a></td>
    <td><%= escapeXml(Bytes.toString(meta.getStartKey())) %></td>
    <td><%= escapeXml(Bytes.toString(meta.getEndKey())) %></td>
    <td>-</td>
    <td>-</td>
</tr>
<%  } %>
<%} %>
</table>
<%} else {
  Admin admin = master.getConnection().getAdmin();
  RegionLocator r = master.getConnection().getRegionLocator(table.getName());
  try { %>
<h2>Table Attributes</h2>
<table class="table table-striped">
  <tr>
      <th>Attribute Name</th>
      <th>Value</th>
      <th>Description</th>
  </tr>
  <tr>
      <td>Enabled</td>
      <td><%= admin.isTableEnabled(table.getName()) %></td>
      <td>Is the table enabled</td>
  </tr>
  <tr>
      <td>Compaction</td>
      <td>
<%
  try {
    CompactionState compactionState = admin.getCompactionState(table.getName());
%>
<%= compactionState %>
<%
  } catch (Exception e) {
  // Nothing really to do here
    e.printStackTrace();
%> Unknown <%
  }
%>
      </td>
      <td>Is the table compacting</td>
  </tr>
<%  if (showFragmentation) { %>
  <tr>
      <td>Fragmentation</td>
      <td><%= frags.get(fqtn) != null ? frags.get(fqtn).intValue() + "%" : "n/a" %></td>
      <td>How fragmented is the table. After a major compaction it is 0%.</td>
  </tr>
<%  } %>
</table>
<h2>Table Schema</h2>
<table class="table table-striped">
  <tr>
      <th>Column Name</th>
      <th></th>
  </tr>
  <%
    Collection<HColumnDescriptor> families = table.getTableDescriptor().getFamilies();
    for (HColumnDescriptor family: families) {
  %>
  <tr>
    <td><%= family.getNameAsString() %></td>
    <td>
    <table class="table table-striped">
      <tr>
       <th>Property</th>
       <th>Value</th>
      </tr>
    <%
    Map<Bytes, Bytes> familyValues = family.getValues();
    for (Bytes familyKey: familyValues.keySet()) {
    %>
      <tr>
        <td>
          <%= familyKey %>
		</td>
        <td>
          <%= familyValues.get(familyKey) %>
        </td>
      </tr>
    <% } %>
    </table>
    </td>
  </tr>
  <% } %>
</table>
<%
  Map<ServerName, Integer> regDistribution = new TreeMap<ServerName, Integer>();
  Map<ServerName, Integer> primaryRegDistribution = new TreeMap<ServerName, Integer>();
  List<HRegionLocation> regions = r.getAllRegionLocations();
  if(regions != null && regions.size() > 0) { %>
<%=     tableHeader %>
<%
  for (HRegionLocation hriEntry : regions) {
    HRegionInfo regionInfo = hriEntry.getRegionInfo();
    ServerName addr = hriEntry.getServerName();
    long req = 0;
    float locality = 0.0f;
    String urlRegionServer = null;

    if (addr != null) {
      ServerLoad sl = master.getServerManager().getLoad(addr);
      if (sl != null) {
        Map<byte[], RegionLoad> map = sl.getRegionsLoad();
        if (map.containsKey(regionInfo.getRegionName())) {
          req = map.get(regionInfo.getRegionName()).getRequestsCount();
          locality = map.get(regionInfo.getRegionName()).getDataLocality();
        }
        Integer i = regDistribution.get(addr);
        if (null == i) i = Integer.valueOf(0);
        regDistribution.put(addr, i + 1);
        if (withReplica && RegionReplicaUtil.isDefaultReplica(regionInfo.getReplicaId())) {
          i = primaryRegDistribution.get(addr);
          if (null == i) i = Integer.valueOf(0);
          primaryRegDistribution.put(addr, i+1);
        }
      }
    }
%>
<tr>
  <td><%= escapeXml(Bytes.toStringBinary(HRegionInfo.getRegionNameForDisplay(regionInfo,
                    conf))) %></td>
  <%
  if (addr != null) {
    String url = "//" + addr.getHostname() + ":" + master.getRegionServerInfoPort(addr) + "/";
  %>
  <td>
     <a href="<%= url %>"><%= addr.getHostname().toString() + ":" + addr.getPort() %></a>
  </td>
  <%
  } else {
  %>
  <td class="undeployed-region">not deployed</td>
  <%
  }
  %>
  <td><%= escapeXml(Bytes.toStringBinary(HRegionInfo.getStartKeyForDisplay(regionInfo,
                    conf))) %></td>
  <td><%= escapeXml(Bytes.toStringBinary(HRegionInfo.getEndKeyForDisplay(regionInfo,
                    conf))) %></td>
  <td><%= locality%></td>
  <td><%= req%></td>
  <%
  if (withReplica) {
  %>
  <td><%= regionInfo.getReplicaId() %></td>
  <%
  }
  %>
</tr>
<% } %>
</table>
<h2>Regions by Region Server</h2>
<%
if (withReplica) {
%>
<table class="table table-striped"><tr><th>Region Server</th><th>Region Count</th><th>Primary Region Count</th></tr>
<%
} else {
%>
<table class="table table-striped"><tr><th>Region Server</th><th>Region Count</th></tr>
<%
}
%>
<%
  for (Map.Entry<ServerName, Integer> rdEntry : regDistribution.entrySet()) {
     ServerName addr = rdEntry.getKey();
     String url = "//" + addr.getHostname() + ":" + master.getRegionServerInfoPort(addr) + "/";
%>
<tr>
  <td><a href="<%= url %>"><%= addr.getHostname().toString() + ":" + addr.getPort() %></a></td>
  <td><%= rdEntry.getValue()%></td>
<%
if (withReplica) {
%>
  <td><%= primaryRegDistribution.get(addr)%></td>
<%
}
%>
</tr>
<% } %>
</table>
<% }
} catch(Exception ex) {
  ex.printStackTrace(System.err);
} finally {
  admin.close();
}
} // end else
%>


<% if (!readOnly) { %>
<p><hr/></p>
Actions:
<p>
<center>
<table class="table" width="95%" >
<tr>
  <form method="get">
  <input type="hidden" name="action" value="compact">
  <input type="hidden" name="name" value="<%= fqtn %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Compact" class="btn"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">Region Key (optional):<input type="text" name="key" size="40"></td>
  <td style="border-style: none">This action will force a compaction of all
  regions of the table, or, if a key is supplied, only the region containing the
  given key.</td>
  </form>
</tr>
<tr><td style="border-style: none" colspan="4">&nbsp;</td></tr>
<tr>
  <form method="get">
  <input type="hidden" name="action" value="split">
  <input type="hidden" name="name" value="<%= fqtn %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Split" class="btn"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">Region Key (optional):<input type="text" name="key" size="40"></td>
  <td style="border-style: none">This action will force a split of all eligible
  regions of the table, or, if a key is supplied, only the region containing the
  given key. An eligible region is one that does not contain any references to
  other regions. Split requests for noneligible regions will be ignored.</td>
  </form>
</tr>
</table>
</center>
</p>
<% } %>
</div>
</div>
<% }
} else { // handle the case for fqtn is null with error message + redirect
%>
<div class="container">
    <div class="row inner_header">
        <div class="page-header">
            <h1>Table not ready</h1>
        </div>
    </div>
<p><hr><p>
<p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
</div>
<% } %>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
