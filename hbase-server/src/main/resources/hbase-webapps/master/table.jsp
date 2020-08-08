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
  import="com.google.protobuf.ByteString"
  import="java.net.URLEncoder"
  import="java.util.ArrayList"
  import="java.util.HashMap"
  import="java.util.TreeMap"
  import="java.util.List"
  import="java.util.LinkedHashMap"
  import="java.util.Map"
  import="java.util.Set"
  import="java.util.HashSet"
  import="java.util.Collections"
  import="java.util.Collection"
  import="org.apache.commons.lang.StringEscapeUtils"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.hbase.client.HTable"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.client.HConnectionManager"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.ServerName"
  import="org.apache.hadoop.hbase.ServerLoad"
  import="org.apache.hadoop.hbase.RegionLoad"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.io.ImmutableBytesWritable"
  import="org.apache.hadoop.hbase.master.HMaster" 
  import="org.apache.hadoop.hbase.zookeeper.MetaTableLocator"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.util.FSUtils"
  import="org.apache.hadoop.hbase.io.ImmutableBytesWritable"
  import="org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest"
  import="org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState"
  import="org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos"
  import="org.apache.hadoop.hbase.protobuf.generated.HBaseProtos"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.HColumnDescriptor"
  import="org.apache.hadoop.hbase.client.RegionReplicaUtil"
  import="org.apache.hadoop.hbase.HBaseConfiguration" %>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  MetaTableLocator metaTableLocator = new MetaTableLocator();
  String fqtn = request.getParameter("name");
  final String escaped_fqtn = StringEscapeUtils.escapeHtml(fqtn);
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
  String left = request.getParameter("left");
  String right = request.getParameter("right");
  long totalStoreFileSizeMB = 0;

  final String numRegionsParam = request.getParameter("numRegions");
  // By default, the page render up to 10000 regions to improve the page load time
  int numRegionsToRender = 10000;
  if (numRegionsParam != null) {
    // either 'all' or a number
    if (numRegionsParam.equals("all")) {
      numRegionsToRender = -1;
    } else {
      try {
        numRegionsToRender = Integer.parseInt(numRegionsParam);
      } catch (NumberFormatException ex) {
        // ignore
      }
    }
  }
  int numRegions = 0;

%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta charset="utf-8">
    <% if ( !readOnly && action != null ) { %>
        <title>HBase Master: <%= StringEscapeUtils.escapeHtml(master.getServerName().toString()) %></title>
    <% } else { %>
        <title>Table: <%= escaped_fqtn %></title>
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
<% 
if ( fqtn != null ) {
  table = new HTable(conf, fqtn);
  if (table.getTableDescriptor().getRegionReplication() > 1) {
    withReplica = true;
  }
  if ( !readOnly && action != null ) { 
%>
<div class="container-fluid content">
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
    } else if (action.equals("merge")) {
        if (left != null && left.length() > 0 && right != null && right.length() > 0) {
            admin.mergeRegions(Bytes.toBytesBinary(left), Bytes.toBytesBinary(right), false);
        }
        %> Merge request accepted. <%
    }
  }
%>
<p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
</div>
<%
  } else {
%>
<div class="container-fluid content">
    <div class="row inner_header">
        <div class="page-header">
            <h1>Table <small><%= escaped_fqtn %></small></h1>
        </div>
    </div>
    <div class="row">
<%
  if(fqtn.equals(TableName.META_TABLE_NAME.getNameAsString())) {
%>
<h2>Table Regions</h2>
<div class="tabbable">
  <ul class="nav nav-pills">
    <li class="active">
      <a href="#metaTab_baseStats" data-toggle="tab">Base Stats</a>
    </li>
    <li class="">
      <a href="#metaTab_compactStats" data-toggle="tab">Compactions</a>
    </li>
  </ul>
  <div class="tab-content" style="padding-bottom: 9px; border-bottom: 1px solid #ddd;">
    <div class="tab-pane active" id="metaTab_baseStats">
      <table id="tableRegionTable" class="tablesorter table table-striped">
        <thead>
          <tr>
            <th>Name</th>
            <th>Region Server</th>
            <th>ReadRequests</th>
            <th>WriteRequests</th>
            <th>StorefileSize</th>
            <th>Num.Storefiles</th>
            <th>MemSize</th>
            <th>Locality</th>
            <th>Start Key</th>
            <th>End Key</th>
            <%
              if (withReplica) {
            %>
            <th>ReplicaID</th>
            <%
              }
            %>
          </tr>
        </thead>
        <tbody>
        <%
          // NOTE: Presumes meta with one or more replicas
          for (int j = 0; j < numMetaReplicas; j++) {
            HRegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                                    HRegionInfo.FIRST_META_REGIONINFO, j);
            ServerName metaLocation = metaTableLocator.waitMetaRegionLocation(master.getZooKeeper(), j, 1);
            for (int i = 0; i < 1; i++) {
              String url = "";
              String readReq = "N/A";
              String writeReq = "N/A";
              String fileSize = "N/A";
              String fileCount = "N/A";
              String memSize = "N/A";
              float locality = 0.0f;

              if (metaLocation != null) {
                ServerLoad sl = master.getServerManager().getLoad(metaLocation);
                // The host name portion should be safe, but I don't know how we handle IDNs so err on the side of failing safely.
                url = "//" + URLEncoder.encode(metaLocation.getHostname()) + ":" + master.getRegionServerInfoPort(metaLocation) + "/";
                if (sl != null) {
                  Map<byte[], RegionLoad> map = sl.getRegionsLoad();
                  if (map.containsKey(meta.getRegionName())) {
                    RegionLoad load = map.get(meta.getRegionName());
                    readReq = String.format("%,1d", load.getReadRequestsCount());
                    writeReq = String.format("%,1d", load.getWriteRequestsCount());
                    fileSize = StringUtils.byteDesc(load.getStorefileSizeMB()*1024l*1024);
                    fileCount = String.format("%,1d", load.getStorefiles());
                    memSize = StringUtils.byteDesc(load.getMemStoreSizeMB()*1024l*1024);
                    locality = load.getDataLocality();
                  }
                }
              }
        %>
          <tr>
            <%
            String metaLocationString = metaLocation != null ?
                StringEscapeUtils.escapeHtml(metaLocation.getHostname().toString())
                  + ":" + master.getRegionServerInfoPort(metaLocation) :
                "(null)";
            %>
            <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
            <td><a href="<%= url %>"><%= metaLocationString %></a></td>
            <td><%= readReq%></td>
            <td><%= writeReq%></td>
            <td><%= fileSize%></td>
            <td><%= fileCount%></td>
            <td><%= memSize%></td>
            <td><%= locality%></td>
            <td><%= escapeXml(Bytes.toString(meta.getStartKey())) %></td>
            <td><%= escapeXml(Bytes.toString(meta.getEndKey())) %></td>
          <%
                if (withReplica) {
          %>
              <td><%= meta.getReplicaId() %></td>
          <%
                }
          %>
          </tr>
          <%  } %>
          <%} %>
        </tbody>
      </table>
    </div>
    <div class="tab-pane" id="metaTab_compactStats">
      <table id="metaTableCompactStatsTable" class="tablesorter table table-striped">
        <thead>
          <tr>
            <th>Name</th>
            <th>Region Server</th>
            <th>Num. Compacting KVs</th>
            <th>Num. Compacted KVs</th>
            <th>Remaining KVs</th>
            <th>Compaction Progress</th>
          </tr>
        </thead>
        <tbody>
        <%
          // NOTE: Presumes meta with one or more replicas
          for (int j = 0; j < numMetaReplicas; j++) {
            HRegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                                    HRegionInfo.FIRST_META_REGIONINFO, j);
            ServerName metaLocation = metaTableLocator.waitMetaRegionLocation(master.getZooKeeper(), j, 1);
            for (int i = 0; i < 1; i++) {
              String url = "";
              long compactingKVs = 0;
              long compactedKVs = 0;
              String compactionProgress = "";

              if (metaLocation != null) {
                ServerLoad sl = master.getServerManager().getLoad(metaLocation);
                // The host name portion should be safe, but I don't know how we handle IDNs so err on the side of failing safely.
                url = "//" + URLEncoder.encode(metaLocation.getHostname()) + ":" + master.getRegionServerInfoPort(metaLocation) + "/";
                if (sl != null) {
                  Map<byte[], RegionLoad> map = sl.getRegionsLoad();
                  if (map.containsKey(meta.getRegionName())) {
                    RegionLoad load = map.get(meta.getRegionName());
                    compactingKVs = load.getTotalCompactingKVs();
                    compactedKVs = load.getCurrentCompactedKVs();
                    if (compactingKVs > 0) {
                      compactionProgress = String.format("%.2f", 100 * ((float)
                        compactedKVs / compactingKVs)) + "%";
                    }
                  }
                }
              }
        %>
          <tr>
            <%
            String metaLocationString = metaLocation != null ?
                StringEscapeUtils.escapeHtml(metaLocation.getHostname().toString())
                  + ":" + master.getRegionServerInfoPort(metaLocation) :
                "(null)";
            %>
            <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
            <td><a href="<%= url %>"><%= metaLocationString %></a></td>
            <td><%= String.format("%,1d", compactingKVs)%></td>
            <td><%= String.format("%,1d", compactedKVs)%></td>
            <td><%= String.format("%,1d", compactingKVs - compactedKVs)%></td>
            <td><%= compactionProgress%></td>
          </tr>
        <%  } %>
        <%} %>
        </tbody>
      </table>
    </div>
  </div>
</div>
<%} else {
  Admin admin = master.getConnection().getAdmin();
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
    for(StackTraceElement element : e.getStackTrace()) {
      %><%= StringEscapeUtils.escapeHtml(element.toString()) %><%
    }
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
<%
  Collection<HColumnDescriptor> families = table.getTableDescriptor().getFamilies();
  Set<ImmutableBytesWritable> familyKeySet = new HashSet<>();
  for (HColumnDescriptor family: families) {
    familyKeySet.addAll(family.getValues().keySet());
  }
%>
  <tr>
    <th>Property \ Column Family Name</th>
    <%
    for (HColumnDescriptor family: families) {
    %>
    <th>
      <%= StringEscapeUtils.escapeHtml(family.getNameAsString()) %>
    </th>
    <% } %>
  </tr>
    <%
    for (ImmutableBytesWritable familyKey: familyKeySet) {
    %>
      <tr>
        <td>
          <%= StringEscapeUtils.escapeHtml(Bytes.toString(familyKey.get(), familyKey.getOffset(), familyKey.getLength())) %>
        </td>
        <%
        for (HColumnDescriptor family: families) {
          String familyValueStr = "-";
          Map<ImmutableBytesWritable, ImmutableBytesWritable> familyValues = family.getValues();
          if(familyValues.containsKey(familyKey)){
            final ImmutableBytesWritable familyValue = familyValues.get(familyKey);
            familyValueStr = Bytes.toString(familyValue.get(), familyValue.getOffset(), familyValue.getLength());
          }
        %>
        <td>
          <%= StringEscapeUtils.escapeHtml(familyValueStr) %>
        </td>
        <% } %>
      </tr>
    <% } %>
</table>
<%
  long totalReadReq = 0;
  long totalWriteReq = 0;
  long totalSize = 0;
  long totalStoreFileCount = 0;
  long totalMemSize = 0;
  long totalCompactingKVs = 0;
  long totalCompactedKVs = 0;
  String percentDone = "";
  String urlRegionServer = null;
  Map<ServerName, Integer> regDistribution = new TreeMap<ServerName, Integer>();
  Map<ServerName, Integer> primaryRegDistribution = new TreeMap<ServerName, Integer>();
  Map<HRegionInfo, RegionLoad> regionsToLoad = new LinkedHashMap<HRegionInfo, RegionLoad>();
  Map<HRegionInfo, ServerName> regions = table.getRegionLocations();
  if (regions == null) {
    regions = new HashMap<HRegionInfo, ServerName>();
  }
  for (Map.Entry<HRegionInfo, ServerName> hriEntry : regions.entrySet()) {
    HRegionInfo regionInfo = hriEntry.getKey();
    ServerName addr = hriEntry.getValue();

    if (addr != null) {
      ServerLoad sl = master.getServerManager().getLoad(addr);
      if (sl != null) {
        Map<byte[], RegionLoad> map = sl.getRegionsLoad();
        RegionLoad regionload = map.get(regionInfo.getRegionName());
        regionsToLoad.put(regionInfo, regionload);
        if(regionload != null) {
          totalReadReq += regionload.getReadRequestsCount();
          totalWriteReq += regionload.getWriteRequestsCount();
          totalSize += regionload.getStorefileSizeMB();
          totalStoreFileCount += regionload.getStorefiles();
          totalMemSize += regionload.getMemStoreSizeMB();
          totalStoreFileSizeMB += regionload.getStorefileSizeMB();
          totalCompactingKVs += regionload.getTotalCompactingKVs();
          totalCompactedKVs += regionload.getCurrentCompactedKVs();
        } else {
          RegionLoad load0 = new RegionLoad(ClusterStatusProtos.RegionLoad.newBuilder().setRegionSpecifier(HBaseProtos.RegionSpecifier.newBuilder().setValue(ByteString.copyFrom(regionInfo.getRegionName())).build()).build());
          regionsToLoad.put(regionInfo, load0);
        }
      }else{
        RegionLoad load0 = new RegionLoad(ClusterStatusProtos.RegionLoad.newBuilder().setRegionSpecifier(HBaseProtos.RegionSpecifier.newBuilder().setValue(ByteString.copyFrom(regionInfo.getRegionName())).build()).build());
        regionsToLoad.put(regionInfo, load0);
      }
    }else{
      RegionLoad load0 = new RegionLoad(ClusterStatusProtos.RegionLoad.newBuilder().setRegionSpecifier(HBaseProtos.RegionSpecifier.newBuilder().setValue(ByteString.copyFrom(regionInfo.getRegionName())).build()).build());
      regionsToLoad.put(regionInfo, load0);
    }
  }
  if  (totalCompactingKVs > 0) {
    percentDone = String.format("%.2f", 100 *
      ((float) totalCompactedKVs / totalCompactingKVs)) + "%";
  }

  if(regions != null && regions.size() > 0) { %>
<h2>Table Regions</h2>
<div class="tabbable">
  <ul class="nav nav-pills">
    <li class="active">
      <a href="#tab_baseStats" data-toggle="tab">Base Stats</a>
    </li>
    <li class="">
      <a href="#tab_compactStats" data-toggle="tab">Compactions</a>
    </li>
  </ul>
  <div class="tab-content" style="padding-bottom: 9px; border-bottom: 1px solid #ddd;">
    <div class="tab-pane active" id="tab_baseStats">
      <table id="regionServerDetailsTable" class="tablesorter table table-striped">
        <thead>
          <tr>
            <th>Name(<%= String.format("%,1d", regions.size())%>)</th>
            <th>Region Server</th>
            <th>ReadRequests<br>(<%= String.format("%,1d", totalReadReq)%>)</th>
            <th>WriteRequests<br>(<%= String.format("%,1d", totalWriteReq)%>)</th>
            <th>StorefileSize<br>(<%= StringUtils.byteDesc(totalSize*1024l*1024)%>)</th>
            <th>Num.Storefiles<br>(<%= String.format("%,1d", totalStoreFileCount)%>)</th>
            <th>MemSize<br>(<%= StringUtils.byteDesc(totalMemSize*1024l*1024)%>)</th>
            <th>Locality</th>
            <th>Start Key</th>
            <th>End Key</th>
            <%
              if (withReplica) {
            %>
            <th>ReplicaID</th>
            <%
              }
            %>
          </tr>
        </thead>
        <tbody>
        <%
          List<Map.Entry<HRegionInfo, RegionLoad>> entryList = new ArrayList<Map.Entry<HRegionInfo, RegionLoad>>(regionsToLoad.entrySet());
          numRegions = regions.size();
          int numRegionsRendered = 0;
          // render all regions
          if (numRegionsToRender < 0) {
            numRegionsToRender = numRegions;
          }
          for (Map.Entry<HRegionInfo, RegionLoad> hriEntry : entryList) {
            HRegionInfo regionInfo = hriEntry.getKey();
            ServerName addr = regions.get(regionInfo);
            RegionLoad load = hriEntry.getValue();
            String readReq = "N/A";
            String writeReq = "N/A";
            String regionSize = "N/A";
            String fileCount = "N/A";
            String memSize = "N/A";
            float locality = 0.0f;
            if(load != null) {
              readReq = String.format("%,1d", load.getReadRequestsCount());
              writeReq = String.format("%,1d", load.getWriteRequestsCount());
              regionSize = StringUtils.byteDesc(load.getStorefileSizeMB()*1024l*1024);
              fileCount = String.format("%,1d", load.getStorefiles());
              memSize = StringUtils.byteDesc(load.getMemStoreSizeMB()*1024l*1024);
              locality = load.getDataLocality();
            }

            if (addr != null) {
              ServerLoad sl = master.getServerManager().getLoad(addr);
              // This port might be wrong if RS actually ended up using something else.
              urlRegionServer =
                  "//" + URLEncoder.encode(addr.getHostname()) + ":" + master.getRegionServerInfoPort(addr) + "/";
              if(sl != null) {
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
            if (numRegionsRendered < numRegionsToRender) {
              numRegionsRendered++;
        %>
        <tr>
          <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getRegionName())) %></td>
          <%
          if (urlRegionServer != null) {
          %>
          <td>
             <a href="<%= urlRegionServer %>"><%= StringEscapeUtils.escapeHtml(addr.getHostname().toString()) + ":" + master.getRegionServerInfoPort(addr) %></a>
          </td>
          <%
          } else {
          %>
          <td class="undeployed-region">not deployed</td>
          <%
          }
          %>
          <td><%= readReq%></td>
          <td><%= writeReq%></td>
          <td><%= regionSize%></td>
          <td><%= fileCount%></td>
          <td><%= memSize%></td>
          <td><%= locality%></td>
          <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getStartKey()))%></td>
          <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getEndKey()))%></td>
          <%
          if (withReplica) {
          %>
          <td><%= regionInfo.getReplicaId() %></td>
          <%
          }
          %>
        </tr>
        <% } %>
        <% } %>
        </tbody>
      </table>
      <% if (numRegions > numRegionsRendered) {
           String allRegionsUrl = "?name=" + URLEncoder.encode(fqtn,"UTF-8") + "&numRegions=all";
      %>
        <p>This table has <b><%= numRegions %></b> regions in total, in order to improve the page load time,
           only <b><%= numRegionsRendered %></b> regions are displayed here, <a href="<%= allRegionsUrl %>">click
           here</a> to see all regions.</p>
      <% } %>
    </div>
    <div class="tab-pane" id="tab_compactStats">
      <table id="tableCompactStatsTable" class="tablesorter table table-striped">
        <thead>
          <tr>
            <th>Name(<%= String.format("%,1d", regions.size())%>)</th>
            <th>Region Server</th>
            <th>Num. Compacting KVs<br>(<%= String.format("%,1d", totalCompactingKVs)%>)</th>
            <th>Num. Compacted KVs<br>(<%= String.format("%,1d", totalCompactedKVs)%>)</th>
            <th>Remaining KVs<br>(<%= String.format("%,1d", totalCompactingKVs - totalCompactedKVs)%>)</th>
            <th>Compaction Progress<br>(<%= percentDone %>)</th>
          </tr>
        </thead>
        <tbody>
        <%
          numRegionsRendered = 0;
          for (Map.Entry<HRegionInfo, RegionLoad> hriEntry : entryList) {
            HRegionInfo regionInfo = hriEntry.getKey();
            ServerName addr = regions.get(regionInfo);
            RegionLoad load = hriEntry.getValue();
            long compactingKVs = 0;
            long compactedKVs = 0;
            String compactionProgress = "";
            if(load != null) {
              compactingKVs = load.getTotalCompactingKVs();
              compactedKVs = load.getCurrentCompactedKVs();
              if (compactingKVs > 0) {
                compactionProgress = String.format("%.2f", 100 * ((float)
                  compactedKVs / compactingKVs)) + "%";
              }
            }

            if (addr != null) {
              // This port might be wrong if RS actually ended up using something else.
              urlRegionServer =
                  "//" + URLEncoder.encode(addr.getHostname()) + ":" + master.getRegionServerInfoPort(addr) + "/";
            }
            if (numRegionsRendered < numRegionsToRender) {
              numRegionsRendered++;
        %>
        <tr>
          <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getRegionName())) %></td>
          <%
          if (urlRegionServer != null) {
          %>
          <td>
             <a href="<%= urlRegionServer %>"><%= StringEscapeUtils.escapeHtml(addr.getHostname().toString()) + ":" + master.getRegionServerInfoPort(addr) %></a>
          </td>
          <%
          } else {
          %>
          <td class="undeployed-region">not deployed</td>
          <%
          }
          %>
          <td><%= String.format("%,1d", compactingKVs)%></td>
          <td><%= String.format("%,1d", compactedKVs)%></td>
          <td><%= String.format("%,1d", compactingKVs - compactedKVs)%></td>
          <td><%= compactionProgress%></td>
        </tr>
        <% } %>
        <% } %>
        </tbody>
      </table>
      <% if (numRegions > numRegionsRendered) {
           String allRegionsUrl = "?name=" + URLEncoder.encode(fqtn,"UTF-8") + "&numRegions=all";
      %>
      <p>This table has <b><%= numRegions %></b> regions in total, in order to improve the page load time,
        only <b><%= numRegionsRendered %></b> regions are displayed here, <a href="<%= allRegionsUrl %>">click
        here</a> to see all regions.</p>
      <% } %>
    </div>
  </div>
</div>
<h2>Regions by Region Server</h2>
<%
if (withReplica) {
%>
<table id="regionServerTable" class="tablesorter table table-striped"><thead><tr><th>Region Server</th><th>Region Count</th><th>Primary Region Count</th></tr><thead>
<%
} else {
%>
<table id="regionServerTable" class="tablesorter table table-striped"><thead><tr><th>Region Server</th><th>Region Count</th></tr></thead>
<tbody>
<%
}
%>
<%
  for (Map.Entry<ServerName, Integer> rdEntry : regDistribution.entrySet()) {
     ServerName addr = rdEntry.getKey();
     String url = "//" + URLEncoder.encode(addr.getHostname()) + ":" + master.getRegionServerInfoPort(addr) + "/";
%>
<tr>
  <td><a href="<%= url %>"><%= StringEscapeUtils.escapeHtml(addr.getHostname().toString()) + ":" + master.getRegionServerInfoPort(addr) %></a></td>
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
</tbody>
</table>
<% }
} catch(Exception ex) {
  for(StackTraceElement element : ex.getStackTrace()) {
    %><%= StringEscapeUtils.escapeHtml(element.toString()) %><%
  }
} finally {
  admin.close();
}
} // end else
%>

<h2>Table Stats</h2>
<table class="table table-striped">
  <tr>
    <th>Name</th>
    <th>Value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Size</td>
    <td><%= StringUtils.TraditionalBinaryPrefix.long2String(totalStoreFileSizeMB * 1024 * 1024, "B", 2)%></td>
    <td>Total size of store files (in bytes)</td>
  </tr>
</table>

<% if (!readOnly) { %>
<p><hr/></p>
Actions:
<p>
<center>
<table class="table" width="95%" >
<tr>
  <form method="get">
  <input type="hidden" name="action" value="compact">
  <input type="hidden" name="name" value="<%= escaped_fqtn %>">
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
  <input type="hidden" name="name" value="<%= escaped_fqtn %>">
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
<tr>
  <form method="get">
  <input type="hidden" name="action" value="merge">
  <input type="hidden" name="name" value="<%= escaped_fqtn %>">
  <td style="border-style: none; text-align: center">
      <input style="font-size: 12pt; width: 10em" type="submit" value="Merge" class="btn"></td>
  <td style="border-style: none" width="5%">&nbsp;</td>
  <td style="border-style: none">Region Key (Required):<input type="text" name="left" size="40">
  Region Key (Required) :<input type="text" name="right" size="40"></td>
  <td style="border-style: none">This action will merge two
  regions of the table, Merge requests for noneligible regions will be ignored.</td>
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
<div class="container-fluid content">
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
<script src="/static/js/jquery.tablesorter.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>

<script>
$(document).ready(function() 
    { 
        $("#regionServerTable").tablesorter();
        $("#regionServerDetailsTable").tablesorter();
        $("#tableRegionTable").tablesorter();
        $("#tableCompactStatsTable").tablesorter();
        $("#metaTableCompactStatsTable").tablesorter();
    } 
);
</script>
