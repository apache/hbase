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
  import="java.util.ArrayList"
  import="java.util.TreeMap"
  import="java.util.List"
  import="java.util.LinkedHashMap"
  import="java.util.Map"
  import="java.util.Set"
  import="java.util.Collection"
  import="java.util.Collections"
  import="java.util.Comparator"
  import="org.owasp.esapi.ESAPI"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.util.StringUtils"
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
  import="org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos"
  import="org.apache.hadoop.hbase.protobuf.generated.HBaseProtos"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.HColumnDescriptor"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.TableNotFoundException"%>
<%@ page import="org.apache.hadoop.hbase.client.*" %>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();

  MetaTableLocator metaTableLocator = new MetaTableLocator();
  String fqtn = request.getParameter("name");
  String sortKey = request.getParameter("sort");
  String reverse = request.getParameter("reverse");
  final boolean reverseOrder = (reverse==null||!reverse.equals("false"));
  String showWholeKey = request.getParameter("showwhole");
  final boolean showWhole = (showWholeKey!=null && showWholeKey.equals("true"));
  Table table;
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
        <title>Table: <%= ESAPI.encoder().encodeForHTML(fqtn) %></title>
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
                <% if (HBaseConfiguration.isShowConfInServlet()) { %>
                <li><a href="/conf">HBase Configuration</a></li>
                <% } %>
            </ul>
        </div><!--/.nav-collapse -->
    </div>
</div>
<%
if ( fqtn != null ) {
  try {
  table = master.getConnection().getTable(TableName.valueOf(fqtn));
  if (table.getTableDescriptor().getRegionReplication() > 1) {
    tableHeader = "<h2>Table Regions</h2><table class=\"table table-striped\" style=\"table-layout: fixed; word-wrap: break-word;\"><tr><th>Name</th><th>Region Server</th><th>ReadRequests</th><th>WriteRequests</th><th>StorefileSize</th><th>Num.Storefiles</th><th>MemSize</th><th>Locality</th><th>Start Key</th><th>End Key</th><th>ReplicaID</th></tr>";
    withReplica = true;
  } else {
    tableHeader = "<h2>Table Regions</h2><table class=\"table table-striped\" style=\"table-layout: fixed; word-wrap: break-word;\"><tr><th>Name</th><th>Region Server</th><th>ReadRequests</th><th>WriteRequests</th><th>StorefileSize</th><th>Num.Storefiles</th><th>MemSize</th><th>Locality</th><th>Start Key</th><th>End Key</th></tr>";
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
            <h1>Table <small><%= ESAPI.encoder().encodeForHTML(fqtn) %></small></h1>
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
      String url = "";
      String readReq = "N/A";
      String writeReq = "N/A";
      String fileSize = "N/A";
      String fileCount = "N/A";
      String memSize = "N/A";
      float locality = 0.0f;

      if (metaLocation != null) {
        ServerLoad sl = master.getServerManager().getLoad(metaLocation);
        url = "//" + metaLocation.getHostname() + ":" + master.getRegionServerInfoPort(metaLocation) + "/";
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
  <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
    <td><a href="<%= url %>"><%= metaLocation.getHostname().toString() + ":" + master.getRegionServerInfoPort(metaLocation) %></a></td>
    <td><%= readReq%></td>
    <td><%= writeReq%></td>
    <td><%= fileSize%></td>
    <td><%= fileCount%></td>
    <td><%= memSize%></td>
    <td><%= locality%></td>
    <td><%= escapeXml(Bytes.toString(meta.getStartKey())) %></td>
    <td><%= escapeXml(Bytes.toString(meta.getEndKey())) %></td>
</tr>
<%  } %>
<%} %>
</table>
<%} else {
  Admin admin = master.getConnection().getAdmin();
  RegionLocator r = master.getClusterConnection().getRegionLocator(table.getName());
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
  long totalReadReq = 0;
  long totalWriteReq = 0;
  long totalSize = 0;
  long totalStoreFileCount = 0;
  long totalMemSize = 0;
  String urlRegionServer = null;
  Map<ServerName, Integer> regDistribution = new TreeMap<ServerName, Integer>();
  Map<ServerName, Integer> primaryRegDistribution = new TreeMap<ServerName, Integer>();
  List<HRegionLocation> regions = r.getAllRegionLocations();
  Map<HRegionInfo, RegionLoad> regionsToLoad = new LinkedHashMap<HRegionInfo, RegionLoad>();
  Map<HRegionInfo, ServerName> regionsToServer = new LinkedHashMap<HRegionInfo, ServerName>();
  for (HRegionLocation hriEntry : regions) {
    HRegionInfo regionInfo = hriEntry.getRegionInfo();
    ServerName addr = hriEntry.getServerName();
    regionsToServer.put(regionInfo, addr);

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

  if(regions != null && regions.size() > 0) { %>
<h2>Table Regions</h2>
Sort As
<select id="sel" style="margin-right: 10px">
<option value="regionName">RegionName</option>
<option value="readrequest">ReadRequest</option>
<option value="writerequest">WriteRequest</option>
<option value="size">StorefileSize</option>
<option value="filecount">Num.Storefiles</option>
<option value="memstore">MemstoreSize</option>
<option value="locality">Locality</option>
</select>
Ascending<input type="checkbox" id="ascending" value="Ascending" style="margin-right:10px">
ShowDetailName&Start/End Key<input type="checkbox" id="showWhole" style="margin-right:10px">
<input type="button" id="submit" value="Reorder" onClick="reloadAsSort()" style="font-size: 12pt; width: 5em; margin-bottom: 5px" class="btn">
<p>

<table class="table table-striped">
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

<%
  List<Map.Entry<HRegionInfo, RegionLoad>> entryList = new ArrayList<Map.Entry<HRegionInfo, RegionLoad>>(regionsToLoad.entrySet());
  if(sortKey != null) {
    if (sortKey.equals("readrequest")) {
      Collections.sort(entryList,
          new Comparator<Map.Entry<HRegionInfo, RegionLoad>>() {
            public int compare(
                Map.Entry<HRegionInfo, RegionLoad> entry1,
                Map.Entry<HRegionInfo, RegionLoad> entry2) {
              if (entry1 == null || entry1.getValue() == null) {
                return -1;
              } else if (entry2 == null || entry2.getValue() == null) {
                return 1;
              }
              int result = 0;
              if (entry1.getValue().getReadRequestsCount() < entry2.getValue().getReadRequestsCount()) {
                result = -1;
              } else if (entry1.getValue().getReadRequestsCount() > entry2.getValue().getReadRequestsCount()) {
                result = 1;
              }
              if (reverseOrder) {
                result = -1 * result;
              }
              return result;
            }
          });
    } else if (sortKey.equals("writerequest")) {
      Collections.sort(entryList,
          new Comparator<Map.Entry<HRegionInfo, RegionLoad>>() {
            public int compare(
                Map.Entry<HRegionInfo, RegionLoad> entry1,
                Map.Entry<HRegionInfo, RegionLoad> entry2) {
              if (entry1 == null || entry1.getValue() == null) {
                return -1;
              } else if (entry2 == null || entry2.getValue() == null) {
                return 1;
              }
              int result = 0;
              if (entry1.getValue().getWriteRequestsCount() < entry2.getValue()
                  .getWriteRequestsCount()) {
                result = -1;
              } else if (entry1.getValue().getWriteRequestsCount() > entry2.getValue()
                  .getWriteRequestsCount()) {
                result = 1;
              }
              if (reverseOrder) {
                result = -1 * result;
              }
              return result;
            }
          });
    } else if (sortKey.equals("size")) {
      Collections.sort(entryList,
          new Comparator<Map.Entry<HRegionInfo, RegionLoad>>() {
            public int compare(
                Map.Entry<HRegionInfo, RegionLoad> entry1,
                Map.Entry<HRegionInfo, RegionLoad> entry2) {
              if (entry1 == null || entry1.getValue() == null) {
                return -1;
              } else if (entry2 == null || entry2.getValue() == null) {
                return 1;
              }
              int result = 0;
              if (entry1.getValue().getStorefileSizeMB() < entry2.getValue()
                  .getStorefileSizeMB()) {
                result = -1;
              } else if (entry1.getValue().getStorefileSizeMB() > entry2
                  .getValue().getStorefileSizeMB()) {
                result = 1;
              }
              if (reverseOrder) {
                result = -1 * result;
              }
              return result;
            }
          });
    } else if (sortKey.equals("filecount")) {
      Collections.sort(entryList,
          new Comparator<Map.Entry<HRegionInfo, RegionLoad>>() {
            public int compare(
                Map.Entry<HRegionInfo, RegionLoad> entry1,
                Map.Entry<HRegionInfo, RegionLoad> entry2) {
              if (entry1 == null || entry1.getValue() == null) {
                return -1;
              } else if (entry2 == null || entry2.getValue() == null) {
                return 1;
              }
              int result = 0;
              if (entry1.getValue().getStorefiles() < entry2.getValue()
                  .getStorefiles()) {
                result = -1;
              } else if (entry1.getValue().getStorefiles() > entry2.getValue()
                  .getStorefiles()) {
                result = 1;
              }
              if (reverseOrder) {
                result = -1 * result;
              }
              return result;
            }
          });
    } else if (sortKey.equals("memstore")) {
      Collections.sort(entryList,
          new Comparator<Map.Entry<HRegionInfo, RegionLoad>>() {
            public int compare(
                Map.Entry<HRegionInfo, RegionLoad> entry1,
                Map.Entry<HRegionInfo, RegionLoad> entry2) {
              if (entry1 == null || entry1.getValue()==null) {
                return -1;
              } else if (entry2 == null || entry2.getValue()==null) {
                return 1;
              }
              int result = 0;
              if (entry1.getValue().getMemStoreSizeMB() < entry2.getValue()
                  .getMemStoreSizeMB()) {
                result = -1;
              } else if (entry1.getValue().getMemStoreSizeMB() > entry2
                  .getValue().getMemStoreSizeMB()) {
                result = 1;
              }
              if (reverseOrder) {
                result = -1 * result;
              }
              return result;
            }
          });
    } else if (sortKey.equals("locality")) {
      Collections.sort(entryList,
          new Comparator<Map.Entry<HRegionInfo, RegionLoad>>() {
            public int compare(
                Map.Entry<HRegionInfo, RegionLoad> entry1,
                Map.Entry<HRegionInfo, RegionLoad> entry2) {
              if (entry1 == null || entry1.getValue()==null) {
                return -1;
              } else if (entry2 == null || entry2.getValue()==null) {
                return 1;
              }
              int result = 0;
              if (entry1.getValue().getDataLocality() < entry2.getValue()
                  .getDataLocality()) {
                result = -1;
              } else if (entry1.getValue().getDataLocality() > entry2
                  .getValue().getDataLocality()) {
                result = 1;
              }
              if (reverseOrder) {
                result = -1 * result;
              }
              return result;
            }
          });
    }
  }

  for (Map.Entry<HRegionInfo, RegionLoad> hriEntry : entryList) {
    HRegionInfo regionInfo = hriEntry.getKey();
    ServerName addr = regionsToServer.get(regionInfo);
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
          "//" + addr.getHostname() + ":" + master.getRegionServerInfoPort(addr) + "/";
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
%>
<tr>
  <td><%= escapeXml(showWhole?Bytes.toStringBinary(regionInfo.getRegionName()):regionInfo.getEncodedName()) %></td>
  <%
  if (urlRegionServer != null) {
  %>
  <td>
     <a href="<%= urlRegionServer %>"><%= addr.getHostname().toString() + ":" + master.getRegionServerInfoPort(addr) %></a>
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
  <td><%= escapeXml(showWhole?Bytes.toStringBinary(regionInfo.getStartKey()):"-")%></td>
  <td><%= escapeXml(showWhole?Bytes.toStringBinary(regionInfo.getEndKey()):"-")%></td>
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
  <td><a href="<%= url %>"><%= addr.getHostname().toString() + ":" + master.getRegionServerInfoPort(addr) %></a></td>
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
  } catch(TableNotFoundException e) { %>
  <div class="container-fluid content">
    <div class="row inner_header">
      <div class="page-header">
        <h1>Table not found</h1>
       </div>
    </div>
    <p><hr><p>
    <p>Go <a href="javascript:history.back()">Back</a>
  </div> <%
  } catch(IllegalArgumentException e) { %>
  <div class="container-fluid content">
    <div class="row inner_header">
      <div class="page-header">
        <h1>Table qualifier must not be empty</h1>
      </div>
    </div>
    <p><hr><p>
    <p>Go <a href="javascript:history.back()">Back</a>
  </div> <%
  }
}
  else { // handle the case for fqtn is null with error message + redirect
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
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>

<script>
var index=0;
var sortKeyValue='<%= sortKey %>';
if(sortKeyValue=="readrequest")index=1;
else if(sortKeyValue=="writerequest")index=2;
else if(sortKeyValue=="size")index=3;
else if(sortKeyValue=="filecount")index=4;
else if(sortKeyValue=="memstore")index=5;
else if(sortKeyValue=="locality")index=6;
document.getElementById("sel").selectedIndex=index;

var reverse='<%= reverseOrder %>';
if(reverse=='false')document.getElementById("ascending").checked=true;

var showWhole='<%= showWhole %>';
if(showWhole=='true')document.getElementById("showWhole").checked=true;

function reloadAsSort(){
  var url="?name="+'<%= fqtn %>';
  if(document.getElementById("sel").selectedIndex>0){
    url=url+"&sort="+document.getElementById("sel").value;
  }
  if(document.getElementById("ascending").checked){
    url=url+"&reverse=false";
  }
  if(document.getElementById("showWhole").checked){
    url=url+"&showwhole=true";
  }
  location.href=url;
}
</script>
