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
  import="java.util.Collections"
  import="java.util.Date"
  import="java.util.List"
  import="java.util.Map"
  import="java.util.regex.Pattern"
  import="java.util.stream.Stream"
  import="java.util.stream.Collectors"
  import="org.apache.hadoop.hbase.HTableDescriptor"
  import="org.apache.hadoop.hbase.RSGroupTableAccessor"
  import="org.apache.hadoop.hbase.ServerLoad"
  import="org.apache.hadoop.hbase.ServerName"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.client.RegionInfo"
  import="org.apache.hadoop.hbase.client.TableState"
  import="org.apache.hadoop.hbase.client.TableDescriptor"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.master.RegionState"
  import="org.apache.hadoop.hbase.net.Address"
  import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.util.VersionInfo"
  import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"%>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  String rsGroupName = request.getParameter("name");
  List<Address> rsGroupServers = new ArrayList<>();
  List<TableName> rsGroupTables = new ArrayList<>();
  RSGroupInfo rsGroupInfo = null;
  if (rsGroupName != null && !rsGroupName.isEmpty()) {
    rsGroupInfo = RSGroupTableAccessor.getRSGroupInfo(
        master.getConnection(), Bytes.toBytes(rsGroupName));
    if (rsGroupInfo != null) {
      rsGroupServers.addAll(rsGroupInfo.getServers());
      rsGroupTables.addAll(rsGroupInfo.getTables());
    }
  }
  Collections.sort(rsGroupServers);
    rsGroupTables.sort((o1, o2) -> {
      int compare = Bytes.compareTo(o1.getNamespace(), o2.getNamespace());
      if (compare != 0)
          return compare;
      compare = Bytes.compareTo(o1.getQualifier(), o2.getQualifier());
      if (compare != 0)
          return compare;
      return 0;
    });

  Map<Address, ServerLoad> onlineServers = Collections.emptyMap();
  Map<Address, ServerName> serverMaping = Collections.emptyMap();
  if (master.getServerManager() != null) {
    onlineServers = master.getServerManager().getOnlineServers().entrySet().stream()
            .collect(Collectors.toMap(p -> p.getKey().getAddress(), Map.Entry::getValue));
    serverMaping =
        master.getServerManager().getOnlineServers().entrySet().stream()
            .collect(Collectors.toMap(p -> p.getKey().getAddress(), Map.Entry::getKey));
  }
  pageContext.setAttribute("pageTitle", "RSGroup: " + rsGroupName);
%>
<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>
<div class="container-fluid content">
  <% if (rsGroupName == null || rsGroupName.isEmpty() || rsGroupInfo == null) { %>
  <div class="row inner_header">
      <div class="page-header">
          <h1>RSGroup: "<%= rsGroupName %>" does not exist</h1>
      </div>
  </div>
  <p>Go <a href="javascript:history.back()">Back</a>, or wait for the redirect.
  <% } else { %>
  <div class="container-fluid content">
    <div class="row">
      <div class="page-header">
        <h1>RSGroup: <%= rsGroupName %></h1>
      </div>
    </div>
  </div>
  <div class="container-fluid content">
    <div class="row">
      <div class="inner_header">
        <h1>Region Servers</h1>
      </div>
    </div>
    <div class="tabbable">
      <% if (rsGroupServers != null && rsGroupServers.size() > 0) { %>
        <ul class="nav nav-pills">
          <li class="active">
            <a href="#tab_baseStats" data-toggle="tab">Base Stats</a>
          </li>
          <li class="">
            <a href="#tab_memoryStats" data-toggle="tab">Memory</a>
          </li>
          <li class="">
            <a href="#tab_requestStats" data-toggle="tab">Requests</a>
          </li>
          <li class="">
            <a href="#tab_storeStats" data-toggle="tab">Storefiles</a>
          </li>
          <li class="">
            <a href="#tab_compactStats" data-toggle="tab">Compactions</a>
          </li>
        </ul>

      <div class="tab-content" style="padding-bottom: 9px; border-bottom: 1px solid #ddd;">
        <div class="tab-pane active" id="tab_baseStats">
          <table class="table table-striped">
            <tr>
              <th>ServerName</th>
              <th>Start time</th>
              <th>Last contact</th>
              <th>Version</th>
              <th>Requests Per Second</th>
              <th>Num. Regions</th>
            </tr>
            <% int totalRegions = 0;
               int totalRequests = 0;
               int inconsistentNodeNum = 0;
               String masterVersion = VersionInfo.getVersion();
               for (Address server: rsGroupServers) {
                 ServerName serverName = serverMaping.get(server);
                 if (serverName != null) {
                   ServerLoad sl = onlineServers.get(server);
                   String version = master.getRegionServerVersion(serverName);
                   if (!masterVersion.equals(version)) {
                     inconsistentNodeNum ++;
                   }
                   double requestsPerSecond = 0.0;
                   int numRegionsOnline = 0;
                   long lastContact = 0;
                   if (sl != null) {
                     requestsPerSecond = sl.getRequestsPerSecond();
                     numRegionsOnline = sl.getNumberOfRegions();
                     totalRegions += sl.getNumberOfRegions();
                     totalRequests += sl.getNumberOfRequests();
                     lastContact = (System.currentTimeMillis() - sl.getReportTime())/1000;
                   }
                   long startcode = serverName.getStartcode();
                   int infoPort = master.getRegionServerInfoPort(serverName);
                   String url = "//" + serverName.getHostname() + ":" + infoPort + "/rs-status";%>
                   <tr>
                     <td><a href="<%= url %>"><%= serverName.getServerName() %></a></td>
                     <td><%= new Date(startcode) %></td>
                     <td><%= lastContact %></td>
                     <td><%= version %></td>
                     <td><%= String.format("%.0f", requestsPerSecond) %></td>
                     <td><%= numRegionsOnline %></td>
                   </tr>
              <% } else { %>
                   <tr>
                     <td style="color:rgb(192,192,192);"><%= server %></td>
                     <td style="color:rgb(192,192,192);"><%= "Dead" %></td>
                     <td></td>
                     <td></td>
                     <td></td>
                     <td></td>
                   </tr>
              <% } %>
            <% } %>
            <tr><td>Total:<%= rsGroupServers.size() %></td>
            <td></td>
            <td></td>
            <%if (inconsistentNodeNum > 0) { %>
                <td style="color:red;"><%= inconsistentNodeNum %> nodes with inconsistent version</td>
            <%} else { %>
                <td></td>
            <%} %>
            <td><%= totalRequests %></td>
            <td><%= totalRegions %></td>
            </tr>
          </table>
        </div>
        <div class="tab-pane" id="tab_memoryStats">
          <table class="table table-striped">
            <tr>
              <th>ServerName</th>
              <th>Used Heap</th>
              <th>Max Heap</th>
              <th>Memstore Size</th>
            </tr>
            <% for (Address server: rsGroupServers) {
                 ServerName serverName = serverMaping.get(server);
                 ServerLoad sl = onlineServers.get(server);
                 if (sl != null && serverName != null) {
                   int infoPort = master.getRegionServerInfoPort(serverName);
                   String url = "//" + serverName.getHostname() + ":" + infoPort + "/rs-status";
            %>
                   <tr>
                     <td><a href="<%= url %>"><%= serverName.getServerName() %></a></td>
                     <td><%= TraditionalBinaryPrefix.long2String(sl.getUsedHeapMB()
                       * TraditionalBinaryPrefix.MEGA.value, "B", 1) %></td>
                     <td><%= TraditionalBinaryPrefix.long2String(sl.getMaxHeapMB()
                       * TraditionalBinaryPrefix.MEGA.value, "B", 1) %></td>
                     <td><%= TraditionalBinaryPrefix.long2String(sl.getMemstoreSizeInMB()
                       * TraditionalBinaryPrefix.MEGA.value, "B", 1) %></td>
                   </tr>
              <% } else { %>
                   <tr>
                     <td style="color:rgb(192,192,192);"><%= server %></td>
                     <td></td>
                     <td></td>
                     <td></td>
                   </tr>
              <% }
               } %>
          </table>
        </div>
        <div class="tab-pane" id="tab_requestStats">
          <table class="table table-striped">
            <tr>
                <th>ServerName</th>
                <th>Request Per Second</th>
                <th>Read Request Count</th>
                <th>Write Request Count</th>
            </tr>
            <% for (Address server: rsGroupServers) {
                 ServerName serverName = serverMaping.get(server);
                 ServerLoad sl = onlineServers.get(server);
                 if (sl != null && serverName != null) {
                   int infoPort = master.getRegionServerInfoPort(serverName);
                   String url = "//" + serverName.getHostname() + ":" + infoPort + "/rs-status";
            %>
                   <tr>
                     <td><a href="<%= url %>"><%= serverName.getServerName() %></a></td>
                     <td><%= String.format("%.0f", sl.getRequestsPerSecond()) %></td>
                     <td><%= sl.getReadRequestsCount() %></td>
                     <td><%= sl.getWriteRequestsCount() %></td>
                   </tr>
              <% } else { %>
                   <tr>
                     <td style="color:rgb(192,192,192);"><%= server %></td>
                     <td></td>
                     <td></td>
                     <td></td>
                   </tr>
              <% }
              } %>
          </table>
        </div>
        <div class="tab-pane" id="tab_storeStats">
          <table class="table table-striped">
            <tr>
                <th>ServerName</th>
                <th>Num. Stores</th>
                <th>Num. Storefiles</th>
                <th>Storefile Size Uncompressed</th>
                <th>Storefile Size</th>
                <th>Index Size</th>
                <th>Bloom Size</th>
            </tr>
            <%  for (Address server: rsGroupServers) {
                  ServerName serverName = serverMaping.get(server);
                  ServerLoad sl = onlineServers.get(server);
                  if (sl != null && serverName != null) {
                    int infoPort = master.getRegionServerInfoPort(serverName);
                    String url = "//" + serverName.getHostname() + ":" + infoPort + "/rs-status";
            %>
                    <tr>
                      <td><a href="<%= url %>"><%= serverName.getServerName() %></a></td>
                      <td><%= sl.getStores() %></td>
                      <td><%= sl.getStorefiles() %></td>
                      <td><%= TraditionalBinaryPrefix.long2String(
                          sl.getStoreUncompressedSizeMB() * TraditionalBinaryPrefix.MEGA.value, "B", 1) %></td>
                      <td><%= TraditionalBinaryPrefix.long2String(sl.getStorefileSizeInMB()
                          * TraditionalBinaryPrefix.MEGA.value, "B", 1) %></td>
                      <td><%= TraditionalBinaryPrefix.long2String(sl.getTotalStaticIndexSizeKB()
                          * TraditionalBinaryPrefix.KILO.value, "B", 1) %></td>
                      <td><%= TraditionalBinaryPrefix.long2String(sl.getTotalStaticBloomSizeKB()
                          * TraditionalBinaryPrefix.KILO.value, "B", 1) %></td>
                    </tr>
               <% } else { %>
                    <tr>
                      <td style="color:rgb(192,192,192);"><%= server %></td>
                      <td></td>
                      <td></td>
                      <td></td>
                      <td></td>
                      <td></td>
                      <td></td>
                    </tr>
              <% }
              } %>
          </table>
        </div>
        <div class="tab-pane" id="tab_compactStats">
          <table class="table table-striped">
            <tr>
              <th>ServerName</th>
              <th>Num. Compacting KVs</th>
              <th>Num. Compacted KVs</th>
              <th>Remaining KVs</th>
              <th>Compaction Progress</th>
            </tr>
            <%  for (Address server: rsGroupServers) {
                  ServerName serverName = serverMaping.get(server);
                  ServerLoad sl = onlineServers.get(server);
                  if (sl != null && serverName != null) {
                    String percentDone = "";
                    if  (sl.getTotalCompactingKVs() > 0) {
                         percentDone = String.format("%.2f", 100 *
                            ((float) sl.getCurrentCompactedKVs() / sl.getTotalCompactingKVs())) + "%";
                    }
                    int infoPort = master.getRegionServerInfoPort(serverName);
                    String url = "//" + serverName.getHostname() + ":" + infoPort + "/rs-status";
            %>
                    <tr>
                      <td><a href="<%= url %>"><%= serverName.getServerName() %></a></td>
                      <td><%= sl.getTotalCompactingKVs() %></td>
                      <td><%= sl.getCurrentCompactedKVs() %></td>
                      <td><%= sl.getTotalCompactingKVs() - sl.getCurrentCompactedKVs() %></td>
                      <td><%= percentDone %></td>
                    </tr>
               <% } else { %>
                    <tr>
                      <td style="color:rgb(192,192,192);"><%= server %></td>
                      <td></td>
                      <td></td>
                      <td></td>
                      <td></td>
                    </tr>
               <% }
               } %>
          </table>
        </div>
      </div>
      <% } else { %>
      <p> No Region Servers</p>
      <% } %>
    </div>
  </div>
  <br />

  <div class="container-fluid content">
    <div class="row inner_header">
        <div class="page-header">
            <h1>Tables</h1>
        </div>
    </div>

    <% if (rsGroupTables != null && rsGroupTables.size() > 0) {
        HTableDescriptor[] tables = null;
        try (Admin admin = master.getConnection().getAdmin()) {
            tables = master.isInitialized() ? admin.listTables((Pattern)null, true) : null;
        }
         Map<TableName, HTableDescriptor> tableDescriptors
            = Stream.of(tables).collect(Collectors.toMap(TableDescriptor::getTableName, p -> p));
    %>
         <table class="table table-striped">
         <tr>
             <th>Namespace</th>
             <th>Table</th>
             <th>Stats</th>
             <th>Online Regions</th>
             <th>Offline Regions</th>
             <th>Failed Regions</th>
             <th>Split Regions</th>
             <th>Other Regions</th>
             <th>Description</th>
         </tr>
         <% for(TableName tableName : rsGroupTables) {
             HTableDescriptor htDesc = tableDescriptors.get(tableName);
             if(htDesc == null) {
         %>
               <tr>
                 <td><%= tableName.getNamespaceAsString() %></td>
                 <td><%= tableName.getQualifierAsString() %></td>
                 <td style="color:rgb(0,0,255);"><%= "DELETED" %></td>
                 <td></td>
                 <td></td>
                 <td></td>
                 <td></td>
                 <td></td>
                 <td></td>
               </tr>
           <% } else { %>
                <tr>
                  <td><%= tableName.getNamespaceAsString() %></td>
                  <td><a href="/table.jsp?name=<%= tableName.getNameAsString() %>"><%= tableName.getQualifierAsString() %></a></td>
              <% TableState.State tableState = master.getTableStateManager().getTableState(tableName);
                  if(TableState.isInStates(tableState,
                          TableState.State.DISABLED, TableState.State.DISABLING)) {
              %>
                  <td style="color:red;"><%= tableState.name() %></td>
              <% } else {  %>
                  <td><%= tableState.name() %></td>
              <% } %>
              <% Map<RegionState.State, List<RegionInfo>> tableRegions =
                     master.getAssignmentManager().getRegionStates().getRegionByStateOfTable(tableName);
                 int openRegionsCount = tableRegions.get(RegionState.State.OPEN).size();
                 int offlineRegionsCount = tableRegions.get(RegionState.State.OFFLINE).size();
                 int splitRegionsCount = tableRegions.get(RegionState.State.SPLIT).size();
                 int failedRegionsCount = tableRegions.get(RegionState.State.FAILED_OPEN).size()
                         + tableRegions.get(RegionState.State.FAILED_CLOSE).size();
                 int otherRegionsCount = 0;
                 for (List<RegionInfo> list: tableRegions.values()) {
                     otherRegionsCount += list.size();
                 }
                 // now subtract known states
                 otherRegionsCount = otherRegionsCount - openRegionsCount
                         - failedRegionsCount - offlineRegionsCount
                         - splitRegionsCount;
              %>
                  <td><%= openRegionsCount %></td>
                  <td><%= offlineRegionsCount %></td>
                  <td><%= failedRegionsCount %></td>
                  <td><%= splitRegionsCount %></td>
                  <td><%= otherRegionsCount %></td>
                  <td><%= htDesc.toStringCustomizedValues() %></td>
                </tr>
           <% }
            } %>
           <p> <%= rsGroupTables.size() %> table(s) in set.</p>
         </table>
    <% } else { %>
      <p> No Tables</p>
    <% } %>
  </div>
<% } %>
</div>
<jsp:include page="footer.jsp" />
