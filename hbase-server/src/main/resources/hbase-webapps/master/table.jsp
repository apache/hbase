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
<%@page import="java.net.URLEncoder"%>
<%@ page contentType="text/html;charset=UTF-8"
         import="static org.apache.commons.lang3.StringEscapeUtils.escapeXml"
         import="java.util.ArrayList"
         import="java.util.Collection"
         import="java.util.HashMap"
         import="java.util.LinkedHashMap"
         import="java.util.List"
         import="java.util.Map"
         import="java.util.TreeMap"
         import=" java.util.concurrent.TimeUnit"
         import="org.apache.commons.lang3.StringEscapeUtils"
         import="org.apache.hadoop.conf.Configuration"
         import="org.apache.hadoop.hbase.HColumnDescriptor"
         import="org.apache.hadoop.hbase.HConstants"
         import="org.apache.hadoop.hbase.HRegionLocation"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.TableName"
         import="org.apache.hadoop.hbase.TableNotFoundException"
         import="org.apache.hadoop.hbase.client.AsyncAdmin"
         import="org.apache.hadoop.hbase.client.AsyncConnection"
         import="org.apache.hadoop.hbase.client.CompactionState"
         import="org.apache.hadoop.hbase.client.ConnectionFactory"
         import="org.apache.hadoop.hbase.client.RegionInfo"
         import="org.apache.hadoop.hbase.client.RegionInfoBuilder"
         import="org.apache.hadoop.hbase.client.RegionLocator"
         import="org.apache.hadoop.hbase.client.RegionReplicaUtil"
         import="org.apache.hadoop.hbase.client.Table"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.quotas.QuotaSettingsFactory"
         import="org.apache.hadoop.hbase.quotas.QuotaTableUtil"
         import="org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot"
         import="org.apache.hadoop.hbase.quotas.ThrottleSettings"
         import="org.apache.hadoop.hbase.util.Bytes"
         import="org.apache.hadoop.hbase.util.FSUtils"
         import="org.apache.hadoop.hbase.zookeeper.MetaTableLocator"
         import="org.apache.hadoop.util.StringUtils"
         import="org.apache.hbase.thirdparty.com.google.protobuf.ByteString"%>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos" %>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos" %>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas" %>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota" %>
<%@ page import="org.apache.hadoop.hbase.ServerMetrics" %>
<%@ page import="org.apache.hadoop.hbase.RegionMetrics" %>
<%@ page import="org.apache.hadoop.hbase.Size" %>
<%@ page import="org.apache.hadoop.hbase.RegionMetricsBuilder" %>
<%@ page import="org.apache.hadoop.hbase.master.assignment.RegionStates" %>
<%@ page import="org.apache.hadoop.hbase.master.RegionState" %>
<%!
  /**
   * @return An empty region load stamped with the passed in <code>regionInfo</code>
   * region name.
   */
  private RegionMetrics getEmptyRegionMetrics(final RegionInfo regionInfo) {
    return RegionMetricsBuilder.toRegionMetrics(ClusterStatusProtos.RegionLoad.newBuilder().
            setRegionSpecifier(HBaseProtos.RegionSpecifier.newBuilder().
                    setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME).
                    setValue(ByteString.copyFrom(regionInfo.getRegionName())).build()).build());
  }
%>
<%
  final String ZEROKB = "0 KB";
  final String ZEROMB = "0 MB";
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  String fqtn = request.getParameter("name");
  final String escaped_fqtn = StringEscapeUtils.escapeHtml4(fqtn);
  Table table;
  String tableHeader;
  boolean withReplica = false;
  boolean showFragmentation = conf.getBoolean("hbase.master.ui.fragmentation.enabled", false);
  boolean readOnly = conf.getBoolean("hbase.master.ui.readonly", false);
  int numMetaReplicas = conf.getInt(HConstants.META_REPLICAS_NUM,
          HConstants.DEFAULT_META_REPLICA_NUM);
  Map<String, Integer> frags = null;
  if (showFragmentation) {
    frags = FSUtils.getTableFragmentation(master);
  }
  boolean quotasEnabled = conf.getBoolean("hbase.quota.enabled", false);
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

  String pageTitle;
  if ( !readOnly && action != null ) {
    pageTitle = "HBase Master: " + StringEscapeUtils.escapeHtml4(master.getServerName().toString());
  } else {
    pageTitle = "Table: " + escaped_fqtn;
  }
  pageContext.setAttribute("pageTitle", pageTitle);
  AsyncConnection connection = ConnectionFactory.createAsyncConnection(master.getConfiguration()).get();
  AsyncAdmin admin = connection.getAdminBuilder().setOperationTimeout(5, TimeUnit.SECONDS).build();
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<%
  if (fqtn != null && master.isInitialized()) {
    try {
      table = master.getConnection().getTable(TableName.valueOf(fqtn));
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
    if (action.equals("split")) {
      if (key != null && key.length() > 0) {
        admin.split(TableName.valueOf(fqtn), Bytes.toBytes(key));
      } else {
        admin.split(TableName.valueOf(fqtn));
      }

    %> Split request accepted. <%
    } else if (action.equals("compact")) {
      if (key != null && key.length() > 0) {
        List<RegionInfo> regions = admin.getRegions(TableName.valueOf(fqtn)).get();
        byte[] row = Bytes.toBytes(key);

        for (RegionInfo region : regions) {
          if (region.containsRow(row)) {
            admin.compactRegion(region.getRegionName());
          }
        }
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
%>
  <jsp:include page="redirect.jsp" />
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
                RegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                        RegionInfoBuilder.FIRST_META_REGIONINFO, j);
                ServerName metaLocation = MetaTableLocator.waitMetaRegionLocation(master.getZooKeeper(), j, 1);
                for (int i = 0; i < 1; i++) {
                  String hostAndPort = "";
                  String readReq = "N/A";
                  String writeReq = "N/A";
                  String fileSize = ZEROMB;
                  String fileCount = "N/A";
                  String memSize = ZEROMB;
                  float locality = 0.0f;
                  if (metaLocation != null) {
                    ServerMetrics sl = master.getServerManager().getLoad(metaLocation);
                    // The host name portion should be safe, but I don't know how we handle IDNs so err on the side of failing safely.
                    hostAndPort = URLEncoder.encode(metaLocation.getHostname()) + ":" + master.getRegionServerInfoPort(metaLocation);
                    if (sl != null) {
                      Map<byte[], RegionMetrics> map = sl.getRegionMetrics();
                      if (map.containsKey(meta.getRegionName())) {
                        RegionMetrics load = map.get(meta.getRegionName());
                        readReq = String.format("%,1d", load.getReadRequestCount());
                        writeReq = String.format("%,1d", load.getWriteRequestCount());
                        double rSize = load.getStoreFileSize().get(Size.Unit.BYTE);
                        if (rSize > 0) {
                          fileSize = StringUtils.byteDesc((long) rSize);
                        }
                        fileCount = String.format("%,1d", load.getStoreFileCount());
                        double mSize = load.getMemStoreSize().get(Size.Unit.BYTE);
                        if (mSize > 0) {
                          memSize = StringUtils.byteDesc((long)mSize);
                        }
                        locality = load.getDataLocality();
                      }
                    }
                  }
            %>
            <tr>
              <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
              <td><a href="http://<%= hostAndPort %>/rs-status"><%= StringEscapeUtils.escapeHtml4(hostAndPort) %></a></td>
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
              <th>Num. Compacting Cells</th>
              <th>Num. Compacted Cells</th>
              <th>Remaining Cells</th>
              <th>Compaction Progress</th>
            </tr>
            </thead>
            <tbody>
            <%
              // NOTE: Presumes meta with one or more replicas
              for (int j = 0; j < numMetaReplicas; j++) {
                RegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                        RegionInfoBuilder.FIRST_META_REGIONINFO, j);
                ServerName metaLocation = MetaTableLocator.waitMetaRegionLocation(master.getZooKeeper(), j, 1);
                for (int i = 0; i < 1; i++) {
                  String hostAndPort = "";
                  long compactingCells = 0;
                  long compactedCells = 0;
                  String compactionProgress = "";
                  if (metaLocation != null) {
                    ServerMetrics sl = master.getServerManager().getLoad(metaLocation);
                    hostAndPort = URLEncoder.encode(metaLocation.getHostname()) + ":" + master.getRegionServerInfoPort(metaLocation);
                    if (sl != null) {
                      Map<byte[], RegionMetrics> map = sl.getRegionMetrics();
                      if (map.containsKey(meta.getRegionName())) {
                        RegionMetrics load = map.get(meta.getRegionName());
                        compactingCells = load.getCompactingCellCount();
                        compactedCells = load.getCompactedCellCount();
                        if (compactingCells > 0) {
                          compactionProgress = String.format("%.2f", 100 * ((float)
                                  compactedCells / compactingCells)) + "%";
                        }
                      }
                    }
                  }
            %>
            <tr>
              <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
              <td><a href="http://<%= hostAndPort %>/rs-status"><%= StringEscapeUtils.escapeHtml4(hostAndPort) %></a></td>
              <td><%= String.format("%,1d", compactingCells)%></td>
              <td><%= String.format("%,1d", compactedCells)%></td>
              <td><%= String.format("%,1d", compactingCells - compactedCells)%></td>
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
      RegionStates states = master.getAssignmentManager().getRegionStates();
      Map<RegionState.State, List<RegionInfo>> regionStates = states.getRegionByStateOfTable(table.getName());
      Map<String, RegionState.State> stateMap = new HashMap<>();
      for (RegionState.State regionState : regionStates.keySet()) {
        for (RegionInfo regionInfo : regionStates.get(regionState)) {
          stateMap.put(regionInfo.getEncodedName(), regionState);
        }
      }
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
        <td><%= master.getAssignmentManager().isTableEnabled(table.getName()) %></td>
        <td>Is the table enabled</td>
      </tr>
      <tr>
        <td>Compaction</td>
        <td>
          <%
            if (master.getAssignmentManager().isTableEnabled(table.getName())) {
              try {
                CompactionState compactionState = admin.getCompactionState(table.getName()).get();
          %><%= compactionState %><%
        } catch (Exception e) {
          // Nothing really to do here
          for(StackTraceElement element : e.getStackTrace()) {
        %><%= StringEscapeUtils.escapeHtml4(element.toString()) %><%
          }
        %> Unknown <%
          }
        } else {
        %><%= CompactionState.NONE %><%
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
      <%
        if (quotasEnabled) {
          TableName tn = TableName.valueOf(fqtn);
          SpaceQuotaSnapshot masterSnapshot = null;
          Quotas quota = QuotaTableUtil.getTableQuota(master.getConnection(), tn);
          if (quota == null || !quota.hasSpace()) {
            quota = QuotaTableUtil.getNamespaceQuota(master.getConnection(), tn.getNamespaceAsString());
            if (quota != null) {
              masterSnapshot = master.getQuotaObserverChore().getNamespaceQuotaSnapshots()
                      .get(tn.getNamespaceAsString());
            }
          } else {
            masterSnapshot = master.getQuotaObserverChore().getTableQuotaSnapshots().get(tn);
          }
          if (quota != null && quota.hasSpace()) {
            SpaceQuota spaceQuota = quota.getSpace();
      %>
      <tr>
        <td>Space Quota</td>
        <td>
          <table>
            <tr>
              <th>Property</th>
              <th>Value</th>
            </tr>
            <tr>
              <td>Limit</td>
              <td><%= StringUtils.byteDesc(spaceQuota.getSoftLimit()) %></td>
            </tr>
            <tr>
              <td>Policy</td>
              <td><%= spaceQuota.getViolationPolicy() %></td>
            </tr>
            <%
              if (masterSnapshot != null) {
            %>
            <tr>
              <td>Usage</td>
              <td><%= StringUtils.byteDesc(masterSnapshot.getUsage()) %></td>
            </tr>
            <tr>
              <td>State</td>
              <td><%= masterSnapshot.getQuotaStatus().isInViolation() ? "In Violation" : "In Observance" %></td>
            </tr>
            <%
              }
            %>
          </table>
        </td>
        <td>Information about a Space Quota on this table, if set.</td>
      </tr>
      <%
        }
        if (quota != null && quota.hasThrottle()) {
          List<ThrottleSettings> throttles = QuotaSettingsFactory.fromTableThrottles(table.getName(), quota.getThrottle());
          if (throttles.size() > 0) {
      %>
      <tr>
        <td>Throttle Quota</td>
        <td>
          <table>
            <tr>
              <th>Limit</th>
              <th>Type</th>
              <th>TimeUnit</th>
              <th>Scope</th>
            </tr>
            <%
              for (ThrottleSettings throttle : throttles) {
            %>
            <tr>
              <td><%= throttle.getSoftLimit() %></td>
              <td><%= throttle.getThrottleType() %></td>
              <td><%= throttle.getTimeUnit() %></td>
              <td><%= throttle.getQuotaScope() %></td>
            </tr>
            <%
              }
            %>
          </table>
        </td>
        <td>Information about a Throttle Quota on this table, if set.</td>
      </tr>
      <%
            }
          }
        }
      %>
    </table>
    <h2>Table Schema</h2>
    <table class="table table-striped">
      <tr>
        <th>Column Family Name</th>
        <th></th>
      </tr>
      <%
        Collection<HColumnDescriptor> families = table.getTableDescriptor().getFamilies();
        for (HColumnDescriptor family: families) {
      %>
      <tr>
        <td><%= StringEscapeUtils.escapeHtml4(family.getNameAsString()) %></td>
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
                <%= StringEscapeUtils.escapeHtml4(familyKey.toString()) %>
              </td>
              <td>
                <%= StringEscapeUtils.escapeHtml4(familyValues.get(familyKey).toString()) %>
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
      long totalCompactingCells = 0;
      long totalCompactedCells = 0;
      String totalCompactionProgress = "";
      String totalMemSizeStr = ZEROMB;
      String totalSizeStr = ZEROMB;
      String urlRegionServer = null;
      Map<ServerName, Integer> regDistribution = new TreeMap<>();
      Map<ServerName, Integer> primaryRegDistribution = new TreeMap<>();
      List<HRegionLocation> regions = r.getAllRegionLocations();
      Map<RegionInfo, RegionMetrics> regionsToLoad = new LinkedHashMap<>();
      Map<RegionInfo, ServerName> regionsToServer = new LinkedHashMap<>();
      for (HRegionLocation hriEntry : regions) {
        RegionInfo regionInfo = hriEntry.getRegionInfo();
        ServerName addr = hriEntry.getServerName();
        regionsToServer.put(regionInfo, addr);

        if (addr != null) {
          ServerMetrics sl = master.getServerManager().getLoad(addr);
          if (sl != null) {
            RegionMetrics regionMetrics = sl.getRegionMetrics().get(regionInfo.getRegionName());
            regionsToLoad.put(regionInfo, regionMetrics);
            if (regionMetrics != null) {
              totalReadReq += regionMetrics.getReadRequestCount();
              totalWriteReq += regionMetrics.getWriteRequestCount();
              totalSize += regionMetrics.getStoreFileSize().get(Size.Unit.MEGABYTE);
              totalStoreFileCount += regionMetrics.getStoreFileCount();
              totalMemSize += regionMetrics.getMemStoreSize().get(Size.Unit.MEGABYTE);
              totalStoreFileSizeMB += regionMetrics.getStoreFileSize().get(Size.Unit.MEGABYTE);
              totalCompactingCells += regionMetrics.getCompactingCellCount();
              totalCompactedCells += regionMetrics.getCompactedCellCount();
            } else {
              RegionMetrics load0 = getEmptyRegionMetrics(regionInfo);
              regionsToLoad.put(regionInfo, load0);
            }
          } else{
            RegionMetrics load0 = getEmptyRegionMetrics(regionInfo);
            regionsToLoad.put(regionInfo, load0);
          }
        } else {
          RegionMetrics load0 = getEmptyRegionMetrics(regionInfo);
          regionsToLoad.put(regionInfo, load0);
        }
      }
      if (totalSize > 0) {
        totalSizeStr = StringUtils.byteDesc(totalSize*1024l*1024);
      }
      if (totalMemSize > 0) {
        totalMemSizeStr = StringUtils.byteDesc(totalMemSize*1024l*1024);
      }
      if (totalCompactingCells > 0) {
        totalCompactionProgress = String.format("%.2f", 100 *
                ((float) totalCompactedCells / totalCompactingCells)) + "%";
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
              <th>StorefileSize<br>(<%= totalSizeStr %>)</th>
              <th>Num.Storefiles<br>(<%= String.format("%,1d", totalStoreFileCount)%>)</th>
              <th>MemSize<br>(<%= totalMemSizeStr %>)</th>
              <th>Locality</th>
              <th>Start Key</th>
              <th>End Key</th>
              <th>Region State</th>
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
              List<Map.Entry<RegionInfo, RegionMetrics>> entryList = new ArrayList<>(regionsToLoad.entrySet());
              numRegions = regions.size();
              int numRegionsRendered = 0;
              // render all regions
              if (numRegionsToRender < 0) {
                numRegionsToRender = numRegions;
              }
              for (Map.Entry<RegionInfo, RegionMetrics> hriEntry : entryList) {
                RegionInfo regionInfo = hriEntry.getKey();
                ServerName addr = regionsToServer.get(regionInfo);
                RegionMetrics load = hriEntry.getValue();
                String readReq = "N/A";
                String writeReq = "N/A";
                String regionSize = ZEROMB;
                String fileCount = "N/A";
                String memSize = ZEROMB;
                float locality = 0.0f;
                String state = "N/A";
                if (load != null) {
                  readReq = String.format("%,1d", load.getReadRequestCount());
                  writeReq = String.format("%,1d", load.getWriteRequestCount());
                  double rSize = load.getStoreFileSize().get(Size.Unit.BYTE);
                  if (rSize > 0) {
                    regionSize = StringUtils.byteDesc((long)rSize);
                  }
                  fileCount = String.format("%,1d", load.getStoreFileCount());
                  double mSize = load.getMemStoreSize().get(Size.Unit.BYTE);
                  if (mSize > 0) {
                    memSize = StringUtils.byteDesc((long)mSize);
                  }
                  locality = load.getDataLocality();
                }
                if (stateMap.containsKey(regionInfo.getEncodedName())) {
                  state = stateMap.get(regionInfo.getEncodedName()).toString();
                }
                if (addr != null) {
                  ServerMetrics sl = master.getServerManager().getLoad(addr);
                  // This port might be wrong if RS actually ended up using something else.
                  urlRegionServer =
                          "//" + URLEncoder.encode(addr.getHostname()) + ":" + master.getRegionServerInfoPort(addr) + "/rs-status";
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
                <a href="<%= urlRegionServer %>"><%= addr == null? "-": StringEscapeUtils.escapeHtml4(addr.getHostname().toString()) + ":" + master.getRegionServerInfoPort(addr) %></a>
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
              <td><%= state%></td>
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
              <th>Num. Compacting Cells<br>(<%= String.format("%,1d", totalCompactingCells)%>)</th>
              <th>Num. Compacted Cells<br>(<%= String.format("%,1d", totalCompactedCells)%>)</th>
              <th>Remaining Cells<br>(<%= String.format("%,1d", totalCompactingCells-totalCompactedCells)%>)</th>
              <th>Compaction Progress<br>(<%= totalCompactionProgress %>)</th>
            </tr>
            </thead>
            <tbody>
            <%
              numRegionsRendered = 0;
              for (Map.Entry<RegionInfo, RegionMetrics> hriEntry : entryList) {
                RegionInfo regionInfo = hriEntry.getKey();
                ServerName addr = regionsToServer.get(regionInfo);
                RegionMetrics load = hriEntry.getValue();
                long compactingCells = 0;
                long compactedCells = 0;
                String compactionProgress = "";
                if (load != null) {
                  compactingCells = load.getCompactingCellCount();
                  compactedCells = load.getCompactedCellCount();
                  if (compactingCells > 0) {
                    compactionProgress = String.format("%.2f", 100 * ((float)
                            compactedCells / compactingCells)) + "%";
                  }
                }
                if (addr != null) {
                  // This port might be wrong if RS actually ended up using something else.
                  urlRegionServer =
                          "//" + URLEncoder.encode(addr.getHostname()) + ":" + master.getRegionServerInfoPort(addr) + "/rs-status";
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
                <a href="<%= urlRegionServer %>"><%= addr == null? "-": StringEscapeUtils.escapeHtml4(addr.getHostname().toString()) + ":" + master.getRegionServerInfoPort(addr) %></a>
              </td>
              <%
              } else {
              %>
              <td class="undeployed-region">not deployed</td>
              <%
                }
              %>
              <td><%= String.format("%,1d", compactingCells)%></td>
              <td><%= String.format("%,1d", compactedCells)%></td>
              <td><%= String.format("%,1d", compactingCells - compactedCells)%></td>
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
    <table id="regionServerTable" class="tablesorter table table-striped"><thead><tr><th>Region Server</th><th>Region Count</th><th>Primary Region Count</th></tr></thead>
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
            String url = "//" + URLEncoder.encode(addr.getHostname()) + ":" + master.getRegionServerInfoPort(addr) + "/rs-status";
        %>
        <tr>
          <td><a href="<%= url %>"><%= StringEscapeUtils.escapeHtml4(addr.getHostname().toString()) + ":" + master.getRegionServerInfoPort(addr) %></a></td>
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
    %><%= StringEscapeUtils.escapeHtml4(element.toString()) %><%
  }
} finally {
  connection.close();
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
          <td>
            <%
              if (totalStoreFileSizeMB > 0) {
            %>
            <%= StringUtils.TraditionalBinaryPrefix.
                    long2String(totalStoreFileSizeMB * 1024 * 1024, "B", 2)%></td>
          <%
          } else {
          %>
          0 MB </td>
          <%
            }
          %>
          <td>Total size of store files</td>
        </tr>
      </table>

        <% if (!readOnly) { %>
      <p><hr/></p>
      Actions:
      <p>
      <center>
        <table class="table" style="border: 0;" width="95%" >
          <tr>
            <form method="get">
              <input type="hidden" name="action" value="compact" />
              <input type="hidden" name="name" value="<%= escaped_fqtn %>" />
              <td class="centered">
                <input style="font-size: 12pt; width: 10em" type="submit" value="Compact" class="btn" />
              </td>
              <td style="text-align: center;">
                <input type="text" name="key" size="40" placeholder="Row Key (optional)" />
              </td>
              <td>
                This action will force a compaction of all regions of the table, or,
                if a key is supplied, only the region containing the
                given key.
              </td>
            </form>
          </tr>
          <tr>
            <form method="get">
              <input type="hidden" name="action" value="split" />
              <input type="hidden" name="name" value="<%= escaped_fqtn %>" />
              <td class="centered">
                <input style="font-size: 12pt; width: 10em" type="submit" value="Split" class="btn" />
              </td>
              <td style="text-align: center;">
                <input type="text" name="key" size="40" placeholder="Row Key (optional)" />
              </td>
              <td>
                This action will force a split of all eligible
                regions of the table, or, if a key is supplied, only the region containing the
                given key. An eligible region is one that does not contain any references to
                other regions. Split requests for noneligible regions will be ignored.
              </td>
            </form>
          </tr>
          <tr>
            <form method="get">
              <input type="hidden" name="action" value="merge" />
              <input type="hidden" name="name" value="<%= escaped_fqtn %>" />
              <td class="centered">
                <input style="font-size: 12pt; width: 10em" type="submit" value="Merge" class="btn" />
              </td>
              <td style="text-align: center;">
                <input type="text" name="left" size="40" placeholder="Region Key (required)" />
                <input type="text" name="right" size="40" placeholder="Region Key (required)" />
              </td>
              <td>
                This action will merge two regions of the table, Merge requests for
                noneligible regions will be ignored.
              </td>
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
else { // handle the case for fqtn is null or master is not initialized with error message + redirect
%>
<div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>Table not ready</h1>
    </div>
  </div>
  <p><hr><p>
  <jsp:include page="redirect.jsp" />
</div>
<% } %>

<jsp:include page="footer.jsp" />
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/jquery.tablesorter.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

<script>
    $(document).ready(function()
        {
            $.tablesorter.addParser(
                {
                    id: 'filesize',
                    is: function(s) {
                        return s.match(new RegExp( /([\.0-9]+)\ (B|KB|MB|GB|TB)/ ));
                    },
                    format: function(s) {
                        var suf = s.match(new RegExp( /(B|KB|MB|GB|TB)$/ ))[1];
                        var num = parseFloat(s.match( new RegExp( /([\.0-9]+)\ (B|KB|MB|GB|TB)/ ))[0]);
                        switch(suf) {
                            case 'B':
                                return num;
                            case 'KB':
                                return num * 1024;
                            case 'MB':
                                return num * 1024 * 1024;
                            case 'GB':
                                return num * 1024 * 1024 * 1024;
                            case 'TB':
                                return num * 1024 * 1024 * 1024 * 1024;
                        }
                    },
                    type: 'numeric'
                });
            $.tablesorter.addParser(
                {
                    id: "separator",
                    is: function (s) {
                        return /^[0-9]?[0-9,]*$/.test(s);
                    }, format: function (s) {
                        return $.tablesorter.formatFloat( s.replace(/,/g,'') );
                    }, type: "numeric"
                });
            $("#regionServerTable").tablesorter({
                headers: {
                    1: {sorter: 'separator'}
                }
            });
            $("#regionServerDetailsTable").tablesorter({
                headers: {
                    2: {sorter: 'separator'},
                    3: {sorter: 'separator'},
                    4: {sorter: 'filesize'},
                    5: {sorter: 'separator'},
                    6: {sorter: 'filesize'}
                }
            });
            $("#tableRegionTable").tablesorter({
                headers: {
                    2: {sorter: 'separator'},
                    3: {sorter: 'separator'},
                    4: {sorter: 'filesize'},
                    5: {sorter: 'separator'},
                    6: {sorter: 'filesize'}
                }
            });
            $("#tableCompactStatsTable").tablesorter({
                headers: {
                    2: {sorter: 'separator'},
                    3: {sorter: 'separator'},
                    4: {sorter: 'separator'}
                }
            });
            $("#metaTableCompactStatsTable").tablesorter({
                headers: {
                    2: {sorter: 'separator'},
                    3: {sorter: 'separator'},
                    4: {sorter: 'separator'}
                }
            });
        }
    );
</script>
