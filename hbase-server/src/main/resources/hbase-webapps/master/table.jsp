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
  import="static org.apache.commons.lang3.StringEscapeUtils.escapeXml"
  import="java.util.ArrayList"
  import="java.util.HashMap"
  import="java.util.LinkedHashMap"
  import="java.util.List"
  import="java.util.Map"
  import="java.util.Set"
  import="java.util.HashSet"
  import="java.util.Optional"
  import="java.util.TreeMap"
  import="java.util.concurrent.TimeUnit"
  import="java.text.DecimalFormat"
  import="java.math.RoundingMode"
  import="org.apache.commons.lang3.StringEscapeUtils"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.HRegionLocation"
  import="org.apache.hadoop.hbase.RegionMetrics"
  import="org.apache.hadoop.hbase.RegionMetricsBuilder"
  import="org.apache.hadoop.hbase.ServerMetrics"
  import="org.apache.hadoop.hbase.ServerName"
  import="org.apache.hadoop.hbase.Size"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.client.AsyncAdmin"
  import="org.apache.hadoop.hbase.client.AsyncConnection"
  import="org.apache.hadoop.hbase.client.CompactionState"
  import="org.apache.hadoop.hbase.client.ConnectionFactory"
  import="org.apache.hadoop.hbase.client.RegionInfo"
  import="org.apache.hadoop.hbase.client.RegionInfoBuilder"
  import="org.apache.hadoop.hbase.client.RegionLocator"
  import="org.apache.hadoop.hbase.client.RegionReplicaUtil"
  import="org.apache.hadoop.hbase.client.Table"
  import="org.apache.hadoop.hbase.client.TableState"
  import="org.apache.hadoop.hbase.client.ColumnFamilyDescriptor"
  import="org.apache.hadoop.hbase.http.InfoServer"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.master.RegionState"
  import="org.apache.hadoop.hbase.master.assignment.RegionStateNode"
  import="org.apache.hadoop.hbase.master.assignment.RegionStates"
  import="org.apache.hadoop.hbase.master.http.MetaBrowser"
  import="org.apache.hadoop.hbase.master.http.RegionReplicaInfo"
  import="org.apache.hadoop.hbase.quotas.QuotaSettingsFactory"
  import="org.apache.hadoop.hbase.quotas.QuotaTableUtil" %>
<%@ page import="org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot" %>
<%@ page import="org.apache.hadoop.hbase.quotas.ThrottleSettings" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.hadoop.hbase.util.FSUtils" %>
<%@ page import="org.apache.hadoop.util.StringUtils" %>
<%@ page import="org.apache.hbase.thirdparty.com.google.protobuf.ByteString" %>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos" %>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos" %>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas" %>
<%@ page import="org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.SpaceQuota" %>
<%@ page import="java.net.URLEncoder" %>
<%@ page import="java.util.stream.Collectors" %>
<%@ page import="java.nio.charset.StandardCharsets" %>
<%@ page import="java.io.UnsupportedEncodingException" %>
<%!
  /**
   * @return An empty region load stamped with the passed in <code>regionInfo</code>
   * region name.
   */
  private static RegionMetrics getEmptyRegionMetrics(final RegionInfo regionInfo) {
    return RegionMetricsBuilder.toRegionMetrics(ClusterStatusProtos.RegionLoad.newBuilder().
            setRegionSpecifier(HBaseProtos.RegionSpecifier.newBuilder().
                    setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME).
                    setValue(ByteString.copyFrom(regionInfo.getRegionName())).build()).build());
  }

  /**
   * Given dicey information that may or not be available in meta, render a link to the region on
   * its region server.
   * @return an anchor tag if one can be built, {@code null} otherwise.
   */
  private static String buildRegionLink(final ServerName serverName, final int rsInfoPort,
    final RegionInfo regionInfo, final RegionState.State regionState) {
    if (serverName == null || regionInfo == null) { return null; }

    if (regionState != RegionState.State.OPEN) {
      // region is assigned to RS, but RS knows nothing of it. don't bother with a link.
      return serverName.getServerName();
    }

    final String socketAddress = serverName.getHostname() + ":" + rsInfoPort;
    final String URI = "//" + socketAddress + "/region.jsp"
      + "?name=" + regionInfo.getEncodedName();
    return "<a href=\"" + URI + "\">" + serverName.getServerName() + "</a>";
  }

  /**
   * Render an <td> tag contents server name which the given region deploys.
   * Links to the server rs-status page.
   * <td class="undeployed-region">not deployed</td> instead if can not find the deploy message.
   * @return an <td> tag contents server name links to server rs-status page.
   */
  private static String buildRegionDeployedServerTag(RegionInfo regionInfo, HMaster master,
                                                     Map<RegionInfo, ServerName> regionsToServer) throws UnsupportedEncodingException {
    ServerName serverName = regionsToServer.get(regionInfo);

    if (serverName == null) {
      return "<td class=\"undeployed-region\">not deployed</td>";
    }

    String hostName = serverName.getHostname();
    String hostNameEncoded = URLEncoder.encode(hostName, StandardCharsets.UTF_8.toString());
    // This port might be wrong if RS actually ended up using something else.
    int serverInfoPort = master.getRegionServerInfoPort(serverName);
    String urlRegionServer = "//" + hostNameEncoded + ":" + serverInfoPort + "/rs-status";

    return "<td><a href=\"" + urlRegionServer + "\">" + StringEscapeUtils.escapeHtml4(hostName)
      + ":" + serverInfoPort + "</a></td>";
  }

  /**
   * @return an <p> tag guide user to see all region messages.
   */
  private static String moreRegionsToRender(int numRegionsRendered, int numRegions, String fqtn) throws UnsupportedEncodingException {
    if (numRegions > numRegionsRendered) {
      String allRegionsUrl = "?name=" + URLEncoder.encode(fqtn, StandardCharsets.UTF_8.toString()) + "&numRegions=all";

      return "This table has <b>" + numRegions
        + "</b> regions in total, in order to improve the page load time, only <b>"
        + numRegionsRendered + "</b> regions are displayed here, <a href=\""
        + allRegionsUrl + "\">click here</a> to see all regions.</p>";
    }
    return "";
  }
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<%
  final String ZEROMB = "0 MB";
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  String fqtn = request.getParameter("name");
  // handle the case for fqtn is null or master is not initialized with error message + redirect
  if (fqtn == null || !master.isInitialized()) {
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
<%  return;
  } %>
  
<%
  // handle the case for fqtn is not null with IllegalArgumentException message + redirect
  try {
    TableName tn = TableName.valueOf(fqtn);
    TableName.isLegalNamespaceName(tn.getNamespace());
    TableName.isLegalTableQualifierName(tn.getQualifier());
  } catch (IllegalArgumentException e) {
%>
    <div class="container-fluid content">
      <div class="row inner_header">
        <div class="page-header">
          <h1>Table not legal</h1>
        </div>
      </div>
      <p><hr><p>
      <jsp:include page="redirect.jsp" />
    </div>
<%  return;
  } %>

<%
  final String escaped_fqtn = StringEscapeUtils.escapeHtml4(fqtn);
  Table table = master.getConnection().getTable(TableName.valueOf(fqtn));
  boolean showFragmentation = conf.getBoolean("hbase.master.ui.fragmentation.enabled", false);
  boolean readOnly = !InfoServer.canUserModifyUI(request, getServletContext(), conf);
  int numMetaReplicas =
    master.getTableDescriptors().get(TableName.META_TABLE_NAME).getRegionReplication();
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
  final AsyncConnection connection = ConnectionFactory.createAsyncConnection(master.getConfiguration()).get();
  final AsyncAdmin admin = connection.getAdminBuilder()
    .setOperationTimeout(5, TimeUnit.SECONDS)
    .build();
  final MetaBrowser metaBrowser = new MetaBrowser(connection, request);
%>

<% // unknown table
  if (! admin.tableExists(TableName.valueOf(fqtn)).get()) { %>
    <div class="container-fluid content">
      <div class="row inner_header">
        <div class="page-header">
          <h1>Table not found</h1>
        </div>
      </div>
      <p><hr><p>
      <jsp:include page="redirect.jsp" />
    </div>
<%  return;
  } %>

<% // table split/compact/merge actions
  if ( !readOnly && action != null ) { %>
    <div class="container-fluid content">
      <div class="row inner_header">
        <div class="page-header">
          <h1>Table action request accepted</h1>
        </div>
      </div>
      <p><hr><p>
<%  if (action.equals("split")) {
      if (key != null && key.length() > 0) {
        admin.split(TableName.valueOf(fqtn), Bytes.toBytes(key));
      } else {
        admin.split(TableName.valueOf(fqtn));
      }
%>    Split request accepted. <%
    } else if (action.equals("major compact")) {
      if (key != null && key.length() > 0) {
        List<RegionInfo> regions = admin.getRegions(TableName.valueOf(fqtn)).get();
        byte[] row = Bytes.toBytes(key);
        for (RegionInfo region : regions) {
          if (region.containsRow(row)) {
            admin.majorCompactRegion(region.getRegionName());
          }
        }
      } else {
        admin.majorCompact(TableName.valueOf(fqtn));
      }
%>    major Compact request accepted. <%
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
%>    Compact request accepted. <%
    } else if (action.equals("merge")) {
      if (left != null && left.length() > 0 && right != null && right.length() > 0) {
        admin.mergeRegions(Bytes.toBytesBinary(left), Bytes.toBytesBinary(right), false);
      }
%>    Merge request accepted. <%
    } %>
    <jsp:include page="redirect.jsp" />
    </div>
<%  return;
  } %>

<div class="container-fluid content">
<div class="row inner_header">
  <div class="page-header">
    <h1>Table <small><%= escaped_fqtn %></small></h1>
  </div>
</div>

<div class="row">
<% //Meta table.
  if(fqtn.equals(TableName.META_TABLE_NAME.getNameAsString())) { %>
  <section>
    <h2>Table Regions</h2>
    <div class="tabbable">
      <ul class="nav nav-pills" role="tablist">
        <li class="nav-item"><a class="nav-link active" href="#metaTab_baseStats" data-bs-toggle="tab" role="tab">Base Stats</a></li>
        <li class="nav-item"><a class="nav-link" href="#metaTab_localityStats" data-bs-toggle="tab" role="tab">Localities</a></li>
        <li class="nav-item"><a class="nav-link" href="#metaTab_compactStats" data-bs-toggle="tab" role="tab">Compactions</a></li>
      </ul>

      <div class="tab-content">
        <div class="tab-pane active" id="metaTab_baseStats" role="tabpanel">
          <table id="metaTableBaseStatsTable" class="tablesorter table table-striped">
            <thead>
              <tr>
                <th>Name</th>
                <th>Region Server</th>
                <th class="cls_separator">ReadRequests</th>
                <th class="cls_separator">WriteRequests</th>
                <th class="cls_filesize">Uncompressed StoreFileSize</th>
                <th class="cls_filesize">StorefileSize</th>
                <th class="cls_separator">Num.Storefiles</th>
                <th class="cls_filesize">MemSize</th>
                <th class="cls_emptyMin">Start Key</th>
                <th class="cls_emptyMax">End Key</th>
                <th>ReplicaID</th>
              </tr>
            </thead>
            <tbody>
            <%
              // NOTE: Presumes meta with one or more replicas
              for (int j = 0; j < numMetaReplicas; j++) {
                RegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                            RegionInfoBuilder.FIRST_META_REGIONINFO, j);
                //If a metaLocation is null, All of its info would be empty here to be displayed.
                RegionStateNode rsn = master.getAssignmentManager().getRegionStates()
                  .getRegionStateNode(meta);
                ServerName metaLocation = rsn != null ? rsn.getRegionLocation() : null;
                for (int i = 0; i < 1; i++) {
                  //If metaLocation is null, default value below would be displayed in UI.
                  String hostAndPort = "";
                  String readReq = "N/A";
                  String writeReq = "N/A";
                  String fileSizeUncompressed = ZEROMB;
                  String fileSize = ZEROMB;
                  String fileCount = "N/A";
                  String memSize = ZEROMB;

                  if (metaLocation != null) {
                    ServerMetrics sl = master.getServerManager().getLoad(metaLocation);
                    // The host name portion should be safe, but I don't know how we handle IDNs so err on the side of failing safely.
                    hostAndPort = URLEncoder.encode(metaLocation.getHostname(), StandardCharsets.UTF_8.toString()) + ":" + master.getRegionServerInfoPort(metaLocation);
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
                        double rSizeUncompressed = load.getUncompressedStoreFileSize().get(Size.Unit.BYTE);
                        if (rSizeUncompressed > 0) {
                          fileSizeUncompressed = StringUtils.byteDesc((long) rSizeUncompressed);
                        }
                        fileCount = String.format("%,1d", load.getStoreFileCount());
                        double mSize = load.getMemStoreSize().get(Size.Unit.BYTE);
                        if (mSize > 0) {
                          memSize = StringUtils.byteDesc((long)mSize);
                        }
                      }
                    }
                  }
                %>
              <tr>
                <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
                <td><a href="http://<%= hostAndPort %>/rs-status"><%= StringEscapeUtils.escapeHtml4(hostAndPort) %></a></td>
                <td><%= readReq%></td>
                <td><%= writeReq%></td>
                <td><%= fileSizeUncompressed%></td>
                <td><%= fileSize%></td>
                <td><%= fileCount%></td>
                <td><%= memSize%></td>
                <td><%= escapeXml(Bytes.toString(meta.getStartKey())) %></td>
                <td><%= escapeXml(Bytes.toString(meta.getEndKey())) %></td>
                <td><%= meta.getReplicaId() %></td>
              </tr>
            <%  } %>
            <%} %>
            </tbody>
          </table>
        </div>
        <div class="tab-pane" id="metaTab_localityStats" role="tabpanel">
           <table id="metaTableLocalityStatsTable" class="tablesorter table table-striped">
             <thead>
               <tr>
                 <th>Name</th>
                 <th>Region Server</th>
                 <th class="cls_separator">Locality</th>
                 <th class="cls_separator">LocalityForSsd</th>
               </tr>
             </thead>
             <tbody>
             <%
               // NOTE: Presumes meta with one or more replicas
               for (int j = 0; j < numMetaReplicas; j++) {
                 RegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                                         RegionInfoBuilder.FIRST_META_REGIONINFO, j);
                 //If a metaLocation is null, All of its info would be empty here to be displayed.
                 RegionStateNode rsn = master.getAssignmentManager().getRegionStates()
                   .getRegionStateNode(meta);
                 ServerName metaLocation = rsn != null ? rsn.getRegionLocation() : null;
                 for (int i = 0; i < 1; i++) {
                   //If metaLocation is null, default value below would be displayed in UI.
                   String hostAndPort = "";
                   float locality = 0.0f;
                   float localityForSsd = 0.0f;

                   if (metaLocation != null) {
                     ServerMetrics sl = master.getServerManager().getLoad(metaLocation);
                     hostAndPort = URLEncoder.encode(metaLocation.getHostname(), StandardCharsets.UTF_8.toString()) + ":" + master.getRegionServerInfoPort(metaLocation);
                     if (sl != null) {
                       Map<byte[], RegionMetrics> map = sl.getRegionMetrics();
                       if (map.containsKey(meta.getRegionName())) {
                         RegionMetrics load = map.get(meta.getRegionName());
                         locality = load.getDataLocality();
                         localityForSsd = load.getDataLocalityForSsd();
                       }
                     }
                   }
                 %>
               <tr>
                 <td><%= escapeXml(meta.getRegionNameAsString()) %></td>
                 <td><a href="http://<%= hostAndPort %>/rs-status"><%= StringEscapeUtils.escapeHtml4(hostAndPort) %></a></td>
                 <td><%= locality%></td>
                 <td><%= localityForSsd%></td>
               </tr>
             <%  } %>
             <%} %>
             </tbody>
           </table>
         </div>
        <div class="tab-pane" id="metaTab_compactStats" role="tabpanel">
          <table id="metaTableCompactStatsTable" class="tablesorter table table-striped">
            <thead>
              <tr>
                <th>Name</th>
                <th>Region Server</th>
                <th class="cls_separator">Num. Compacting Cells</th>
                <th class="cls_separator">Num. Compacted Cells</th>
                <th class="cls_separator">Remaining Cells</th>
                <th>Compaction Progress</th>
              </tr>
            </thead>
            <tbody>
            <%
              // NOTE: Presumes meta with one or more replicas
              for (int j = 0; j < numMetaReplicas; j++) {
                RegionInfo meta = RegionReplicaUtil.getRegionInfoForReplica(
                           RegionInfoBuilder.FIRST_META_REGIONINFO, j);
                //If a metaLocation is null, All of its info would be empty here to be displayed.
                RegionStateNode rsn = master.getAssignmentManager().getRegionStates()
                  .getRegionStateNode(meta);
                ServerName metaLocation = rsn != null ? rsn.getRegionLocation() : null;
                for (int i = 0; i < 1; i++) {
                  //If metaLocation is null, default value below would be displayed in UI.
                  String hostAndPort = "";
                  long compactingCells = 0;
                  long compactedCells = 0;
                  String compactionProgress = "";

                  if (metaLocation != null) {
                    ServerMetrics sl = master.getServerManager().getLoad(metaLocation);
                    hostAndPort = URLEncoder.encode(metaLocation.getHostname(), StandardCharsets.UTF_8.toString()) + ":" + master.getRegionServerInfoPort(metaLocation);
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
  </section>
</div>

<div class="row">
    <h2 id="meta-entries">Meta Entries</h2>
<%
  if (!metaBrowser.getErrorMessages().isEmpty()) {
    for (final String errorMessage : metaBrowser.getErrorMessages()) {
%>
    <div class="alert alert-warning" role="alert">
      <%= errorMessage %>
    </div>
<%
    }
  }

  String regionInfoColumnName = HConstants.CATALOG_FAMILY_STR + ":" + HConstants.REGIONINFO_QUALIFIER_STR;
  String serverColumnName = HConstants.CATALOG_FAMILY_STR + ":" + HConstants.SERVER_QUALIFIER_STR;
  String startCodeColumnName = HConstants.CATALOG_FAMILY_STR + ":" + HConstants.STARTCODE_QUALIFIER_STR;
  String serverNameColumnName = HConstants.CATALOG_FAMILY_STR + ":" + HConstants.SERVERNAME_QUALIFIER_STR;
  String seqNumColumnName = HConstants.CATALOG_FAMILY_STR + ":" + HConstants.SEQNUM_QUALIFIER_STR;
%>
    <div style="overflow-x: auto">
      <table class="table table-striped nowrap">
        <tr>
          <th title="Region name, stored in <%= regionInfoColumnName %> column">RegionName</th>
          <th title="The startKey of this region">Start Key</th>
          <th title="The endKey of this region">End Key</th>
          <th title="Region replica id">Replica ID</th>
          <th title="State of the region while undergoing transitions">RegionState</th>
          <th title="Server hosting this region replica, stored in <%= serverColumnName %> column">Server</th>
          <th title="The seqNum for the region at the time the server opened this region replica, stored in <%= seqNumColumnName %>">Sequence Number</th>
          <th title="The server to which the region is transiting, stored in <%= serverNameColumnName %> column">Target Server</th>
          <th title="The parents regions if this region is undergoing a merge">info:merge*</th>
          <th title="The daughter regions if this region is split">info:split*</th>
        </tr>
<%
  final boolean metaScanHasMore;
  byte[] lastRow = null;
  try (final MetaBrowser.Results results = metaBrowser.getResults()) {
    for (final RegionReplicaInfo regionReplicaInfo : results) {
      lastRow = Optional.ofNullable(regionReplicaInfo)
        .map(RegionReplicaInfo::getRow)
        .orElse(null);
      if (regionReplicaInfo == null) {
%>
        <tr>
          <td colspan="6">Null result</td>
        </tr>
<%
        continue;
      }

      final String regionNameDisplay = regionReplicaInfo.getRegionName() != null
        ? Bytes.toStringBinary(regionReplicaInfo.getRegionName())
        : "";
      final String startKeyDisplay = regionReplicaInfo.getStartKey() != null
        ? Bytes.toStringBinary(regionReplicaInfo.getStartKey())
        : "";
      final String endKeyDisplay = regionReplicaInfo.getEndKey() != null
        ? Bytes.toStringBinary(regionReplicaInfo.getEndKey())
        : "";
      final String replicaIdDisplay = regionReplicaInfo.getReplicaId() != null
        ? regionReplicaInfo.getReplicaId().toString()
        : "";
      final String regionStateDisplay = regionReplicaInfo.getRegionState() != null
        ? regionReplicaInfo.getRegionState().toString()
        : "";

      final RegionInfo regionInfo = regionReplicaInfo.getRegionInfo();
      final ServerName serverName = regionReplicaInfo.getServerName();
      final RegionState.State regionState = regionReplicaInfo.getRegionState();
      final int rsPort = master.getRegionServerInfoPort(serverName);

      final long seqNum = regionReplicaInfo.getSeqNum();

      final String regionSpanFormat = "<span title=" + HConstants.CATALOG_FAMILY_STR + ":%s>%s</span>";
      final String targetServerName = regionReplicaInfo.getTargetServerName() != null
        ? regionReplicaInfo.getTargetServerName().toString()
        : "";
      final Map<String, RegionInfo> mergeRegions = regionReplicaInfo.getMergeRegionInfo();
      final String mergeRegionNames = (mergeRegions == null) ? "" :
        mergeRegions.entrySet().stream()
          .map(entry -> String.format(regionSpanFormat, entry.getKey(), entry.getValue().getRegionNameAsString()))
          .collect(Collectors.joining("<br/>"));
      final Map<String, RegionInfo> splitRegions = regionReplicaInfo.getSplitRegionInfo();
      final String splitName = (splitRegions == null) ? "" :
        splitRegions.entrySet().stream()
          .map(entry -> String.format(regionSpanFormat, entry.getKey(), entry.getValue().getRegionNameAsString()))
          .collect(Collectors.joining("<br/>"));
  %>
    <tr>
      <td title="<%= regionInfoColumnName %>"><%= regionNameDisplay %></td>
      <td title="startKey"><%= startKeyDisplay %></td>
      <td title="endKey"><%= endKeyDisplay %></td>
      <td title="replicaId"><%= replicaIdDisplay %></td>
      <td title="regionState"><%= regionStateDisplay %></td>
      <td title="<%= serverColumnName + "," + startCodeColumnName %>"><%= buildRegionLink(serverName, rsPort, regionInfo, regionState) %></td>
      <td title="<%= seqNumColumnName %>"><%= seqNum %></td>
      <td title="<%= serverNameColumnName %>"><%= targetServerName %></td>
      <td><%= mergeRegionNames %></td>
      <td><%= splitName %></td>
    </tr>
  <%
    }

      metaScanHasMore = results.hasMoreResults();
    }
  %>
  </table>
</div>
</div>
<div class="row mb-5">
  <div class="col-md-4">
    <ul class="pagination" style="margin: 20px 0">
      <li class="page-item">
        <a class="page-link" href="<%= metaBrowser.buildFirstPageUrl() %>" aria-label="First" title="First">
          <span aria-hidden="true">&#x21E4;</span>
        </a>
      </li>
      <li<%= metaScanHasMore ? " class=\"page-item\"" : " class=\"page-item disabled\"" %>>
        <a class="page-link" <%= metaScanHasMore ? " href=\"" + metaBrowser.buildNextPageUrl(lastRow) + "\"" : "" %> aria-label="Next" title="Next">
          <span aria-hidden="true">&raquo;</span>
        </a>
      </li>
    </ul>
  </div>
  <div class="col-md-8">
    <form action="/table.jsp" method="get" class="row g-1 justify-content-end align-items-center" style="margin: 20px 0">
      <input type="hidden" name="name" value="<%= TableName.META_TABLE_NAME %>" />
      <div class="col-sm-auto">
        <label for="scan-limit" class="form-label">Scan Limit</label>
      </div>
      <div class="col-sm-auto">
        <input type="text" id="scan-limit" name="<%= MetaBrowser.SCAN_LIMIT_PARAM %>"
          class="form-control" placeholder="<%= MetaBrowser.SCAN_LIMIT_DEFAULT %>"
          <%= metaBrowser.getScanLimit() != null
            ? "value=\"" + metaBrowser.getScanLimit() + "\""
            : ""
          %>
          aria-describedby="scan-limit" />
      </div>
      <div class="col-sm-auto">
        <label for="table-name-filter" class="form-label">Table</label>
      </div>
      <div class="col-sm-auto">
        <input type="text" id="table-name-filter" name="<%= MetaBrowser.SCAN_TABLE_PARAM %>"
          class="form-control"
          <%= metaBrowser.getScanTable() != null
            ? "value=\"" + metaBrowser.getScanTable() + "\""
            : ""
          %>
          aria-describedby="scan-filter-table" />
      </div>
      <div class="col-sm-auto">
        <label for="region-state-filter" class="form-label">Region State</label>
      </div>
      <div class="col-sm-auto">
        <select class="form-control" id="region-state-filter"
          name="<%= MetaBrowser.SCAN_REGION_STATE_PARAM %>">
          <option></option>
<%
  for (final RegionState.State state : RegionState.State.values()) {
    final boolean selected = metaBrowser.getScanRegionState() == state;
%>
              <option<%= selected ? " selected" : "" %>><%= state %></option>
<%
  }
%>
        </select>
      </div>
      <div class="col-sm-auto">
        <button type="submit" class="btn btn-primary">
          Filter Results
        </button>
      </div>
    </form>
  </div><!--/.col-md-8 -->
</div><!--/.row .mb-5 -->
<%} else {
  //Common tables
  RegionStates states = master.getAssignmentManager().getRegionStates();
  Map<RegionState.State, List<RegionInfo>> regionStates = states.getRegionByStateOfTable(table.getName());
  Map<String, RegionState.State> stateMap = new HashMap<>();
  for (RegionState.State regionState : regionStates.keySet()) {
    for (RegionInfo regionInfo : regionStates.get(regionState)) {
        stateMap.put(regionInfo.getEncodedName(), regionState);
    }
  }
  RegionLocator r = master.getConnection().getRegionLocator(table.getName());

  try {
%>
<h2>Table Attributes</h2>
<table class="table table-striped">
  <tr>
      <th>Attribute Name</th>
      <th>Value</th>
      <th>Description</th>
  </tr>
  <tr>
      <td>Enabled</td>
      <td><%= master.getTableStateManager().isTableState(table.getName(), TableState.State.ENABLED) %></td>
      <td>Is the table enabled</td>
  </tr>
  <tr>
      <td>Compaction</td>
      <td>
<%
  if (master.getTableStateManager().isTableState(table.getName(), TableState.State.ENABLED)) {
    CompactionState compactionState = master.getCompactionState(table.getName());
    %><%= compactionState==null?"UNKNOWN":compactionState %><%
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
    <%
      ColumnFamilyDescriptor[] families = table.getDescriptor().getColumnFamilies();
      Set<Bytes> familyKeySet = new HashSet<>();
      for (ColumnFamilyDescriptor family: families) {
        familyKeySet.addAll(family.getValues().keySet());
      }
    %>
      <tr>
        <th>Property \ Column Family Name</th>
        <%
        for (ColumnFamilyDescriptor family: families) {
        %>
        <th>
          <%= StringEscapeUtils.escapeHtml4(family.getNameAsString()) %>
        </th>
        <% } %>
      </tr>
        <%
        for (Bytes familyKey: familyKeySet) {
        %>
          <tr>
            <td>
              <%= StringEscapeUtils.escapeHtml4(familyKey.toString()) %>
            </td>
            <%
            for (ColumnFamilyDescriptor family: families) {
              String familyValue = "-";
              if(family.getValues().containsKey(familyKey)){
                familyValue = family.getValues().get(familyKey).toString();
              }
            %>
            <td>
              <%= StringEscapeUtils.escapeHtml4(familyValue) %>
            </td>
            <% } %>
          </tr>
        <% } %>
    </table>
    <%
      long totalReadReq = 0;
      long totalWriteReq = 0;
      long totalSizeUncompressed = 0;
      long totalSize = 0;
      long totalStoreFileCount = 0;
      long totalMemSize = 0;
      long totalCompactingCells = 0;
      long totalCompactedCells = 0;
      long totalBlocksTotalWeight = 0;
      long totalBlocksLocalWeight = 0;
      long totalBlocksLocalWithSsdWeight = 0;
      String totalCompactionProgress = "";
      String totalMemSizeStr = ZEROMB;
      String totalSizeUncompressedStr = ZEROMB;
      String totalSizeStr = ZEROMB;
      String totalLocality = "";
      String totalLocalityForSsd = "";
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
          totalSizeUncompressed += regionMetrics.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE);
          totalSize += regionMetrics.getStoreFileSize().get(Size.Unit.MEGABYTE);
          totalStoreFileCount += regionMetrics.getStoreFileCount();
          totalMemSize += regionMetrics.getMemStoreSize().get(Size.Unit.MEGABYTE);
          totalStoreFileSizeMB += regionMetrics.getStoreFileSize().get(Size.Unit.MEGABYTE);
          totalCompactingCells += regionMetrics.getCompactingCellCount();
          totalCompactedCells += regionMetrics.getCompactedCellCount();
          totalBlocksTotalWeight += regionMetrics.getBlocksTotalWeight();
          totalBlocksLocalWeight += regionMetrics.getBlocksLocalWeight();
          totalBlocksLocalWithSsdWeight += regionMetrics.getBlocksLocalWithSsdWeight();
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
  if (totalSizeUncompressed > 0){
    totalSizeUncompressedStr = StringUtils.byteDesc(totalSizeUncompressed*1024l*1024);
  }
  if (totalMemSize > 0) {
    totalMemSizeStr = StringUtils.byteDesc(totalMemSize*1024l*1024);
  }
  if (totalCompactingCells > 0) {
    totalCompactionProgress = String.format("%.2f", 100 *
            ((float) totalCompactedCells / totalCompactingCells)) + "%";
  }
  if (totalBlocksTotalWeight > 0) {
    DecimalFormat df = new DecimalFormat("0.0#");
    df.setRoundingMode(RoundingMode.DOWN);
    totalLocality = df.format(((float) totalBlocksLocalWeight / totalBlocksTotalWeight));
    totalLocalityForSsd = df.format(((float) totalBlocksLocalWithSsdWeight / totalBlocksTotalWeight));
  }
  if(regions != null && regions.size() > 0) { %>
<div class="row">
  <div class="col">
    <section>
      <h2>Table Regions</h2>
      <div class="tabbable">
        <ul class="nav nav-pills" role="tablist">
          <li class="nav-item"><a class="nav-link active" href="#tab_baseStats" data-bs-toggle="tab" role="tab">Base Stats</a></li>
          <li class="nav-item"><a class="nav-link" href="#tab_localityStats" data-bs-toggle="tab" role="tab">Localities</a></li>
          <li class="nav-item"><a class="nav-link" href="#tab_compactStats" data-bs-toggle="tab" role="tab">Compactions</a></li>
        </ul>
        <div class="tab-content">
          <div class="tab-pane active" id="tab_baseStats" role="tabpanel">
            <table id="tableBaseStatsTable" class="tablesorter table table-striped">
              <thead>
                <tr>
                  <th>Name(<%= String.format("%,1d", regions.size())%>)</th>
                  <th>Region Server</th>
                  <th class="cls_separator">ReadRequests<br>(<%= String.format("%,1d", totalReadReq)%>)</th>
                  <th class="cls_separator">WriteRequests<br>(<%= String.format("%,1d", totalWriteReq)%>)</th>
                  <th class="cls_filesize">Uncompressed StoreFileSize<br>(<%= totalSizeUncompressedStr %>)</th>
                  <th class="cls_filesize">StorefileSize<br>(<%= totalSizeStr %>)</th>
                  <th class="cls_separator">Num.Storefiles<br>(<%= String.format("%,1d", totalStoreFileCount)%>)</th>
                  <th class="cls_filesize">MemSize<br>(<%= totalMemSizeStr %>)</th>
                  <th class="cls_emptyMin">Start Key</th>
                  <th class="cls_emptyMax">End Key</th>
                  <th>Region State</th>
                  <th>ReplicaID</th>
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
                  String regionSizeUncompressed = ZEROMB;
                  String regionSize = ZEROMB;
                  String fileCount = "N/A";
                  String memSize = ZEROMB;
                  String state = "N/A";
                  if (load != null) {
                    readReq = String.format("%,1d", load.getReadRequestCount());
                    writeReq = String.format("%,1d", load.getWriteRequestCount());
                    double rSize = load.getStoreFileSize().get(Size.Unit.BYTE);
                    if (rSize > 0) {
                      regionSize = StringUtils.byteDesc((long)rSize);
                    }
                    double rSizeUncompressed = load.getUncompressedStoreFileSize().get(Size.Unit.BYTE);
                    if (rSizeUncompressed > 0) {
                      regionSizeUncompressed = StringUtils.byteDesc((long)rSizeUncompressed);
                    }
                    fileCount = String.format("%,1d", load.getStoreFileCount());
                    double mSize = load.getMemStoreSize().get(Size.Unit.BYTE);
                    if (mSize > 0) {
                      memSize = StringUtils.byteDesc((long)mSize);
                    }
                  }

                  if (stateMap.containsKey(regionInfo.getEncodedName())) {
                    state = stateMap.get(regionInfo.getEncodedName()).toString();
                  }

                  if (addr != null) {
                    ServerMetrics sl = master.getServerManager().getLoad(addr);
                    if(sl != null) {
                      Integer i = regDistribution.get(addr);
                      if (null == i) i = Integer.valueOf(0);
                      regDistribution.put(addr, i + 1);
                      if (RegionReplicaUtil.isDefaultReplica(regionInfo.getReplicaId())) {
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
                <%= buildRegionDeployedServerTag(regionInfo, master, regionsToServer) %>
                <td><%= readReq%></td>
                <td><%= writeReq%></td>
                <td><%= regionSizeUncompressed%></td>
                <td><%= regionSize%></td>
                <td><%= fileCount%></td>
                <td><%= memSize%></td>
                <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getStartKey()))%></td>
                <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getEndKey()))%></td>
                <td><%= state%></td>
                <td><%= regionInfo.getReplicaId() %></td>
              </tr>
              <% } %>
              <% } %>
              </tbody>
            </table>
            <%= moreRegionsToRender(numRegionsRendered, numRegions, fqtn) %>
          </div>
          <div class="tab-pane" id="tab_localityStats" role="tabpanel">
            <table id="tableLocalityStatsTable" class="tablesorter table table-striped">
              <thead>
                <tr>
                  <th>Name(<%= String.format("%,1d", regions.size())%>)</th>
                  <th>Region Server</th>
                  <th class="cls_separator">Locality<br>(<%= totalLocality %>)</th>
                  <th class="cls_separator">LocalityForSsd<br>(<%= totalLocalityForSsd %>)</th>
                </tr>
              </thead>
              <tbody>
              <%
                numRegionsRendered = 0;
                for (Map.Entry<RegionInfo, RegionMetrics> hriEntry : entryList) {
                  RegionInfo regionInfo = hriEntry.getKey();
                  RegionMetrics load = hriEntry.getValue();
                  float locality = 0.0f;
                  float localityForSsd = 0.0f;
                  if (load != null) {
                    locality = load.getDataLocality();
                    localityForSsd = load.getDataLocalityForSsd();
                  }

                  if (numRegionsRendered < numRegionsToRender) {
                    numRegionsRendered++;
              %>
              <tr>
                <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getRegionName())) %></td>
                <%= buildRegionDeployedServerTag(regionInfo, master, regionsToServer) %>
                <td><%= locality%></td>
                <td><%= localityForSsd%></td>
              </tr>
              <% } %>
              <% } %>
              </tbody>
            </table>
            <%= moreRegionsToRender(numRegionsRendered, numRegions, fqtn) %>
          </div>
          <div class="tab-pane" id="tab_compactStats" role="tabpanel">
            <table id="tableCompactStatsTable" class="tablesorter table table-striped">
              <thead>
                <tr>
                  <th>Name(<%= String.format("%,1d", regions.size())%>)</th>
                  <th>Region Server</th>
                  <th class="cls_separator">Num. Compacting Cells<br>(<%= String.format("%,1d", totalCompactingCells)%>)</th>
                  <th class="cls_separator">Num. Compacted Cells<br>(<%= String.format("%,1d", totalCompactedCells)%>)</th>
                  <th class="cls_separator">Remaining Cells<br>(<%= String.format("%,1d", totalCompactingCells-totalCompactedCells)%>)</th>
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

                  if (numRegionsRendered < numRegionsToRender) {
                    numRegionsRendered++;
              %>
              <tr>
                <td><%= escapeXml(Bytes.toStringBinary(regionInfo.getRegionName())) %></td>
                <%= buildRegionDeployedServerTag(regionInfo, master, regionsToServer) %>
                <td><%= String.format("%,1d", compactingCells)%></td>
                <td><%= String.format("%,1d", compactedCells)%></td>
                <td><%= String.format("%,1d", compactingCells - compactedCells)%></td>
                <td><%= compactionProgress%></td>
              </tr>
              <% } %>
              <% } %>
              </tbody>
            </table>
            <%= moreRegionsToRender(numRegionsRendered, numRegions, fqtn) %>
          </div>
        </div>
      </div>
    </section>
  </div>
</div>

<section>
<h2>Regions by Region Server</h2>
<table id="regionServerTable" class="tablesorter table table-striped">
  <thead>
    <tr>
      <th>Region Server</th>
      <th class="cls_separator">Region Count</th>
      <th>Primary Region Count</th>
    </tr>
  </thead>

  <tbody>
  <%
    for (Map.Entry<ServerName, Integer> rdEntry : regDistribution.entrySet()) {
      ServerName addr = rdEntry.getKey();
      String url = "//" + URLEncoder.encode(addr.getHostname(), StandardCharsets.UTF_8.toString()) + ":"
        + master.getRegionServerInfoPort(addr) + "/rs-status";
  %>
      <tr>
        <td><a href="<%= url %>"><%= StringEscapeUtils.escapeHtml4(addr.getHostname())
          + ":" + master.getRegionServerInfoPort(addr) %></a></td>
        <td><%= rdEntry.getValue()%></td>
        <td><%= primaryRegDistribution.get(addr) == null ? 0 : primaryRegDistribution.get(addr)%></td>
      </tr>
  <% } %>
  </tbody>
</table>

<% }
} catch(Exception ex) { %>
  Unknown Issue with Regions
  <div onclick="document.getElementById('closeStackTrace').style.display='block';document.getElementById('openStackTrace').style.display='none';">
    <a id="openStackTrace" style="cursor:pointer;"> Show StackTrace</a>
  </div>
  <div id="closeStackTrace" style="display:none;clear:both;">
    <div onclick="document.getElementById('closeStackTrace').style.display='none';document.getElementById('openStackTrace').style.display='block';">
      <a style="cursor:pointer;"> Close StackTrace</a>
    </div>
  <%
  for(StackTraceElement element : ex.getStackTrace()) {
    %><%= StringEscapeUtils.escapeHtml4(element.toString() + "\n") %><%
  }
} finally {
  connection.close();
}
} // end else
%>
</section>

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

<h2>Actions</h2>

<table class="table">
  <tr>
    <form method="get">
      <input type="hidden" name="action" value="major compact" />
      <input type="hidden" name="name" value="<%= escaped_fqtn %>" />
      <td class="centered">
        <input type="submit" value="Major Compact" class="btn btn-secondary" />
      </td>
      <td style="text-align: center;">
        <input type="text" class="form-control" name="key" size="40" placeholder="Row Key (optional)" />
      </td>
      <td>
      This action will force a major compaction of all regions of the table, or,
      if a key is supplied, only the region major containing the
      given key.
      </td>
    </form>
  </tr>
  <tr>
    <form method="get">
      <input type="hidden" name="action" value="compact" />
      <input type="hidden" name="name" value="<%= escaped_fqtn %>" />
      <td class="centered">
        <input type="submit" value="Compact" class="btn btn-secondary" />
      </td>
      <td style="text-align: center;">
        <input type="text" class="form-control" name="key" size="40" placeholder="Row Key (optional)" />
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
        <input type="submit" value="Split" class="btn btn-secondary" />
      </td>
      <td style="text-align: center;">
        <input type="text" class="form-control" name="key" size="40" placeholder="Row Key (optional)" />
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
      <input type="submit" value="Merge" class="btn btn-secondary" />
    </td>
    <td style="text-align: center;">
      <input type="text" class="form-control mb-2" name="left" size="40" required="required" placeholder="Region Key (required)" />
      <input type="text" class="form-control" name="right" size="40" required="required" placeholder="Region Key (required)" />
    </td>
    <td>
      This action will merge two regions of the table, Merge requests for
      noneligible regions will be ignored.
    </td>
    </form>
  </tr>
</table>

<% } %>
</div><!--/.row -->
</div> <!--/.container-fluid -->

<jsp:include page="scripts.jsp" />
<script src="/static/js/jquery.tablesorter.min.js" type="text/javascript"></script>

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
                '.cls_separator': {sorter: 'separator'}
            }
        });
        $("#tableBaseStatsTable").tablesorter({
            headers: {
                '.cls_separator': {sorter: 'separator'},
                '.cls_filesize': {sorter: 'filesize'},
                '.cls_emptyMin': {empty: 'emptyMin'},
                '.cls_emptyMax': {empty: 'emptyMax'}
            }
        });
        $("#metaTableBaseStatsTable").tablesorter({
            headers: {
                '.cls_separator': {sorter: 'separator'},
                '.cls_filesize': {sorter: 'filesize'},
                '.cls_emptyMin': {empty: 'emptyMin'},
                '.cls_emptyMax': {empty: 'emptyMax'}
            }
        });
        $("#tableLocalityStatsTable").tablesorter({
            headers: {
               '.cls_separator': {sorter: 'separator'}
            }
        });
        $("#metaTableLocalityStatsTable").tablesorter({
            headers: {
                '.cls_separator': {sorter: 'separator'}
            }
        });
        $("#tableCompactStatsTable").tablesorter({
            headers: {
                '.cls_separator': {sorter: 'separator'}
            }
        });
        $("#metaTableCompactStatsTable").tablesorter({
            headers: {
                '.cls_separator': {sorter: 'separator'}
            }
        });
    }
);
</script>
</body>
</html>
