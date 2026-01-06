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
         import="java.util.*"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.util.*"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import="org.apache.hadoop.hbase.client.RegionInfo"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil"
         import="org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ServerInfo"
         import="org.apache.hadoop.hbase.zookeeper.MasterAddressTracker"
         import="org.apache.hadoop.hbase.regionserver.http.RSStatusConstants" %>
<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  String format = request.getParameter(RSStatusConstants.FORMAT);
  if (format == null) {
    format = RSStatusConstants.FORMAT_HTML;
  }
  if (RSStatusConstants.FORMAT_JSON.equals(format)) {
    response.setContentType("application/json");
  } else {
    response.setContentType("text/html;charset=UTF-8");
  }

  if (!regionServer.isOnline()) {
    response.getWriter().write("The RegionServer is initializing!");
    response.getWriter().close();
    return;
  }

  String filter = request.getParameter(RSStatusConstants.FILTER);
  if (filter == null) {
    filter = RSStatusConstants.FILTER_GENERAL;
  }
  String bcn = request.getParameter(RSStatusConstants.BLOCK_CACHE_NAME);
  String bcv = request.getParameter(RSStatusConstants.BLOCK_CACHE_V);
  if (bcv == null) {
    bcv = "";
  }
  // If json AND bcn is NOT an empty string presume it a block cache view request.
  if (format.equals(RSStatusConstants.FORMAT_JSON) && bcn != null && bcn.length() > 0) {
    request.setAttribute("conf", regionServer.getConfiguration());
    request.setAttribute(RSStatusConstants.BLOCK_CACHE_NAME, bcn);
    request.setAttribute(RSStatusConstants.BLOCK_CACHE_V, bcv);
    request.setAttribute("blockCache", regionServer.getBlockCache().orElse(null));
%><jsp:include page="blockCacheView.jsp"/>
<%
    return;
  }
  else if (format.equals(RSStatusConstants.FORMAT_JSON)) {
    request.setAttribute(RSStatusConstants.FILTER, filter);
    request.setAttribute(RSStatusConstants.FORMAT, RSStatusConstants.FORMAT_JSON); %>
  <jsp:include page="taskMonitor.jsp"/>
<%
    return;
  }

  ServerInfo serverInfo = ProtobufUtil.getServerInfo(null, regionServer.getRSRpcServices());
  ServerName serverName = ProtobufUtil.toServerName(serverInfo.getServerName());
  List<RegionInfo> onlineRegions = ProtobufUtil.getOnlineRegions(regionServer.getRSRpcServices());
  MasterAddressTracker masterAddressTracker = regionServer.getMasterAddressTracker();
  ServerName masterServerName = masterAddressTracker == null ? null
    : masterAddressTracker.getMasterAddress();
  int infoPort = masterAddressTracker == null ? 0 : masterAddressTracker.getMasterInfoPort();

  String title = "HBase Region Server: " + serverName.getHostname();
  pageContext.setAttribute("pageTitle", title);
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>RegionServer <small><%= serverName %></small></h1>
    </div>
  </div>
  <div class="row">

    <section>
      <h2>Server Metrics</h2>
      <jsp:include page="serverMetrics.jsp"/>
    </section>

    <section>
      <h2>Block Cache</h2>
      <jsp:include page="blockCache.jsp"/>
    </section>

    <section>
      <% request.setAttribute(RSStatusConstants.PARENT, "/regionserver.jsp"); %>
      <jsp:include page="taskMonitor.jsp"/>
    </section>

    <section>
      <h2>Regions</h2>
      <% request.setAttribute("onlineRegions", onlineRegions); %>
      <jsp:include page="regionList.jsp"/>
    </section>

    <section>
      <h2>Replication Status</h2>
      <jsp:include page="replicationStatus.jsp"/>
    </section>

    <section>
      <h2>Software Attributes</h2>
      <% request.setAttribute("masterServerName", masterServerName); %>
      <% request.setAttribute("infoPort", infoPort); %>
      <jsp:include page="softwareAttributes.jsp"/>
    </section>

  </div> <!--/.row -->
</div> <!-- /.container-fluid content -->

  <jsp:include page="scripts.jsp" />
  <script src="/static/js/jquery.tablesorter.min.js" type="text/javascript"></script>
  <script src ="/static/js/regionServerStatusInit.js" type="text/javascript"></script>
</body>
</html>
