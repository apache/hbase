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
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.conf.Configuration"
         import="org.apache.hadoop.hbase.util.*"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import="org.apache.hadoop.hbase.client.RegionInfo"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.HBaseConfiguration"
         import="org.apache.hadoop.hbase.io.hfile.CacheConfig"
         import="org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil"
         import="org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.ServerInfo"
         import="org.apache.hadoop.hbase.util.JvmVersion"
         import="org.apache.hadoop.hbase.zookeeper.MasterAddressTracker" %>

<%!
  public String formatZKString(HRegionServer regionServer) {
    StringBuilder quorums = new StringBuilder();
    String zkQuorum = regionServer.getZooKeeper().getQuorum();

    if (null == zkQuorum) {
      return quorums.toString();
    }

    String[] zks = zkQuorum.split(",");

    if (zks.length == 0) {
      return quorums.toString();
    }

    for(int i = 0; i < zks.length; ++i) {
      quorums.append(zks[i].trim());

      if (i != (zks.length - 1)) {
        quorums.append("<br/>");
      }
    }

    return quorums.toString();
  }
%>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  String format = request.getParameter("format");
  if (format == null) {
    format = "html";
  }
  if ("json".equals(format)) {
    response.setContentType("application/json");
  } else {
    response.setContentType("text/html");
  }

  if (!regionServer.isOnline()) {
    response.getWriter().write("The RegionServer is initializing!");
    response.getWriter().close();
    return;
  }

  String filter = request.getParameter("filter");
  if (filter == null) {
    filter = "general";
  }
  String bcn = request.getParameter("bcn");
  String bcv = request.getParameter("bcv");
%>

<%-- If json AND bcn is NOT an empty string presume it a block cache view request. --%>
<% if (format.equals("json") && bcn != null && bcn.length() > 0) {  %>
  <%-- TODO: Migrate BlockCacheViewTmpl  --%>
  <& BlockCacheViewTmpl; conf = regionServer.getConfiguration(); cacheConfig = new CacheConfig(regionServer.getConfiguration()); bcn = bcn; bcv = bcv; blockCache = regionServer.getBlockCache().orElse(null)  &>
<%
    return;
  }
  else if (format.equals("json")) {
    request.setAttribute(MasterStatusConstants.FILTER, filter);
    request.setAttribute(MasterStatusConstants.FORMAT, "json"); %>
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
      <& BlockCacheTmpl; cacheConfig = new CacheConfig(regionServer.getConfiguration()); config = regionServer.getConfiguration(); bc = regionServer.getBlockCache().orElse(null) &>
    </section>

  </div>
</div> <!-- /.container-fluid content -->

<jsp:include page="footer.jsp"/>


