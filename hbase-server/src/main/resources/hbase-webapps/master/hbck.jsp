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
         import="java.util.Date"
         import="java.util.List"
         import="java.util.Map"
         import="java.util.stream.Collectors"
%>
<%@ page import="org.apache.hadoop.hbase.master.HbckChecker" %>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.ServerName" %>
<%@ page import="org.apache.hadoop.hbase.util.Pair" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master HBCK Report: " + master.getServerName());
  HbckChecker hbckChecker = master.getHbckChecker();
  Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions = null;
  Map<String, ServerName> orphanRegionsOnRS = null;
  List<String> orphanRegionsOnFS = null;
  long startTimestamp = 0;
  long endTimestamp = 0;
  if (hbckChecker != null) {
    inconsistentRegions = hbckChecker.getInconsistentRegions();
    orphanRegionsOnRS = hbckChecker.getOrphanRegionsOnRS();
    orphanRegionsOnFS = hbckChecker.getOrphanRegionsOnFS();
    startTimestamp = hbckChecker.getCheckingStartTimestamp();
    endTimestamp = hbckChecker.getCheckingEndTimestamp();
  }
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">

  <% if (!master.isInitialized()) { %>
  <div class="row">
    <div class="page-header">
      <h1>Master is not initialized</h1>
    </div>
  </div>
  <jsp:include page="redirect.jsp" />
  <% } else { %>

  <div class="row">
    <div class="page-header">
      <h1>HBCK Report</h1>
      <p>
        <span>Checking started at <%= new Date(startTimestamp) %> and generated report at <%= new Date(endTimestamp) %></span>
      </p>
    </div>
  </div>

  <div class="row">
    <div class="page-header">
      <h2>Inconsistent Regions</h2>
      <p>
        <span>
        There are three case: 1. Master thought this region opened, but no regionserver reported it.
        2. Master thought this region opened on Server1, but regionserver reported Server2.
        3. More than one regionservers reported opened this region.
        Notice: the reported online regionservers may be not right when there are regions in transition.
        Please check them in regionserver's web UI.
        </span>
      </p>
    </div>
  </div>

  <% if (inconsistentRegions != null && inconsistentRegions.size() > 0) { %>
  <table class="table table-striped">
    <tr>
      <th>Region</th>
      <th>Location in META</th>
      <th>Reported Online RegionServers</th>
    </tr>
    <% for (Map.Entry<String, Pair<ServerName, List<ServerName>>> entry : inconsistentRegions.entrySet()) { %>
    <tr>
      <td><%= entry.getKey() %></td>
      <td><%= entry.getValue().getFirst() %></td>
      <td><%= entry.getValue().getSecond().stream().map(ServerName::getServerName)
                        .collect(Collectors.joining(", ")) %></td>
    </tr>
    <% } %>

    <p><%= inconsistentRegions.size() %> region(s) in set.</p>
  </table>
  <% } %>

  <div class="row">
    <div class="page-header">
      <h2>Orphan Regions on RegionServer</h2>
    </div>
  </div>

  <% if (orphanRegionsOnRS != null && orphanRegionsOnRS.size() > 0) { %>
  <table class="table table-striped">
    <tr>
      <th>Region</th>
      <th>Reported Online RegionServer</th>
    </tr>
    <% for (Map.Entry<String, ServerName> entry : orphanRegionsOnRS.entrySet()) { %>
    <tr>
      <td><%= entry.getKey() %></td>
      <td><%= entry.getValue() %></td>
    </tr>
    <% } %>

    <p><%= orphanRegionsOnRS.size() %> region(s) in set.</p>
  </table>
  <% } %>

  <div class="row">
    <div class="page-header">
      <h2>Orphan Regions on FileSystem</h2>
    </div>
  </div>

  <% if (orphanRegionsOnFS != null && orphanRegionsOnFS.size() > 0) { %>
  <table class="table table-striped">
    <tr>
      <th>Region</th>
    </tr>
    <% for (String region : orphanRegionsOnFS) { %>
    <tr>
      <td><%= region %></td>
    </tr>
    <% } %>

    <p><%= orphanRegionsOnFS.size() %> region(s) in set.</p>
  </table>
  <% } %>

  <% } %>
</div>

<jsp:include page="footer.jsp"/>