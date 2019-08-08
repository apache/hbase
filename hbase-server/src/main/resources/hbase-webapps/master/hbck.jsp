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
         import="java.time.Instant"
         import="java.time.ZoneId"
         import="java.util.Date"
         import="java.util.List"
         import="java.util.Map"
         import="java.util.Set"
         import="java.util.stream.Collectors"
         import="java.time.ZonedDateTime"
         import="java.time.format.DateTimeFormatter"
%>
<%@ page import="org.apache.hadoop.hbase.client.RegionInfo" %>
<%@ page import="org.apache.hadoop.hbase.master.HbckChore" %>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.ServerName" %>
<%@ page import="org.apache.hadoop.hbase.util.Bytes" %>
<%@ page import="org.apache.hadoop.hbase.util.Pair" %>
<%@ page import="org.apache.hadoop.hbase.master.CatalogJanitor" %>
<%@ page import="org.apache.hadoop.hbase.master.CatalogJanitor.Report" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master HBCK Report: " + master.getServerName());
  HbckChore hbckChore = master.getHbckChore();
  Map<String, Pair<ServerName, List<ServerName>>> inconsistentRegions = null;
  Map<String, ServerName> orphanRegionsOnRS = null;
  Set<String> orphanRegionsOnFS = null;
  long startTimestamp = 0;
  long endTimestamp = 0;
  if (hbckChore != null) {
    inconsistentRegions = hbckChore.getInconsistentRegions();
    orphanRegionsOnRS = hbckChore.getOrphanRegionsOnRS();
    orphanRegionsOnFS = hbckChore.getOrphanRegionsOnFS();
    startTimestamp = hbckChore.getCheckingStartTimestamp();
    endTimestamp = hbckChore.getCheckingEndTimestamp();
  }
  ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTimestamp),
    ZoneId.systemDefault());
  String iso8601start = startTimestamp == 0? "-1": zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTimestamp),
    ZoneId.systemDefault());
  String iso8601end = startTimestamp == 0? "-1": zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  CatalogJanitor cj = master.getCatalogJanitor();
  CatalogJanitor.Report report = cj == null? null: cj.getLastReport();
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
      <h1>HBCK Chore Report</h1>
      <p>
        <span>Checking started at <%= iso8601start %> and generated report at <%= iso8601end %>. Execute 'hbck_chore_run' in hbase shell to generate a new sub-report.</span>
      </p>
    </div>
  </div>


  <div class="row">
    <div class="page-header">
      <h2>Inconsistent Regions</h2>
    </div>
  </div>

  <% if (inconsistentRegions != null && inconsistentRegions.size() > 0) { %>
      <p>
        <span>
        There are three cases: 1. Master thought this region opened, but no regionserver reported it (Fix: use assigns
        command; 2. Master thought this region opened on Server1, but regionserver reported Server2 (Fix:
        need to check the server is still exist. If not, schedule SCP for it. If exist, restart Server2 and Server1):
        3. More than one regionservers reported opened this region (Fix: restart the RegionServers).
        Notice: the reported online regionservers may be not right when there are regions in transition.
        Please check them in regionserver's web UI.
        </span>
      </p>

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

  <div class="row inner_header">
    <div class="page-header">
      <h1>CatalogJanitor <em>hbase:meta</em> Consistency Issues</h1>
    </div>
  </div>
  <% if (report != null && !report.isEmpty()) {
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(report.getCreateTime()),
      ZoneId.systemDefault());
    String iso8601reportTime = zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()),
      ZoneId.systemDefault());
    String iso8601Now = zdt.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  %>
  <p>Report created: <%= iso8601reportTime %> (now=<%= iso8601Now %>). Run <i>catalogjanitor_run</i> in hbase shell to generate a new sub-report.</p>
      <% if (!report.getHoles().isEmpty()) { %>
          <div class="row inner_header">
            <div class="page-header">
              <h2>Holes</h2>
            </div>
          </div>
          <table class="table table-striped">
            <tr>
              <th>RegionInfo</th>
              <th>RegionInfo</th>
            </tr>
            <% for (Pair<RegionInfo, RegionInfo> p : report.getHoles()) { %>
            <tr>
              <td><%= p.getFirst() %></td>
              <td><%= p.getSecond() %></td>
            </tr>
            <% } %>

            <p><%= report.getHoles().size() %> hole(s).</p>
          </table>
      <% } %>
      <% if (!report.getOverlaps().isEmpty()) { %>
            <div class="row inner_header">
              <div class="page-header">
                <h2>Overlaps</h2>
              </div>
            </div>
            <table class="table table-striped">
              <tr>
                <th>RegionInfo</th>
                <th>Other RegionInfo</th>
              </tr>
              <% for (Pair<RegionInfo, RegionInfo> p : report.getOverlaps()) { %>
              <tr>
                <td><%= p.getFirst() %></td>
                <td><%= p.getSecond() %></td>
              </tr>
              <% } %>

              <p><%= report.getOverlaps().size() %> overlap(s).</p>
            </table>
      <% } %>
      <% if (!report.getUnknownServers().isEmpty()) { %>
            <div class="row inner_header">
              <div class="page-header">
                <h2>Unknown Servers</h2>
              </div>
            </div>
            <table class="table table-striped">
              <tr>
                <th>RegionInfo</th>
                <th>ServerName</th>
              </tr>
              <% for (Pair<RegionInfo, ServerName> p: report.getUnknownServers()) { %>
              <tr>
                <td><%= p.getFirst() %></td>
                <td><%= p.getSecond() %></td>
              </tr>
              <% } %>

              <p><%= report.getUnknownServers().size() %> unknown servers(s).</p>
            </table>
      <% } %>
      <% if (!report.getEmptyRegionInfo().isEmpty()) { %>
            <div class="row inner_header">
              <div class="page-header">
                <h2>Empty <em>info:regioninfo</em></h2>
              </div>
            </div>
            <table class="table table-striped">
              <tr>
                <th>Row</th>
              </tr>
              <% for (byte [] row: report.getEmptyRegionInfo()) { %>
              <tr>
                <td><%= Bytes.toStringBinary(row) %></td>
              </tr>
              <% } %>

              <p><%= report.getEmptyRegionInfo().size() %> emptyRegionInfo(s).</p>
            </table>
      <% } %>
  <% } %>

  <% } %>
</div>

<jsp:include page="footer.jsp"/>
