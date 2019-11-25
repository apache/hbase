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
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.client.SnapshotDescription"
  import="org.apache.hadoop.hbase.http.InfoServer"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.snapshot.SnapshotInfo"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.hbase.TableName"
%>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  boolean readOnly = !InfoServer.canUserModifyUI(request, getServletContext(), conf);
  String snapshotName = request.getParameter("name");
  SnapshotDescription snapshot = null;
  SnapshotInfo.SnapshotStats stats = null;
  TableName snapshotTable = null;
  boolean tableExists = false;
  long snapshotTtl = 0;
  if(snapshotName != null && master.isInitialized()) {
    try (Admin admin = master.getConnection().getAdmin()) {
      for (SnapshotDescription snapshotDesc: admin.listSnapshots()) {
        if (snapshotName.equals(snapshotDesc.getName())) {
          snapshot = snapshotDesc;
          stats = SnapshotInfo.getSnapshotStats(conf, snapshot);
          snapshotTable = snapshot.getTableName();
          snapshotTtl = snapshot.getTtl();
          tableExists = admin.tableExists(snapshotTable);
          break;
        }
      }
    }
  }

  String action = request.getParameter("action");
  boolean isActionResultPage = (!readOnly && action != null);
  String pageTitle;
  if (isActionResultPage) {
    pageTitle = "HBase Master: " + master.getServerName();
  } else {
    pageTitle = "Snapshot: " + snapshotName;
  }
  pageContext.setAttribute("pageTitle", pageTitle);
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
<% if (!master.isInitialized()) { %>
    <div class="row inner_header">
    <div class="page-header">
    <h1>Master is not initialized</h1>
    </div>
    </div>
    <jsp:include page="redirect.jsp" />
<% } else if (snapshot == null) { %>
  <div class="row inner_header">
    <div class="page-header">
      <h1>Snapshot "<%= snapshotName %>" does not exist</h1>
    </div>
  </div>
  <jsp:include page="redirect.jsp" />
<% } else { %>
  <div class="row">
      <div class="page-header">
          <h1>Snapshot: <%= snapshotName %></h1>
      </div>
  </div>
  <h2>Snapshot Attributes</h2>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>Table</th>
        <th>Creation Time</th>
        <th>Time To Live(Sec)</th>
        <th>Type</th>
        <th>Format Version</th>
        <th>State</th>
    </tr>
    <tr>

        <td>
          <% if (tableExists) { %>
            <a href="table.jsp?name=<%= snapshotTable.getNameAsString() %>">
              <%= snapshotTable.getNameAsString() %></a>
          <% } else { %>
            <%= snapshotTable.getNameAsString() %>
          <% } %>
        </td>
        <td><%= new Date(snapshot.getCreationTime()) %></td>
        <td>
          <% if (snapshotTtl == 0) { %>
            FOREVER
          <% } else { %>
            <%= snapshotTtl %>
          <% } %>
        </td>
        <td><%= snapshot.getType() %></td>
        <td><%= snapshot.getVersion() %></td>
        <% if (stats.isSnapshotCorrupted()) { %>
          <td style="font-weight: bold; color: #dd0000;">CORRUPTED</td>
        <% } else { %>
          <td>ok</td>
        <% } %>
    </tr>
  </table>
  <div class="row">
    <div class="span12">
    <%= stats.getStoreFilesCount() %> HFiles (<%= stats.getArchivedStoreFilesCount() %> in archive),
    total size <%= StringUtils.humanReadableInt(stats.getStoreFilesSize()) %>
    (<%= stats.getSharedStoreFilePercentage() %>&#37;
    <%= StringUtils.humanReadableInt(stats.getSharedStoreFilesSize()) %> shared with the source
    table)
    </div>
    <div class="span12">
    <%= stats.getLogsCount() %> Logs, total size
    <%= StringUtils.humanReadableInt(stats.getLogsSize()) %>
    </div>
  </div>
  <% if (stats.isSnapshotCorrupted()) { %>
    <div class="row">
      <div class="span12">
          <h3>CORRUPTED Snapshot</h3>
      </div>
      <div class="span12">
        <%= stats.getMissingStoreFilesCount() %> hfile(s) and
        <%= stats.getMissingLogsCount() %> log(s) missing.
      </div>
    </div>
  <% } %>
<%
  } // end else
%>
</div>

<jsp:include page="footer.jsp" />
