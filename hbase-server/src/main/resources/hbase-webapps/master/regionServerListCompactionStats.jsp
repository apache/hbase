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
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.RegionMetrics"
         import="org.apache.hadoop.hbase.master.http.MasterStatusConstants"
         import="org.apache.hadoop.hbase.master.http.MasterStatusUtil" %>

<%
  ServerName[] serverNames = (ServerName[]) request.getAttribute(MasterStatusConstants.SERVER_NAMES);
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
%>

<table id="compactionStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th>ServerName</th>
    <th class="cls_separator">Num. Compacting Cells</th>
    <th class="cls_separator">Num. Compacted Cells</th>
    <th class="cls_separator">Remaining Cells</th>
    <th>Compaction Progress</th>
  </tr>
  </thead>
  <tbody>
    <%
for (ServerName serverName: serverNames) {

ServerMetrics sl = master.getServerManager().getLoad(serverName);
if (sl != null) {
long totalCompactingCells = 0;
long totalCompactedCells = 0;
for (RegionMetrics rl : sl.getRegionMetrics().values()) {
  totalCompactingCells += rl.getCompactingCellCount();
  totalCompactedCells += rl.getCompactedCellCount();
}
String percentDone = "";
if  (totalCompactingCells > 0) {
     percentDone = String.format("%.2f", 100 *
        ((float) totalCompactedCells / totalCompactingCells)) + "%";
}
%>
<tr>
  <td><%= MasterStatusUtil.serverNameLink(master, serverName) %></td>
  <td><%= String.format("%,d", totalCompactingCells) %></td>
  <td><%= String.format("%,d", totalCompactedCells) %></td>
  <td><%= String.format("%,d", totalCompactingCells - totalCompactedCells) %></td>
  <td><%= percentDone %></td>
  </tr>
<%
}  else {
%>
    <% request.setAttribute("serverName", serverName); %>
    <jsp:include page="regionServerListEmptyStat.jsp"/>
<%
  }
}
%>
</tbody>
</table>
