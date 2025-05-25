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
         import="org.apache.hadoop.hbase.util.MasterStatusConstants"
         import="org.apache.hadoop.hbase.util.MasterStatusUtil" %>

<%
  ServerName[] serverNames = (ServerName[]) request.getAttribute(MasterStatusConstants.SERVER_NAMES);
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
%>

<table id="requestStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th>ServerName</th>
    <th class="cls_separator">Request Per Second</th>
    <th class="cls_separator">Read Request Count</th>
    <th class="cls_separator">Filtered Read Request Count</th>
    <th class="cls_separator">Write Request Count</th>
  </tr>
  </thead>
  <tbody>
<%
  for (ServerName serverName: serverNames) {

  ServerMetrics sl = master.getServerManager().getLoad(serverName);
  if (sl != null) {
    long readRequestCount = 0;
    long writeRequestCount = 0;
    long filteredReadRequestCount = 0;
    for (RegionMetrics rl : sl.getRegionMetrics().values()) {
      readRequestCount += rl.getReadRequestCount();
      writeRequestCount += rl.getWriteRequestCount();
      filteredReadRequestCount += rl.getFilteredReadRequestCount();
    }
%>
<tr>
  <td><%= MasterStatusUtil.serverNameLink(master, serverName) %></td>
  <td><%= String.format("%,d", sl.getRequestCountPerSecond()) %></td>
  <td><%= String.format("%,d", readRequestCount) %></td>
  <td><%= String.format("%,d", filteredReadRequestCount) %></td>
  <td><%= String.format("%,d", writeRequestCount) %></td>
</tr>
<%
} else {
%>
  <% request.setAttribute(MasterStatusConstants.SERVER_NAME, serverName); %>
  <jsp:include page="regionServerListEmptyStat.jsp"/>
<%
  }
}
%>
</tbody>
</table>
