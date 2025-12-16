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
         import="org.apache.hadoop.hbase.util.VersionInfo"
         import="org.apache.hadoop.hbase.RSGroupTableAccessor"
         import="org.apache.hadoop.hbase.master.ServerManager"
         import="org.apache.hadoop.hbase.net.Address"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="java.util.*"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.util.StringUtils"
         import="org.apache.hadoop.hbase.master.http.MasterStatusConstants"
         import="org.apache.hadoop.hbase.master.http.MasterStatusUtil" %>
<%
  ServerName[] serverNames = (ServerName[]) request.getAttribute(MasterStatusConstants.SERVER_NAMES);
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
%>

<table id="baseStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th>ServerName</th>
    <th class="cls_dateTime">Start time</th>
    <th>Last contact</th>
    <th>Version</th>
    <th class="cls_separator">Requests Per Second</th>
    <th class="cls_separator">Num. Regions</th>
  </tr>
  </thead>
  <tbody>
  <%
    int totalRegions = 0;
    int totalRequestsPerSecond = 0;
    int inconsistentNodeNum = 0;
    String masterVersion = VersionInfo.getVersion();

    for (ServerName serverName: serverNames) {
      ServerMetrics sl = master.getServerManager().getLoad(serverName);
      String version = master.getRegionServerVersion(serverName);
      if (!masterVersion.equals(version)) {
        inconsistentNodeNum ++;
      }

      double requestsPerSecond = 0.0;
      int numRegionsOnline = 0;
      long lastContact = 0;

      if (sl != null) {
        requestsPerSecond = sl.getRequestCountPerSecond();
        numRegionsOnline = sl.getRegionMetrics().size();
        totalRegions += sl.getRegionMetrics().size();
        totalRequestsPerSecond += sl.getRequestCountPerSecond();
        lastContact = (System.currentTimeMillis() - sl.getReportTimestamp())/1000;
      }
      long startcode = serverName.getStartcode();
%>
<tr>
  <td><%= MasterStatusUtil.serverNameLink(master, serverName) %></td>
  <td><%= new Date(startcode) %></td>
  <td><%= StringUtils.TraditionalBinaryPrefix.long2String(lastContact, "s", 1) %></td>
  <td><%= version %></td>
  <td><%= String.format("%,.0f", requestsPerSecond) %></td>
  <td><%= String.format("%,d", numRegionsOnline) %></td>
</tr>
<% } %>
  <tr><td>Total:<%= serverNames.length %></td>
<td></td>
<td></td>
<% if(inconsistentNodeNum > 0) {%>
<td style="color:red;"><%= inconsistentNodeNum %> nodes with inconsistent version</td>
<% } else { %>
  <td></td>
<% } %>
  <td><%= totalRequestsPerSecond %></td>
<td><%= totalRegions %></td>
</tr>
  </tbody>
</table>
