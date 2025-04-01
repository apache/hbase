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
         import="org.apache.hadoop.hbase.master.HMaster"
         import="java.util.Map"
         import="java.util.Set"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.net.Address"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupUtil"
         import="org.apache.hadoop.util.StringUtils"
         import="org.apache.hadoop.hbase.util.MasterStatusConstants" %>

<%!
  // TODO: Extract to common place!
  private static String rsGroupLink(String rsGroupName) {
    return "<a href=\"rsgroup.jsp?name=" + rsGroupName + "\">" + rsGroupName + "</a>";
  }
%>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  RSGroupInfo [] rsGroupInfos = (RSGroupInfo[]) request.getAttribute(MasterStatusConstants.RS_GROUP_INFOS);
  Map<Address, ServerMetrics> collectServers = (Map<Address, ServerMetrics>) request.getAttribute(MasterStatusConstants.COLLECT_SERVERS);
%>

<table class="table table-striped">
  <tr>
    <th>RSGroup Name</th>
    <th>Num. Online Servers</th>
    <th>Num. Dead Servers</th>
    <th>Num. Tables</th>
    <th>Requests Per Second</th>
    <th>Num. Regions</th>
    <th>Average Load</th>
  </tr>
    <%
    int totalOnlineServers = 0;
    int totalDeadServers = 0;
    int totalTables = 0;
    int totalRequests = 0;
    int totalRegions = 0;
    for (RSGroupInfo rsGroupInfo: rsGroupInfos) {
      String rsGroupName = rsGroupInfo.getName();
      int onlineServers = 0;
      int deadServers = 0;
      int tables = 0;
      long requestsPerSecond = 0;
      int numRegionsOnline = 0;
      Set<Address> servers = rsGroupInfo.getServers();
      for (Address server : servers) {
        ServerMetrics sl = collectServers.get(server);
        if (sl != null) {
          requestsPerSecond += sl.getRequestCountPerSecond();
          numRegionsOnline += sl.getRegionMetrics().size();
          //rsgroup total
          totalRegions += sl.getRegionMetrics().size();
          totalRequests += sl.getRequestCountPerSecond();
          totalOnlineServers++;
          onlineServers++;
        } else {
          totalDeadServers++;
          deadServers++;
        }
      }
      tables = RSGroupUtil.listTablesInRSGroup(master, rsGroupInfo.getName()).size();
      totalTables += tables;
      double avgLoad = onlineServers == 0 ? 0 :
            (double)numRegionsOnline / (double)onlineServers;
%>
<tr>
  <td><%= rsGroupLink(rsGroupName) %></td>
  <td><%= onlineServers %></td>
  <td><%= deadServers %></td>
  <td><%= tables %></td>
  <td><%= requestsPerSecond %></td>
  <td><%= numRegionsOnline %></td>
  <td><%= StringUtils.limitDecimalTo2(avgLoad) %></td>
</tr>
<%
  }
%>
  <tr>
  <td>Total:<%= rsGroupInfos.length %></td>
  <td><%= totalOnlineServers %></td>
  <td><%= totalDeadServers %></td>
  <td><%= totalTables %></td>
  <td><%= totalRequests %></td>
  <td><%= totalRegions %></td>
  <td><%= StringUtils.limitDecimalTo2(master.getServerManager().getAverageLoad()) %></td>
  </tr>
</table>
