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
         import="java.util.*"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupUtil"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager"
         import="org.apache.hadoop.hbase.master.DeadServer"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.hbase.master.ServerManager" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);

  ServerManager serverManager = master.getServerManager();

  Set<ServerName> deadServers = null;

  if (master.isActiveMaster()) {
    if (serverManager != null) {
      deadServers = serverManager.getDeadServers().copyServerNames();
    }
  }
%>

<% if (deadServers != null && deadServers.size() > 0) { %>
<h2>Dead Region Servers</h2>
<table class="table table-striped">
  <tr>
    <th></th>
    <th>ServerName</th>
    <th>Stop time</th>
    <% if (!master.isInMaintenanceMode() && master.getMasterCoprocessorHost() != null) { %>
    <% if (RSGroupUtil.isRSGroupEnabled(master.getConfiguration())) { %>
    <th>RSGroup</th>
    <% } %>
  <% } %>
</tr>
<%
  RSGroupInfoManager inMgr = null;
  DeadServer deadServerUtil = master.getServerManager().getDeadServers();
  ServerName [] deadServerNames = deadServers.toArray(new ServerName[deadServers.size()]);
  Arrays.sort(deadServerNames);
  if (!master.isInMaintenanceMode() && master.getMasterCoprocessorHost() != null
    && RSGroupUtil.isRSGroupEnabled(master.getConfiguration())) {
    inMgr = master.getRSGroupInfoManager();
  }
  for (ServerName deadServerName: deadServerNames) {
    String rsGroupName = null;
    if (inMgr != null){
      RSGroupInfo groupInfo = inMgr.getRSGroupOfServer(deadServerName.getAddress());
      rsGroupName = groupInfo == null ? RSGroupInfo.DEFAULT_GROUP : groupInfo.getName();
    }
    %>
      <tr>
        <th></th>
          <td><%= deadServerName %></td>
          <td><%= new Date(deadServerUtil.getTimeOfDeath(deadServerName)) %></td>
          <% if (rsGroupName != null) { %>
          <td><%= rsGroupName %></td>
          <% } %>
      </tr>
  <%
  }
  %>
  <tr>
    <th>Total: </th>
    <td>servers: <%= deadServers.size() %></td>
    <th></th>
  </tr>
</table>
<% } %>
