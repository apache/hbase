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
         import="org.apache.hbase.thirdparty.com.google.common.base.Preconditions" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  if (!master.isActiveMaster()) {

  ServerName active_master = master.getActiveMaster().orElse(null);
  Preconditions.checkState(active_master != null, "Failed to retrieve active master's ServerName!");
  int activeInfoPort = master.getActiveMasterInfoPort();
%>
  <div class="row inner_header">
    <div class="page-header">
      <h1>Backup Master <small><%= master.getServerName().getHostname() %></small></h1>
    </div>
  </div>
  <h4>Current Active Master: <a href="//<%= active_master.getHostname() %>:<%= activeInfoPort %>/master.jsp"
                              target="_blank"><%= active_master.getHostname() %></a></h4>
  <% } else { %>
   <h2>Backup Masters</h2>

      <table class="table table-striped">
      <tr>
      <th>ServerName</th>
      <th>Port</th>
      <th>Start Time</th>
      </tr>
  <%
    Collection<ServerName> backup_masters = master.getBackupMasters();
    ServerName [] backupServerNames = backup_masters.toArray(new ServerName[backup_masters.size()]);
    Arrays.sort(backupServerNames);
    for (ServerName serverName : backupServerNames) {
      int infoPort = master.getBackupMasterInfoPort(serverName);
    %>
    <tr>
      <td><a href="//<%= serverName.getHostname() %>:<%= infoPort %>/master.jsp"
             target="_blank"><%= serverName.getHostname() %></a>
      </td>
      <td><%= serverName.getPort() %></td>
      <td><%= new Date(serverName.getStartCode()) %></td>
    </tr>
    <% } %>
    <tr><td>Total:<%= backupServerNames.length %></td>
  </table>
<% } %>
