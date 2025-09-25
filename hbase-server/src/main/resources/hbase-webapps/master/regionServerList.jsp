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
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.master.ServerManager" %>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  ServerManager serverManager = master.getServerManager();

  List<ServerName> servers = null;

  if (master.isActiveMaster()) {
    if (serverManager != null) {
      servers = serverManager.getOnlineServersList();
    }
  }
%>

<%
  if (servers != null && servers.size() > 0) {

    ServerName [] serverNames = servers.toArray(new ServerName[servers.size()]);
    Arrays.sort(serverNames);
%>

<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item">
      <a class="nav-link active" href="#tab_baseStats" data-bs-toggle="tab" role="tab">Base Stats</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#tab_memoryStats" data-bs-toggle="tab" role="tab">Memory</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#tab_requestStats" data-bs-toggle="tab" role="tab">Requests</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#tab_storeStats" data-bs-toggle="tab" role="tab">Storefiles</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#tab_compactStats" data-bs-toggle="tab" role="tab">Compactions</a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#tab_replicationStats" data-bs-toggle="tab" role="tab">Replications</a>
    </li>
  </ul>
  <div class="tab-content">
    <% request.setAttribute("serverNames", serverNames); %>
    <div class="tab-pane active" id="tab_baseStats" role="tabpanel">
      <jsp:include page="regionServerListBaseStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_memoryStats" role="tabpanel">
      <jsp:include page="regionServerListMemoryStats.jsp" />
    </div>
    <div class="tab-pane" id="tab_requestStats" role="tabpanel">
      <jsp:include page="regionServerListRequestStats.jsp" />
    </div>
    <div class="tab-pane" id="tab_storeStats" role="tabpanel">
      <jsp:include page="regionServerListStoreStats.jsp" />
    </div>
    <div class="tab-pane" id="tab_compactStats" role="tabpanel">
      <jsp:include page="regionServerListCompactionStats.jsp" />
    </div>
    <div class="tab-pane" id="tab_replicationStats" role="tabpanel">
      <jsp:include page="regionServerListReplicationStats.jsp" />
    </div>
  </div>
</div>

<% } %>
