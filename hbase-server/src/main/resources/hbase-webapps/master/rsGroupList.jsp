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
         import="java.util.Collections"
         import="java.util.List"
         import="java.util.Map"
         import="java.util.stream.Collectors"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.net.Address"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.hbase.util.MasterStatusConstants" %>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  List<RSGroupInfo> groups = master.getRSGroupInfoManager().listRSGroups();
%>

<%if (groups != null && groups.size() > 0)%>

<%
  RSGroupInfo [] rsGroupInfos = groups.toArray(new RSGroupInfo[groups.size()]);
  Map<Address, ServerMetrics> collectServers = Collections.emptyMap();
  if (master.getServerManager() != null) {
  collectServers =
  master.getServerManager().getOnlineServers().entrySet().stream()
  .collect(Collectors.toMap(p -> p.getKey().getAddress(), Map.Entry::getValue));
  }
%>

<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item"><a class="nav-link active" href="#tab_rsgroup_baseStats" data-bs-toggle="tab" role="tab">Base Stats</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_rsgroup_memoryStats" data-bs-toggle="tab" role="tab">Memory</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_rsgroup_requestStats" data-bs-toggle="tab" role="tab">Requests</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_rsgroup_storeStats" data-bs-toggle="tab" role="tab">Storefiles</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_rsgroup_compactStats" data-bs-toggle="tab" role="tab">Compactions</a></li>
  </ul>
  <div class="tab-content">

    <% request.setAttribute(MasterStatusConstants.RS_GROUP_INFOS, rsGroupInfos); %>
    <% request.setAttribute(MasterStatusConstants.COLLECT_SERVERS, collectServers); %>

    <div class="tab-pane active" id="tab_rsgroup_baseStats" role="tabpanel">
      <jsp:include page="rsGroupList_baseStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_rsgroup_memoryStats" role="tabpanel">
      <jsp:include page="rsGroupList_memoryStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_rsgroup_requestStats" role="tabpanel">
      <jsp:include page="rsGroupList_requestStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_rsgroup_storeStats" role="tabpanel">
      <jsp:include page="rsGroupList_storeStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_rsgroup_compactStats" role="tabpanel">
      <jsp:include page="rsGroupList_compactStats.jsp"/>
    </div>
  </div>
</div>
