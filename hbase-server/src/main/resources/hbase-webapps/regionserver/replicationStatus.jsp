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
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import ="org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus" %>
<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  Map<String, ReplicationStatus> walGroupsReplicationStatus = regionServer.getWalGroupsReplicationStatus();

  if (walGroupsReplicationStatus != null && walGroupsReplicationStatus.size() > 0) {
%>

<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item"><a class="nav-link active" href="#tab_currentLog" data-bs-toggle="tab" role="tab">Current Log</a> </li>
    <li class="nav-item"><a class="nav-link" href="#tab_replicationDelay" data-bs-toggle="tab" role="tab">Replication Delay</a></li>
  </ul>
  <div class="tab-content">
    <% request.setAttribute("metrics", walGroupsReplicationStatus); %>
    <div class="tab-pane active" id="tab_currentLog" role="tabpanel">
      <jsp:include page="replicationStatusCurrentLog.jsp"/>
    </div>
    <div class="tab-pane" id="tab_replicationDelay" role="tabpanel">
      <jsp:include page="replicationStatusReplicationDelay.jsp"/>
    </div>
  </div>
</div>
<p> If the replication delay is UNKNOWN, that means this walGroup doesn't start replicate yet and it may get disabled.
  If the size of log is 0, it means we are replicating current HLog, thus we can't get accurate size since it's not closed yet.</p>

<% } else { %>
  <p>No Replication Metrics for Peers</p>
<% } %>
