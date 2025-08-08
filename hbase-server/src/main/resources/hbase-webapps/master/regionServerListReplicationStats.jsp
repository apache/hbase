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
         import="org.apache.hadoop.hbase.replication.ReplicationLoadSource"
         import="org.apache.hadoop.hbase.util.Pair"
         import="java.util.*"
         import="org.apache.hadoop.hbase.procedure2.util.StringUtils"
         import="org.apache.hadoop.hbase.util.MasterStatusConstants"
         import="org.apache.hadoop.hbase.util.MasterStatusUtil" %>

<%
  ServerName[] serverNames = (ServerName[]) request.getAttribute(MasterStatusConstants.SERVER_NAMES);
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);

  HashMap<String, List<Pair<ServerName, ReplicationLoadSource>>> replicationLoadSourceMap
  = master.getReplicationLoad(serverNames);
  List<String> peers = null;
  if (replicationLoadSourceMap != null && replicationLoadSourceMap.size() > 0){
    peers = new ArrayList<>(replicationLoadSourceMap.keySet());
    Collections.sort(peers);
  }
%>

<% if (replicationLoadSourceMap != null && replicationLoadSourceMap.size() > 0) { %>

<div class="tabbable">
  <ul class="nav nav-tabs">
      <%
        String active = "active";
        for (String peer : peers){
        %>
            <li class=<%= active %>><a href="#tab_<%= peer %>" data-bs-toggle="tab">Peer <%= peer %></a> </li>
      <%
        active = "";
        }
        %>
    </ul>
    <div class="tab-content">
        <%
            active = "active";
            for (String peer : peers){
        %>
            <div class="tab-pane <%= active %>" id="tab_<%= peer %>">
    <table class="table table-striped">
      <tr>
        <th>Server</th>
        <th>AgeOfLastShippedOp</th>
        <th>SizeOfLogQueue</th>
        <th>ReplicationLag</th>
      </tr>

      <% for (Pair<ServerName, ReplicationLoadSource> pair: replicationLoadSourceMap.get(peer)) { %>
      <tr>
        <td><%= MasterStatusUtil.serverNameLink(master, pair.getFirst()) %></td>
        <td><%= StringUtils.humanTimeDiff(pair.getSecond().getAgeOfLastShippedOp()) %></td>
        <td><%= pair.getSecond().getSizeOfLogQueue() %></td>
        <td><%= pair.getSecond().getReplicationLag() == Long.MAX_VALUE ? "UNKNOWN" : StringUtils.humanTimeDiff(pair.getSecond().getReplicationLag()) %></td>
      </tr>
      <% } %>
    </table>
</div>
<%
  active = "";
  }
  %>
  </div>
  <p>If the replication delay is UNKNOWN, that means this walGroup doesn't start replicate yet and it may get disabled.</p>
  </div>
<% } else { %>
  <p>No Peers Metrics</p>
<% } %>
