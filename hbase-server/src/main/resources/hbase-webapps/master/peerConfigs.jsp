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
         import="org.apache.hadoop.hbase.replication.ReplicationPeerDescription"
         import="org.apache.hadoop.hbase.replication.ReplicationPeerConfig"
         import="org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil"
         import="org.apache.hadoop.hbase.util.Strings" %>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);

  List<ReplicationPeerDescription> peers = null;
  if (master.getReplicationPeerManager() != null) {
    peers = master.getReplicationPeerManager().listPeers(null);
  }
%>
  <table class="table table-striped">
    <tr>
      <th>Peer Id</th>
      <th>Cluster Key</th>
      <th>Endpoint</th>
      <th>State</th>
      <th>IsSerial</th>
      <th>Remote WAL</th>
      <th>Sync Replication State</th>
      <th>Bandwidth</th>
      <th>ReplicateAll</th>
      <th>Namespaces</th>
      <th>Exclude Namespaces</th>
      <th>Table Cfs</th>
      <th>Exclude Table Cfs</th>
    </tr>
<% if (peers != null && peers.size() > 0) { %>
  <% for (ReplicationPeerDescription peer : peers) { %>
  <%
    String peerId = peer.getPeerId();
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    %>
    <tr>
      <td><%= peerId %></td>
      <td><%= peerConfig.getClusterKey() %></td>
      <td><%= peerConfig.getReplicationEndpointImpl() %></td>
      <td><%= peer.isEnabled() ? "ENABLED" : "DISABLED" %></td>
      <td><%= peerConfig.isSerial() %></td>
      <td><%= peerConfig.getRemoteWALDir() == null ? "" : peerConfig.getRemoteWALDir() %></td>
      <td><%= peer.getSyncReplicationState() %></td>
      <td><%= peerConfig.getBandwidth() == 0? "UNLIMITED" : Strings.humanReadableInt(peerConfig.getBandwidth()) %></td>
      <td><%= peerConfig.replicateAllUserTables() %></td>
      <td>
        <%= peerConfig.getNamespaces() == null ? "" : ReplicationPeerConfigUtil.convertToString(peerConfig.getNamespaces()).replaceAll(";", "; ") %>
      </td>
      <td>
        <%= peerConfig.getExcludeNamespaces() == null ? "" : ReplicationPeerConfigUtil.convertToString(peerConfig.getExcludeNamespaces()).replaceAll(";", "; ") %>
      </td>
      <td>
        <%= peerConfig.getTableCFsMap() == null ? "" : ReplicationPeerConfigUtil.convertToString(peerConfig.getTableCFsMap()).replaceAll(";", "; ") %>
      </td>
      <td>
        <%= peerConfig.getExcludeTableCFsMap() == null ? "" : ReplicationPeerConfigUtil.convertToString(peerConfig.getExcludeTableCFsMap()).replaceAll(";", "; ") %>
      </td>
    </tr>
    <% } %>
<% } %>

<tr><td>Total: <%= (peers != null) ? peers.size() : 0 %></td></tr>
</table>
