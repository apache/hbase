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
         import="org.apache.hadoop.hbase.procedure2.util.StringUtils"
         import ="org.apache.hadoop.hbase.replication.regionserver.ReplicationStatus" %>
<%
  Map<String, ReplicationStatus> metrics = (Map<String, ReplicationStatus>) request.getAttribute("metrics");
%>
<table class="table table-striped">
  <tr>
    <th>PeerId</th>
    <th>WalGroup</th>
    <th>Current Log</th>
    <th>Last Shipped Age</th>
    <th>Replication Delay</th>
  </tr>
  <%  for (Map.Entry<String, ReplicationStatus> entry: metrics.entrySet()) { %>
    <tr>
      <td><%= entry.getValue().getPeerId() %></td>
      <td><%= entry.getValue().getWalGroup() %></td>
      <td><%= entry.getValue().getCurrentPath() %> </td>
      <td><%= StringUtils.humanTimeDiff(entry.getValue().getAgeOfLastShippedOp()) %></td>
      <td><%= entry.getValue().getReplicationDelay() == Long.MAX_VALUE ? "UNKNOWN" : StringUtils.humanTimeDiff(entry.getValue().getReplicationDelay()) %></td>
    </tr>
  <% } %>
</table>
