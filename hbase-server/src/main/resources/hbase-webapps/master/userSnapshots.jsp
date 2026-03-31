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
<%@ page contentType="text/plain;charset=UTF-8"
 import="java.util.List"
 import="java.util.Date"
 import="org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils"
 import="org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription"
 import="org.apache.hadoop.hbase.master.HMaster"
 import="org.apache.hadoop.hbase.TableName"
 import="org.apache.hadoop.hbase.util.PrettyPrinter"
%>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  List<SnapshotDescription> snapshots = master.isInitialized() ?
     master.getSnapshotManager().getCompletedSnapshots() : null;
%>
<%if (snapshots != null && snapshots.size() > 0) { %>
<table class="table table-striped">
    <tr>
        <th>Snapshot Name</th>
        <th>Table</th>
        <th>Creation Time</th>
        <th>Owner</th>
        <th>TTL</th>
        <th>Expired</th>
    </tr>
    <% for (SnapshotDescription snapshotDesc : snapshots){ %>
    <% TableName snapshotTable = TableName.valueOf(snapshotDesc.getTable()); %>
    <tr>
        <td><a href="snapshot.jsp?name=<%= snapshotDesc.getName() %>"><%= snapshotDesc.getName() %></a> </td>
        <td><a href="table.jsp?name=<%= snapshotTable.getNameAsString() %>"><%= snapshotTable.getNameAsString() %></a>
        </td>
        <td><%= new Date(snapshotDesc.getCreationTime()) %></td>
        <td><%= snapshotDesc.getOwner() %></td>

        <td>
          <%= snapshotDesc.getTtl() == 0 ? "FOREVER": PrettyPrinter.format(String.valueOf(snapshotDesc.getTtl()), PrettyPrinter.Unit.TIME_INTERVAL) %>
        </td>
        <td>
          <%= SnapshotDescriptionUtils.isExpiredSnapshot(snapshotDesc.getTtl(), snapshotDesc.getCreationTime(), System.currentTimeMillis()) ? "Yes" : "No" %>
        </td>
    </tr>
    <% } %>
    <p><%= snapshots.size() %> snapshot(s) in set. [<a href="/snapshotsStats.jsp">Snapshot Storefile stats</a>]</p>
</table>
<% } %>
