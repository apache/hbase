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
         import="java.util.Map"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.net.Address"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.hbase.RegionMetrics" %>

<%!
  // TODO: Extract to common place!
  private static String rsGroupLink(String rsGroupName) {
    return "<a href=\"rsgroup.jsp?name=" + rsGroupName + "\">" + rsGroupName + "</a>";
  }
%>

<%
  RSGroupInfo [] rsGroupInfos = (RSGroupInfo[]) request.getAttribute("rsGroupInfos"); // TODO: intro constant!
  Map<Address, ServerMetrics> collectServers = (Map<Address, ServerMetrics>) request.getAttribute("collectServers"); // TODO: intro constant!
%>

<table class="table table-striped">
  <tr>
    <th>RSGroup Name</th>
    <th>Num. Compacting Cells</th>
    <th>Num. Compacted Cells</th>
    <th>Remaining Cells</th>
    <th>Compaction Progress</th>
  </tr>
    <%
    for (RSGroupInfo rsGroupInfo: rsGroupInfos) {
      String rsGroupName = rsGroupInfo.getName();
      long totalCompactingCells = 0;
      long totalCompactedCells = 0;
      long remainingCells = 0;
      for (Address server : rsGroupInfo.getServers()) {
        ServerMetrics sl = collectServers.get(server);
        if (sl != null) {
          for (RegionMetrics rl : sl.getRegionMetrics().values()) {
            totalCompactingCells += rl.getCompactingCellCount();
            totalCompactedCells += rl.getCompactedCellCount();
          }
        }
      }
      remainingCells = totalCompactingCells - totalCompactedCells;
      String percentDone = "";
      if  (totalCompactingCells > 0) {
           percentDone = String.format("%.2f", 100 *
              ((float) totalCompactedCells / totalCompactingCells)) + "%";
      }
%>
<tr>
  <td><%= rsGroupLink(rsGroupName) %></td>
  <td><%= totalCompactingCells %></td>
  <td><%= totalCompactedCells %></td>
  <td><%= remainingCells %></td>
  <td><%= percentDone %></td>
  </tr>
<%
}
%>
</table>
