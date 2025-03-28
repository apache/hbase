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
         import="java.net.URLEncoder"
         import="java.io.IOException"
         import="org.apache.hadoop.hbase.TableName"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.master.RegionState"
         import="org.apache.hadoop.hbase.client.*" %>
<%!
  public static String getUserTables(HMaster master, List<TableDescriptor> tables){
    if (master.isInitialized()){
      try {
        Map<String, TableDescriptor> descriptorMap = master.getTableDescriptors().getAll();
        if (descriptorMap != null) {
          for (TableDescriptor desc : descriptorMap.values()) {
            if (!desc.getTableName().isSystemTable()) {
              tables.add(desc);
            }
          }
        }
      } catch (IOException e) {
        return "Got user tables error, " + e.getMessage();
      }
    }
    return null;
  }
%>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);

  Map<String, Integer> frags = (Map<String, Integer>) request.getAttribute("frags"); // TODO: intro constant!

  List<TableDescriptor> tables = new ArrayList<>();
  String errorMessage = getUserTables(master, tables);
%>

<% if (tables.size() == 0 && errorMessage != null) { %>
<p> <%= errorMessage %> </p>
<% } %>

<% if (tables != null && tables.size() > 0) { %>
<table id="userTables" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th style="vertical-align: middle;" rowspan="2">Namespace</th>
    <th style="vertical-align: middle;" rowspan="2">Name</th>
    <% if (frags != null) { %>
    <th title="Fragmentation - Will be 0% after a major compaction and fluctuate during normal usage.">Frag.</th>
  <% } %>
  <th style="vertical-align:middle;" rowspan="2">State</th>
  <th style="text-align: center" colspan="7">Regions</th>
  <th style="vertical-align:middle;" rowspan="2">Description</th>
  </tr>
  <tr>
    <th>OPEN</th>
    <th>OPENING</th>
    <th>CLOSED</th>
    <th>CLOSING</th>
    <th>OFFLINE</th>
    <th>SPLIT</th>
    <th>Other</th>
  </tr>
  </thead>
  <tbody>
  <% for (TableDescriptor desc : tables) { %>
    <%
      TableName tableName = desc.getTableName();
      TableState tableState = master.getTableStateManager().getTableState(tableName);
      Map<RegionState.State, List<RegionInfo>> tableRegions =
          master.getAssignmentManager().getRegionStates()
            .getRegionByStateOfTable(tableName);
      int openRegionsCount = tableRegions.get(RegionState.State.OPEN).size();
      int openingRegionsCount = tableRegions.get(RegionState.State.OPENING).size();
      int closedRegionsCount = tableRegions.get(RegionState.State.CLOSED).size();
      int closingRegionsCount = tableRegions.get(RegionState.State.CLOSING).size();
      int offlineRegionsCount = tableRegions.get(RegionState.State.OFFLINE).size();
      int splitRegionsCount = tableRegions.get(RegionState.State.SPLIT).size();
      int otherRegionsCount = 0;
      for (List<RegionInfo> list: tableRegions.values()) {
         otherRegionsCount += list.size();
      }
      // now subtract known states
      otherRegionsCount = otherRegionsCount - openRegionsCount
                     - offlineRegionsCount - splitRegionsCount
                     - openingRegionsCount - closedRegionsCount
                     - closingRegionsCount;
    String encodedTableName = URLEncoder.encode(tableName.getNameAsString());
    %>
    <tr>
        <td><%= tableName.getNamespaceAsString() %></td>
        <% if (tableState.isDisabledOrDisabling()) { %> <td><a style="color:red;" href=table.jsp?name=<%= encodedTableName %>><%= URLEncoder.encode(tableName.getQualifierAsString()) %></a></td> <% } else { %><td><a href=table.jsp?name=<%= encodedTableName %>><%= URLEncoder.encode(tableName.getQualifierAsString()) %></a></td> <% } %>
        <% if (frags != null) { %>
        <td align="center"><%= frags.get(tableName.getNameAsString()) != null ? frags.get(tableName.getNameAsString()).intValue() + "%" : "n/a" %></td>
        <% } %>
      <% if (tableState.isDisabledOrDisabling()) { %> <td style="color:red;"><%= tableState.getState().name() %></td> <% } else { %><td><%= tableState.getState() %></td> <% } %>
      <td><%= openRegionsCount %></td>
      <% if (openingRegionsCount > 0) { %> <td><a href="/table.jsp?name=hbase%3Ameta&scan_table=<%= encodedTableName %>&scan_region_state=OPENING"><%= openingRegionsCount %></a></td> <% } else { %><td><%= openingRegionsCount %></td> <% } %>
      <% if (closedRegionsCount > 0) { %> <td><a href="/table.jsp?name=hbase%3Ameta&scan_table=<%= encodedTableName %>&scan_region_state=CLOSED"><%= closedRegionsCount %></a></td> <% } else { %><td><%= closedRegionsCount %></td> <% } %>
      <% if (closingRegionsCount > 0) { %> <td><a href="/table.jsp?name=hbase%3Ameta&scan_table=<%= encodedTableName %>&scan_region_state=CLOSING"><%= closingRegionsCount %></a></td> <% } else { %><td><%= closingRegionsCount %></td> <% } %>
      <% if (offlineRegionsCount > 0) { %> <td><a href="/table.jsp?name=hbase%3Ameta&scan_table=<%= encodedTableName %>&scan_region_state=OFFLINE"><%= offlineRegionsCount %></a></td> <% } else { %><td><%= offlineRegionsCount %></td> <% } %>
      <% if (splitRegionsCount > 0) { %> <td><a href="/table.jsp?name=hbase%3Ameta&scan_table=<%= encodedTableName %>&scan_region_state=SPLIT"><%= splitRegionsCount %></a></td> <% } else { %><td><%= splitRegionsCount %></td> <% } %>
      <td><%= otherRegionsCount %></td>
      <td><%= desc.toStringCustomizedValues() %></td>
    </tr>
<% } %>
<p><%= tables.size() %> table(s) in set. [<a href=tablesDetailed.jsp>Details</a>]. Click count below to
  see list of regions currently in 'state' designated by the column title. For 'Other' Region state,
  browse to <a href="/table.jsp?name=hbase%3Ameta">hbase:meta</a> and adjust filter on 'Meta Entries' to
  query on states other than those listed here. Queries may take a while if the <i>hbase:meta</i> table
  is large.</p>
</tbody>
</table>
<% } %>
