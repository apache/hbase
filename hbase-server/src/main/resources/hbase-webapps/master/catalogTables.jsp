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
         import="org.apache.hadoop.hbase.MetaTableName"
         import="org.apache.hadoop.hbase.NamespaceDescriptor"
         import="org.apache.hadoop.hbase.TableName"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.quotas.QuotaUtil"
         import="org.apache.hadoop.hbase.security.access.PermissionStorage"
         import="org.apache.hadoop.hbase.security.visibility.VisibilityConstants"
         import="org.apache.hadoop.hbase.tool.CanaryTool"
         import="org.apache.hadoop.hbase.client.*"
         import="org.apache.hadoop.hbase.master.http.MasterStatusConstants" %>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);

  Map<String, Integer> frags = (Map<String, Integer>) request.getAttribute(MasterStatusConstants.FRAGS);

  List<TableDescriptor> sysTables = master.isInitialized() ?
  master.listTableDescriptorsByNamespace(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR) : null;
%>

<%if (sysTables != null && sysTables.size() > 0) { %>
<table class="table table-striped">
  <tr>
    <th>Table Name</th>
    <% if (frags != null) { %>
      <th title="Fragmentation - Will be 0% after a major compaction and fluctuate during normal usage.">Frag.</th>
    <% } %>
    <th>Description</th>
  </tr>
  <% for (TableDescriptor systemTable : sysTables) { %>
  <tr>
    <% TableName tableName = systemTable.getTableName();%>
      <td><a href="table.jsp?name=<%= tableName %>"><%= tableName %></a></td>
      <% if (frags != null) { %>
        <td align="center"><%= frags.get(tableName.getNameAsString()) != null ? frags.get(tableName.getNameAsString()) + "%" : "n/a" %></td>
      <% } %>
    <% String description = "";
        if (tableName.equals(master.getConnection().getMetaTableName())){
            description = "The hbase:meta table holds references to all User Table regions.";
        } else if (tableName.equals(CanaryTool.DEFAULT_WRITE_TABLE_NAME)){
            description = "The hbase:canary table is used to sniff the write availability of"
              + " each regionserver.";
        } else if (tableName.equals(PermissionStorage.ACL_TABLE_NAME)){
            description = "The hbase:acl table holds information about acl.";
        } else if (tableName.equals(VisibilityConstants.LABELS_TABLE_NAME)){
            description = "The hbase:labels table holds information about visibility labels.";
        } else if (tableName.equals(QuotaUtil.QUOTA_TABLE_NAME)){
            description = "The hbase:quota table holds quota information about number" +
            " or size of requests in a given time frame.";
        } else if (tableName.equals(TableName.valueOf("hbase:rsgroup"))){
            description = "The hbase:rsgroup table holds information about regionserver groups.";
        } else if (tableName.equals(TableName.valueOf("hbase:replication"))) {
            description = "The hbase:replication table tracks cross cluster replication through " +
            "WAL file offsets.";
        } else if (tableName.equals(TableName.valueOf("hbase:slowlog"))) {
            description = "The hbase:slowlog table holds information about slow and large rpc " +
            "operations.";
        }
    %>
    <td><%= description %></td>
  </tr>
  <% } %>
</table>
<% } %>
