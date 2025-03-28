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
    <th>Request Per Second</th>
    <th>Read Request Count</th>
    <th>Write Request Count</th>
  </tr>
    <%
    for (RSGroupInfo rsGroupInfo: rsGroupInfos) {
      String rsGroupName = rsGroupInfo.getName();
      long requestsPerSecond = 0;
      long readRequests = 0;
      long writeRequests = 0;
      for (Address server : rsGroupInfo.getServers()) {
        ServerMetrics sl = collectServers.get(server);
        if (sl != null) {
          for (RegionMetrics rm : sl.getRegionMetrics().values()) {
            readRequests += rm.getReadRequestCount();
            writeRequests += rm.getWriteRequestCount();
          }
          requestsPerSecond += sl.getRequestCountPerSecond();
        }
      }
  %>
  <tr>
    <td><%= rsGroupLink(rsGroupName) %></td>
    <td><%= requestsPerSecond %></td>
    <td><%= readRequests %></td>
    <td><%= writeRequests %></td>
  </tr>
  <%
  }
  %>
  </table>
