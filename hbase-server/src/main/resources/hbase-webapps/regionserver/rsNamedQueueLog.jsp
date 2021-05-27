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
  import="java.util.Date"
  import="java.util.List"
  import="java.util.Arrays"
  import="java.util.HashSet"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.client.SnapshotDescription"
  import="org.apache.hadoop.hbase.http.InfoServer"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.snapshot.SnapshotInfo"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.client.ServerType"
  import="org.apache.hadoop.hbase.client.LogEntry"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.client.OnlineLogRecord"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.ServerName"
%>
<%
  HRegionServer rs = (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  Configuration conf = rs.getConfiguration();
  List<OnlineLogRecord> slowLogs = null;
  List<OnlineLogRecord> largeLogs = null;

  if(rs.isOnline()) {
    try (Admin rsAdmin = rs.getConnection().getAdmin()) {
      slowLogs = (List<OnlineLogRecord>)(List<?>)rsAdmin.getLogEntries(new HashSet<ServerName>(Arrays.asList(rs.getServerName())), "SLOW_LOG", ServerType.REGION_SERVER, HConstants.DEFAULT_SLOW_LOG_RING_BUFFER_SIZE, null);
      largeLogs = (List<OnlineLogRecord>)(List<?>)rsAdmin.getLogEntries(new HashSet<ServerName>(Arrays.asList(rs.getServerName())), "LARGE_LOG", ServerType.REGION_SERVER, HConstants.DEFAULT_SLOW_LOG_RING_BUFFER_SIZE, null);
    }
  }
%>

<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>


<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
    <h2>Named Queues</h2>
    </div>
    </div>
<div class="tabbable">
  <ul class="nav nav-pills">
    <li class="active">
      <a href="#tab_named_queue1" data-toggle="tab"> Slow Logs </a>
    </li>
    <li class="">
          <a href="#tab_named_queue2" data-toggle="tab"> Large Logs </a>
    </li>
  </ul>
    <div class="tab-content" style="padding-bottom: 9px; border-bottom: 1px solid #ddd;">
      <div class="tab-pane active" id="tab_named_queue1">
        <table class="table table-striped" style="white-space:nowrap">
        <tr>
            <th>Start Time</th>
            <th>Processing Time</th>
            <th>Queue Time</th>
            <th>Response Size</th>
            <th>Client Address</th>
            <th>Server Class</th>
            <th>Method Name</th>
            <th>Region Name</th>
            <th>User Name</th>
            <th>MultiGets Count</th>
            <th>MultiMutations Count</th>
            <th>MultiService Calls</th>
            <th>Call Details</th>
            <th>Param</th>
          </tr>
          <% if (slowLogs != null && !slowLogs.isEmpty()) {%>
            <% for (OnlineLogRecord r : slowLogs) { %>
            <tr>
             <td><%=new Date(r.getStartTime())%></td>
             <td><%=r.getProcessingTime()%>ms</td>
             <td><%=r.getQueueTime()%>ms</td>
             <td><%=StringUtils.byteDesc(r.getResponseSize())%></td>
             <td><%=r.getClientAddress()%></td>
             <td><%=r.getServerClass()%></td>
             <td><%=r.getMethodName()%></td>
             <td><%=r.getRegionName()%></td>
             <td><%=r.getUserName()%></td>
             <td><%=r.getMultiGetsCount()%></td>
             <td><%=r.getMultiMutationsCount()%></td>
             <td><%=r.getMultiServiceCalls()%></td>
             <td><%=r.getCallDetails()%></td>
             <td><%=r.getParam()%></td>
            </tr>
            <% } %>
          <% } %>
          </table>
      </div>
      <div class="tab-pane" id="tab_named_queue2">
          <table class="table table-striped" style="white-space:nowrap">
          <tr>
            <th>Start Time</th>
            <th>Processing Time</th>
            <th>Queue Time</th>
            <th>Response Size</th>
            <th>Client Address</th>
            <th>Server Class</th>
            <th>Method Name</th>
            <th>Region Name</th>
            <th>User Name</th>
            <th>MultiGets Count</th>
            <th>MultiMutations Count</th>
            <th>MultiService Calls</th>
            <th>Call Details</th>
            <th>Param</th>
          </tr>
          <% if (largeLogs != null && !largeLogs.isEmpty()) {%>
            <% for (OnlineLogRecord r : largeLogs) { %>
            <tr>
             <td><%=new Date(r.getStartTime())%></td>
             <td><%=r.getProcessingTime()%>ms</td>
             <td><%=r.getQueueTime()%>ms</td>
             <td><%=StringUtils.byteDesc(r.getResponseSize())%></td>
             <td><%=r.getClientAddress()%></td>
             <td><%=r.getServerClass()%></td>
             <td><%=r.getMethodName()%></td>
             <td><%=r.getRegionName()%></td>
             <td><%=r.getUserName()%></td>
             <td><%=r.getMultiGetsCount()%></td>
             <td><%=r.getMultiMutationsCount()%></td>
             <td><%=r.getMultiServiceCalls()%></td>
             <td><%=r.getCallDetails()%></td>
             <td><%=r.getParam()%></td>
            </tr>
            <% } %>
          <% } %>
          </table>
      </div>
  </div>
</div>
<jsp:include page="footer.jsp" />
