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
  import="java.util.Collections"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.shaded.protobuf.generated.TooSlowLog"
  import="org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil"
  import="org.apache.hadoop.hbase.namequeues.NamedQueueRecorder"
  import="org.apache.hadoop.hbase.namequeues.RpcLogDetails"
  import="org.apache.hadoop.hbase.namequeues.request.NamedQueueGetRequest"
  import="org.apache.hadoop.hbase.namequeues.response.NamedQueueGetResponse"
  import="org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponseRequest"
  import="org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.SlowLogResponseRequest.LogType"
%>
<%
  HRegionServer rs = (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  List<TooSlowLog.SlowLogPayload> slowLogs = Collections.emptyList();
  List<TooSlowLog.SlowLogPayload> largeLogs = Collections.emptyList();
  Configuration conf = rs.getConfiguration();
  boolean isSlowLogEnabled = conf.getBoolean(HConstants.SLOW_LOG_BUFFER_ENABLED_KEY, false);

  if(rs.isOnline() && isSlowLogEnabled) {
    NamedQueueRecorder namedQueueRecorder = rs.getNamedQueueRecorder();

    NamedQueueGetRequest slowRequest = new NamedQueueGetRequest();
    slowRequest.setNamedQueueEvent(RpcLogDetails.SLOW_LOG_EVENT);
    slowRequest.setSlowLogResponseRequest(SlowLogResponseRequest.newBuilder()
      .setLogType(LogType.SLOW_LOG)
      .setLimit(Integer.MAX_VALUE)
      .build());
    NamedQueueGetResponse slowResponse =
      namedQueueRecorder.getNamedQueueRecords(slowRequest);
    slowLogs = slowResponse.getSlowLogPayloads();


    NamedQueueGetRequest largeRequest = new NamedQueueGetRequest();
    largeRequest.setNamedQueueEvent(RpcLogDetails.SLOW_LOG_EVENT);
    largeRequest.setSlowLogResponseRequest(SlowLogResponseRequest.newBuilder()
      .setLogType(LogType.LARGE_LOG)
      .setLimit(Integer.MAX_VALUE)
      .build());
    NamedQueueGetResponse largeResponse =
      namedQueueRecorder.getNamedQueueRecords(largeRequest);
    largeLogs = largeResponse.getSlowLogPayloads();
  }
%>

<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>


<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
    <h1>Operations Details</h1>
    <p>HBase uses some fixed-size ring buffers to maintain rolling window history of specific server-side operation details.
    This page list all operation details retrieve from these ring buffers</p>
    </div>
  </div>
<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item">
      <a class="nav-link active" href="#tab_named_queue1" data-bs-toggle="tab" role="tab"> Slow RPCs </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#tab_named_queue2" data-bs-toggle="tab" role="tab"> Large Response RPCs </a>
    </li>
  </ul>
    <div class="tab-content">
      <div class="tab-pane active" id="tab_named_queue1" role="tabpanel">
      <p>Slow RPCs record those RPCs whose processing time is greater than the threshold (see the setting 'hbase.ipc.warn.response.time' for details)</p>
        <table class="table table-striped" style="white-space:nowrap">
        <tr>
            <th>Start Time</th>
            <th>Processing Time</th>
            <th>Queue Time</th>
            <th>FsRead Time</th>
            <th>Response Size</th>
            <th>Block Bytes Scanned</th>
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
            <th>Request Attributes</th>
            <th>Connection Attributes</th>
          </tr>
          <% if (slowLogs != null && !slowLogs.isEmpty()) {%>
            <% for (TooSlowLog.SlowLogPayload r : slowLogs) { %>
            <tr>
             <td><%=new Date(r.getStartTime() + 1800*1000)%></td>
             <td><%=r.getProcessingTime()%>ms</td>
             <td><%=r.getQueueTime()%>ms</td>
             <td><%=r.getFsReadTime()%>ms</td>
             <td><%=StringUtils.byteDesc(r.getResponseSize())%></td>
             <td><%=StringUtils.byteDesc(r.getBlockBytesScanned())%></td>
             <td><%=r.getClientAddress()%></td>
             <td><%=r.getServerClass()%></td>
             <td><%=r.getMethodName()%></td>
             <td><%=r.getRegionName()%></td>
             <td><%=r.getUserName()%></td>
             <td><%=r.getMultiGets()%></td>
             <td><%=r.getMultiMutations()%></td>
             <td><%=r.getMultiServiceCalls()%></td>
             <td><%=r.getCallDetails()%></td>
             <td><%=r.getParam()%></td>
             <td><%=ProtobufUtil.convertAttributesToCsv(r.getRequestAttributeList())%></td>
             <td><%=ProtobufUtil.convertAttributesToCsv(r.getConnectionAttributeList())%></td>
            </tr>
            <% } %>
          <% } %>
          </table>
      </div>
      <div class="tab-pane" id="tab_named_queue2" role="tabpanel">
        <p>Large response RPCs record those RPCs whose returned data size is greater than the threshold (see the setting'hbase.ipc.warn.response.size' for details)</p>
          <table class="table table-striped" style="white-space:nowrap">
          <tr>
            <th>Start Time</th>
            <th>Processing Time</th>
            <th>Queue Time</th>
            <th>FsRead Time</th>
            <th>Response Size</th>
            <th>Block Bytes Scanned</th>
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
            <th>Request Attributes</th>
            <th>Connection Attributes</th>
          </tr>
          <% if (largeLogs != null && !largeLogs.isEmpty()) {%>
            <% for (TooSlowLog.SlowLogPayload r : largeLogs) { %>
            <tr>
             <td><%=new Date(r.getStartTime() + 1800*1000)%></td>
             <td><%=r.getProcessingTime()%>ms</td>
             <td><%=r.getQueueTime()%>ms</td>
             <td><%=r.getFsReadTime()%>ms</td>
             <td><%=StringUtils.byteDesc(r.getResponseSize())%></td>
             <td><%=StringUtils.byteDesc(r.getBlockBytesScanned())%></td>
             <td><%=r.getClientAddress()%></td>
             <td><%=r.getServerClass()%></td>
             <td><%=r.getMethodName()%></td>
             <td><%=r.getRegionName()%></td>
             <td><%=r.getUserName()%></td>
             <td><%=r.getMultiGets()%></td>
             <td><%=r.getMultiMutations()%></td>
             <td><%=r.getMultiServiceCalls()%></td>
             <td><%=r.getCallDetails()%></td>
             <td><%=r.getParam()%></td>
             <td><%=ProtobufUtil.convertAttributesToCsv(r.getRequestAttributeList())%></td>
             <td><%=ProtobufUtil.convertAttributesToCsv(r.getConnectionAttributeList())%></td>
            </tr>
            <% } %>
          <% } %>
          </table>
      </div>
  </div>
</div>
<jsp:include page="footer.jsp" />
