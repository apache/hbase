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
  import="java.util.concurrent.TimeUnit"
  import="java.util.ArrayList"
  import="java.util.List"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.quotas.MasterQuotaManager"
  import="org.apache.hadoop.hbase.quotas.QuotaRetriever"
  import="org.apache.hadoop.hbase.quotas.QuotaSettings"
  import="org.apache.hadoop.hbase.quotas.ThrottleSettings"
%>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
  pageContext.setAttribute("pageTitle", "HBase Master Quotas: " + master.getServerName());
  List<ThrottleSettings> regionServerThrottles = new ArrayList<>();
  List<ThrottleSettings> namespaceThrottles = new ArrayList<>();
  List<ThrottleSettings> userThrottles = new ArrayList<>();
  MasterQuotaManager quotaManager = master.getMasterQuotaManager();
  boolean exceedThrottleQuotaEnabled = false;
  if (quotaManager != null) {
    exceedThrottleQuotaEnabled = quotaManager.isExceedThrottleQuotaEnabled();
    try (QuotaRetriever scanner = QuotaRetriever.open(conf, null)) {
      for (QuotaSettings quota : scanner) {
        if (quota instanceof ThrottleSettings) {
          ThrottleSettings throttle = (ThrottleSettings) quota;
          if (throttle.getUserName() != null) {
            userThrottles.add(throttle);
          } else if (throttle.getNamespace() != null) {
            namespaceThrottles.add(throttle);
          } else if (throttle.getRegionServer() != null) {
            regionServerThrottles.add(throttle);
          }
        }
      }
    }
  }
%>
<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>
<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
      <h1>Throttle Quotas</h1>
    </div>
  </div>
</div>

<%if (quotaManager != null) {%>

<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
      <h2>Rpc Throttle Enabled</h2>
    </div>
  </div>
  <%if (quotaManager.isRpcThrottleEnabled()) {%>
  <div class="alert alert-success">
    Rpc throttle is enabled.
  </div>
  <% } else {%>
  <div class="alert alert-info">
    Rpc throttle is disabled. All requests will not be throttled.<br/>
    Use 'enable_rpc_throttle' shell command to enable it.
  </div>
  <% } %>
</div>

<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
      <h2>Exceed Throttle Quota Enabled</h2>
    </div>
  </div>
  <%if (exceedThrottleQuotaEnabled) {%>
  <div class="alert alert-success">
    Exceed throttle quota is enabled. The user/table/namespace throttle quotas can exceed the limit
    if a region server has available quotas.<br/>
    Use 'disable_exceed_throttle_quota' shell command to disable it.
  </div>
  <% } else {%>
  <div class="alert alert-info">
    Exceed throttle quota is disabled.
  </div>
  <% } %>
</div>

<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
      <h2>RegionServer Throttle Quotas</h2>
    </div>
  </div>
<%
  if (regionServerThrottles.size() > 0) {
%>
  <table class="table table-striped" width="90%" >
    <tr>
      <th>RegionServer</th>
      <th>Limit</th>
      <th>Type</th>
      <th>TimeUnit</th>
      <th>Scope</th>
    </tr>
    <% for (ThrottleSettings throttle : regionServerThrottles) { %>
      <tr>
        <td><%= throttle.getRegionServer() == null ? "" : throttle.getRegionServer() %></td>
        <td><%= throttle.getSoftLimit() %></td>
        <td><%= throttle.getThrottleType() %></td>
        <td><%= throttle.getTimeUnit() %></td>
        <td><%= throttle.getQuotaScope() %></td>
        <% if (exceedThrottleQuotaEnabled && throttle.getTimeUnit() != null && throttle.getTimeUnit() != TimeUnit.SECONDS) { %>
        <td style="color:red;">Exceed throttle quota is enabled, but RegionServer throttle is not in SECONDS time unit.</td>
        <% }%>
      </tr>
    <% } %>
  </table>
  <% } else if (exceedThrottleQuotaEnabled) { %>
  <div class="alert alert-danger">
    Exceed throttle quota is enabled, but RegionServer throttle quotas are not set.<br/>
    Please set RegionServer read and write throttle quotas in SECONDS time unit.<br/>
    eg. set_quota TYPE => THROTTLE, REGIONSERVER => 'all', THROTTLE_TYPE => WRITE, LIMIT => '20000req/sec'
  </div>
  <%}%>
</div>

<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
      <h2>Namespace Throttle Quotas</h2>
    </div>
  </div>
  <%
    if (namespaceThrottles.size() > 0) {
  %>
  <table class="table table-striped" width="90%" >
    <tr>
      <th>Namespace</th>
      <th>Limit</th>
      <th>Type</th>
      <th>TimeUnit</th>
      <th>Scope</th>
    </tr>
    <% for (ThrottleSettings throttle : namespaceThrottles) { %>
    <tr>
      <td><%= throttle.getNamespace() == null ? "" : throttle.getNamespace() %></td>
      <td><%= throttle.getSoftLimit() %></td>
      <td><%= throttle.getThrottleType() %></td>
      <td><%= throttle.getTimeUnit() %></td>
      <td><%= throttle.getQuotaScope() %></td>
    </tr>
    <% } %>
  </table>
  <% } %>
</div>

<div class="container-fluid content">
  <div class="row">
    <div class="page-header">
      <h2>User Throttle Quotas</h2>
    </div>
  </div>
  <%
    if (userThrottles.size() > 0) {
  %>
  <table class="table table-striped" width="90%" >
    <tr>
      <th>User</th>
      <th>Namespace</th>
      <th>Table</th>
      <th>Limit</th>
      <th>Type</th>
      <th>TimeUnit</th>
      <th>Scope</th>
    </tr>
    <% for (ThrottleSettings throttle : userThrottles) { %>
    <tr>
      <td><%= throttle.getUserName() == null ? "" : throttle.getUserName() %></td>
      <td><%= throttle.getNamespace() == null ? "" : throttle.getNamespace() %></td>
      <td><%= throttle.getTableName() == null ? "" : throttle.getTableName() %></td>
      <td><%= throttle.getSoftLimit() %></td>
      <td><%= throttle.getThrottleType() %></td>
      <td><%= throttle.getTimeUnit() %></td>
      <td><%= throttle.getQuotaScope() %></td>
    </tr>
    <% } %>
  </table>
  <% } %>
</div>

<% } %>

<jsp:include page="footer.jsp" />
