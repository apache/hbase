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
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.hbase.client.ServerType"
  import="org.apache.hadoop.hbase.client.LogEntry"
  import="org.apache.hadoop.hbase.client.BalancerRejection"
  import="org.apache.hadoop.hbase.client.BalancerDecision"
%>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master Operation Details: " + master.getServerName());
  Configuration conf = master.getConfiguration();

  List<BalancerRejection> logList = null;
  List<BalancerDecision> decisionList = null;
  if(master.isInitialized()) {
    try (Admin admin = master.getConnection().getAdmin()) {
      logList = (List<BalancerRejection>)(List<?>)admin.getLogEntries(null, "BALANCER_REJECTION", ServerType.MASTER, 250, null);
      decisionList = (List<BalancerDecision>)(List<?>)admin.getLogEntries(null, "BALANCER_DECISION", ServerType.MASTER, 250, null);
    }
  }
%>

<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>


<div class="container-fluid content">
  <div class="row top_header">
    <div class="page-header">
    <h2>Operations Details</h2>
    <p>HBase uses some fixed-size ring buffers to maintain rolling window history of specific server-side operation details.
    This page list all operation details retrieve from these ring buffers</p>
    </div>
    </div>
<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item">
      <a class="nav-link active" href="#tab_named_queue1" data-bs-toggle="tab" role="tab"> Balancer Rejection </a>
    </li>
    <li class="nav-item">
      <a class="nav-link" href="#tab_named_queue2" data-bs-toggle="tab" role="tab"> Balancer Decision </a>
    </li>
  </ul>
    <div class="tab-content">
      <div class="tab-pane active" id="tab_named_queue1" role="tabpanel">
      <p>Balancer Rejection explain why balancer is skipping runs and explain all factors considered</p>
        <table class="table table-striped">
          <tr>
            <th>Reason</th>
            <th>CostFunctions Details</th>
          </tr>
          <% if (logList == null) { %>
          <% } else { %>
            <% for (BalancerRejection entry: logList) {  %>
              <tr>
                <td><%=entry.getReason()%></td>
                <td>
                <% List<String> costFunctions = entry.getCostFuncInfoList();
                   if (costFunctions != null && !costFunctions.isEmpty()) { %>
                  <table>
                  <% for (String costFunctionInfo: entry.getCostFuncInfoList() ) { %>
                    <tr><td><%= costFunctionInfo %></td></tr>
                  <% }%>
                  </table>
                  <% } %>
                </td>
              </tr>
            <% } %>
          <% } %>
          </table>
      </div>
      <div class="tab-pane" id="tab_named_queue2" role="tabpanel">
      <p>Balancer Decision displayed the history of decision(factor details and weights and costs) made by LoadBalancers</p>
          <table class="table table-striped">
            <tr>
              <th>Initial Function Costs</th>
              <th>Final Function Costs</th>
              <th>Init Total Cost</th>
              <th>Computed Total Cost</th>
              <th>Computed Steps</th>
              <th>Region Plans</th>
            </tr>
            <% if (decisionList == null) { %>
            <% } else { %>
              <% for (BalancerDecision decision: decisionList) {  %>
                <tr>
                  <td><%=decision.getInitialFunctionCosts()%></td>
                  <td><%=decision.getFinalFunctionCosts()%></td>
                  <td><%=StringUtils.format("%.2f", decision.getInitTotalCost())%></td>
                  <td><%=StringUtils.format("%.2f", decision.getComputedTotalCost())%></td>
                  <td><%=decision.getComputedSteps()%></td>
                  <%
                  List<String> regionPlans = decision.getRegionPlans();
                  if ( regionPlans == null) {%>
                    <td></td>
                  <% }  else { %>
                    <td>
                    <table>
                    <% for (String plan : regionPlans) { %>
                      <tr><td><%=plan%></td></tr>
                    <% } %>
                    </table>
                    </td>
                  <% } %>
                </tr>
              <% } %>
            <% } %>
        </div>
      </div>
  </div>
</div>
<jsp:include page="footer.jsp" />
