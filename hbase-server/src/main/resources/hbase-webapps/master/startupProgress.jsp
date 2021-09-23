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
%>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.master.startupprogress.Phase" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master: " + master.getServerName());
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>Startup Progress</h1>
    </div>
  </div>

  <table class="table table-striped">
    <tr>
      <th>Phase</th>
      <th>Status</th>
      <th>Start Time (UTC)</th>
      <th>End Time (UTC)</th>
      <th>Elapsed Time</th>

    </tr>
    <% for (Phase phase : master.getStartupPhases()) { %>
    <tr>
      <td><%= phase.getName() %></td>
      <td><%= phase.getStatus() %></td>
      <td><%= phase.getStartTime() %></td>
      <td><%= phase.getEndTime() %></td>
      <td><%= phase.getElapsedTime() %></td>
    </tr>
    <% } %>

  </table>

</div>

<jsp:include page="footer.jsp"/>
