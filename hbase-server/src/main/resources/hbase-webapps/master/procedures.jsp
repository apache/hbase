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
  import="static org.apache.commons.lang3.StringEscapeUtils.escapeXml"
  import="java.util.Collections"
  import="java.util.Comparator"
  import="java.util.ArrayList"
  import="java.util.Date"
  import="java.util.List"
  import="java.util.Set"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv"
  import="org.apache.hadoop.hbase.procedure2.LockedResource"
  import="org.apache.hadoop.hbase.procedure2.Procedure"
  import="org.apache.hadoop.hbase.procedure2.ProcedureExecutor"
  import="org.apache.hadoop.hbase.procedure2.util.StringUtils"
  import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
%>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  ProcedureExecutor<MasterProcedureEnv> procExecutor = master.getMasterProcedureExecutor();
  List<Procedure<MasterProcedureEnv>> procedures = procExecutor.getProcedures();
  Collections.sort(procedures, new Comparator<Procedure>() {
    @Override
    public int compare(Procedure lhs, Procedure rhs) {
      long cmp = lhs.getParentProcId() - rhs.getParentProcId();
      cmp = cmp != 0 ? cmp : lhs.getProcId() - rhs.getProcId();
      return cmp < 0 ? -1 : cmp > 0 ? 1 : 0;
    }
  });

  List<LockedResource> lockedResources = master.getLocks();
  pageContext.setAttribute("pageTitle", "HBase Master Procedures: " + master.getServerName());
%>
<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <div class="row">
      <div class="page-header">
          <h1>Procedures</h1>
      </div>
  </div>
  <p>We do not list procedures that have completed successfully; their number makes it hard to spot the problematics.</p>
  <table class="table table-striped" width="90%" >
    <tr>
        <th>Id</th>
        <th>Parent</th>
        <th>State</th>
        <th>Owner</th>
        <th>Type</th>
        <th>Start Time</th>
        <th>Last Update</th>
        <th>Errors</th>
        <th>Parameters</th>
    </tr>
    <%
      int displayCount = 0;
      for (Procedure<?> proc : procedures) {
      // Don't show SUCCESS procedures.
      if (proc.isSuccess()) {
        continue;
      }
      displayCount++;
    %>
      <tr>
        <td><%= proc.getProcId() %></td>
        <td><%= proc.hasParent() ? proc.getParentProcId() : "" %></td>
        <td><%= escapeXml(proc.getState().toString() + (proc.isBypass() ? "(Bypass)" : "")) %></td>
        <td><%= proc.hasOwner() ? escapeXml(proc.getOwner()) : "" %></td>
        <td><%= escapeXml(proc.getProcName()) %></td>
        <td><%= new Date(proc.getSubmittedTime()) %></td>
        <td><%= new Date(proc.getLastUpdate()) %></td>
        <td><%= escapeXml(proc.isFailed() ? proc.getException().unwrapRemoteIOException().getMessage() : "") %></td>
        <td><%= escapeXml(proc.toString()) %></td>
      </tr>
    <% } %>
    <%
    if (displayCount > 0) {
    %>
      <p><%= displayCount %> procedure(s).</p>
    <%
    }
    %>
  </table>
</div>
<br />
<div class="container-fluid content">
  <div class="row">
      <div class="page-header">
          <h1>Locks</h1>
      </div>
  </div>
    <%
    if (lockedResources.size() > 0) {
    %>
    <p><%= lockedResources.size() %> lock(s).</p>
    <%
    }
    %>
  <% for (LockedResource lockedResource : lockedResources) { %>
    <h2><%= lockedResource.getResourceType() %>: <%= lockedResource.getResourceName() %></h2>
    <%
      switch (lockedResource.getLockType()) {
      case EXCLUSIVE:
    %>
    <p>Lock type: EXCLUSIVE</p>
    <p>Owner procedure: <%= escapeXml(lockedResource.getExclusiveLockOwnerProcedure().toStringDetails()) %></p>
    <%
        break;
      case SHARED:
    %>
    <p>Lock type: SHARED</p>
    <p>Number of shared locks: <%= lockedResource.getSharedLockCount() %></p>
    <%
        break;
      }

      List<Procedure<?>> waitingProcedures = lockedResource.getWaitingProcedures();

      if (!waitingProcedures.isEmpty()) {
    %>
        <h3>Waiting procedures</h3>
        <table class="table table-striped" width="90%" >
        <% for (Procedure<?> proc : procedures) { %>
         <tr>
            <td><%= escapeXml(proc.toStringDetails()) %></td>
          </tr>
        <% } %>
        </table>
    <% } %>
  <% } %>
</div>
<br />
<jsp:include page="footer.jsp" />
