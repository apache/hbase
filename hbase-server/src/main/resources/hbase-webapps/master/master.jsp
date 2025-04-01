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
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.conf.Configuration"
         import="org.apache.hadoop.hbase.util.*" %>

<%
  String filter = request.getParameter("filter");
  String format = request.getParameter("format");
  if (format != null && format.equals("json")) {
    request.setAttribute("filter", filter);
    request.setAttribute("format", "json");
  %>
  <jsp:include page="taskMonitor.jsp"/>
<%
   return;
  }
%>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);

  Configuration conf = master.getConfiguration();
  Map<String, Integer> frags = MasterStatusUtil.getFragmentationInfo(master, conf);

  String title;
  if(master.isActiveMaster()) {
    title = "Master: ";
  } else {
    title = "Backup Master: ";
  }
  title += master.getServerName().getHostname();
  pageContext.setAttribute("pageTitle", title);

  ServerName metaLocation = null;

  if (master.isActiveMaster()) {
    metaLocation = MasterStatusUtil.getMetaLocationOrNull(master);
  }
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <% if(master.isActiveMaster()) { %>
  <div class="row inner_header top_header">
    <div class="page-header">
      <h1>Master <small><%= master.getServerName().getHostname() %></small></h1>
    </div>
  </div>

  <div class="row">
    <jsp:include page="warnings.jsp"/>
  </div>
  <div class="row">
    <section>
      <h2><a name="regionservers">Region Servers</a></h2>
      <jsp:include page="regionServerList.jsp"/>

      <jsp:include page="deadRegionServers.jsp"/>
    </section>
  </div>
  <div class="row">
    <section>
      <jsp:include page="backupMasterStatus.jsp"/>
    </section>
  </div>
  <div class="row">
    <section>
      <h2><a name="tables">Tables</a></h2>
      <div class="tabbable">
        <ul class="nav nav-pills" role="tablist">
          <li class="nav-item">
            <a class="nav-link active" href="#tab_userTables" data-bs-toggle="tab" role="tab">User Tables</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="#tab_catalogTables" data-bs-toggle="tab" role="tab">System Tables</a>
          </li>
          <li class="nav-item">
            <a class="nav-link" href="#tab_userSnapshots" data-bs-toggle="tab" role="tab">Snapshots</a>
          </li>
        </ul>
        <div class="tab-content">
          <div class="tab-pane active" id="tab_userTables" role="tabpanel">
            <%if (metaLocation != null) { %>
              <% request.setAttribute("frags", frags); %>
              <jsp:include page="userTables.jsp"/>
            <% } %>
          </div>
          <div class="tab-pane" id="tab_catalogTables" role="tabpanel">
            <%if (metaLocation != null) { %>
            <% request.setAttribute("frags", frags); %>
              <jsp:include page="catalogTables.jsp"/>
            <% } %>
          </div>
        <div class="tab-pane" id="tab_userSnapshots" role="tabpanel">
          <%-- tab.js will load userSnapshots.jsp with an AJAX request here. --%>
        </div>
      </div>
    </div>
    </section>
  </div>
  <div class="row">
    <section>
      <h2><a name="region_visualizer"></a>Region Visualizer</h2>
      <jsp:include page="regionVisualizer.jsp"/>
    </section>
  </div>
  <div class="row">
    <section>
      <h2><a name="peers">Peers</a></h2>
      <jsp:include page="peerConfigs.jsp"/>
    </section>
  </div>
<% } else { %>
    <section>
      <jsp:include page="backupMasterStatus.jsp"/>
    </section>
<% } %>

  <section>
    <jsp:include page="taskMonitor.jsp"/>
  </section>

  <section>
    <% request.setAttribute("frags", frags); %>
    <jsp:include page="softwareAttributes.jsp"/>
  </section>

</div> <!-- /.container-fluid content -->


<jsp:include page="footer.jsp"/>

<script src="/static/js/jquery.tablesorter.min.js" type="text/javascript"></script>
<script src="/static/js/parser-date-iso8601.min.js" type="text/javascript"></script>
<script src="/static/js/jqSpager.js" type="text/javascript"></script>
<script src="/static/js/masterStatusInit.js" type="text/javascript"></script>
