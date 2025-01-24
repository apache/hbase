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
         import="org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil"
         import="org.apache.hadoop.hbase.replication.ReplicationPeerConfig"
         import="org.apache.hadoop.hbase.replication.ReplicationPeerDescription"
         import="org.apache.hadoop.hbase.HBaseConfiguration"
         import="org.apache.hadoop.hbase.HConstants"
         import="org.apache.hadoop.hbase.NamespaceDescriptor"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.TableName"
         import="org.apache.hadoop.hbase.master.assignment.AssignmentManager"
         import="org.apache.hadoop.hbase.master.DeadServer"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.master.RegionState"
         import="org.apache.hadoop.hbase.master.ServerManager"
         import="org.apache.hadoop.hbase.quotas.QuotaUtil"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfoManager"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupUtil"
         import="org.apache.hadoop.hbase.security.access.PermissionStorage"
         import="org.apache.hadoop.hbase.security.visibility.VisibilityConstants"
         import="org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription"
         import="org.apache.hadoop.hbase.tool.CanaryTool"
         import="org.apache.hadoop.hbase.util.Bytes"
         import="org.apache.hadoop.hbase.util.CommonFSUtils"
         import="org.apache.hadoop.hbase.util.JvmVersion"
         import="org.apache.hadoop.hbase.util.PrettyPrinter"
         import="org.apache.hadoop.util.StringUtils"
         import="org.apache.hadoop.hbase.master.assignment.RegionStateNode"
         import="org.apache.hadoop.hbase.client.*" %>
<%!
  private static ServerName getMetaLocationOrNull(HMaster master) {
    RegionStateNode rsn = master.getAssignmentManager().getRegionStates()
      .getRegionStateNode(RegionInfoBuilder.FIRST_META_REGIONINFO);
    if (rsn != null) {
      return rsn.isInState(RegionState.State.OPEN) ? rsn.getRegionLocation() : null;
    }
    return null;
  }
%>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  String title;
  if(master.isActiveMaster()) {
    title = "Master: ";
  } else {
    title = "Backup Master: ";
  }
  title += master.getServerName().getHostname();
  pageContext.setAttribute("pageTitle", title);

  boolean catalogJanitorEnabled = master.isCatalogJanitorEnabled();
  ServerManager serverManager = master.getServerManager();

  ServerName metaLocation = null;
  List<ServerName> servers = null;
  Set<ServerName> deadServers = null;

  if (master.isActiveMaster()) {
    metaLocation = getMetaLocationOrNull(master);
    if (serverManager != null) {
      deadServers = serverManager.getDeadServers().copyServerNames();
      servers = serverManager.getOnlineServersList();
    }
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
    <!-- Various warnings that cluster admins should be aware of -->
    <% if(JvmVersion.isBadJvmVersion()) { %>
    <div class="alert alert-danger" role="alert">
      Your current JVM version <%= System.getProperty("java.version") %> is known to be
      unstable with HBase. Please see the
      <a href="http://hbase.apache.org/book.html#trouble.log.gc">HBase Reference Guide</a>
      for details.
    </div>
    <% } %>
  <% if(master.isInitialized() && !catalogJanitorEnabled) { %>
  <div class="alert alert-danger" role="alert">
    Please note that your cluster is running with the CatalogJanitor disabled. It can be
    re-enabled from the hbase shell by running the command 'catalogjanitor_switch true'
  </div>
  <% } %>
  <% if(master.isInMaintenanceMode()) { %>
  <div class="alert alert-warning" role="alert">
    Your Master is in maintenance mode. This is because hbase.master.maintenance_mode is
    set to true. Under the maintenance mode, no quota or no Master coprocessor is loaded.
  </div>
  <% } %>
  <% if(!master.isBalancerOn()) { %>
  <div class="alert alert-warning" role="alert">
    The Load Balancer is not enabled which will eventually cause performance degradation
    in HBase as Regions will not be distributed across all RegionServers. The balancer
    is only expected to be disabled during rolling upgrade scenarios.
  </div>
  <% } %>
  <% if(!master.isSplitOrMergeEnabled(MasterSwitchType.SPLIT)) { %>
  <div class="alert alert-warning" role="alert">
    Region splits are disabled. This may be the result of HBCK aborting while
    running in repair mode. Manually enable splits from the HBase shell,
    or re-run HBCK in repair mode.
  </div>
  <% } %>
  <% if(!master.isSplitOrMergeEnabled(MasterSwitchType.MERGE)) { %>
  <div class="alert alert-warning" role="alert">
    Region merges are disabled. This may be the result of HBCK aborting while
    running in repair mode. Manually enable merges from the HBase shell,
    or re-run HBCK in repair mode.
  </div>
  <% } %>
  <% if(master.getAssignmentManager() != null) { %>
    <jsp:include page="assignmentManagerStatus.jsp"/>
  <% } %>
  <% if (!master.isInMaintenanceMode() && master.getMasterCoprocessorHost() != null) { %>
    <% if (RSGroupUtil.isRSGroupEnabled(master.getConfiguration()) &&
    serverManager.getOnlineServersList().size() > 0) { %>
      <section>
        <h2><a name="rsgroup">RSGroup</a></h2>
        <jsp:include page="rsGroupList.jsp"/>
      </section>
    <% } %>
  <% } %>
  </div>
  <div class="row">
    <section>
      <h2><a name="regionservers">Region Servers</a></h2>
      <jsp:include page="regionServerList.jsp"/>

      <%if (deadServers != null) %>
      <& deadRegionServers &> TODO
    </%if>
    </section>
  </div>

  TODO

  <% } %>
</div> <!-- /.container-fluid content -->

<jsp:include page="footer.jsp"/>
