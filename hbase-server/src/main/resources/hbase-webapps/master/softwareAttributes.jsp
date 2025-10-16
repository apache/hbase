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
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.util.JvmVersion"
         import="org.apache.hadoop.hbase.util.CommonFSUtils"
         import="org.apache.hadoop.util.StringUtils"
         import="org.apache.hadoop.hbase.master.http.MasterStatusConstants"
         import="org.apache.hadoop.hbase.util.ZKStringFormatter" %>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);

  Map<String, Integer> frags = (Map<String, Integer>) request.getAttribute(MasterStatusConstants.FRAGS);
%>

<h2><a name="attributes">Software Attributes</a></h2>
<table id="attributes_table" class="table table-striped">
  <tr>
    <th>Attribute Name</th>
    <th>Value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>JVM Version</td>
    <td><%= JvmVersion.getVersion() %></td>
    <td>JVM vendor and version</td>
  </tr>
  <tr>
    <td>HBase Version</td>
    <td><%= org.apache.hadoop.hbase.util.VersionInfo.getVersion() %>, revision=<%= org.apache.hadoop.hbase.util.VersionInfo.getRevision() %></td><td>HBase version and revision</td>
  </tr>
  <tr>
    <td>HBase Compiled</td>
    <td><%= org.apache.hadoop.hbase.util.VersionInfo.getDate() %>, <%= org.apache.hadoop.hbase.util.VersionInfo.getUser() %></td>
    <td>When HBase version was compiled and by whom</td>
  </tr>
  <tr>
    <td>HBase Source Checksum</td>
    <td><%= org.apache.hadoop.hbase.util.VersionInfo.getSrcChecksum() %></td>
    <td>HBase source SHA512 checksum</td>
  </tr>
  <tr>
    <td>Hadoop Version</td>
    <td><%= org.apache.hadoop.util.VersionInfo.getVersion() %>, revision=<%= org.apache.hadoop.util.VersionInfo.getRevision() %></td>
    <td>Hadoop version and revision</td>
  </tr>
  <tr>
    <td>Hadoop Compiled</td>
    <td><%= org.apache.hadoop.util.VersionInfo.getDate() %>, <%= org.apache.hadoop.util.VersionInfo.getUser() %></td>
    <td>When Hadoop version was compiled and by whom</td>
  </tr>
  <tr>
    <td>Hadoop Source Checksum</td>
    <td><%= org.apache.hadoop.util.VersionInfo.getSrcChecksum() %></td>
    <td>Hadoop source MD5 checksum</td>
  </tr>
  <tr>
    <td>ZooKeeper Client Version</td>
    <td><%= org.apache.zookeeper.Version.getVersion() %>, revision=<%= org.apache.zookeeper.Version.getRevisionHash() %></td>
    <td>ZooKeeper client version and revision hash</td>
  </tr>
  <tr>
    <td>ZooKeeper Client Compiled</td>
    <td><%= org.apache.zookeeper.Version.getBuildDate() %></td>
    <td>When ZooKeeper client version was compiled</td>
  </tr>
  <tr>
    <td>ZooKeeper Quorum</td>
      <td> <%= ZKStringFormatter.formatZKString(master.getZooKeeper()) %> </td>
    <td>Addresses of all registered ZK servers. For more, see <a href="/zk.jsp">zk dump</a>.</td>
  </tr>
  <tr>
    <td>ZooKeeper Base Path</td>
    <td> <%= master.getZooKeeper().getZNodePaths().baseZNode %></td>
    <td>Root node of this cluster in ZK.</td>
  </tr>
  <tr>
    <td>Cluster Key</td>
    <td> <%= ZKStringFormatter.formatZKString(master.getZooKeeper()) %>:<%= master.getZooKeeper().getZNodePaths().baseZNode %></td>
    <td>Key to add this cluster as a peer for replication. Use 'help "add_peer"' in the shell for details.</td>
  </tr>
  <tr>
    <td>HBase Root Directory</td>
    <td><%= CommonFSUtils.getRootDir(master.getConfiguration()).toString() %></td>
    <td>Location of HBase home directory</td>
  </tr>
  <tr>
    <td>HMaster Start Time</td>
    <td><%= new Date(master.getMasterStartTime()) %></td>
    <td>Date stamp of when this HMaster was started</td>
  </tr>
  <% if (master.isActiveMaster()) { %>
  <tr>
    <td>HMaster Active Time</td>
    <td><%= new Date(master.getMasterActiveTime()) %></td>
    <td>Date stamp of when this HMaster became active</td>
  </tr>
  <tr>
    <td>HBase Cluster ID</td>
    <td><%= master.getClusterId() != null ? master.getClusterId() : "Not set" %></td>
    <td>Unique identifier generated for each HBase cluster</td>
  </tr>
  <tr>
    <td>Load average</td>
    <td><%= master.getServerManager() == null ? "0.00" :
      StringUtils.limitDecimalTo2(master.getServerManager().getAverageLoad()) %></td>
    <td>Average number of regions per regionserver. Naive computation.</td>
  </tr>
  <% if (frags != null) { %>
  <tr>
    <td>Fragmentation</td>
    <td><%= frags.get("-TOTAL-") != null ? frags.get("-TOTAL-") + "%" : "n/a" %></td>
    <td>Overall fragmentation of all tables, including hbase:meta</td>
  </tr>
  <% } %>
  <tr>
    <td>Coprocessors</td>
    <td><%= master.getMasterCoprocessorHost() == null ? "[]" :
      java.util.Arrays.toString(master.getMasterCoprocessors()) %></td>
    <td>Coprocessors currently loaded by the master</td>
  </tr>
  <tr>
    <td>LoadBalancer</td>
    <td><%= master.getLoadBalancerClassName() %></td>
    <td>LoadBalancer to be used in the Master</td>
  </tr>
  <% } %>
</table>
