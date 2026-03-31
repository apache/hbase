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
         import="org.apache.hadoop.hbase.util.JvmVersion"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.util.ZKStringFormatter"%>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  ServerName masterServerName = (ServerName) request.getAttribute("masterServerName");
  int infoPort = (int) request.getAttribute("infoPort");
%>
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
    <td><%= org.apache.hadoop.hbase.util.VersionInfo.getVersion() %>, revision=<%= org.apache.hadoop.hbase.util.VersionInfo.getRevision() %></td>
    <td>HBase version and revision</td>
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
    <td><%= ZKStringFormatter.formatZKString(regionServer.getZooKeeper()) %></td>
    <td>Addresses of all registered ZK servers</td>
  </tr>
  <tr>
    <td>Coprocessors</td>
    <td><%= Arrays.toString(regionServer.getRegionServerCoprocessors()) %></td>
    <td>Coprocessors currently loaded by this regionserver</td>
  </tr>
  <tr>
    <td>RS Start Time</td>
    <td><%= new Date(regionServer.getStartcode()) %></td>
    <td>Date stamp of when this region server was started</td>
  </tr>
  <tr>
    <td>HBase Master</td>
    <td>
      <% if (masterServerName == null) { %>
      No master found
      <% } else {
        String host = masterServerName.getHostname() + ":" + infoPort;
        String url = "//" + host + "/master.jsp";
      %>
        <a href="<%= url %>"><%= host %></a>
      <% } %>
    </td>
    <td>Address of HBase Master</td>
  </tr>
</table>
