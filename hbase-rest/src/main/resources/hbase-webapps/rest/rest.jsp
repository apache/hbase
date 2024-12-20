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
         import="org.apache.hadoop.conf.Configuration"
         import="org.apache.hadoop.hbase.rest.RESTServer"
         import="org.apache.hadoop.hbase.rest.model.VersionModel"
         import="org.apache.hadoop.hbase.util.VersionInfo"
         import="java.util.Date"%>

<%
  Configuration conf = (Configuration) getServletContext().getAttribute("hbase.conf");
  long startcode = conf.getLong("startcode", System.currentTimeMillis());

  final RESTServer restServer = (RESTServer) getServletContext().getAttribute(RESTServer.REST_SERVER);
  final String hostName = restServer.getServerName().getHostname();
  pageContext.setAttribute("pageTitle", "HBase REST Server: " + hostName);
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
    <div class="row inner_header">
        <div class="page-header">
            <h1>RESTServer <small><%= hostName %></small></h1>
        </div>
    </div>
    <div class="row">

    <section>
    <h2>Software Attributes</h2>
    <table id="attributes_table" class="table table-striped">
        <tr>
            <th>Attribute Name</th>
            <th>Value</th>
            <th>Description</th>
        </tr>
      <tr>
        <td>JVM Version</td>
        <td><%= new VersionModel(getServletContext()).getJVMVersion() %></td>
        <td>JVM vendor and version</td>
      </tr>
        <tr>
            <td>HBase Version</td>
            <td><%= VersionInfo.getVersion() %>, revision=<%= VersionInfo.getRevision() %></td>
            <td>HBase version and revision</td>
        </tr>
        <tr>
            <td>HBase Compiled</td>
            <td><%= VersionInfo.getDate() %>, <%= VersionInfo.getUser() %></td>
            <td>When HBase version was compiled and by whom</td>
        </tr>
        <tr>
            <td>REST Server Start Time</td>
            <td><%= new Date(startcode) %></td>
            <td>Date stamp of when this REST server was started</td>
        </tr>
    </table>
    </section>
    </div>
    <div class="row">

    <section>
<a href="http://hbase.apache.org/book.html#_rest">Apache HBase documentation about REST</a>
    </section>
    </div>
</div>

<jsp:include page="footer.jsp" />

