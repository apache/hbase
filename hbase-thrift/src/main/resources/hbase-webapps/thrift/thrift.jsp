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
  import="org.apache.hadoop.hbase.util.VersionInfo"
  import="java.util.Date"
%>
<%@ page import="org.apache.hadoop.hbase.thrift.ImplType" %>
<%@ page import="org.apache.hadoop.hbase.util.JvmVersion" %>

<%
  Configuration conf = (Configuration)getServletContext().getAttribute("hbase.conf");
  String serverType = (String)getServletContext().getAttribute("hbase.thrift.server.type");
  long startcode = conf.getLong("startcode", System.currentTimeMillis());
  String listenPort = conf.get("hbase.regionserver.thrift.port", "9090");
  ImplType implType = ImplType.getServerImpl(conf);

  String transport =
    (implType.isAlwaysFramed() ||
      conf.getBoolean("hbase.regionserver.thrift.framed", false)) ? "Framed" : "Standard";
  String protocol =
    conf.getBoolean("hbase.regionserver.thrift.compact", false) ? "Compact" : "Binary";
  String qop = conf.get("hbase.thrift.security.qop", "None");

  pageContext.setAttribute("pageTitle", "HBase Thrift Server: " + listenPort);
%>

<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
    <div class="row inner_header">
        <div class="page-header">
            <h1>ThriftServer <small><%= listenPort %></small></h1>
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
        <td><%= JvmVersion.getVersion() %></td>
        <td>JVM vendor and version information</td>
      </tr>
        <tr>
            <td>HBase Version</td>
            <td><%= VersionInfo.getVersion() %>, r<%= VersionInfo.getRevision() %></td>
            <td>HBase version and revision</td>
        </tr>
        <tr>
            <td>HBase Compiled</td>
            <td><%= VersionInfo.getDate() %>, <%= VersionInfo.getUser() %></td>
            <td>When HBase version was compiled and by whom</td>
        </tr>
        <tr>
            <td>Thrift Server Start Time</td>
            <td><%= new Date(startcode) %></td>
            <td>Date stamp of when this Thrift server was started</td>
        </tr>
        <tr>
            <td>Thrift Impl Type</td>
            <td><%= implType.getOption() %></td>
            <td>Thrift RPC engine implementation type chosen by this Thrift server</td>
        </tr>
        <tr>
            <td>Protocol</td>
            <td><%= protocol %></td>
            <td>Thrift RPC engine protocol type</td>
        </tr>
        <tr>
            <td>Transport</td>
            <td><%= transport %></td>
            <td>Thrift RPC engine transport type</td>
        </tr>
        <tr>
            <td>Thrift Server Type</td>
            <td><%= serverType %></td>
            <td>The type of this Thrift server</td>
        </tr>
      <tr>
        <td>Quality of Protection</td>
        <td><%= qop %></td>
        <td>QOP Settings for SASL</td>
      </tr>
    </table>
    </section>
    </div>
    <div class="row">
        <section>
            <a href="http://hbase.apache.org/book.html#_thrift">
              Apache HBase Reference Guide chapter on Thrift</a>
        </section>
    </div>
</div>

<jsp:include page="footer.jsp" />
