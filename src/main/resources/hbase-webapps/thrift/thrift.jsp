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
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.util.VersionInfo"
  import="java.util.Date"
%>

<%
Configuration conf = (Configuration)getServletContext().getAttribute("hbase.conf");
long startcode = conf.getLong("startcode", System.currentTimeMillis());
String listenPort = conf.get("hbase.regionserver.thrift.port", "9090");
String serverInfo = listenPort + "," + String.valueOf(startcode);
String implType = conf.get("hbase.regionserver.thrift.server.type", "threadpool");
String compact = conf.get("hbase.regionserver.thrift.compact", "false");
String framed = conf.get("hbase.regionserver.thrift.framed", "false");
%>

<?xml version="1.0" encoding="UTF-8" ?>
<!-- Commenting out DOCTYPE so our blue outline shows on hadoop 0.20.205.0, etc.
     See tail of HBASE-2110 for explaination.
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
-->
<html xmlns="http://www.w3.org/1999/xhtml">
<head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
<title>HBase Thrift Server</title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>

<body>
<a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase"><img src="/static/hbase_logo.png" alt="HBase Logo" title="HBase Logo" /></a>
<h1 id="page_title">ThriftServer: <%= serverInfo %></h1>
<p id="links_menu">
  <a href="/logs/">Local logs</a>,
  <a href="/stacks">Thread Dump</a>,
  <a href="/logLevel">Log Level</a>,
<% if (HBaseConfiguration.isShowConfInServlet()) { %>
  <a href="/conf">HBase Configuration</a>
<% } %>
</p>
<hr id="head_rule" />

<h2>Attributes</h2>
<table id="attributes_table">
<col style="width: 10%;"/>
<col />
<col style="width: 20%;"/>
<tr><th>Attribute Name</th><th>Value</th><th>Description</th></tr>
<tr><td>HBase Version</td><td><%= VersionInfo.getVersion() %>, r<%= VersionInfo.getRevision() %></td><td>HBase version and revision</td></tr>
<tr><td>HBase Compiled</td><td><%= VersionInfo.getDate() %>, <%= VersionInfo.getUser() %></td><td>When HBase version was compiled and by whom</td></tr>
<tr><td>Thrift Server Start Time</td><td><%= new Date(startcode) %></td><td>Date stamp of when this Thrift server was started</td></tr>
<tr><td>Thrift Impl Type</td><td><%= implType %></td><td>Thrift RPC engine implementation type chosen by this Thrift server</td></tr>
<tr><td>Compact Protocol</td><td><%= compact %></td><td>Thrift RPC engine uses compact protocol</td></tr>
<tr><td>Framed Transport</td><td><%= framed %></td><td>Thrift RPC engine uses framed transport</td></tr>
</table>

<hr id="foot_rule" />
<a href="http://wiki.apache.org/hadoop/Hbase/ThriftApi">Apache HBase Wiki on Thrift</a>

</body>
</html>
