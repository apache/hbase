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
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>HBase Thrift Server: <%= listenPort %></title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">

    <link href="/static/css/bootstrap.min.css" rel="stylesheet">
    <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
    <link href="/static/css/hbase.css" rel="stylesheet">
  </head>

  <body>
  <div class="navbar  navbar-fixed-top navbar-default">
      <div class="container-fluid">
          <div class="navbar-header">
              <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
                  <span class="icon-bar"></span>
                  <span class="icon-bar"></span>
                  <span class="icon-bar"></span>
              </button>
              <a class="navbar-brand" href="/thrift.jsp"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
          </div>
          <div class="collapse navbar-collapse">
              <ul class="nav navbar-nav">
                <li class="active"><a href="/">Home</a></li>
                <li><a href="/logs/">Local logs</a></li>
                <li><a href="/logLevel">Log Level</a></li>
                <li><a href="/jmx">Metrics Dump</a></li>
                <% if (HBaseConfiguration.isShowConfInServlet()) { %>
                <li><a href="/conf">HBase Configuration</a></li>
                <% } %>
            </ul>
          </div><!--/.nav-collapse -->
        </div>
      </div>
    </div>

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
            <td><%= implType %></td>
            <td>Thrift RPC engine implementation type chosen by this Thrift server</td>
        </tr>
        <tr>
            <td>Compact Protocol</td>
            <td><%= compact %></td>
            <td>Thrift RPC engine uses compact protocol</td>
        </tr>
        <tr>
            <td>Framed Transport</td>
            <td><%= framed %></td>
            <td>Thrift RPC engine uses framed transport</td>
        </tr>
    </table>
    </section>
    </div>
    <div class="row">
        <section>
            <a href="http://hbase.apache.org/book.html#_thrift">Apache HBase Reference Guide chapter on Thrift</a>
        </section>
    </div>
</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>
<script src="/static/js/tab.js" type="text/javascript"></script>
</body>
</html>
