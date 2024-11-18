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
         import="org.apache.hadoop.hbase.HBaseConfiguration"%>

<!DOCTYPE html>
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title><%= request.getParameter("pageTitle")%></title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="description" content="">

  <link href="/static/css/bootstrap.min.css" rel="stylesheet">
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
      <a class="navbar-brand" href="/rest.jsp"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
    </div>
    <div class="collapse navbar-collapse">
      <ul class="nav navbar-nav">
        <li class="active"><a href="/rest.jsp">Home</a></li>
        <li><a href="/logs/">Local logs</a></li>
        <li><a href="/processRest.jsp">Process Metrics</a></li>
        <li><a href="/logLevel">Log Level</a></li>
        <li><a href="/dump">Debug Dump</a></li>
        <li class="nav-item dropdown">
          <a class="nav-link dropdown-toggle" href="#" id="navbarDropdownMenuLink" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
            Metrics <span class="caret"></span>
          </a>
          <ul class="dropdown-menu" aria-labelledby="navbarDropdownMenuLink">
            <li><a target="_blank" href="/jmx">JMX</a></li>
            <li><a target="_blank" href="/jmx?description=true">JMX with description</a></li>
            <li><a target="_blank" href="/prometheus">Prometheus</a></li>
            <li><a target="_blank" href="/prometheus?description=true">Prometheus with description</a></li>
          </ul>
        </li>
        <li><a href="/prof">Profiler</a></li>
        <% if (HBaseConfiguration.isShowConfInServlet()) { %>
        <li><a href="/conf">HBase Configuration</a></li>
        <% } %>
      </ul>
    </div><!--/.nav-collapse -->
  </div>
</div>

