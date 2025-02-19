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
  <link rel="shortcut icon" href="/static/favicon.ico">
</head>

<body>
<nav class="navbar navbar-expand-md navbar-light fixed-top bg-light">
  <div class="container-fluid">
    <a class="navbar-brand" href="/thrift.jsp">
      <img src="/static/hbase_logo_small.png" alt="HBase Logo"/>
    </a>
    <button type="button" class="navbar-toggler" data-bs-toggle="collapse" data-bs-target=".navbar-collapse">
      <span class="navbar-toggler-icon"></span>
    </button>
    <div class="collapse navbar-collapse">
      <div class="navbar-nav">
        <li class="nav-item"><a class="nav-link" href="/thrift.jsp">Home</a></li>
        <li class="nav-item"><a class="nav-link" href="/logs/">Local logs</a></li>
        <li class="nav-item"><a class="nav-link" href="/logLevel">Log Level</a></li>
        <li class="nav-item dropdown">
          <a class="nav-link dropdown-toggle" href="#" id="navbarDropdownMenuLink" data-bs-toggle="dropdown" aria-haspopup="true" aria-expanded="false">
            Metrics <span class="caret"></span>
          </a>
          <div class="dropdown-menu" aria-labelledby="navbarDropdownMenuLink">
            <a class="dropdown-item" target="_blank" href="/jmx">JMX</a>
            <a class="dropdown-item" target="_blank" href="/jmx?description=true">JMX with description</a>
          </div>
        </li>
        <li class="nav-item"><a class="nav-link" href="/prof">Profiler</a></li>
        <% if (HBaseConfiguration.isShowConfInServlet()) { %>
        <li class="nav-item"><a class="nav-link" href="/conf">HBase Configuration</a></li>
        <% } %>
      </ul>
    </div><!--/.navbar-collapse -->
  </div><!--/.container-fluid -->
</nav>
