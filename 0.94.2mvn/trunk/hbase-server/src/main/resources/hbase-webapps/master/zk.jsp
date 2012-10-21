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
  import="java.io.IOException"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.client.HConnection"
  import="org.apache.hadoop.hbase.client.HConnectionManager"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.zookeeper.ZKUtil"
  import="org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.HConstants"%><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  ZooKeeperWatcher watcher = master.getZooKeeperWatcher();
%>

<?xml version="1.0" encoding="UTF-8" ?>
<!-- Dont put a doctype jetty doesnt serve the css/js out correctly so we need quirks mode on -->
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta charset="utf-8">
    <title>ZooKeeper Dump</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">


    <link href="/static/css/bootstrap.css" rel="stylesheet">
    <link href="/static/css/hbase.css" rel="stylesheet">
    <link href="/static/css/bootstrap-responsive.css" rel="stylesheet">
    <!--[if lt IE 9]>
      <script src="/static/js/html5shiv.js"></script>
    <![endif]-->
</head>
    <body>

        <div class="navbar navbar-fixed-top">
          <div class="navbar-inner">
            <div class="container">
              <a class="btn btn-navbar" data-toggle="collapse" data-target=".nav-collapse">
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
                <span class="icon-bar"></span>
              </a>
              <a class="brand" href="/rs-status">HBase Region Server</a>
              <div class="nav-collapse">
                <ul class="nav">
                    <li><a href="/">Home</a></li>
                    <li><a href="/logs/">Local logs</a></li>
                    <li><a href="/logLevel">Log Level</a></li>
                    <li><a href="/dump">Debug dump</a></li>
                    <li><a href="/jmx">Metrics Dump</a></li>
                </ul>
              </div><!--/.nav-collapse -->
            </div>
          </div>
        </div>

        <div class="container">
            <div class="row inner_header">
                <div class="span8">
                    <h1>Zookeeper Dump</h1>
                </div>
                <div class="span4 logo">
                    <img src="/static/hbase_logo.png" height="66" width="266" alt="HBase logo"/>
                </div>
            </div>
            <div class="row">
                <div class="span12">
                    <pre><%= ZKUtil.dump(watcher).trim() %></pre>
                </div>
            </div>
        </div>
     <script src="/static/js/jquery.min.js" type="text/javascript"></script>
     <script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

    </body>
</html>
