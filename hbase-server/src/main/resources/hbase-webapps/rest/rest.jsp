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
  import="java.io.ByteArrayOutputStream"
  import="java.io.PrintStream"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.fs.Path"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.regionserver.StoreFile"
  %>
<%
Configuration conf = (Configuration)getServletContext().getAttribute("hbase.conf");
long startcode = conf.getLong("startcode", System.currentTimeMillis());
String listenPort = conf.get("hbase.rest.port", "8080");
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>HBase REST Server: <%= listenPort %></title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">

    <link href="/static/css/bootstrap.min.css" rel="stylesheet">
    <link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
    <link href="/static/css/hbase.css" rel="stylesheet">
  </head>

  <body>
  <div class="navbar  navbar-fixed-top navbar-default">
      <div class="container">
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
                  <li><a href="/logLevel">Log Level</a></li>
                  <li><a href="/jmx">Metrics Dump</a></li>
                  <% if (HBaseConfiguration.isShowConfInServlet()) { %>
                  <li><a href="/conf">HBase Configuration</a></li>
                  <% } %>
              </ul>
          </div><!--/.nav-collapse -->
      </div>
  </div>

<div class="container">
    <div class="row inner_header">
        <div class="page-header">
            <h1>RESTServer <small><%= listenPort %></small></h1>
        </div>
    </div>
    <pre>
<%
   try {
     ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
     PrintStream printerOutput = new PrintStream(byteStream);
     HFilePrettyPrinter printer = new HFilePrettyPrinter();
     printer.setPrintStreams(printerOutput, printerOutput);
     printer.setConf(conf);
     String[] options = {"-s"};
     printer.parseOptions(options);
     printer.processFile(new Path(storeFile));
     String text = byteStream.toString();%>
     <%=
       text
     %>
   <%}
   catch (Exception e) {%>
     <%= e %>
   <%}
%>
  </pre>
</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>
<script src="/static/js/tab.js" type="text/javascript"></script>
</body>
</html>

