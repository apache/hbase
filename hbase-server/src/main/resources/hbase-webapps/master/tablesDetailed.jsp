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
  import="static org.apache.commons.lang.StringEscapeUtils.escapeXml"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.client.Admin"
  import="org.apache.hadoop.hbase.client.Connection"
  import="org.apache.hadoop.hbase.client.ConnectionFactory"
  import="org.apache.hadoop.hbase.HTableDescriptor"
  import="org.apache.hadoop.hbase.HBaseConfiguration" %>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>HBase Master: <%= master.getServerName() %></title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">


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
              <a class="navbar-brand" href="/master-status"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
          </div>
          <div class="collapse navbar-collapse">
              <ul class="nav navbar-nav">
                  <li class="active"><a href="/master-status">Home</a></li>
                  <li><a href="/tablesDetailed.jsp">Table Details</a></li>
                  <li><a href="/logs/">Local Logs</a></li>
                  <li><a href="/logLevel">Log Level</a></li>
                  <li><a href="/dump">Debug Dump</a></li>
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
            <h1>User Tables</h1>
        </div>
    </div>

<% HTableDescriptor[] tables;
   Connection connection = master.getConnection();
   Admin admin = connection.getAdmin();
   try {
     tables = admin.listTables();
   } finally {
     admin.close();
   }
   if(tables != null && tables.length > 0) { %>
<table class="table table-striped">
<tr>
    <th>Table</th>
    <th>Description</th>
</tr>
<%   for(HTableDescriptor htDesc : tables ) { %>
<tr>
    <td><a href="/table.jsp?name=<%= escapeXml(htDesc.getTableName().getNameAsString()) %>"><%= escapeXml(htDesc.getTableName().getNameAsString()) %></a></td>
    <td><%= htDesc.toString() %></td>
</tr>
<%   }  %>

<p> <%= tables.length %> table(s) in set.</p>
</table>
<% } %>
</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
