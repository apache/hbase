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
  import="java.util.Collection"
  import="java.util.Date"
  import="java.util.List"
  import="static org.apache.commons.lang.StringEscapeUtils.escapeXml"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.HTableDescriptor"
  import="org.apache.hadoop.hbase.HColumnDescriptor"
  import="org.apache.hadoop.hbase.HBaseConfiguration"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.regionserver.Region"
  import="org.apache.hadoop.hbase.regionserver.Store"
  import="org.apache.hadoop.hbase.regionserver.StoreFile"%>
<%
  String regionName = request.getParameter("name");
  HRegionServer rs = (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  Configuration conf = rs.getConfiguration();

  Region region = rs.getFromOnlineRegions(regionName);
  String displayName = HRegionInfo.getRegionNameAsStringForDisplay(region.getRegionInfo(),
    rs.getConfiguration());
%>
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>HBase RegionServer: <%= rs.getServerName() %></title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta name="description" content="">
    <meta name="author" content="">


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
              <a class="navbar-brand" href="/rs-status"><img src="/static/hbase_logo_small.png" alt="HBase Logo"/></a>
          </div>
          <div class="collapse navbar-collapse">
              <ul class="nav navbar-nav">
                  <li class="active"><a href="/rs-status">Home</a></li>
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

  <div class="container-fluid content">
    <div class="row inner_header">
        <div class="page-header">
            <h1>Region: <%= displayName %></h1>
        </div>
    </div>

<% if(region != null) { //
     List<Store> stores = region.getStores();
     for (Store store : stores) {
       String cf = store.getColumnFamilyName();
       Collection<StoreFile> storeFiles = store.getStorefiles(); %>

       <h3>Column Family: <%= cf %></h2>

       <h4>Memstore size (MB): <%= (int) (store.getMemStoreSize() / 1024 / 1024) %></h3>

       <h4>Store Files</h3>

       <table class="table table-striped">
         <tr>
           <th>Store File</th>
           <th>Size (MB)</th>
           <th>Modification time</th>
         </tr>
       <%   for(StoreFile sf : storeFiles) { %>
         <tr>
           <td><a href="storeFile.jsp?name=<%= sf.getPath() %>"><%= sf.getPath() %></a></td>
           <td><%= (int) (rs.getFileSystem().getLength(sf.getPath()) / 1024 / 1024) %></td>
           <td><%= new Date(sf.getModificationTimeStamp()) %></td>
         </tr>
         <% } %>

         <p> <%= storeFiles.size() %> StoreFile(s) in set.</p>
         </table>
   <%  }
   }%>
</div>
<script src="/static/js/jquery.min.js" type="text/javascript"></script>
<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>

</body>
</html>
