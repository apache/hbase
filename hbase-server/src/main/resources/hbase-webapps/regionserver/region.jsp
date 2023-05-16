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
  import="java.net.URLEncoder"
  import="java.util.Collection"
  import="java.util.Date"
  import="java.util.List"
  import="org.apache.hadoop.fs.FileStatus"
  import="org.apache.hadoop.fs.Path"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.client.RegionInfoDisplay"
  import="org.apache.hadoop.hbase.mob.MobUtils"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.regionserver.HMobStore"
  import="org.apache.hadoop.hbase.regionserver.HStoreFile"
  import="org.apache.hadoop.hbase.regionserver.Region"
  import="org.apache.hadoop.hbase.regionserver.Store"
  import="org.apache.hadoop.hbase.regionserver.StoreFile"
%>
<%
  String regionName = request.getParameter("name");
  HRegionServer rs = (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  Region region = rs.getRegion(regionName);
  String displayName;
  if (region != null) {
    displayName = RegionInfoDisplay.getRegionNameAsStringForDisplay(region.getRegionInfo(),
            rs.getConfiguration());
  } else {
    displayName = "region {" + regionName + "} is not currently online on this region server";
  }
  pageContext.setAttribute("pageTitle", "HBase RegionServer: " + rs.getServerName());
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

  <div class="container-fluid content">
    <div class="row inner_header">
        <div class="page-header">
            <h1>Region: <%= displayName %></h1>
        </div>
    </div>

<% if(region != null) { //
     List<? extends Store> stores = region.getStores();
     for (Store store : stores) {
       String cf = store.getColumnFamilyName();
       Collection<? extends StoreFile> storeFiles = store.getStorefiles(); %>

       <h3>Column Family: <%= cf %></h3>

       <h4>Memstore size (MB): <%= (int) (store.getMemStoreSize().getHeapSize() / 1024 / 1024) %></h4>

       <h4>Store Files</h4>

       <table class="table table-striped">
         <tr>
           <th>Store File</th>
           <th>Size (MB)</th>
           <th>Modification time</th>
           <th>Len Of Biggest Cell</th>
           <th>Key Of Biggest Cell</th>
         </tr>
       <%   for(StoreFile sf : storeFiles) { %>
         <tr>
           <td><a href="storeFile.jsp?name=<%= sf.getEncodedPath() %>"><%= sf.getPath() %></a></td>
           <td><%= (int) (rs.getFileSystem().getLength(sf.getPath()) / 1024 / 1024) %></td>
           <td><%= new Date(sf.getModificationTimestamp()) %></td>
           <td><%= String.format("%,1d", ((HStoreFile)sf).getFileInfo().getHFileInfo().getLenOfBiggestCell()) %></td>
           <td><%= ((HStoreFile)sf).getFileInfo().getHFileInfo().getKeyOfBiggestCell() %></td>
         </tr>
         <% } %>

         <p> <%= storeFiles.size() %> StoreFile(s) in set.</p>
         </table>

       <% if (store instanceof HMobStore) { %>
       <h4>MOB Files</h4>
       <table class="table table-striped">
         <tr>
           <th>MOB File</th>
           <th>Size (MB)</th>
           <th>Modification time</th>
         </tr>

         <%
         int mobCnt = 0;
         for (StoreFile sf : storeFiles) {
           try {
             byte[] value = ((HStoreFile)sf).getMetadataValue(HStoreFile.MOB_FILE_REFS);
             if (value == null) {
               continue;
             }

             Collection<String> fileNames = MobUtils.deserializeMobFileRefs(value).build().values();
             mobCnt += fileNames.size();
             for (String fileName : fileNames) {
               Path mobPath = new Path(((HMobStore) store).getPath(), fileName);
               FileStatus status = rs.getFileSystem().getFileStatus(mobPath);
               String mobPathStr = mobPath.toString();
               String encodedStr = URLEncoder.encode(mobPathStr, HConstants.UTF8_ENCODING); %>

               <tr>
                 <td><a href="storeFile.jsp?name=<%= encodedStr%>"><%= mobPathStr%></a></td>
                 <td><%= status.getLen() / 1024 / 1024 %></td>
                 <td><%= new Date(status.getModificationTime()) %></td>
               </tr>

             <% }
           } catch (Exception e) { %>
             <tr>
              <td><%= e %></td>
             </tr>
           <% }
         } %>

         <p> <%= mobCnt %> MobFile(s) in set.</p>
       </table>
       <% }
     }
   }%>
</div>

<jsp:include page="footer.jsp" />
