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
  import="java.util.Map"
  import="org.apache.hadoop.fs.FileSystem"
  import="org.apache.hadoop.fs.FileStatus"
  import="org.apache.hadoop.fs.Path"
  import="org.apache.hadoop.hbase.TableName"
  import="org.apache.hadoop.hbase.client.RegionInfo"
  import="org.apache.hadoop.hbase.client.RegionInfoDisplay"
  import="org.apache.hadoop.hbase.mob.MobUtils"
  import="org.apache.hadoop.hbase.regionserver.HMobStore"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.regionserver.HRegion"
  import="org.apache.hadoop.hbase.regionserver.HStore"
  import="org.apache.hadoop.hbase.regionserver.HStoreFile"
  import="org.apache.hadoop.hbase.util.CommonFSUtils"
%>
<%@ page import="java.nio.charset.StandardCharsets" %>
<%
  String regionName = request.getParameter("name");
  HRegionServer rs = (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  FileSystem fs = rs.getFileSystem();
  HRegion region = null;
  if (regionName != null) {
    region = rs.getRegion(regionName);
  }
  String displayName;
  boolean isReplicaRegion = false;
  if (region != null) {
    displayName = RegionInfoDisplay.getRegionNameAsStringForDisplay(region.getRegionInfo(),
            rs.getConfiguration());
    isReplicaRegion = region.getRegionInfo().getReplicaId() > RegionInfo.DEFAULT_REPLICA_ID;
  } else {
    if (regionName != null) {
      displayName = "region {" + regionName + "} is not currently online on this region server";
    } else {
      displayName = "you must specify a region name when accessing this page";
    }
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
     List<HStore> stores = region.getStores();
     for (HStore store : stores) {
       String cf = store.getColumnFamilyName();
       Collection<HStoreFile> storeFiles = store.getStorefiles(); %>

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
       <% int count = 0;
          for(HStoreFile sf : storeFiles) {
            if (isReplicaRegion && !fs.exists(sf.getPath())) continue;
            count++; %>
         <tr>
           <td><a href="storeFile.jsp?name=<%= sf.getEncodedPath() %>"><%= sf.getPath() %></a></td>
           <td><%= (int) (fs.getFileStatus(sf.getPath()).getLen() / 1024 / 1024) %></td>
           <td><%= new Date(sf.getModificationTimestamp()) %></td>
           <td><%= String.format("%,1d", sf.getFileInfo().getHFileInfo().getLenOfBiggestCell()) %></td>
           <td><%= sf.getFileInfo().getHFileInfo().getKeyOfBiggestCell() %></td>
         </tr>
         <% } %>

         <p> <%= count %> StoreFile(s) in set. <%= isReplicaRegion ? "The information about storefile(s) may not up-to-date because it's not the primary region." : "" %></p>
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
         HMobStore mobStore = (HMobStore) store;
         for (HStoreFile sf : storeFiles) {
           try {
             byte[] value = sf.getMetadataValue(HStoreFile.MOB_FILE_REFS);
             if (value == null) {
               continue;
             }

             for (Map.Entry<TableName, String> mobRef :
               MobUtils.deserializeMobFileRefs(value).build().entries()) {
               TableName mobTable = mobRef.getKey();
               String fileName = mobRef.getValue();
               Path mobPath = null;
               FileStatus status = null;
               for (Path candidate : mobStore.getLocations(mobTable)) {
                 Path p = new Path(candidate, fileName);
                 FileStatus[] fileStatuses = CommonFSUtils.listStatus(fs, p);
                 if (fileStatuses == null) {
                   continue;
                 }
                 mobPath = p;
                 status = fileStatuses[0];
                 break;
               }

               if (mobPath == null) {
                 // File not found in any known location (working dir or archive dir) for the referenced table.
                 // Nothing to show on the UI, so skip it.
                 continue;
               }

               mobCnt ++;
               String mobPathStr = mobPath.toString();
               String encodedStr = URLEncoder.encode(mobPathStr, StandardCharsets.UTF_8.toString()); %>

               <tr>
                 <td><a href="storeFile.jsp?name=<%= encodedStr %>"><%= mobPathStr %></a></td>
                 <td><%= status.getLen() / 1024 / 1024 %></td>
                 <td><%= new Date(status.getModificationTime()) %></td>
               </tr>

             <% }
           } catch (Exception e) { %>
             <tr>
               <td colspan="3"><%= e %></td>
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
