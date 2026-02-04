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
         import="java.util.*"
         import="org.apache.hadoop.hbase.util.*"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import="org.apache.hadoop.hbase.client.RegionInfo"
         import="org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos"
         import="org.apache.hadoop.hbase.client.RegionInfoDisplay" %>
<%@ page import="org.apache.hadoop.util.StringUtils" %>
<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  List<RegionInfo> onlineRegions = (List<RegionInfo>) request.getAttribute("onlineRegions");

%>
<table id="memstoreStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th>Region Name</th>
    <th class="cls_filesize">Memstore Size</th>
  </tr>
  </thead>

  <tbody>
    <% for (RegionInfo r: onlineRegions) { %>
      <tr>

        <%
          final String ZEROMB = "0 MB";
          String memStoreSizeMBStr = ZEROMB;
          ClusterStatusProtos.RegionLoad load = regionServer.createRegionLoad(r.getEncodedName());
          String displayName = RegionInfoDisplay.getRegionNameAsStringForDisplay(r,
            regionServer.getConfiguration());
          if (load != null) {
            long memStoreSizeMB = load.getMemStoreSizeMB();
            if (memStoreSizeMB > 0) {
              memStoreSizeMBStr = StringUtils.TraditionalBinaryPrefix.long2String(
                memStoreSizeMB * StringUtils.TraditionalBinaryPrefix.MEGA.value, "B", 1);
            }
          }
        %>

        <td><a href="region.jsp?name=<%= r.getEncodedName() %>"><%= displayName %></a></td>
        <% if (load != null) { %>
        <td><%= memStoreSizeMBStr %></td>
        <% } %>

      </tr>
    <% } %>
  </tbody>
</table>
