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
         import="org.apache.hadoop.hbase.client.RegionInfo"
         import="org.apache.hadoop.hbase.shaded.protobuf.generated.ClusterStatusProtos"
         import="org.apache.hadoop.hbase.client.RegionInfoDisplay"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
         import="org.apache.hadoop.hbase.regionserver.*"
         import="org.apache.hadoop.util.StringUtils" %>
<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  List<RegionInfo> onlineRegions = (List<RegionInfo>) request.getAttribute("onlineRegions");
%>
<table id="storeStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th>Region Name</th>
    <th class="cls_separator">Num. Stores</th>
    <th class="cls_separator">Num. Storefiles</th>
    <th class="cls_filesize">Storefile Size Uncompressed</th>
    <th class="cls_filesize">Storefile Size</th>
    <th class="cls_filesize">Index Size</th>
    <th class="cls_filesize">Bloom Size</th>
    <th>Data Locality</th>
    <th>Len Of Biggest Cell</th>
    <th>% Cached</th>
  </tr>
  </thead>

  <tbody>

  <% for (RegionInfo r: onlineRegions) { %>
    <tr>
      <%
        final String ZEROMB = "0 MB";
        final String ZEROKB = "0 KB";
        String uncompressedStorefileSizeStr = ZEROMB;
        String storefileSizeStr = ZEROMB;
        String indexSizeStr = ZEROKB;
        String bloomSizeStr = ZEROKB;
        ClusterStatusProtos.RegionLoad load = regionServer.createRegionLoad(r.getEncodedName());
        String displayName = RegionInfoDisplay.getRegionNameAsStringForDisplay(r,
          regionServer.getConfiguration());
        if (load != null) {
          long uncompressedStorefileSize  = load.getStoreUncompressedSizeMB();
          long storefileSize  = load.getStorefileSizeMB();
          long indexSize  = load.getTotalStaticIndexSizeKB();
          long bloomSize  = load.getTotalStaticBloomSizeKB();
          if (uncompressedStorefileSize > 0) {
            uncompressedStorefileSizeStr = TraditionalBinaryPrefix.long2String(
              uncompressedStorefileSize * TraditionalBinaryPrefix.MEGA.value, "B", 1);
          }
          if (storefileSize > 0) {
            storefileSizeStr = TraditionalBinaryPrefix.long2String(storefileSize
              * TraditionalBinaryPrefix.MEGA.value, "B", 1);
          }
          if(indexSize > 0) {
            indexSizeStr = TraditionalBinaryPrefix.long2String(indexSize
              * TraditionalBinaryPrefix.KILO.value, "B", 1);
          }
          if (bloomSize > 0) {
            bloomSizeStr = TraditionalBinaryPrefix.long2String(bloomSize
              * TraditionalBinaryPrefix.KILO.value, "B", 1);
          }
        }
      %>

      <td><a href="region.jsp?name=<%= r.getEncodedName() %>"><%= displayName %></a></td>
      <% if (load != null) { %>
        <td><%= String.format("%,1d", load.getStores()) %></td>
        <td><%= String.format("%,1d", load.getStorefiles()) %></td>
        <td><%= uncompressedStorefileSizeStr %></td>
        <td><%= storefileSizeStr %></td>
        <td><%= indexSizeStr %></td>
        <td><%= bloomSizeStr %></td>
        <td><%= load.getDataLocality() %></td>
      <% } %>
    </tr>
  <% } %>

  </tbody>
</table>
