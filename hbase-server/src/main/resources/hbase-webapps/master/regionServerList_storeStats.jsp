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
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
         import="org.apache.hadoop.hbase.RegionMetrics"
         import="org.apache.hadoop.hbase.Size"
         import="org.apache.hadoop.hbase.util.MasterStatusConstants"
         import="org.apache.hadoop.hbase.util.MasterStatusUtil" %>

<%
  ServerName[] serverNames = (ServerName[]) request.getAttribute(MasterStatusConstants.SERVER_NAMES);
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
%>
<table id="storeStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th>ServerName</th>
    <th class="cls_separator">Num. Stores</th>
    <th class="cls_separator">Num. Storefiles</th>
    <th class="cls_filesize">Storefile Size Uncompressed</th>
    <th class="cls_filesize">Storefile Size</th>
    <th class="cls_filesize">Index Size</th>
    <th class="cls_filesize">Bloom Size</th>
  </tr>
  </thead>
  <tbody>
    <%
final String ZEROKB = "0 KB";
final String ZEROMB = "0 MB";
for (ServerName serverName: serverNames) {

  String storeUncompressedSizeMBStr = ZEROMB;
  String storeFileSizeMBStr = ZEROMB;
  String totalStaticIndexSizeKBStr = ZEROKB;
  String totalStaticBloomSizeKBStr = ZEROKB;
  ServerMetrics sl = master.getServerManager().getLoad(serverName);
  if (sl != null) {
    long storeCount = 0;
    long storeFileCount = 0;
    long storeUncompressedSizeMB = 0;
    long storeFileSizeMB = 0;
    long totalStaticIndexSizeKB = 0;
    long totalStaticBloomSizeKB = 0;
    for (RegionMetrics rl : sl.getRegionMetrics().values()) {
      storeCount += rl.getStoreCount();
      storeFileCount += rl.getStoreFileCount();
      storeUncompressedSizeMB += rl.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE);
      storeFileSizeMB += rl.getStoreFileSize().get(Size.Unit.MEGABYTE);
      totalStaticIndexSizeKB += rl.getStoreFileUncompressedDataIndexSize().get(Size.Unit.KILOBYTE);
      totalStaticBloomSizeKB += rl.getBloomFilterSize().get(Size.Unit.KILOBYTE);
    }
    if (storeUncompressedSizeMB > 0) {
      storeUncompressedSizeMBStr = TraditionalBinaryPrefix.
      long2String(storeUncompressedSizeMB * TraditionalBinaryPrefix.MEGA.value, "B", 1);
    }
    if (storeFileSizeMB > 0) {
      storeFileSizeMBStr = TraditionalBinaryPrefix.
      long2String(storeFileSizeMB * TraditionalBinaryPrefix.MEGA.value, "B", 1);
    }
    if (totalStaticIndexSizeKB > 0) {
      totalStaticIndexSizeKBStr = TraditionalBinaryPrefix.
      long2String(totalStaticIndexSizeKB * TraditionalBinaryPrefix.KILO.value, "B", 1);
    }
    if (totalStaticBloomSizeKB > 0) {
      totalStaticBloomSizeKBStr = TraditionalBinaryPrefix.
      long2String(totalStaticBloomSizeKB * TraditionalBinaryPrefix.KILO.value, "B", 1);
    }
%>
<tr>
  <td><%= MasterStatusUtil.serverNameLink(master, serverName) %></td>
  <td><%= String.format("%,d", storeCount) %></td>
  <td><%= String.format("%,d", storeFileCount) %></td>
  <td><%= storeUncompressedSizeMBStr %></td>
  <td><%= storeFileSizeMBStr %></td>
  <td><%= totalStaticIndexSizeKBStr %></td>
  <td><%= totalStaticBloomSizeKBStr %></td>
  </tr>
<%
} else {
%>
    <% request.setAttribute("serverName", serverName); %>
    <jsp:include page="regionServerList_emptyStat.jsp"/>
<%
  }
}
%>
</tbody>
</table>
