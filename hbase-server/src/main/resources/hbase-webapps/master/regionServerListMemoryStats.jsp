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
         import="org.apache.hadoop.hbase.master.http.MasterStatusConstants"
         import="org.apache.hadoop.hbase.master.http.MasterStatusUtil" %>

<%
  ServerName[] serverNames = (ServerName[]) request.getAttribute(MasterStatusConstants.SERVER_NAMES);
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
%>

<table id="memoryStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th>ServerName</th>
    <th class="cls_filesize">Used Heap</th>
    <th class="cls_filesize">Max Heap</th>
    <th class="cls_filesize">Memstore Size</th>

  </tr>
  </thead>
  <tbody>
<%
final String ZEROMB = "0 MB";
for (ServerName serverName: serverNames) {
  String usedHeapStr = ZEROMB;
  String maxHeapStr = ZEROMB;
  String memStoreSizeMBStr = ZEROMB;
  ServerMetrics sl = master.getServerManager().getLoad(serverName);
  if (sl != null) {
    long memStoreSizeMB = 0;
    for (RegionMetrics rl : sl.getRegionMetrics().values()) {
      memStoreSizeMB += rl.getMemStoreSize().get(Size.Unit.MEGABYTE);
    }
    if (memStoreSizeMB > 0) {
      memStoreSizeMBStr = TraditionalBinaryPrefix.long2String(memStoreSizeMB
                                * TraditionalBinaryPrefix.MEGA.value, "B", 1);
    }

    double usedHeapSizeMB = sl.getUsedHeapSize().get(Size.Unit.MEGABYTE);
    if (usedHeapSizeMB > 0) {
      usedHeapStr = TraditionalBinaryPrefix.long2String((long) usedHeapSizeMB
                          * TraditionalBinaryPrefix.MEGA.value, "B", 1);
    }
    double maxHeapSizeMB = sl.getMaxHeapSize().get(Size.Unit.MEGABYTE);
    if (maxHeapSizeMB > 0) {
      maxHeapStr = TraditionalBinaryPrefix.long2String((long) maxHeapSizeMB
                         * TraditionalBinaryPrefix.MEGA.value, "B", 1);
    }
%>
<tr>
  <td><%= MasterStatusUtil.serverNameLink(master, serverName) %></td>
  <td><%= usedHeapStr %></td>
  <td><%= maxHeapStr %></td>
  <td><%= memStoreSizeMBStr %></td>
  </tr>
<%
} else {
%>
  <% request.setAttribute("serverName", serverName); %>
  <jsp:include page="regionServerListEmptyStat.jsp"/>
<%
  }
}
%>
</tbody>
</table>
