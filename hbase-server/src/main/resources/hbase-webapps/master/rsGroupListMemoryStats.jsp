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
         import="java.util.Map"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.Size"
         import="org.apache.hadoop.hbase.net.Address"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
         import="org.apache.hadoop.hbase.master.http.MasterStatusConstants" %>

<%
  RSGroupInfo [] rsGroupInfos = (RSGroupInfo[]) request.getAttribute(MasterStatusConstants.RS_GROUP_INFOS);
  Map<Address, ServerMetrics> collectServers = (Map<Address, ServerMetrics>) request.getAttribute(MasterStatusConstants.COLLECT_SERVERS);
%>

<table class="table table-striped">
  <tr>
    <th>RSGroup Name</th>
    <th>Used Heap</th>
    <th>Max Heap</th>
    <th>Memstore Size</th>

  </tr>
    <%
    final String ZEROMB = "0 MB";
    for (RSGroupInfo rsGroupInfo: rsGroupInfos) {
      String usedHeapStr = ZEROMB;
      String maxHeapStr = ZEROMB;
      String memstoreSizeStr = ZEROMB;
      String rsGroupName = rsGroupInfo.getName();
      long usedHeap = 0;
      long maxHeap = 0;
      long memstoreSize = 0;
      for (Address server : rsGroupInfo.getServers()) {
        ServerMetrics sl = collectServers.get(server);
        if (sl != null) {
          usedHeap += (long) sl.getUsedHeapSize().get(Size.Unit.MEGABYTE);
          maxHeap += (long) sl.getMaxHeapSize().get(Size.Unit.MEGABYTE);
          memstoreSize += (long) sl.getRegionMetrics().values().stream().mapToDouble(
            rm -> rm.getMemStoreSize().get(Size.Unit.MEGABYTE)).sum();
        }
      }

      if (usedHeap > 0) {
        usedHeapStr = TraditionalBinaryPrefix.long2String(usedHeap
                                      * TraditionalBinaryPrefix.MEGA.value, "B", 1);
      }
      if (maxHeap > 0) {
        maxHeapStr = TraditionalBinaryPrefix.long2String(maxHeap
                                      * TraditionalBinaryPrefix.MEGA.value, "B", 1);
      }
      if (memstoreSize > 0) {
        memstoreSizeStr = TraditionalBinaryPrefix.long2String(memstoreSize
                                      * TraditionalBinaryPrefix.MEGA.value, "B", 1);
      }
%>
<tr>
  <td><a href="rsgroup.jsp?name=<%= rsGroupName %>"><%= rsGroupName %></a></td>
  <td><%= usedHeapStr %></td>
  <td><%= maxHeapStr %></td>
  <td><%= memstoreSizeStr %></td>

  </tr>
<%
}
%>
</table>
