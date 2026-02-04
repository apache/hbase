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
         import="org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapper"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
         import="org.apache.hadoop.hbase.ipc.MetricsHBaseServerWrapper" %>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  MetricsRegionServerWrapper mWrap = regionServer.getMetrics().getRegionServerWrapper();
  MetricsHBaseServerWrapper mServerWrap = regionServer.getRpcServer().getMetrics().getHBaseServerWrapper();
%>

<table class="table table-striped">
  <tr>
    <th>Compaction Queue Length</th>
    <th>Flush Queue Length</th>
    <th>Priority Call Queue Length</th>
    <th>General Call Queue Length</th>
    <th>Replication Call Queue Length</th>
    <th>Total Call Queue Size</th>
  </tr>
  <tr>
    <td><%= mWrap.getCompactionQueueSize() %></td>
    <td><%= mWrap.getFlushQueueSize() %></td>
    <td><%= mServerWrap.getPriorityQueueLength() %></td>
    <td><%= mServerWrap.getGeneralQueueLength() %></td>
    <td><%= mServerWrap.getReplicationQueueLength() %></td>
    <td><%= TraditionalBinaryPrefix.long2String(mServerWrap.getTotalQueueSize(), "B", 1) %></td>
  </tr>
</table>
