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
         import="org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapper" %>


<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  MetricsRegionServerWrapper mWrap = regionServer.getMetrics().getRegionServerWrapper();
%>

<table class="table table-striped">
  <tr>
    <th>Requests Per Second</th>
    <th>Num. Regions</th>
    <th>Block locality</th>
    <th>Block locality (Secondary replicas)</th>
    <th>Slow WAL Append Count</th>
  </tr>
  <tr>
    <td><%= String.format("%.0f", mWrap.getRequestsPerSecond()) %></td>
    <td><%= mWrap.getNumOnlineRegions() %></td>
    <td><%= String.format("%.3f",mWrap.getPercentFileLocal()) %><%= "%" %></td>
    <td><%= String.format("%.3f",mWrap.getPercentFileLocalSecondaryRegions()) %><%= "%" %></td>
    <td><%= mWrap.getNumWALSlowAppend() %></td>
  </tr>
</table>
