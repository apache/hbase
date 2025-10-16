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
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix" %>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  MetricsRegionServerWrapper mWrap = regionServer.getMetrics().getRegionServerWrapper();
%>

<table class="table table-striped">
  <tr>
    <th>Num. Stores</th>
    <th>Num. Storefiles</th>
    <th>Root Index Size</th>
    <th>Index Size</th>
    <th>Bloom Size</th>
  </tr>
  <tr>
    <td><%= mWrap.getNumStores() %></td>
    <td><%= mWrap.getNumStoreFiles() %></td>
    <td><%= TraditionalBinaryPrefix.long2String(mWrap.getStoreFileIndexSize(), "B", 1) %></td>
    <td><%= TraditionalBinaryPrefix.long2String(mWrap.getTotalStaticIndexSize(), "B", 1) %></td>
    <td><%= TraditionalBinaryPrefix.long2String(mWrap.getTotalStaticBloomSize(), "B", 1) %></td>
  </tr>
</table>
