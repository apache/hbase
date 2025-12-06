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
         import="org.apache.hadoop.hbase.client.RegionInfo" %>
<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  List<RegionInfo> onlineRegions = (List<RegionInfo>) request.getAttribute("onlineRegions");

  if (onlineRegions != null && onlineRegions.size() > 0) {

    Collections.sort(onlineRegions, RegionInfo.COMPARATOR);
%>
<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item"><a class="nav-link active" href="#tab_regionBaseInfo" data-bs-toggle="tab" role="tab">Base Info</a> </li>
    <li class="nav-item"><a class="nav-link" href="#tab_regionRequestStats" data-bs-toggle="tab" role="tab">Request metrics</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_regionStoreStats" data-bs-toggle="tab" role="tab">Storefile Metrics</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_regionMemstoreStats" data-bs-toggle="tab" role="tab">Memstore Metrics</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_regionCompactStats" data-bs-toggle="tab" role="tab">Compaction Metrics</a></li>
  </ul>
  <div class="tab-content">
    <div class="tab-pane active" id="tab_regionBaseInfo" role="tabpanel">
      <jsp:include page="regionListBaseInfo.jsp"/>
    </div>
    <div class="tab-pane" id="tab_regionRequestStats" role="tabpanel">
      <jsp:include page="regionListRequestStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_regionStoreStats" role="tabpanel">
      <jsp:include page="regionListStoreStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_regionMemstoreStats" role="tabpanel">
      <jsp:include page="regionListMemstoreStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_regionCompactStats" role="tabpanel">
      <jsp:include page="regionListCompactStats.jsp"/>
    </div>
  </div>
</div>
<p>Region names are made of the containing table's name, a comma,
  the start key, a comma, and a randomly generated region id.  To illustrate,
  the region named
  <em>domains,apache.org,5464829424211263407</em> is party to the table
  <em>domains</em>, has an id of <em>5464829424211263407</em> and the first key
  in the region is <em>apache.org</em>.  The <em>hbase:meta</em> 'table' is an internal
  system table (or a 'catalog' table in db-speak).
  The hbase:meta table keeps a list of all regions in the system. The empty key is used to denote
  table start and table end.  A region with an empty start key is the first region in a table.
  If a region has both an empty start key and an empty end key, it's the only region in the
  table. See <a href="http://hbase.apache.org">HBase Home</a> for further explication.<p>
<% } else { %>
  <p>Not serving regions</p>
<% } %>

