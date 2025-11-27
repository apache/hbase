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
         import="org.apache.hadoop.hbase.io.hfile.BlockCache"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer" %>
<%@ page import="org.apache.hadoop.hbase.io.hfile.CacheConfig" %>
<%@ page import="org.apache.hadoop.hbase.regionserver.RowCacheService" %>
<%@ page import="org.apache.hadoop.hbase.regionserver.RowCache" %>

<%-- Template for rendering Block Cache tabs in RegionServer Status page. --%>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  CacheConfig cacheConfig = new CacheConfig(regionServer.getConfiguration());

  BlockCache bc = regionServer.getBlockCache().orElse(null);

  BlockCache[] bcs = bc == null ? null : bc.getBlockCaches();
  BlockCache l1 = bcs == null ? bc : bcs[0];
  BlockCache l2 = bcs == null ? null : bcs.length <= 1 ? null : bcs[1];

  RowCacheService rowCacheService = regionServer.getRSRpcServices().getRowCacheService();
  RowCache rowCache = rowCacheService == null ? null : rowCacheService.getRowCache();
%>

<div class="tabbable">
  <ul class="nav nav-pills" role="tablist">
    <li class="nav-item"><a class="nav-link active" href="#tab_bc_baseInfo" data-bs-toggle="tab" role="tab">Base Info</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_bc_config" data-bs-toggle="tab" role="tab">Config</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_bc_stats" data-bs-toggle="tab" role="tab">Block Cache Stats</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_bc_l1" data-bs-toggle="tab" role="tab">Block Cache L1</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_bc_l2" data-bs-toggle="tab" role="tab">Block Cache L2</a></li>
    <li class="nav-item"><a class="nav-link" href="#tab_row_cache" data-bs-toggle="tab" role="tab">Row Cache</a></li>
  </ul>
  <div class="tab-content">
    <div class="tab-pane active" id="tab_bc_baseInfo" role="tabpanel">
      <% request.setAttribute("bc", bc); %>
      <jsp:include page="blockCacheBaseInfo.jsp"/>
    </div>
    <div class="tab-pane" id="tab_bc_config" role="tabpanel">
      <% request.setAttribute("cacheConfig", cacheConfig); %>
      <jsp:include page="blockCacheConfig.jsp"/>
    </div>
    <div class="tab-pane" id="tab_bc_stats" role="tabpanel">
      <% request.setAttribute("bc", bc); %>
      <jsp:include page="blockCacheStats.jsp"/>
    </div>
    <div class="tab-pane" id="tab_bc_l1" role="tabpanel">
      <% request.setAttribute("bc", l1); %>
      <% request.setAttribute("name", "L1"); %>
      <jsp:include page="blockCacheLevel.jsp"/>
    </div>
    <div class="tab-pane" id="tab_bc_l2" role="tabpanel">
      <% request.setAttribute("bc", l2); %>
      <% request.setAttribute("name", "L2"); %>
      <jsp:include page="blockCacheLevel.jsp"/>
    </div>
    <div class="tab-pane" id="tab_row_cache" role="tabpanel">
      <% request.setAttribute("rowCache", rowCache); %>
      <jsp:include page="rowCacheStats.jsp"/>
    </div>
  </div>
</div>
