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
         import="org.apache.hadoop.conf.Configuration"
         import="org.apache.hadoop.hbase.io.hfile.BlockCacheUtil"
         import="org.apache.hadoop.hbase.io.hfile.CachedBlock"
         import="org.apache.hadoop.hbase.io.hfile.cache.CacheAccessService"
         import="org.apache.hadoop.hbase.io.hfile.cache.CacheEngine"
         import="org.apache.hadoop.hbase.io.hfile.cache.CacheTier"
         import="org.apache.hadoop.hbase.io.hfile.cache.CacheTopology"
         import="org.apache.hadoop.hbase.io.hfile.cache.CacheTopologyType"
         import="org.apache.hadoop.hbase.io.hfile.cache.TopologyBackedCacheAccessService"
         import="org.apache.hadoop.hbase.regionserver.http.RSStatusConstants" %><%
  // This template is used to give views on an individual cache tier as JSON.

  Configuration conf = (Configuration) request.getAttribute("conf");
  String bcn = (String) request.getAttribute(RSStatusConstants.BLOCK_CACHE_NAME);
  String bcv = (String) request.getAttribute(RSStatusConstants.BLOCK_CACHE_V);
  CacheAccessService cacheAccessService =
    (CacheAccessService) request.getAttribute("blockCache");

  if (!(cacheAccessService instanceof TopologyBackedCacheAccessService)) {
    System.out.println("There is no topology-backed block cache");
    return;
  }

  CacheTopology topology =
    ((TopologyBackedCacheAccessService) cacheAccessService).getTopology();

  CacheTier tier;
  if (bcn.equals(RSStatusConstants.BLOCK_CACHE_NAME_L1)) {
    tier = topology.getType() == CacheTopologyType.SINGLE_TIER
      ? CacheTier.SINGLE
      : CacheTier.L1;
  } else {
    tier = CacheTier.L2;
  }

  Optional<CacheEngine> engineOptional = topology.getEngine(tier);
  if (engineOptional.isEmpty()) {
    System.out.println("There is no " + bcn + " block cache");
    return;
  }

  CacheEngine cacheEngine = engineOptional.get();

  BlockCacheUtil.CachedBlocksByFile cbsbf =
    BlockCacheUtil.getLoadedCachedBlocksByFile(conf, cacheEngine);

  if (bcv.equals(RSStatusConstants.BLOCK_CACHE_V_FILE)) {
    boolean firstEntry = true;
%>
[
<%
  for (Map.Entry<String, NavigableSet<CachedBlock>> e
      : cbsbf.getCachedBlockStatsByFile().entrySet()) {
%>
  <% if (!firstEntry) { %>,<% } %><%= BlockCacheUtil.toJSON(e.getKey(), e.getValue()) %>
<%
    if (firstEntry) {
      firstEntry = false;
    }
  }
%>
]
<% } else { %>
[ <%= BlockCacheUtil.toJSON(cacheEngine) %>, <%= BlockCacheUtil.toJSON(cbsbf) %> ]
<% } %>