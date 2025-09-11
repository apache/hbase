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
         import="org.apache.hadoop.hbase.io.hfile.BlockCache"
         import="org.apache.hadoop.conf.Configuration"
         import="org.apache.hadoop.hbase.io.hfile.BlockCacheUtil"
         import="org.apache.hadoop.hbase.io.hfile.CachedBlock" %><%
  // This template is used to give views on an individual block cache as JSON.

  Configuration conf = (Configuration) request.getAttribute("conf");
  String bcn = (String) request.getAttribute("bcn");
  String bcv = (String) request.getAttribute("bcv");
  BlockCache bc = (BlockCache) request.getAttribute("blockCache");

  BlockCache [] bcs = bc == null ? null : bc.getBlockCaches();
  if (bcn.equals("L1")) {
    bc = bcs == null || bcs.length == 0? bc: bcs[0];
  } else {
    if (bcs == null || bcs.length < 2) {
      System.out.println("There is no L2 block cache");
      return;
    }
    bc = bcs[1];
  }
  if (bc == null) {
    System.out.println("There is no block cache");
    return;
  }
  BlockCacheUtil.CachedBlocksByFile cbsbf = BlockCacheUtil.getLoadedCachedBlocksByFile(conf, bc);
  if (bcv.equals("file")) {
    boolean firstEntry = true; %>
    [<% for (Map.Entry<String, NavigableSet<CachedBlock>> e: cbsbf.getCachedBlockStatsByFile().entrySet()) { %>
    <% if (!firstEntry) { %>,<% } %><%= BlockCacheUtil.toJSON(e.getKey(), e.getValue()) %>
    <%
      if (firstEntry) {
        firstEntry = false;
      }
    %>
    <% } %>]

<% } else { %>
[ <%= BlockCacheUtil.toJSON(bc) %>, <%= BlockCacheUtil.toJSON(cbsbf) %> ]
<% } %>
