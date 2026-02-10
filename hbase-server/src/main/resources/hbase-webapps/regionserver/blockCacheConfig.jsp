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
         import="org.apache.hadoop.hbase.io.hfile.CacheConfig" %>

<%
  CacheConfig cacheConfig = (CacheConfig) request.getAttribute("cacheConfig");
if (cacheConfig == null) { %>
  <p>CacheConfig is null</p>
<% } else { %>
<table class="table table-striped">
  <tr>
    <th>Attribute</th>
    <th>Value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Cache DATA on Read</td>
    <td><%= cacheConfig.shouldCacheDataOnRead() %></td>
    <td>True if DATA blocks are cached on read
      (INDEX & BLOOM blocks are always cached)</td>
  </tr>
  <tr>
    <td>Cache DATA on Write</td>
    <td><%= cacheConfig.shouldCacheDataOnWrite() %></td>
    <td>True if DATA blocks are cached on write.</td>
  </tr>
  <tr>
    <td>Cache INDEX on Write</td>
    <td><%= cacheConfig.shouldCacheIndexesOnWrite() %></td>
    <td>True if INDEX blocks are cached on write</td>
  </tr>
  <tr>
    <td>Cache BLOOM on Write</td>
    <td><%= cacheConfig.shouldCacheBloomsOnWrite() %></td>
    <td>True if BLOOM blocks are cached on write</td>
  </tr>
  <tr>
    <td>Evict blocks on Close</td>
    <td><%= cacheConfig.shouldEvictOnClose() %></td>
    <td>True if blocks are evicted from cache when an HFile
      reader is closed</td>
  </tr>
  <tr>
    <td>Cache DATA in compressed format</td>
    <td><%= cacheConfig.shouldCacheDataCompressed() %></td>
    <td>True if DATA blocks are cached in their compressed form</td>
  </tr>
  <tr>
    <td>Prefetch on Open</td>
    <td><%= cacheConfig.shouldPrefetchOnOpen() %></td>
    <td>True if blocks are prefetched into cache on open</td>
  </tr>
</table>
<% } %>
