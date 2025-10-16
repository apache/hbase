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
         import="org.apache.hadoop.hbase.io.hfile.BlockCache" %>

<%
  BlockCache bc = (BlockCache) request.getAttribute("bc");
%>
<tr>
  <td>Hits</td>
  <td><%= String.format("%,d", bc.getStats().getHitCount()) %></td>
  <td>Number requests that were cache hits</td>
</tr>
<tr>
  <td>Hits Caching</td>
  <td><%= String.format("%,d", bc.getStats().getHitCachingCount()) %></td>
  <td>Cache hit block requests but only requests set to cache block if a miss</td>
</tr>
<tr>
  <td>Misses</td>
  <td><%= String.format("%,d", bc.getStats().getMissCount()) %></td>
  <td>Block requests that were cache misses but set to cache missed blocks</td>
</tr>
<tr>
  <td>Misses Caching</td>
  <td><%= String.format("%,d", bc.getStats().getMissCachingCount()) %></td>
  <td>Block requests that were cache misses but only requests set to use block cache</td>
</tr>
<tr>
  <td>Hit Ratio</td>
  <td><%= String.format("%,.2f", bc.getStats().getHitRatio() * 100) %><%= "%" %></td>
  <td>Hit Count divided by total requests count</td>
</tr>
