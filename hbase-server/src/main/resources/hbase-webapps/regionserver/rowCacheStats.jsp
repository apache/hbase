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
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix" %>
<%@ page import="org.apache.hadoop.hbase.regionserver.RowCache" %>

<%
  RowCache rowCache = (RowCache) request.getAttribute("rowCache");
if (rowCache == null) { %>
<p>RowCache is disabled</p>
<% } else { %>
<table class="table table-striped">
  <tr>
    <th>Attribute</th>
    <th>Value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Size</td>
    <td><%= TraditionalBinaryPrefix.long2String(rowCache.getSize(), "B", 1) %></td>
    <td>Current size of row cache in use</td>
  </tr>
  <tr>
    <td>Free</td>
    <td><%= TraditionalBinaryPrefix.long2String(rowCache.getMaxSize() - rowCache.getSize(), "B", 1) %></td>
    <td>The total free memory currently available to store more cache entries</td>
  </tr>
  <tr>
    <td>Count</td>
    <td><%= String.format("%,d", rowCache.getCount()) %></td>
    <td>The number of rows in row cache</td>
  </tr>
  <tr>
    <td>Evicted Rows</td>
    <td><%= String.format("%,d", rowCache.getEvictedRowCount()) %></td>
    <td>The total number of rows evicted</td>
  </tr>
  <tr>
    <td>Hits</td>
    <td><%= String.format("%,d", rowCache.getHitCount()) %></td>
    <td>The number requests that were cache hits</td>
  </tr>
  <tr>
    <td>Misses</td>
    <td><%= String.format("%,d", rowCache.getMissCount()) %></td>
    <td>The number requests that were cache misses</td>
  </tr>
  <tr>
    <td>All Time Hit Ratio</td>
    <td><%= String.format("%,.2f", rowCache.getHitCount() * 100.0 / (rowCache.getMissCount() + rowCache.getHitCount())) %><%= "%" %></td>
    <td>Hit Count divided by total requests count</td>
  </tr>
</table>
<p>RowCache is a separate cache distinct from BlockCache.</p>
<% } %>
