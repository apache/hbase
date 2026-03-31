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
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix" %>

<%
  BlockCache bc = (BlockCache) request.getAttribute("bc");
if (bc == null) { %>
  <p>BlockCache is null</p>
<% } else { %>
<table class="table table-striped">
  <tr>
    <th>Attribute</th>
    <th>Value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Size</td>
    <td><%= TraditionalBinaryPrefix.long2String(bc.getCurrentSize(),
      "B", 1) %></td>
    <td>Current size of block cache in use</td>
  </tr>
  <tr>
    <td>Free</td>
    <td><%= TraditionalBinaryPrefix.long2String(bc.getFreeSize(),
      "B", 1) %></td>
    <td>The total free memory currently available to store more cache entries</td>
  </tr>
  <tr>
    <td>Count</td>
    <td><%= String.format("%,d", bc.getBlockCount()) %></td>
    <td>Number of blocks in block cache</td>
  </tr>
  <% request.setAttribute("bc", bc); %>
  <jsp:include page="blockCacheEvictions.jsp"/>
  <jsp:include page="blockCacheHits.jsp"/>
</table>
<p>If block cache is made up of more than one cache -- i.e. a L1 and a L2 -- then the above
  are combined counts. Request count is sum of hits and misses.</p>
<% } %>
