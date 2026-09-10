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

  int hitPeriods = 0;
  for (int i = 0; i < bc.getStats().getNumPeriodsInWindow(); i++) {
    if (bc.getStats().getWindowPeriods()[i] != null) {
      hitPeriods++;
    }
  }
%>

<% if (hitPeriods > 0) { %>
<script src="/static/js/blockCacheInit.js" type="text/javascript"></script>
<% } %>
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
  <td>All Time Hit Ratio</td>
  <td><%= String.format("%,.2f", bc.getStats().getHitRatio() * 100) %><%= "%" %></td>
  <td>Hit Count divided by total requests count</td>
</tr>
<% for (int i = 0; i < hitPeriods; i++) { %>
<%-- These rows are hidden on page load, blockCacheInit.js will display these as paginated. --%>
<tr id="row-<%= i %>" class="item-row" style="display: none;">
  <td>Hit Ratio for period starting at <%= bc.getStats().getWindowPeriods()[i] %></td>
  <% if (bc.getStats().getRequestCounts()[i] > 0) { %>
    <td><%= String.format("%,.2f", ((double)bc.getStats().getHitCounts()[i] / (double)bc.getStats().getRequestCounts()[i]) * 100.0) %><%= "%" %></td>
  <% } else { %>
    <td>No requests</td>
  <% } %>
    <td>Hit Count divided by total requests count over the <%= i %>th period of <%= bc.getStats().getPeriodTimeInMinutes() %> minutes</td>
</tr>
<% } %>
<% if (hitPeriods > 0) { %>
  <tr class="pagination-row">
    <td colspan="3">
      <div class="pagination-container">
        <button id="prev-page" onclick="prevPage()">Previous</button>
        <span id="page-buttons" class="page-numbers"></span>
        <button id="next-page" onclick="nextPage()">Next</button>
        <span id="page-info" class="page-info"></span>
      </div>
    </td>
  </tr>
<% } %>
<% if (bc.getStats().getPeriodTimeInMinutes() > 0) { %>
  <tr>
    <td>Last <%= bc.getStats().getNumPeriodsInWindow()*bc.getStats().getPeriodTimeInMinutes() %> minutes Hit Ratio</td>
    <td><%= String.format("%,.2f", bc.getStats().getHitRatioPastNPeriods() * 100.0) %><%= "%" %></td>
    <td>Hit Count divided by total requests count for the last <%= bc.getStats().getNumPeriodsInWindow()*bc.getStats().getPeriodTimeInMinutes() %> minutes</td>
  </tr>
<% } %>
