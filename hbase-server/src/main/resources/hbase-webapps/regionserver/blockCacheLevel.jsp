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
         import="org.apache.hadoop.conf.Configuration"
         import="org.apache.hadoop.hbase.io.hfile.BlockCache"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import="org.apache.hadoop.hbase.io.hfile.BlockCacheUtil"
         import="org.apache.hadoop.hbase.io.hfile.LruBlockCache"
         import="org.apache.hadoop.hbase.io.hfile.bucket.BucketCacheStats"
         import="org.apache.hadoop.hbase.io.hfile.bucket.BucketCache"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix" %>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  Configuration configuration = regionServer.getConfiguration();

  BlockCache bc = (BlockCache) request.getAttribute("bc");

  String name = (String) request.getAttribute("name");
%>

<% if (bc == null) { %>
  <p>No <%= name %> deployed</p>
<% } else { %>

<%
  String bcUrl = "http://hbase.apache.org/devapidocs/" + bc.getClass().getName().replaceAll("\\.", "/") + ".html";
  String bcName = bc.getClass().getSimpleName();
  int maxCachedBlocksByFile = BlockCacheUtil.getMaxCachedBlocksByFile(configuration);

  boolean isLru = bc instanceof LruBlockCache;

  boolean isBucketCache = bc.getClass().getSimpleName().equals("BucketCache");
  BucketCacheStats bucketCacheStats = null;
  BucketCache bucketCache = null;
  if (bc instanceof BucketCache) {
    bucketCache = (BucketCache) bc;
    bucketCacheStats = (BucketCacheStats) bc.getStats();
  }
%>

<table id="blocks_summary" class="table table-striped">
  <tr>
    <th>Attribute</th>
    <th>Value</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Implementation</td>
    <td><a href="<%= bcUrl %>"><%= bcName %></a></td>
    <td>Class implementing this block cache Level</td>
  </tr>
<% if (isBucketCache) { %>
  <tr>
    <td>IOEngine</td>
    <td><%= bucketCache.getIoEngine() %></td>
    <td>Supported IOEngine types: offheap, file, files, mmap or pmem. See <a href="https://hbase.apache.org/book.html#hbase.bucketcache.ioengine">hbase.bucketcache.ioengine</a>.</td>
  </tr>
<% } %>
  <tr>
    <td>Cache Size Limit</td>
    <td><%= TraditionalBinaryPrefix.long2String(bc.getMaxSize(), "B", 1) %></td>
    <td>Max size of cache</td>
  </tr>
  <tr>
    <td>Block Count</td>
    <td><%= String.format("%,d", bc.getBlockCount()) %></td>
    <td>Count of Blocks</td>
  </tr>
<% if (!isBucketCache) { %>
  <tr>
    <td>Data Block Count</td>
    <td><%= String.format("%,d", bc.getDataBlockCount()) %></td>
    <td>Count of DATA Blocks</td>
  </tr>
<% } %>
<% if (isLru) { %>
  <tr>
    <td>Index Block Count</td>
    <td><%= String.format("%,d", ((LruBlockCache)bc).getIndexBlockCount()) %></td>
    <td>Count of INDEX Blocks</td>
  </tr>
  <tr>
    <td>Bloom Block Count</td>
    <td><%= String.format("%,d", ((LruBlockCache)bc).getBloomBlockCount()) %></td>
    <td>Count of BLOOM Blocks</td>
  </tr>
<% } %>
  <tr>
    <td>Size of Blocks</td>
    <td><%= TraditionalBinaryPrefix.long2String(bc.getCurrentSize(), "B", 1) %></td>
    <td>Size of Blocks</td>
  </tr>
<% if (!isBucketCache) { %>
  <tr>
    <td>Size of Data Blocks</td>
    <td><%= TraditionalBinaryPrefix.long2String(bc.getCurrentDataSize(), "B", 1) %></td>
    <td>Size of DATA Blocks</td>
  </tr>
<% } %>
<% if (isLru) { %>
  <tr>
    <td>Size of Index Blocks</td>
    <td><%= TraditionalBinaryPrefix.long2String(((LruBlockCache)bc).getCurrentIndexSize(), "B", 1) %></td>
    <td>Size of INDEX Blocks</td>
  </tr>
  <tr>
    <td>Size of Bloom Blocks</td>
    <td><%= TraditionalBinaryPrefix.long2String(((LruBlockCache)bc).getCurrentBloomSize(), "B", 1) %></td>
    <td>Size of BLOOM Blocks</td>
  </tr>
<% } %>
  <% request.setAttribute("bc", bc); %>
  <jsp:include page="blockCacheEvictions.jsp"/>
  <jsp:include page="blockCacheHits.jsp"/>

<% if (isBucketCache) { %>
  <tr>
    <td>Hits per Second</td>
    <td><%= bucketCacheStats.getIOHitsPerSecond() %></td>
    <td>Block gets against this cache per second</td>
  </tr>
  <tr>
    <td>Time per Hit</td>
    <td><%= bucketCacheStats.getIOTimePerHit() %></td>
    <td>Time per cache hit</td>
  </tr>
<% } %>
</table>

<%-- Call through to block cache Detail rendering template --%>
<p>
  View block cache <a href="?format=json&bcn=<%= name %>">as JSON</a> | Block cache <a href="?format=json&bcn=<%= name %>&bcv=file">as JSON by file</a>
  <% if (bc.getBlockCount() > maxCachedBlocksByFile) { %>
  <br>
  <b>Note</b>: JSON view of block cache will be incomplete, because block count <%= bc.getBlockCount() %> is greater than <i>hbase.ui.blockcache.by.file.max</i> value of <%= maxCachedBlocksByFile %>.
  Increase that value to get a complete picture.
  <% } %>
</p>
<% } %>
