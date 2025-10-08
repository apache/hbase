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
         import="org.apache.hadoop.hbase.io.hfile.AgeSnapshot" %>

<%
  BlockCache bc = (BlockCache) request.getAttribute("bc");

  AgeSnapshot ageAtEvictionSnapshot = bc.getStats().getAgeAtEvictionSnapshot();
  // Only show if non-zero mean and stddev as is the case in combinedblockcache
%>

<tr>
  <td>Evicted</td>
  <td><%= String.format("%,d", bc.getStats().getEvictedCount()) %></td>
  <td>The total number of blocks evicted</td>
</tr>
<tr>
  <td>Evictions</td>
  <td><%= String.format("%,d", bc.getStats().getEvictionCount()) %></td>
  <td>The total number of times an eviction has occurred</td>
</tr>
<tr>
  <td>Mean</td>
  <td><%= String.format("%,d", (long)ageAtEvictionSnapshot.getMean()) %></td>
  <td>Mean age of Blocks at eviction time (seconds)</td>
</tr>
