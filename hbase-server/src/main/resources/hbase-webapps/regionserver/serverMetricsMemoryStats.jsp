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
         import="org.apache.hadoop.hbase.util.*"
         import="org.apache.hadoop.hbase.regionserver.HRegionServer"
         import="org.apache.hadoop.hbase.regionserver.MetricsRegionServerWrapper"
         import="java.lang.management.MemoryUsage"
         import="org.apache.hadoop.hbase.io.util.MemorySizeUtil"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix" %>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  MetricsRegionServerWrapper mWrap = regionServer.getMetrics().getRegionServerWrapper();

  long usedHeap = -1L;
  long maxHeap = -1L;
  final MemoryUsage usage = MemorySizeUtil.safeGetHeapMemoryUsage();
  if (usage != null) {
    maxHeap = usage.getMax();
    usedHeap = usage.getUsed();
  }
%>
<table class="table table-striped">
  <tr>
    <th>Used Heap</th>
    <th>Max Heap</th>
    <th>Direct Memory Used</th>
    <th>Direct Memory Configured</th>
    <th>Memstore On-Heap Size / Limit</th>
    <th>Memstore Off-Heap Size / Limit</th>
    <th>Memstore Data Size (On&&Off Heap)</th>
  </tr>
  <tr>
    <td>
      <%= TraditionalBinaryPrefix.long2String(usedHeap, "B", 1) %>
    </td>
    <td>
      <%= TraditionalBinaryPrefix.long2String(maxHeap, "B", 1) %>
    </td>
    <td>
      <%= TraditionalBinaryPrefix.long2String(DirectMemoryUtils.getDirectMemoryUsage(), "B", 1) %>
    </td>
    <td>
      <%= TraditionalBinaryPrefix.long2String(DirectMemoryUtils.getDirectMemorySize(), "B", 1) %>
    </td>
    <td>
      <%= TraditionalBinaryPrefix.long2String(mWrap.getOnHeapMemStoreSize(), "B", 1) + " / "
        + TraditionalBinaryPrefix.long2String(mWrap.getOnHeapMemStoreLimit(), "B", 1) %>
    </td>
    <td>
      <%= TraditionalBinaryPrefix.long2String(mWrap.getOffHeapMemStoreSize(), "B", 1) + " / "
        + TraditionalBinaryPrefix.long2String(mWrap.getOffHeapMemStoreLimit(), "B", 1) %>
    </td>
    <td>
      <%= TraditionalBinaryPrefix.long2String(mWrap.getMemStoreSize(), "B", 1) %>
    </td>
  </tr>
</table>
