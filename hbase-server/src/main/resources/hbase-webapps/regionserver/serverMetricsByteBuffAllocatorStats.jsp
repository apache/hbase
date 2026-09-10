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
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
         import="org.apache.hadoop.hbase.io.ByteBuffAllocator" %>

<%
  HRegionServer regionServer =
    (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);

  ByteBuffAllocator bbAllocator = regionServer.getRpcServer().getByteBuffAllocator();
%>

<table class="table table-striped">
  <tr>
    <th>Total Heap Allocation</th>
    <th>Total Pool Allocation</th>
    <th>Heap Allocation Ratio</th>
    <th>Total Buffer Count</th>
    <th>Used Buffer Count</th>
    <th>Buffer Size</th>
  </tr>
  <tr>
    <td><%= TraditionalBinaryPrefix.long2String(ByteBuffAllocator.getHeapAllocationBytes(bbAllocator, ByteBuffAllocator.HEAP), "B", 1) %></td>
    <td><%= TraditionalBinaryPrefix.long2String(bbAllocator.getPoolAllocationBytes(), "B", 1) %></td>
    <td><%= String.format("%.3f", ByteBuffAllocator.getHeapAllocationRatio(bbAllocator, ByteBuffAllocator.HEAP) * 100) %><%= "%" %></td>
    <td><%= bbAllocator.getTotalBufferCount() %></td>
    <td><%= bbAllocator.getUsedBufferCount() %></td>
    <td><%= TraditionalBinaryPrefix.long2String(bbAllocator.getBufferSize(), "B", 1) %></td>
  </tr>
</table>
