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
         import="java.util.Map"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.net.Address"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
         import="org.apache.hadoop.hbase.RegionMetrics"
         import="org.apache.hadoop.hbase.Size"
         import="org.apache.hadoop.hbase.util.MasterStatusConstants" %>

<%!
  // TODO: Extract to common place!
  private static String rsGroupLink(String rsGroupName) {
    return "<a href=\"rsgroup.jsp?name=" + rsGroupName + "\">" + rsGroupName + "</a>";
  }
%>

<%
  RSGroupInfo [] rsGroupInfos = (RSGroupInfo[]) request.getAttribute(MasterStatusConstants.RS_GROUP_INFOS);
  Map<Address, ServerMetrics> collectServers = (Map<Address, ServerMetrics>) request.getAttribute(MasterStatusConstants.COLLECT_SERVERS);
%>

<table class="table table-striped">
  <tr>
    <th>RSGroup Name</th>
    <th>Num. Stores</th>
    <th>Num. Storefiles</th>
    <th>Storefile Size Uncompressed</th>
    <th>Storefile Size</th>
    <th>Index Size</th>
    <th>Bloom Size</th>
  </tr>
    <%
    final String ZEROKB = "0 KB";
    final String ZEROMB = "0 MB";
    for (RSGroupInfo rsGroupInfo: rsGroupInfos) {
      String uncompressedStorefileSizeStr = ZEROMB;
      String storefileSizeStr = ZEROMB;
      String indexSizeStr = ZEROKB;
      String bloomSizeStr = ZEROKB;
      String rsGroupName = rsGroupInfo.getName();
      int numStores = 0;
      long numStorefiles = 0;
      long uncompressedStorefileSize  = 0;
      long storefileSize  = 0;
      long indexSize  = 0;
      long bloomSize  = 0;
      for (Address server : rsGroupInfo.getServers()) {
        ServerMetrics sl = collectServers.get(server);
        if (sl != null) {
          for (RegionMetrics rm : sl.getRegionMetrics().values()) {
            numStores += rm.getStoreCount();
            numStorefiles += rm.getStoreFileCount();
            uncompressedStorefileSize += rm.getUncompressedStoreFileSize().get(Size.Unit.MEGABYTE);
            storefileSize += rm.getStoreFileSize().get(Size.Unit.MEGABYTE);
            indexSize += rm.getStoreFileUncompressedDataIndexSize().get(Size.Unit.KILOBYTE);
            bloomSize += rm.getBloomFilterSize().get(Size.Unit.KILOBYTE);
          }
        }
      }
       if (uncompressedStorefileSize > 0) {
          uncompressedStorefileSizeStr = TraditionalBinaryPrefix.
          long2String(uncompressedStorefileSize * TraditionalBinaryPrefix.MEGA.value, "B", 1);
       }
       if (storefileSize > 0) {
           storefileSizeStr = TraditionalBinaryPrefix.
           long2String(storefileSize * TraditionalBinaryPrefix.MEGA.value, "B", 1);
       }
       if (indexSize > 0) {
          indexSizeStr = TraditionalBinaryPrefix.
          long2String(indexSize * TraditionalBinaryPrefix.KILO.value, "B", 1);
       }
       if (bloomSize > 0) {
           bloomSizeStr = TraditionalBinaryPrefix.
           long2String(bloomSize * TraditionalBinaryPrefix.KILO.value, "B", 1);
       }
%>
<tr>
  <td><%= rsGroupLink(rsGroupName) %></td>
  <td><%= numStores %></td>
  <td><%= numStorefiles %></td>
  <td><%= uncompressedStorefileSizeStr %></td>
  <td><%= storefileSizeStr %></td>
  <td><%= indexSizeStr %></td>
  <td><%= bloomSizeStr %></td>
  </tr>
<%
}
%>
</table>
