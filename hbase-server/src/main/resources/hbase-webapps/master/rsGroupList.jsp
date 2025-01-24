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
         import="org.apache.hadoop.hbase.master.HMaster"
         import="java.util.Collections"
         import="java.util.List"
         import="java.util.Map"
         import="java.util.Set"
         import="java.util.stream.Collectors"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.RegionMetrics"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.Size"
         import="org.apache.hadoop.hbase.master.ServerManager"
         import="org.apache.hadoop.hbase.net.Address"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupInfo"
         import="org.apache.hadoop.hbase.rsgroup.RSGroupUtil"
         import="org.apache.hadoop.util.StringUtils"
         import="org.apache.hadoop.util.StringUtils.TraditionalBinaryPrefix"
         import="org.apache.hadoop.hbase.master.assignment.AssignmentManager" %>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  AssignmentManager assignmentManager = master.getAssignmentManager();
  List<RSGroupInfo> groups = master.getRSGroupInfoManager().listRSGroups();

%>

TODO: rsGroupList.jsp
