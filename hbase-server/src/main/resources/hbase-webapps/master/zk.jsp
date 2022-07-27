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
  import="org.apache.commons.lang3.StringEscapeUtils"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.zookeeper.ZKDump"
  import="org.apache.hadoop.hbase.zookeeper.ZKWatcher"
%>
<%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);
  ZKWatcher watcher = master.getZooKeeper();
%>
<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="Zookeeper Dump"/>
</jsp:include>
        <div class="container-fluid content">
            <div class="row inner_header">
                <div class="page-header">
                    <h1>ZooKeeper Dump</h1>
                </div>
            </div>
            <div class="row">
                <div class="span12">
                    <pre><%= StringEscapeUtils.escapeHtml4(ZKDump.dump(watcher).trim()) %></pre>
                </div>
            </div>
        </div>
<jsp:include page="footer.jsp" />
