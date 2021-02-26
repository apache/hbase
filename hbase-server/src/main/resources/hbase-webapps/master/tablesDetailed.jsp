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
         import="static org.apache.commons.lang3.StringEscapeUtils.escapeXml"
         import="java.io.IOException"
         import="java.util.ArrayList"
         import="java.util.List"
         import="java.util.Map"
%>
<%@ page import="org.apache.hadoop.hbase.client.TableDescriptor" %>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.tmpl.master.MasterStatusTmplImpl" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master: " + master.getServerName());
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>User Tables</h1>
    </div>
  </div>

  <% List<TableDescriptor> tables = new ArrayList<TableDescriptor>();
     String errorMessage = MasterStatusTmplImpl.getUserTables(master, tables);
  if (tables.size() == 0 && errorMessage != null) { %>
  <p> <%= errorMessage %> </p>
  <% }
  if (tables != null && tables.size() > 0) { %>
  <table class="table table-striped">
    <tr>
      <th>Table</th>
      <th>Description</th>
    </tr>
    <% for (TableDescriptor htDesc : tables) { %>
    <tr>
      <td>
        <a href="/table.jsp?name=<%= escapeXml(htDesc.getTableName().getNameAsString()) %>"><%= escapeXml(
            htDesc.getTableName().getNameAsString()) %>
        </a></td>
      <td><%= htDesc.toString() %>
      </td>
    </tr>
    <% } %>

    <p><%= tables.size() %> table(s) in set.</p>
  </table>
  <% } %>
</div>

<jsp:include page="footer.jsp"/>
