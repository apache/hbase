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
         import="org.apache.hadoop.hbase.util.MasterStatusConstants" %>

<%
  String filter = (String) request.getAttribute(MasterStatusConstants.FILTER);
  if (filter == null) {
    filter = "general";
  }
  String format = (String) request.getAttribute(MasterStatusConstants.FORMAT);
  if (format == null) {
    format = "html";
  }
  String parent = (String) request.getAttribute(MasterStatusConstants.PARENT);
  if (parent == null) {
    parent = "";
  }
%>

<% if (format.equals("json")) { %>
  <% request.setAttribute(MasterStatusConstants.FILTER, filter); %>
  <jsp:include page="taskMonitor_renderTasks.jsp"/>
<% } else { %>
  <h2><a name="tasks">Tasks</a></h2>

  <div class="tabbable">
    <ul class="nav nav-pills" role="tablist">
      <li class="nav-item">
        <a class="nav-link" href="#tab_alltasks" data-bs-toggle="tab" role="tab">Show All Monitored Tasks</a>
      </li>
      <li class="nav-item">
        <a class="nav-link active" href="#tab_generaltasks" data-bs-toggle="tab" role="tab">Show non-RPC Tasks</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" href="#tab_handlertasks" data-bs-toggle="tab" role="tab">Show All RPC Handler Tasks</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" href="#tab_rpctasks" data-bs-toggle="tab" role="tab">Show Active RPC Calls</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" href="#tab_operationtasks" data-bs-toggle="tab" role="tab">Show Client Operations</a>
      </li>
    </ul>
    <div class="tab-content">
      <div class="tab-pane" id="tab_alltasks" role="tabpanel">
        <a href="<%= parent %>?format=json&filter=all">View as JSON</a>
        <% request.setAttribute(MasterStatusConstants.FILTER, "all"); %>
        <jsp:include page="taskMonitor_renderTasks.jsp"/>
      </div>
      <div class="tab-pane active" id="tab_generaltasks" role="tabpanel">
        <a href="<%= parent %>?format=json&filter=general">View as JSON</a>
        <% request.setAttribute(MasterStatusConstants.FILTER, "general"); %>
        <jsp:include page="taskMonitor_renderTasks.jsp"/>
      </div>
      <div class="tab-pane" id="tab_handlertasks" role="tabpanel">
        <a href="<%= parent %>?format=json&filter=handler">View as JSON</a>
        <% request.setAttribute(MasterStatusConstants.FILTER, "handler"); %>
        <jsp:include page="taskMonitor_renderTasks.jsp"/>
      </div>
      <div class="tab-pane" id="tab_rpctasks" role="tabpanel">
        <a href="<%= parent %>?format=json&filter=rpc">View as JSON</a>
        <% request.setAttribute(MasterStatusConstants.FILTER, "rpc"); %>
        <jsp:include page="taskMonitor_renderTasks.jsp"/>
      </div>
      <div class="tab-pane" id="tab_operationtasks" role="tabpanel">
        <a href="<%= parent %>?format=json&filter=operation">View as JSON</a>
        <% request.setAttribute(MasterStatusConstants.FILTER, "operation"); %>
        <jsp:include page="taskMonitor_renderTasks.jsp"/>
      </div>
    </div>
  </div>
<% } %>
