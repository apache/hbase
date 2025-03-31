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
         import="org.apache.hadoop.hbase.monitoring.*"
         import="org.apache.hadoop.util.StringUtils" %>

<%!
  public static String stateCss(MonitoredTask.State state) {
    if (state == MonitoredTask.State.COMPLETE) {
      return "alert alert-success";
    } else if (state == MonitoredTask.State.ABORTED)  {
      return "alert alert-danger";
    } else {
      return "";
    }
  }
%>

<%
  TaskMonitor taskMonitor = TaskMonitor.get();
  String filter = (String) request.getAttribute("filter"); // TODO: intro constant!
  String format = (String) request.getAttribute("format"); // TODO: intro constant!
  if (format == null) {
    format = "html";
  }

  List<? extends MonitoredTask> tasks = taskMonitor.getTasks(filter);
  long now = System.currentTimeMillis();
  Collections.sort(tasks, (t1, t2) -> Long.compare(t1.getStateTime(), t2.getStateTime()));
  boolean first = true;
  %>

<% if (format.equals("json")) { %>
[<% for (MonitoredTask task : tasks) { %><% if (first) { %><% first = false;%><% } else { %>,<% } %><%= task.toJSON() %><% } %>]
<% } else { %>
  <% if (tasks.isEmpty()) { %>
    <p>No tasks currently running on this node.</p>
  <% } else { %>
    <table class="table table-striped">
      <tr>
        <th>Start Time</th>
        <th>Description</th>
        <th>State</th>
        <th>Status</th>
        <th>Completion Time</th>
      </tr>
      <% for (MonitoredTask task : tasks) { %>
      <tr class="<%= stateCss(task.getState()) %>">
        <td><%= new Date(task.getStartTime()) %></td>
        <td><%= task.getDescription() %></td>
        <td><%= task.getState() %>
          (since <%= StringUtils.formatTimeDiff(now, task.getStateTime()) %> ago)
        </td>
        <td><%= task.getStatus() %>
          (since <%= StringUtils.formatTimeDiff(now, task.getStatusTime()) %>
          ago)</td>
        <td>
          <% if (task.getCompletionTimestamp() < 0) { %>
            <%= task.getState() %>
          <% } else { %>
            <%= new Date(task.getCompletionTimestamp()) %> (since <%= StringUtils.formatTimeDiff(now, task.getCompletionTimestamp()) %> ago)
          <% } %>
        </td>
      </tr>
      <% } %>
    </table>
  <% } %>
<% } %>
