<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.codehaus.jettison.json.JSONArray"
  import="org.codehaus.jettison.json.JSONException"
  import="org.codehaus.jettison.json.JSONObject"
  import="org.apache.hadoop.hbase.ipc.HBaseRPC"
  import="org.apache.hadoop.hbase.monitoring.MonitoredTask"
  import="org.apache.hadoop.hbase.monitoring.TaskMonitor" %><%
  TaskMonitor taskMonitor = TaskMonitor.get();
  long now = System.currentTimeMillis();
  List<MonitoredTask> tasks = taskMonitor.getTasks();
  Collections.reverse(tasks);
%><?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
  <title>Task Monitor</title>
<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
</head>
<body>
  <a id="logo" href="http://wiki.apache.org/lucene-hadoop/Hbase">
    <img src="/static/hbase_logo_med.gif" alt="HBase Logo" title="HBase Logo" />
  </a>
  <h1 id="page_title">Task Monitor</h1>
  <h2>Recent tasks</h2>
  <% if(tasks.isEmpty()) { %>
    <p>No tasks currently running on this node.</p>
  <% } else { %>
    <table>
      <tr>
        <th>Description</th>
        <th>Status</th>
        <th>Age</th>
      </tr>
      <% for(MonitoredTask task : tasks) { %>
        <tr class="task-monitor-<%= task.getState() %>">
          <td><%= task.getDescription() %></td>
          <td><%= task.getStatus() %></td>
          <td><%= now - task.getStartTime() %>ms
            <% if(task.getCompletionTimestamp() != -1) { %>
              (Completed <%= now - task.getCompletionTimestamp() %>ms ago)
            <% } %>
          </td>
        </tr>
      <% } %>
    </table>
  <% } %>
</body>
</html>
