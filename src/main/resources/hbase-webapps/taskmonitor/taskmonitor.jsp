<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="org.apache.hadoop.util.StringUtils"
  import="org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler"
  import="org.apache.hadoop.hbase.monitoring.MonitoredTask"
  import="org.apache.hadoop.hbase.monitoring.TaskMonitor" %><%
  // extract requested filter
  String filter = "general";
  if (request.getParameter("filter") != null) {
    filter = request.getParameter("filter");
  }
  TaskMonitor taskMonitor = TaskMonitor.get();
  List<? extends MonitoredTask> tasks = taskMonitor.getTasks();
  Iterator<? extends MonitoredTask> iter = tasks.iterator();
  // apply requested filter
  while (iter.hasNext()) {
    MonitoredTask t = iter.next();
    if (filter.equals("general")) {
      if (t instanceof MonitoredRPCHandler)
        iter.remove();
    } else if (filter.equals("handler")) {
      if (!(t instanceof MonitoredRPCHandler))
        iter.remove();
    } else if (filter.equals("rpc")) {
      if (!(t instanceof MonitoredRPCHandler) || 
          !((MonitoredRPCHandler) t).isRPCRunning())
        iter.remove();
    } else if (filter.equals("operation")) {
      if (!(t instanceof MonitoredRPCHandler) || 
          !((MonitoredRPCHandler) t).isOperationRunning())
        iter.remove();
    }
  }
  long now = System.currentTimeMillis();
  Collections.reverse(tasks);
  // output to JSON if requested
  if(request.getParameter("format") != null &&
      request.getParameter("format").equals("json")) {
      %><%= "[" %><%
      boolean first = true;
      for(MonitoredTask task : tasks) {
        if (first) {
          first = false;
        } else {
          %><%= "," %><%
        }
        %><%= task.toJSON() %><%
      }
      %><%= "]" %><%
  } else {
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
  <div style="float:right;">
    <a href="taskmonitor.jsp?filter=all">Show All Monitored Tasks</a> |
    <a href="taskmonitor.jsp?filter=general">Show non-RPC Tasks</a> |
    <a href="taskmonitor.jsp?filter=handler">Show All RPC Handler Tasks</a> |
    <a href="taskmonitor.jsp?filter=rpc">Show Active RPC Calls</a> |
    <a href="taskmonitor.jsp?filter=operation">Show Client Operations</a> |
    <a href="taskmonitor.jsp?format=json&filter=<%= filter %>">View as JSON</a>
  </div>
  <h1 id="page_title">Task Monitor</h1>
  <h2>Recent tasks</h2>
  <% if(tasks.isEmpty()) { %>
    <p>No tasks currently running on this node.</p>
  <% } else { %>
    <table style="float:right">
      <tr class="task-monitor-RUNNING">
        <td>RUNNING</td>
      </tr>
      <tr class="task-monitor-COMPLETE">
        <td>COMPLETE</td>
      </tr>
      <tr class="task-monitor-WAITING">
        <td>WAITING</td>
      </tr>
      <tr class="task-monitor-ABORTED">
        <td>ABORTED</td>
      </tr>
    </table>
    <p>Each task's state is indicated by its background color according to the 
    key.</p>
    <table style="clear:right">
      <tr>
        <th>Start Time</th>
        <th>Description</th>
        <th>State</th>
        <th>Status</th>
      </tr>
      <% for(MonitoredTask task : tasks) { %>
        <tr class="task-monitor-<%= task.getState() %>">
          <td><%= new Date(task.getStartTime()) %></td>
          <td><%= task.getDescription() %></td>
          <td><%= task.getState() %>
              (since <%= StringUtils.formatTimeDiff(now, 
                              task.getStateTime()) %> ago)
          </td>
          <td><%= task.getStatus() %>
              (since <%= StringUtils.formatTimeDiff(now, 
                              task.getStatusTime()) %> ago)
          </td>
        </tr>
      <% } %>
    </table>
  <% } %>
</body>
</html>
<% } %>
