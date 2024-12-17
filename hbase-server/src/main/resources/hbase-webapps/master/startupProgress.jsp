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
         import="java.util.Date"
         import="java.util.Iterator"
         import="java.util.List"
%>
<%@ page import="org.apache.hadoop.hbase.master.HMaster" %>
<%@ page import="org.apache.hadoop.hbase.monitoring.MonitoredTask" %>
<%@ page import="org.apache.hadoop.hbase.monitoring.TaskGroup" %>
<%
  final HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  pageContext.setAttribute("pageTitle", "HBase Master Startup Progress: " + master.getServerName());
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

<div class="container-fluid content">
  <div class="row inner_header">
    <div class="page-header">
      <h1>Startup Progress (
      <% TaskGroup startupTaskGroup = master.getStartupProgress();
         if(startupTaskGroup != null){ %>
         <%= getStartupStatusString(startupTaskGroup) %>
         <% } else { %>
         <%= ""%>
         <% } %>
      )</h1>
    </div>
  </div>

  <table class="table table-striped">
    <tr>
      <th>Task</th>
      <th>Current State</th>
      <th>Start Time</th>
      <th>Last status Time</th>
      <th>Elapsed Time(ms)</th>
      <th>Journals</th>

    </tr>
    <%
    if(startupTaskGroup != null){
       for (MonitoredTask task : startupTaskGroup.getTasks()) { %>
    <tr>
      <td><%= task.getDescription() %></td>
      <td><%= task.getState().name() %></td>
      <td><%= new Date(task.getStartTime()) %></td>
      <td><%= new Date(task.getStatusTime()) %></td>
      <td><%= task.getStatusTime() - task.getStartTime() %></td>
      <td><%= printLatestJournals(task, 30) %></td>
    </tr>
    <% }
    } %>

  </table>

</div>
<jsp:include page="footer.jsp"/>

<%!
  private static String printLatestJournals(MonitoredTask task, int count) {
    List<MonitoredTask.StatusJournalEntry> journal = task.getStatusJournal();
    if (journal == null) {
      return "";
    }
    int journalSize = journal.size();
    StringBuilder sb = new StringBuilder();
    int skips = journalSize - count;
    if (skips > 0) {
      sb.append("Current journal size is ").append(journalSize).append(", ");
      sb.append("skip the previous ones and show the latest ").append(count).append(" journals...");
      sb.append(" </br>");
    }
    Iterator<MonitoredTask.StatusJournalEntry> iter = journal.iterator();
    MonitoredTask.StatusJournalEntry previousEntry = null;
    int i = 0;
    while (iter.hasNext()) {
      MonitoredTask.StatusJournalEntry entry = iter.next();
      if (i >= skips) {
        sb.append(entry);
        if (previousEntry != null) {
          long delta = entry.getTimeStamp() - previousEntry.getTimeStamp();
          if (delta != 0) {
            sb.append(" (+").append(delta).append(" ms)");
          }
        }
        sb.append(" </br>");
        previousEntry = entry;
      }
      i++;
    }
    return sb.toString();
  }

  private static String getStartupStatusString(TaskGroup startupTaskGroup) {
      MonitoredTask.State currentState = startupTaskGroup.getState();
      if (currentState.equals(MonitoredTask.State.COMPLETE)) {
        return "Master initialized";
      } else if (currentState.equals(MonitoredTask.State.RUNNING) |
        currentState.equals(MonitoredTask.State.WAITING)) {
        return "Master initialize in progress";
      } else {
        return currentState.toString();
      }
   }
%>
