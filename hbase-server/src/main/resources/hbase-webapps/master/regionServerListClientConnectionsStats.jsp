<%@ page contentType="text/html;charset=UTF-8"
         import="org.apache.hadoop.hbase.ServerName"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.ServerMetrics"
         import="org.apache.hadoop.hbase.master.ServerManager"
         import="org.apache.hadoop.hbase.UserMetrics"
         import="java.util.Map"
         import="org.apache.commons.lang3.StringEscapeUtils" %>

<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  ServerName[] serverNames = (ServerName[]) request.getAttribute("serverNames");
  ServerManager serverManager = master.getServerManager();
%>

<table id="clientConnectionsStatsTable" class="tablesorter table table-striped">
  <thead>
  <tr>
    <th class="cls_separator">ClientIP</th>
    <th class="cls_separator">UserName</th>
    <th class="cls_separator">ClientVersion</th>
    <th class="cls_separator">ServiceName</th>
    <th class="cls_separator">ServerInfo</th>
  </tr>
  </thead>
  <tbody>
  <%
    for (ServerName serverName: serverNames) {
      ServerMetrics serverMetrics = serverManager.getLoad(serverName);
      if(serverMetrics != null) {

        Map<byte[], UserMetrics> userMetricsMap = serverMetrics.getUserMetrics();
        for(Map.Entry<byte[], UserMetrics> entry : userMetricsMap.entrySet()) {
          UserMetrics userMetrics = entry.getValue();
          Map<String, UserMetrics.ClientMetrics> clientMetricsMap = userMetrics.getClientMetrics();

          for(Map.Entry<String, UserMetrics.ClientMetrics> clientEntry : clientMetricsMap.entrySet()) {
            UserMetrics.ClientMetrics clientConnection = clientEntry.getValue();

  %>
  <tr>
    <td><%= StringEscapeUtils.escapeHtml4(clientConnection.getHostAddress()) %></td>
    <td><%= StringEscapeUtils.escapeHtml4(clientConnection.getUserName()) %></td>
    <td><%= StringEscapeUtils.escapeHtml4(clientConnection.getClientVersion()) %></td>
    <td><%= StringEscapeUtils.escapeHtml4(clientConnection.getServiceName()) %></td>
    <td><%= StringEscapeUtils.escapeHtml4(serverName.getServerName()) %></td>
  </tr>
  <%
          }
        }
      }
    }
  %>
  </tbody>
</table>
