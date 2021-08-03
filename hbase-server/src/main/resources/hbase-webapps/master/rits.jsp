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
         import="java.util.Collections"
         import="java.util.Comparator"
         import="java.util.ArrayList"
         import="java.util.List"
         import="java.util.HashMap"
         import="java.util.Map"
         import="java.util.stream.Collectors"
         import="org.apache.hadoop.hbase.master.HMaster"
         import="org.apache.hadoop.hbase.master.RegionState"
         import="org.apache.hadoop.hbase.master.assignment.RegionStateNode"
         import="org.apache.hadoop.hbase.master.assignment.TransitRegionStateProcedure"
         import="org.apache.hadoop.hbase.util.GsonUtil"
         import="org.apache.hbase.thirdparty.com.google.gson.Gson"
%>
<%
    HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
    List<RegionStateNode> rit = master.getAssignmentManager().getRegionsInTransition();
    String table = request.getParameter("table");
    String state = request.getParameter("state");
    if (table != null && state != null && !table.equals("null") && !state.equals("null")) {
        rit = rit.stream().filter(regionStateNode -> regionStateNode.getTable().getNameAsString().equals(table))
                .filter(regionStateNode -> regionStateNode.getState().name().equals(state))
                .collect(Collectors.toList());
    }

    String format = request.getParameter("format");
    if(format == null || format.isEmpty()){
        format = "html";
    }
    String filter = request.getParameter("filter");
    Collections.sort(rit, new Comparator<RegionStateNode>() {
        @Override
        public int compare(RegionStateNode o1, RegionStateNode o2) {
            if (o1.getState() != o2.getState()){
                return o1.getState().ordinal() - o2.getState().ordinal();
            }
            return o1.compareTo(o2);
        }
    });
%>


<% if (format.equals("html")) { %>
<jsp:include page="header.jsp">
    <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>
<div class="container-fluid content">
    <div class="row">
        <div class="page-header">
            <h1>Regions in transition</h1>
        </div>
    </div>
    <div class="row">
        <div class="page-header">
            <a href="/rits.jsp?format=txt&filter=region&table=<%=table%>&state=<%=state%>" class="btn btn-primary">Regions in text format</a>
            <a href="/rits.jsp?format=txt&filter=procedure&table=<%=table%>&state=<%=state%>" class="btn btn-info">Procedures in text format</a>
            <a href="/rits.jsp?format=json&table=<%=table%>&state=<%=state%>" class="btn btn-info">RIT info as JSON</a>
            <p>regions and procedures in text format can be copied and passed to command-line utils such as hbck2</p>
        </div>
    </div>

    <% if (rit != null && rit.size() > 0) { %>
        <table class="table table-striped">
            <tr>
                <th>Region</th>
                <th>Table</th>
                <th>RegionState</th>
                <th>Server</th>
                <th>Procedure</th>
                <th>ProcedureState</th>
                <th>Start Time</th>
                <th>Duration (ms)</th>
            </tr>
            <% for (RegionStateNode regionStateNode : rit) { %>
            <tr>
                <td><%= regionStateNode.getRegionInfo().getEncodedName() %></td>
                <td><%= regionStateNode.getRegionInfo().getTable() %></td>
                <td><%= regionStateNode.getState() %></td>
                <td><%= regionStateNode.getRegionLocation().getServerName() %></td>
                <%
                    TransitRegionStateProcedure procedure = regionStateNode.getProcedure();

                    if (procedure == null) {
                %>
                    <td></td>
                    <td></td>
                <% } else { %>
                    <td><%= procedure.getProcId() %></td>
                    <td><%= escapeXml(procedure.getState().toString() + (procedure.isBypass() ? "(Bypassed)" : "")) %></td>
                <% } %>

                <% RegionState rs = regionStateNode.toRegionState(); %>
                <td><%= rs.getStamp() %></td>
                <td><%= System.currentTimeMillis() - rs.getStamp() %></td>
            </tr>
            <% } %>
            <p><%= rit.size() %> region(s) in transition.</p>
        </table>
    <% } else { %>
    <p> no region in transition right now. </p>
    <% } %>
</div>
<jsp:include page="footer.jsp" />
<% } else if (format.equals("json")) { %>
<%
    Gson GSON = GsonUtil.createGson().create();
    Map<String, List<Map<String, Object>>> map = new HashMap<>();
    List<Map<String, Object>> rits = new ArrayList<>();
    map.put("rits", rits);
    for (RegionStateNode regionStateNode : rit) {
        Map<String, Object> r = new HashMap<>();
        r.put("region", regionStateNode.getRegionInfo().getEncodedName());
        r.put("table", regionStateNode.getRegionInfo().getTable().getNameAsString());
        r.put("state", regionStateNode.getState());
        r.put("server", regionStateNode.getRegionLocation().getServerName());
        TransitRegionStateProcedure procedure = regionStateNode.getProcedure();
        if (procedure != null) {
          r.put("procedureId", procedure.getProcId());
          r.put("procedureState", procedure.getState().toString());
        }
        RegionState rs = regionStateNode.toRegionState();
        r.put("startTime", rs.getStamp());
        r.put("duration", System.currentTimeMillis() - rs.getStamp());
        rits.add(r);
      }
    %>
    <%= GSON.toJson(map) %>
<% } else { %>
<div class="container-fluid content">
    <div class="row">
        <p>
            <%
            if (filter.equals("region")) {
                for (RegionStateNode regionStateNode : rit) { %>
                    <%= regionStateNode.getRegionInfo().getEncodedName() %><br>
            <%    }
            } else if (filter.equals("procedure")) {
                for (RegionStateNode regionStateNode : rit) { %>
                    <%= regionStateNode.getProcedure().getProcId() %><br>
            <%    }
            } else { %>
                "Not a valid filter"
            <% } %>
        </p>
    </div>
</div>
<% } %>
