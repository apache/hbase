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
         import="org.apache.hadoop.hbase.quotas.QuotaUtil"
         import="org.apache.hadoop.hbase.HBaseConfiguration"
         import="org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer"
         import="org.apache.hadoop.hbase.master.assignment.AssignmentManager"
         import="org.apache.hadoop.hbase.master.RegionState"
         import="java.util.SortedSet"
         import="org.apache.hadoop.hbase.master.assignment.RegionStates"
         import="org.apache.hadoop.hbase.client.RegionInfoDisplay" %>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  AssignmentManager assignmentManager = master.getAssignmentManager();
  int limit = 100;

  SortedSet<RegionState> rit = assignmentManager.getRegionsStateInTransition();

if (!rit.isEmpty()) {
  long currentTime = System.currentTimeMillis();
  AssignmentManager.RegionInTransitionStat ritStat = assignmentManager.computeRegionInTransitionStat();

  int numOfRITs = rit.size();
  int ritsPerPage = Math.min(5, numOfRITs);
  int numOfPages = (int) Math.ceil(numOfRITs * 1.0 / ritsPerPage);
%>
  <section>
  <h2><a name="rit">Regions in Transition</a></h2>
  <p><a href="/rits.jsp"><%= numOfRITs %> region(s) in transition.</a>
  <% if(ritStat.hasRegionsTwiceOverThreshold()) { %>
    <span class="label label-danger" style="font-size:100%;font-weight:normal">
  <% } else if ( ritStat.hasRegionsOverThreshold()) { %>
     <span class="label label-warning" style="font-size:100%;font-weight:normal">
  <% } else { %>
       <span>
   <% } %>
         <%= ritStat.getTotalRITsOverThreshold() %> region(s) in transition for
             more than <%= ritStat.getRITThreshold() %> milliseconds.
         </span>
  </p>
  <div class="tabbable">
         <div class="tab-content">
         <% int recordItr = 0; %>
         <% for (RegionState rs : rit) { %>
             <% if((recordItr % ritsPerPage) == 0 ) { %>
                 <% if(recordItr == 0) { %>
             <div class="tab-pane active" id="tab_rits<%= (recordItr / ritsPerPage) + 1 %>">
                 <% } else { %>
             <div class="tab-pane" id="tab_rits<%= (recordItr / ritsPerPage) + 1 %>">
                 <% } %>
                 <table class="table table-striped" style="margin-bottom:0px;"><tr><th>Region</th>
                     <th>State</th><th>RIT time (ms)</th> <th>Retries </th></tr>
             <% } %>

             <% if(ritStat.isRegionTwiceOverThreshold(rs.getRegion())) { %>
                     <tr class="alert alert-danger" role="alert">
             <% } else if ( ritStat.isRegionOverThreshold(rs.getRegion())) { %>
                     <tr class="alert alert-warning" role="alert">
            <% } else { %>
                    <tr>
            <% } %>
                        <%
                          String retryStatus = "0";
                          RegionStates.RegionFailedOpen regionFailedOpen = assignmentManager
                            .getRegionStates().getFailedOpen(rs.getRegion());
                          if (regionFailedOpen != null) {
                            retryStatus = Integer.toString(regionFailedOpen.getRetries());
                          } else if (rs.getState() ==  RegionState.State.FAILED_OPEN) {
                            retryStatus = "Failed";
                          }
                        %>
                        <td><%= rs.getRegion().getEncodedName() %></td><td>
                        <%= RegionInfoDisplay.getDescriptiveNameFromRegionStateForDisplay(rs,
                          assignmentManager.getConfiguration()) %></td>
                        <td><%= (currentTime - rs.getStamp()) %> </td>
                        <td> <%= retryStatus %> </td>
                     </tr>
                     <% recordItr++; %>
             <% if((recordItr % ritsPerPage) == 0) { %>
                </table>
             </div>
         <% } %>
         <% } %>

      <% if((recordItr % ritsPerPage) != 0) { %>
        <% for (; (recordItr % ritsPerPage) != 0 ; recordItr++) { %>
          <tr><td colspan="3" style="height:61px"></td></tr>
        <% } %>
          </table>
        </div>
      <% } %>
      </div>
      <input type="hidden" id ="rit_page_num" value="<%= numOfRITs %>" />
      <input type="hidden" id ="rit_per_page" value="<%= ritsPerPage %>" />
      <nav id="rit_pagination"></nav>
    </div>
  </section>
<% } %>

