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
<%@page
	import="org.apache.hadoop.hbase.regionserver.stats.RegionAccessStats"%>
<%@page import="java.time.Instant"%>
<%@page
	import="org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsGranularity"%>
<%@page
	import="org.apache.hadoop.hbase.regionserver.stats.AccessStats.AccessStatsType"%>
<%@page import="org.apache.hadoop.hbase.TableName"%>
<%@page import="org.apache.hadoop.hbase.regionserver.stats.AccessStats"%>
<%@page
	import="org.apache.hadoop.hbase.regionserver.stats.AccessStatsRecorderTableImpl"%>
<%@page
	import="org.apache.hadoop.hbase.regionserver.stats.IAccessStatsRecorder"%>
<%@page
	import="org.apache.hadoop.hbase.regionserver.stats.AccessStatsRecorderUtils"%>
<%@ page contentType="text/html;charset=UTF-8"
	import="static org.apache.commons.lang.StringEscapeUtils.escapeXml"
	import="java.net.URLEncoder" import="java.util.TreeMap"
	import="java.util.Map" import="java.util.*"
	import="org.apache.commons.lang.StringEscapeUtils"
	import="org.apache.hadoop.conf.Configuration"
	import="org.apache.hadoop.hbase.Cell"
	import="org.apache.hadoop.hbase.client.HTable"
	import="org.apache.hadoop.hbase.client.Admin"
	import="org.apache.hadoop.hbase.client.Result"
	import="org.apache.hadoop.hbase.client.ResultScanner"
	import="org.apache.hadoop.hbase.client.Scan"
	import="org.apache.hadoop.hbase.HRegionInfo"
	import="org.apache.hadoop.hbase.master.HMaster"
	import="org.apache.hadoop.hbase.util.Bytes"
	import="org.apache.hadoop.hbase.HBaseConfiguration"%>
<%
  HMaster master = (HMaster) getServletContext().getAttribute(HMaster.MASTER);
  Configuration conf = master.getConfiguration();

  String fqtn = request.getParameter("name");
  final String escaped_fqtn = StringEscapeUtils.escapeHtml(fqtn);

  AccessStatsRecorderUtils.createInstance(conf);
  int durationInMinutes = AccessStatsRecorderUtils.getInstance().getIterationDuration();

  int iterations = 100; //TODO take this as a parameter

  String countType = request.getParameter("countType");
  if (countType == null) {
	  countType = "WRITECOUNT"; //default
  }
%>	
<!--[if IE]>
<!DOCTYPE html>
<![endif]-->
<?xml version="1.0" encoding="UTF-8" ?>
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta charset="utf-8">
<title>Table: <%=escaped_fqtn%></title>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="description" content="">
<meta name="author" content="">
<link href="/static/css/bootstrap.min.css" rel="stylesheet">
<link href="/static/css/bootstrap-theme.min.css" rel="stylesheet">
<link href="/static/css/hbase.css" rel="stylesheet">
</head>
<body>
	<%!private String getColor(long count, long maxCount, long minCount) {
    int red, green;
    int blue = 0;

    if (maxCount == 0) {
      maxCount = 1; //to avoid divide by zero
    }

    int number = (int) ((count * 254) / (maxCount - minCount));
    
    String colorPart = Integer.toHexString(number);
   	String color = "#"+colorPart+colorPart+colorPart;
   	
    return color;
  }%>
	<%
	  if (fqtn != null) {

	    Map<String, Map<Long, RegionAccessStats>> regionDetails = new HashMap<String, Map<Long, RegionAccessStats>>();

	    long maxCount = 0;
	    long minCount = 0;

	    Date endTime = new Date();
	    Date startTime = new Date();

	    try (IAccessStatsRecorder accessStatsRecorder = new AccessStatsRecorderTableImpl(conf)) {
	      List<AccessStats> accessStatsList = accessStatsRecorder.readAccessStats(
	        TableName.valueOf(escaped_fqtn), AccessStatsType.valueOf(countType),
	        AccessStatsGranularity.REGION, Instant.now().toEpochMilli(), iterations);

	      for (AccessStats accessStats : accessStatsList) {
	        RegionAccessStats regionAccessStats = (RegionAccessStats) accessStats;
	        long value = regionAccessStats.getValue();
	        String regionName = regionAccessStats.getRegionName();

	        if (regionDetails.get(regionName) == null) {
	          regionDetails.put(regionName, new HashMap<Long, RegionAccessStats>());
	        }

	        regionDetails.get(regionName).put(regionAccessStats.getEpochTime(), regionAccessStats);

	        if (maxCount < value) {
	          maxCount = value;
	        }

	        if (value != -1 && minCount > value) {
	          minCount = value;
	        }
	      }

	      if (accessStatsList.size() > 0) {
	        endTime = new Date(accessStatsList.get(0).getEpochTime());
	        startTime = new Date(accessStatsList.get(accessStatsList.size() - 1).getEpochTime());
	      }
	    }
	%>
	<div class="container-fluid content">
		<div class="row">
			<table class="table table-striped">
				<tr>
					<td align="center"><%=escaped_fqtn%></td>
					<td align="center">Metric name : <select  name="countType">
							<option value="READCOUNT">Read Count</option>
							<option value="WRITECOUNT">Write Count</option>
					</select>
					</td>
					<td>Metric interval : <%=durationInMinutes%> minutes
					</td>
				</tr>
				<tr>
					<td align="right">(Minimum value) <%=minCount%>
					</td>
					<td align="center"
						style="background: linear-gradient(to right, #191919, #ffffff);"></td>
					<td><%=maxCount%> (Maximum value)</td>
				</tr>
				<tr>
					<td align="right">&larr;</td>
					<td align="center"><%=startTime.toString()%>
						&nbsp;&nbsp;&nbsp; to &nbsp;&nbsp;&nbsp; <%=endTime.toString()%>
					</td>
					<td>&rarr;</td>
				</tr>
			</table>
		</div>
		<div class="row">
		<table class="table table-striped">
			<%
			  for (Map.Entry<String, Map<Long, RegionAccessStats>> regionDetail : regionDetails.entrySet()) {
			      String regionName = regionDetail.getKey();
			%>
			<tr>
				<%
				  Map<Long, RegionAccessStats> countTimeDetails = regionDetail.getValue();
				      long epochCurrent = 0;
				      for (int i = 0; i < iterations; i++) {
				        epochCurrent = endTime.getTime() - i * durationInMinutes * 60 * 1000;
				        RegionAccessStats regionAccessStats = countTimeDetails.get(epochCurrent);
				        
				        if(regionAccessStats != null)
				        {
				          long count = regionAccessStats.getValue();
					        
					        String hoverText = (new Date(epochCurrent)).toString() + 
					            ", Region:"+ regionName +
					            ", StartKey:"+ Bytes.toString(regionAccessStats.getKeyRangeStart()) +
					            ", EndKey:"+ Bytes.toString(regionAccessStats.getKeyRangeEnd()) +
					            ", Count:"+count;

							%>
							<td title="<%=hoverText%>"
								style="background-color:<%=getColor(count, maxCount, minCount)%>;">&nbsp&nbsp&nbsp</td>
							<%
				        }
				  }
				%>
			</tr>
			<%
			  }
			%>
			</table>
			<%
			  }
			%>
		</div>
	</div>
	<script src="/static/js/jquery.min.js" type="text/javascript"></script>
	<script src="/static/js/bootstrap.min.js" type="text/javascript"></script>
</body>
</html>