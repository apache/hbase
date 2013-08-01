<%@ page contentType="text/html;charset=UTF-8"
  import="java.util.*"
  import="java.text.SimpleDateFormat"
  import="java.util.Date"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.hbase.util.Bytes"
  import="org.apache.hadoop.hbase.util.JvmVersion"
  import="org.apache.hadoop.hbase.util.FSUtils"
  import="org.apache.hadoop.hbase.master.HMaster"
  import="org.apache.hadoop.hbase.HConstants"
  import="org.apache.hadoop.hbase.master.MetaRegion"
  import="org.apache.hadoop.hbase.client.HBaseAdmin"
  import="org.apache.hadoop.hbase.HServerInfo"
  import="org.apache.hadoop.hbase.HServerAddress"
  import="org.apache.hadoop.hbase.HTableDescriptor"
  import="org.apache.hadoop.hbase.HColumnDescriptor"
  import="org.apache.hadoop.hbase.HRegionInfo"
  import="org.apache.hadoop.hbase.master.RegionPlacement"
  import="org.apache.hadoop.hbase.master.RegionAssignmentSnapshot"
  import="org.apache.hadoop.hbase.master.AssignmentPlan"
  import="org.apache.hadoop.hbase.master.RegionChecker.RegionAvailabilityInfo"
  import="org.apache.hadoop.hbase.util.Pair"
%><%
  HMaster master = (HMaster)getServletContext().getAttribute(HMaster.MASTER);

%><?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
  "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
	<head>
		<meta http-equiv="Content-Type" content="text/html;charset=UTF-8"/>
		<title>RegionChecker</title>
		<link rel="stylesheet" type="text/css" href="/static/hbase.css" />
	</head>
	<body>
<%
		if(!master.getServerManager().getRegionChecker().isEnabled())
		{
%>
			<p>Region Checker is disabled</p>
<%
		}
		else
		{
			double lastDayAvailability = master.getServerManager().getRegionChecker().getLastDayAvailability();
			double lastWeekAvailability = master.getServerManager().getRegionChecker().getLastWeekAvailability();
%>

			<table>
				<caption><b>Whole cluster availability information</b></caption>
				<tr>
					<td>Last 24 hours availability of whole cluster</td>
					<td><%=String.format("%.15f", lastDayAvailability)%></td>
				</tr>
				<tr>
					<td>Last 7 days availability of whole cluster</td>
					<td><%=String.format("%.15f", lastWeekAvailability)%></td>
				</tr>
			</table>

<%
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss SSS");
			Map<String, RegionAvailabilityInfo> detailedDayInfo = master.getServerManager().getRegionChecker().getDetailedLastDayAvailability();
			Map<String, RegionAvailabilityInfo> detailedWeekInfo = master.getServerManager().getRegionChecker().getDetailedLastWeekAvailability();
			Set<String> keys = detailedWeekInfo.keySet();
			List<Pair<RegionAvailabilityInfo, String>> list = new ArrayList<Pair<RegionAvailabilityInfo, String>> ();
			for(String key : keys)
				list.add(new Pair<RegionAvailabilityInfo, String>(detailedDayInfo.get(key), key));

			Comparator<Pair<RegionAvailabilityInfo, String>> comparator = new Comparator<Pair<RegionAvailabilityInfo,String>>()
			{
				@Override
				public int compare(Pair<RegionAvailabilityInfo, String> o1, Pair<RegionAvailabilityInfo, String> o2)
				{
					if(Math.abs(o1.getFirst().getAvailability() - o2.getFirst().getAvailability()) < 1e-9)
						return 0;
					else if(o1.getFirst().getAvailability() > o2.getFirst().getAvailability())
						return -1;
					else
						return 1;
				}
			};

			Collections.sort(list, comparator);
			Collections.reverse(list);
%>
			<br>
			<table>
			<caption><b>Detailed availability information about all regions</b></caption>
			<tr>
				<td><b>Region</b></td>
				<td><b>Last 24 hours availability</b></td>
				<td><b>Last 7 days availability</b></td>
				<td><b>Last unassignment interval</b></td>
				<td><b>Last unassignment interval duration(ms)</b></td>
				<td><b>Unassigment intervals count for last 24 hours</b></td>
				<td><b>Unassigment intervals count for last 7 days</b></td>
			</tr>
<%
			for (int i = 0; i < list.size(); i++)
			{
				String key = list.get(i).getSecond();
%>
				<tr>
					<td><%=key%></td>
					<td><%=(detailedDayInfo.containsKey(key)?String.format("%.15f", detailedDayInfo.get(key).getAvailability()):"1.0")%></td>
					<td><%=(detailedWeekInfo.containsKey(key)?String.format("%.15f", detailedWeekInfo.get(key).getAvailability()):"1.0")%></td>
					<td><%=(detailedWeekInfo.containsKey(key)?detailedWeekInfo.get(key).getInterval():"")%></td>
					<td><%=((detailedWeekInfo.containsKey(key) && detailedWeekInfo.get(key).getIntervalsCount()>0)?detailedWeekInfo.get(key).getDuration():"")%></td>
					<td><%=detailedDayInfo.get(key).getIntervalsCount()%></td>
					<td><%=detailedWeekInfo.get(key).getIntervalsCount()%></td>
				</tr>
<%
			}
%>
			</table>
<%
		}
%>
	</body>
</html>
