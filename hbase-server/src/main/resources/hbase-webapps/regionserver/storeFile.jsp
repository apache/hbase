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
  import="java.io.ByteArrayOutputStream"
  import="java.io.PrintStream"
  import="org.apache.hadoop.conf.Configuration"
  import="org.apache.hadoop.fs.FileSystem"
  import="org.apache.hadoop.fs.Path"
  import="org.apache.hadoop.hbase.io.hfile.HFilePrettyPrinter"
  import="org.apache.hadoop.hbase.regionserver.HRegionServer"
  import="org.apache.hadoop.hbase.regionserver.StoreFileInfo"
%>
<%
  String storeFile = request.getParameter("name");
  HRegionServer rs = (HRegionServer) getServletContext().getAttribute(HRegionServer.REGIONSERVER);
  Configuration conf = rs.getConfiguration();
  FileSystem fs = FileSystem.get(conf);
  pageContext.setAttribute("pageTitle", "HBase RegionServer: " + rs.getServerName());
%>
<jsp:include page="header.jsp">
  <jsp:param name="pageTitle" value="${pageTitle}"/>
</jsp:include>

  <div class="container-fluid content">
    <div class="row inner_header">
        <div class="page-header">
            <h4>StoreFile: <%= storeFile %></h4>
        </div>
    </div>
    <pre>
<%
   try {
     ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
     PrintStream printerOutput = new PrintStream(byteStream);
     HFilePrettyPrinter printer = new HFilePrettyPrinter();
     printer.setPrintStreams(printerOutput, printerOutput);
     printer.setConf(conf);
     String[] options = {"-s"};
     printer.parseOptions(options);
     StoreFileInfo sfi = new StoreFileInfo(conf, fs, new Path(storeFile), true);
     printer.processFile(sfi.getFileStatus().getPath(), true);
     String text = byteStream.toString();%>
     <%=
       text
     %>
   <%}
   catch (Exception e) {%>
     <%= e %>
   <%}
%>
  </pre>
</div>

<jsp:include page="footer.jsp" />
